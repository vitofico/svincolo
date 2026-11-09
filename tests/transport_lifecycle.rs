use std::{
    fs,
    io::{self, BufRead, BufReader},
    net::{Ipv4Addr, SocketAddr},
    process::{Child, ChildStderr, Command, ExitStatus, Stdio},
    time::Duration,
};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::StatusCode,
    response::Response,
};
use bytes::Bytes;
use futures_util::{StreamExt, future::join_all, stream};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{sleep, timeout},
};

const CHAT: &str = "/v1/chat/completions";
const BODY: &str = r#"{"model":"mock-model","messages":[]}"#;
const ALIAS_BODY: &str = r#"{"model":"same-upstream","messages":[]}"#;
const OTHER_BODY: &str = r#"{"model":"other-upstream","messages":[]}"#;
#[cfg(unix)]
const CLOUD_BODY: &str = r#"{"model":"cloud-route","messages":[]}"#;
const CALLER_A: &str = "test-alice";
const ROTATED_A: &str = "test-alice-rotated";
const CALLER_B: &str = "test-bob";
const WAIT: Duration = Duration::from_secs(5);
const FIRST: &[u8] = b"data: first\n\n";
const LAST: &[u8] = b"data: [DONE]\n\n";

struct NotifyDrop(Option<oneshot::Sender<()>>);

impl Drop for NotifyDrop {
    fn drop(&mut self) {
        if let Some(signal) = self.0.take() {
            let _ = signal.send(());
        }
    }
}

struct Call {
    finish: oneshot::Sender<()>,
    dropped: oneshot::Receiver<()>,
    #[cfg(unix)]
    authorization: Option<axum::http::HeaderValue>,
}

async fn provider(State(calls): State<mpsc::UnboundedSender<Call>>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    assert_eq!(parts.uri.path(), CHAT);
    assert_eq!(to_bytes(body, 1024).await.unwrap(), BODY);
    let (finish, wait) = oneshot::channel();
    let (signal, dropped) = oneshot::channel();
    let guard = NotifyDrop(Some(signal));
    calls
        .send(Call {
            finish,
            dropped,
            #[cfg(unix)]
            authorization: parts.headers.get("authorization").cloned(),
        })
        .unwrap();

    match parts.uri.query() {
        Some("hold=head") => {
            let _guard = guard;
            let _ = wait.await;
            Response::new(Body::from("{}"))
        }
        Some("hold=stream") => {
            let first = stream::once(async { Ok::<_, io::Error>(Bytes::from_static(FIRST)) });
            let last = stream::once(async move {
                let _guard = guard;
                let _ = wait.await;
                Ok(Bytes::from_static(LAST))
            });
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(first.chain(last)))
                .unwrap()
        }
        _ => Response::new(Body::from("{}")),
    }
}

struct RouterProcess(Child);

impl Drop for RouterProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Fixture {
    router: RouterProcess,
    addr: SocketAddr,
    client: reqwest::Client,
    calls: mpsc::UnboundedReceiver<Call>,
    mock: JoinHandle<()>,
    stderr: Option<BufReader<ChildStderr>>,
    directory: tempfile::TempDir,
}

impl Fixture {
    async fn start(server_options: &str) -> Self {
        Self::start_with_limits(server_options, false).await
    }

    async fn start_with_limits(server_options: &str, scoped_limits: bool) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let (tx, calls) = mpsc::unbounded_channel();
        let app = Router::new().fallback(provider).with_state(tx);
        let mock = tokio::spawn(async { axum::serve(upstream, app).await.unwrap() });

        let directory = tempfile::tempdir().unwrap();
        let config = directory.path().join("svincolo.toml");
        let files = if cfg!(unix) && scoped_limits {
            let files = directory.path().join("initial");
            fs::create_dir(&files).unwrap();
            files
        } else {
            directory.path().to_owned()
        };
        let (upstream_options, scoped_config) = if scoped_limits {
            fs::write(
                files.join("alice.key"),
                format!("{CALLER_A}\n{ROTATED_A}\n"),
            )
            .unwrap();
            fs::write(files.join("bob.key"), CALLER_B).unwrap();
            fs::write(files.join("provider.key"), "test-provider-old").unwrap();
            (
                "max_concurrent = 2\nkey_file = 'provider.key'\nallow_plain_http = true",
                format!(
                    r#"
[[upstream]]
name = "other"
kind = "openai_compatible"
base_url = "http://{upstream_addr}/v1"
max_concurrent = 1

[[route]]
name = "same-upstream"
upstream = "mock"
upstream_model = "mock-model"
locality = "local"
surfaces = {{ chat = "raw" }}

[[route]]
name = "other-upstream"
upstream = "other"
upstream_model = "mock-model"
locality = "local"
surfaces = {{ chat = "raw" }}

[[route]]
name = "cloud-route"
upstream = "mock"
upstream_model = "mock-model"
locality = "cloud"
surfaces = {{ chat = "raw" }}

[[caller]]
name = "alice"
key_file = "alice.key"
routes = ["*"]
max_concurrent = 1

[[caller]]
name = "bob"
key_file = "bob.key"
routes = ["*"]
max_concurrent = 2
"#
                ),
            )
        } else {
            ("", String::new())
        };
        fs::write(
            files.join("svincolo.toml"),
            format!(
                r#"version = 1
[server]
listen = "127.0.0.1:0"
{server_options}

[[upstream]]
name = "mock"
kind = "openai_compatible"
base_url = "http://{upstream_addr}/v1"
{upstream_options}

[[route]]
name = "mock-model"
upstream = "mock"
locality = "local"
surfaces = {{ chat = "raw" }}
{scoped_config}
"#
            ),
        )
        .unwrap();
        #[cfg(unix)]
        if scoped_limits {
            use std::os::unix::fs::symlink;

            symlink("initial", directory.path().join("..data")).unwrap();
            for file in ["svincolo.toml", "alice.key", "bob.key", "provider.key"] {
                symlink(format!("..data/{file}"), directory.path().join(file)).unwrap();
            }
        }
        let mut router = RouterProcess(
            Command::new(env!("CARGO_BIN_EXE_svincolo"))
                .args(["serve", "--config"])
                .arg(config)
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let mut stderr = BufReader::new(router.0.stderr.take().unwrap());
        let (line, stderr) = timeout(
            WAIT,
            tokio::task::spawn_blocking(move || {
                let mut line = String::new();
                stderr.read_line(&mut line).unwrap();
                (line, stderr)
            }),
        )
        .await
        .expect("router did not announce its listener")
        .unwrap();
        let addr = line
            .trim()
            .strip_prefix("svincolo: listening on ")
            .unwrap_or_else(|| panic!("unexpected router startup: {line}"))
            .parse()
            .unwrap();
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(if scoped_limits {
                Duration::from_secs(20)
            } else {
                WAIT
            })
            .build()
            .unwrap();
        Self {
            router,
            addr,
            client,
            calls,
            mock,
            stderr: Some(stderr),
            directory,
        }
    }

    fn chat(&self, query: &str) -> reqwest::RequestBuilder {
        self.client
            .post(format!("http://{}{CHAT}{query}", self.addr))
            .header("content-type", "application/json")
            .body(BODY)
    }

    async fn call(&mut self) -> Call {
        timeout(WAIT, self.calls.recv())
            .await
            .expect("upstream did not receive the request")
            .unwrap()
    }

    fn no_extra_calls(&mut self) {
        assert!(matches!(
            self.calls.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    async fn pending_upload(&self, token: Option<&str>) -> tokio::io::BufReader<TcpStream> {
        let mut connection =
            tokio::io::BufReader::new(TcpStream::connect(self.addr).await.unwrap());
        let authorization = token.map_or_else(String::new, |token| {
            format!("Authorization: Bearer {token}\r\n")
        });
        connection
            .get_mut()
            .write_all(
                format!(
                    "POST {CHAT} HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Type: application/json\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n",
                    self.addr
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut interim = String::new();
        timeout(WAIT, connection.read_line(&mut interim))
            .await
            .expect("router did not poll the upload")
            .unwrap();
        assert_eq!(interim, "HTTP/1.1 100 Continue\r\n");
        interim.clear();
        timeout(WAIT, connection.read_line(&mut interim))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(interim, "\r\n");
        connection
            .get_mut()
            .write_all(b"8\r\n{\"model\"\r\n")
            .await
            .unwrap();
        connection
    }

    #[cfg(unix)]
    fn terminate(&self) {
        self.signal("-TERM");
    }

    #[cfg(unix)]
    fn signal(&self, signal: &str) {
        assert!(
            Command::new("kill")
                .args([signal, &self.router.0.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
    }

    #[cfg(unix)]
    async fn reload_log(&mut self, deadline: Duration) -> String {
        let mut stderr = self.stderr.take().unwrap();
        let (line, stderr) = timeout(
            deadline,
            tokio::task::spawn_blocking(move || {
                let mut line = String::new();
                stderr.read_line(&mut line).unwrap();
                (line, stderr)
            }),
        )
        .await
        .expect("router did not report reload")
        .unwrap();
        self.stderr = Some(stderr);
        line
    }

    #[cfg(unix)]
    fn project_files(&self, generation: &str, updates: &[(&str, &str)]) {
        use std::os::unix::fs::symlink;

        let directory = self.directory.path();
        let next = directory.join(generation);
        fs::create_dir(&next).unwrap();
        for file in ["svincolo.toml", "alice.key", "bob.key", "provider.key"] {
            match updates.iter().find(|(name, _)| *name == file) {
                Some((_, content)) => fs::write(next.join(file), content).unwrap(),
                None => {
                    fs::copy(directory.join(file), next.join(file)).unwrap();
                }
            }
        }
        symlink(generation, directory.join("..data_tmp")).unwrap();
        fs::rename(directory.join("..data_tmp"), directory.join("..data")).unwrap();
    }

    #[cfg(unix)]
    async fn enable_queue(&mut self, upstream: &str) {
        let config = fs::read_to_string(self.directory.path().join("svincolo.toml")).unwrap();
        let config = config
            .replace(
                "max_concurrent = 2\nkey_file",
                &format!("max_concurrent = 1\n{upstream}\nkey_file"),
            )
            .replace(
                "name = \"alice\"\nkey_file",
                "name = \"alice\"\nover_limit = \"queue\"\nkey_file",
            );
        self.project_files("queue", &[("svincolo.toml", &config)]);
        self.signal("-HUP");
        assert_eq!(
            self.reload_log(WAIT).await.trim(),
            "svincolo: configuration reloaded"
        );
    }

    #[cfg(unix)]
    async fn queue_tcp(&self, token: &str, suffix: &str) -> TcpStream {
        let mut client = TcpStream::connect(self.addr).await.unwrap();
        let request = format!(
            "POST {CHAT} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{BODY}{suffix}",
            self.addr,
            BODY.len()
        );
        client.write_all(request.as_bytes()).await.unwrap();
        client
    }

    #[cfg(unix)]
    async fn queued_prefix(&self, model: &str) -> TcpStream {
        let mut client = TcpStream::connect(self.addr).await.unwrap();
        let prefix = format!(r#"{{"model":"{model}","#);
        let request = format!(
            "POST {CHAT} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {ROTATED_A}\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{prefix}\r\n",
            self.addr,
            prefix.len(),
        );
        client.write_all(request.as_bytes()).await.unwrap();
        client
    }

    #[cfg(unix)]
    async fn wait_for_exit(&mut self) -> ExitStatus {
        timeout(WAIT, async {
            loop {
                if let Some(status) = self.router.0.try_wait().unwrap() {
                    break status;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("router did not exit")
    }

    #[cfg(unix)]
    async fn health(&self, endpoint: &str) -> reqwest::Response {
        self.client
            .get(format!("http://{}{endpoint}", self.addr))
            .send()
            .await
            .unwrap()
    }

    #[cfg(unix)]
    async fn expect_draining(&self) {
        timeout(Duration::from_secs(1), async {
            loop {
                let readiness = self.health("/readyz").await;
                if readiness.status() == StatusCode::SERVICE_UNAVAILABLE {
                    expect_rejection(readiness, StatusCode::SERVICE_UNAVAILABLE, "draining").await;
                    break;
                }
                assert_eq!(readiness.status(), StatusCode::OK);
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("readiness did not drop promptly");
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.mock.abort();
    }
}

async fn expect_rejection(response: reqwest::Response, status: StatusCode, code: &str) {
    expect_rejection_with_retry(response, status, code, "1").await;
}

async fn expect_rejection_with_retry(
    response: reqwest::Response,
    status: StatusCode,
    code: &str,
    retry: &str,
) {
    assert_eq!(response.status(), status);
    assert_eq!(response.headers()["x-svincolo-provider-reached"], "false");
    assert_eq!(response.headers()["retry-after"], retry);
    let body: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], code);
}

async fn first_chunk(response: &mut reqwest::Response) {
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        timeout(WAIT, response.chunk())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        FIRST
    );
}

async fn upstream_dropped(call: Call) {
    // Keep finish alive so the mock only ends because its peer disconnected.
    let Call {
        finish, dropped, ..
    } = call;
    timeout(WAIT, dropped)
        .await
        .expect("mock still held work after downstream disconnected")
        .unwrap();
    drop(finish);
}

#[tokio::test]
async fn client_disconnect_cancels_upstream_before_headers_and_mid_stream() {
    let mut fixture = Fixture::start("").await;
    let mut client = TcpStream::connect(fixture.addr).await.unwrap();
    client
        .write_all(
            format!(
                "POST {CHAT}?hold=head HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{BODY}",
                fixture.addr,
                BODY.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let call = fixture.call().await;
    drop(client);
    upstream_dropped(call).await;

    let mut response = fixture.chat("?hold=stream").send().await.unwrap();
    first_chunk(&mut response).await;
    let call = fixture.call().await;
    drop(response);
    upstream_dropped(call).await;
    fixture.no_extra_calls();
}

#[tokio::test]
async fn connection_limit_counts_partial_headers_and_preserves_active_streams() {
    let mut fixture = Fixture::start("max_in_flight = 1\nmax_connections = 2").await;
    let mut stream = fixture.chat("?hold=stream").send().await.unwrap();
    first_chunk(&mut stream).await;
    let call = fixture.call().await;

    let mut idle = tokio::io::BufReader::new(TcpStream::connect(fixture.addr).await.unwrap());
    idle.get_mut()
        .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut status = String::new();
    timeout(WAIT, idle.read_line(&mut status))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, "HTTP/1.1 200 OK\r\n");
    idle.get_mut()
        .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .unwrap();

    let mut excess = TcpStream::connect(fixture.addr).await.unwrap();
    assert_eq!(
        timeout(WAIT, excess.read(&mut [0])).await.unwrap().unwrap(),
        0,
    );
    drop(idle);

    // A completed socket task releases capacity even while a stream is active.
    let ready = timeout(WAIT, async {
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        loop {
            if let Ok(response) = client
                .get(format!("http://{}/readyz", fixture.addr))
                .send()
                .await
            {
                break response;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("closed connection kept its slot");
    assert_eq!(ready.status(), StatusCode::OK);
    call.finish.send(()).unwrap();
    assert_eq!(stream.bytes().await.unwrap(), LAST);
    fixture.no_extra_calls();
}

#[tokio::test]
async fn incomplete_headers_expire_without_cutting_an_admitted_stream() {
    let mut fixture = Fixture::start("").await;
    let mut stream = fixture
        .chat("?hold=stream")
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .unwrap();
    first_chunk(&mut stream).await;
    let call = fixture.call().await;
    let mut client = TcpStream::connect(fixture.addr).await.unwrap();
    client
        .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .unwrap();
    let mut byte = [0];
    assert!(
        timeout(Duration::from_millis(100), client.read(&mut byte))
            .await
            .is_err()
    );
    assert_eq!(fixture.health("/readyz").await.status(), StatusCode::OK);
    assert_eq!(
        timeout(Duration::from_secs(15), client.read(&mut byte))
            .await
            .expect("incomplete HTTP header kept the connection open")
            .unwrap(),
        0,
    );
    call.finish.send(()).unwrap();
    assert_eq!(stream.bytes().await.unwrap(), LAST);
    assert_eq!(fixture.health("/readyz").await.status(), StatusCode::OK);
    fixture.no_extra_calls();
}

#[tokio::test]
async fn concurrent_chunked_uploads_exhaust_buffer_and_disconnect_releases_it() {
    let mut fixture =
        Fixture::start("max_body_bytes = 1024\nmax_buffered_bytes = 2048\nmax_in_flight = 32")
            .await;
    let (left, right) = tokio::join!(fixture.pending_upload(None), fixture.pending_upload(None));
    for response in join_all((0..16).map(|_| fixture.chat("").send())).await {
        expect_rejection(
            response.unwrap(),
            StatusCode::TOO_MANY_REQUESTS,
            "body_budget_exhausted",
        )
        .await;
    }
    fixture.no_extra_calls();
    drop(left);
    timeout(WAIT, async {
        loop {
            let response = fixture.chat("").send().await.unwrap();
            if response.status() == StatusCode::OK {
                assert_eq!(response.bytes().await.unwrap(), "{}");
                break;
            }
            expect_rejection(
                response,
                StatusCode::TOO_MANY_REQUESTS,
                "body_budget_exhausted",
            )
            .await;
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("disconnected upload did not release its body reservation");
    fixture.call().await;
    fixture.no_extra_calls();
    drop(right);
}

#[tokio::test]
async fn concurrent_streams_exhaust_admission_slots_and_completion_releases_them() {
    let mut fixture = Fixture::start("max_in_flight = 4").await;
    let mut admitted = Vec::new();
    for result in join_all((0..4).map(|_| fixture.chat("?hold=stream").send())).await {
        let mut response = result.unwrap();
        first_chunk(&mut response).await;
        admitted.push(response);
    }
    let mut calls = Vec::new();
    for _ in 0..4 {
        calls.push(fixture.call().await);
    }
    for response in join_all((0..16).map(|_| fixture.chat("").send())).await {
        expect_rejection(
            response.unwrap(),
            StatusCode::TOO_MANY_REQUESTS,
            "server_busy",
        )
        .await;
    }
    fixture.no_extra_calls();
    for call in calls {
        call.finish.send(()).unwrap();
    }
    for response in admitted {
        assert_eq!(response.bytes().await.unwrap(), LAST);
    }
    assert_eq!(
        fixture.chat("").send().await.unwrap().status(),
        StatusCode::OK
    );
    fixture.call().await;
    fixture.no_extra_calls();
}

#[tokio::test]
async fn caller_burst_cannot_take_another_callers_slot_and_aliases_share_upstream_slots() {
    let mut fixture = Fixture::start_with_limits("max_in_flight = 4", true).await;
    let mut alice = fixture
        .chat("?hold=stream")
        .bearer_auth(CALLER_A)
        .send()
        .await
        .unwrap();
    first_chunk(&mut alice).await;
    let alice_call = fixture.call().await;

    let burst = join_all((0..16).map(|_| fixture.chat("").bearer_auth(ROTATED_A).send()));
    let bob = fixture.chat("?hold=stream").bearer_auth(CALLER_B).send();
    let (refused, bob) = tokio::join!(burst, bob);
    for response in refused {
        expect_rejection(
            response.unwrap(),
            StatusCode::TOO_MANY_REQUESTS,
            "caller_concurrency_limit",
        )
        .await;
    }
    let mut bob = bob.unwrap();
    first_chunk(&mut bob).await;
    let bob_call = fixture.call().await;

    let alias = fixture.chat("").bearer_auth(CALLER_B).body(ALIAS_BODY);
    expect_rejection(
        alias.send().await.unwrap(),
        StatusCode::TOO_MANY_REQUESTS,
        "upstream_concurrency_limit",
    )
    .await;
    fixture.no_extra_calls();
    let alternate = fixture
        .chat("")
        .bearer_auth(CALLER_B)
        .body(OTHER_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(alternate.status(), StatusCode::OK);
    assert_eq!(alternate.bytes().await.unwrap(), "{}");
    fixture.call().await;

    bob_call.finish.send(()).unwrap();
    assert_eq!(bob.bytes().await.unwrap(), LAST);
    let retried = fixture
        .chat("")
        .bearer_auth(CALLER_B)
        .body(ALIAS_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(retried.status(), StatusCode::OK);
    assert_eq!(retried.bytes().await.unwrap(), "{}");
    fixture.call().await;
    drop(alice);
    upstream_dropped(alice_call).await;
    fixture.no_extra_calls();
}

#[tokio::test]
async fn caller_limit_covers_slow_upload_and_recovers_on_disconnect() {
    let mut fixture = Fixture::start_with_limits("max_in_flight = 4", true).await;
    let upload = fixture.pending_upload(Some(CALLER_A)).await;
    let blocked = fixture
        .chat("")
        .bearer_auth(ROTATED_A)
        .send()
        .await
        .unwrap();
    expect_rejection(
        blocked,
        StatusCode::TOO_MANY_REQUESTS,
        "caller_concurrency_limit",
    )
    .await;
    fixture.no_extra_calls();

    let admitted = fixture.chat("").bearer_auth(CALLER_B).send().await.unwrap();
    assert_eq!(admitted.status(), StatusCode::OK);
    assert_eq!(admitted.bytes().await.unwrap(), "{}");
    fixture.call().await;
    drop(upload);
    timeout(WAIT, async {
        loop {
            let response = fixture
                .chat("")
                .bearer_auth(ROTATED_A)
                .send()
                .await
                .unwrap();
            if response.status() == StatusCode::OK {
                assert_eq!(response.bytes().await.unwrap(), "{}");
                break;
            }
            expect_rejection(
                response,
                StatusCode::TOO_MANY_REQUESTS,
                "caller_concurrency_limit",
            )
            .await;
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("disconnected upload did not release the caller slot");
    fixture.call().await;
    fixture.no_extra_calls();
}

#[cfg(unix)]
#[tokio::test]
async fn completed_chunked_upload_uses_its_retained_bytes_in_the_queue() {
    let mut fixture = Fixture::start_with_limits(
        "max_body_bytes = 1024\nmax_buffered_bytes = 1536\nmax_in_flight = 2",
        true,
    )
    .await;
    fixture
        .enable_queue("queue_limit = 1\nmax_wait_ms = 3000")
        .await;
    let mut held = fixture
        .chat("?hold=stream")
        .bearer_auth(CALLER_A)
        .send()
        .await
        .unwrap();
    first_chunk(&mut held).await;
    let running = fixture.call().await;

    let upload = reqwest::Body::wrap_stream(stream::once(async {
        Ok::<_, io::Error>(Bytes::from_static(BODY.as_bytes()))
    }));
    let queued = tokio::spawn(fixture.chat("").bearer_auth(ROTATED_A).body(upload).send());
    wait_for_full_queue(&fixture).await;
    fixture.no_extra_calls();

    running.finish.send(()).unwrap();
    assert_eq!(held.bytes().await.unwrap(), LAST);
    let response = timeout(WAIT, queued).await.unwrap().unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap(), "{}");
    fixture.call().await;
    fixture.no_extra_calls();
}

#[cfg(unix)]
#[tokio::test]
async fn queued_prefix_releases_ingress_budget_and_validates_before_provider_admission() {
    let mut fixture = Fixture::start_with_limits(
        "max_body_bytes = 1024\nmax_buffered_bytes = 1536\nmax_in_flight = 2",
        true,
    )
    .await;
    fixture
        .enable_queue("queue_limit = 1\nmax_wait_ms = 3000\nrequests_per_minute = 3")
        .await;
    let mut held = fixture
        .chat("?hold=stream")
        .bearer_auth(CALLER_A)
        .send()
        .await
        .unwrap();
    first_chunk(&mut held).await;
    let running = fixture.call().await;

    let mut abandoned = fixture.queued_prefix("mock-model").await;
    wait_for_full_queue(&fixture).await;
    abandoned.write_all(b"1\r\n \r\n").await.unwrap();
    drop(abandoned);
    wait_for_queue_slot(&fixture).await;
    fixture.no_extra_calls();

    let mut prefix = fixture.queued_prefix("mock-model").await;
    wait_for_full_queue(&fixture).await;
    let unrelated = reqwest::Body::wrap_stream(stream::once(async {
        Ok::<_, io::Error>(Bytes::from_static(OTHER_BODY.as_bytes()))
    }));
    let response = fixture
        .chat("")
        .bearer_auth(CALLER_B)
        .body(unrelated)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap(), "{}");
    fixture.call().await;
    fixture.no_extra_calls();

    running.finish.send(()).unwrap();
    assert_eq!(held.bytes().await.unwrap(), LAST);
    // A validated request can use the upstream while the prefix still has
    // no tail. The remaining provider credit also has to survive validation.
    let response = fixture.chat("").bearer_auth(CALLER_B).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap(), "{}");
    fixture.call().await;
    let suffix = r#""mo\u0064el":"other-upstream"}"#;
    prefix
        .write_all(format!("{:x}\r\n{suffix}\r\n0\r\n\r\n", suffix.len()).as_bytes())
        .await
        .unwrap();
    let mut error = String::new();
    timeout(WAIT, prefix.read_to_string(&mut error))
        .await
        .unwrap()
        .unwrap();
    assert!(error.starts_with("HTTP/1.1 415 "), "{error}");
    assert!(error.contains("ambiguous_model"), "{error}");
    fixture.no_extra_calls();

    let response = fixture.chat("").bearer_auth(CALLER_B).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap(), "{}");
    fixture.call().await;
    fixture.no_extra_calls();
}

#[cfg(unix)]
#[tokio::test]
async fn bounded_queue_releases_slots_on_disconnect_and_keeps_other_upstreams_available() {
    let mut fixture = Fixture::start_with_limits("max_in_flight = 2", true).await;
    fixture
        .enable_queue("queue_limit = 1\nmax_wait_ms = 3000")
        .await;
    let mut held = fixture
        .chat("?hold=stream")
        .bearer_auth(CALLER_A)
        .send()
        .await
        .unwrap();
    first_chunk(&mut held).await;
    let running = fixture.call().await;
    let queued = fixture.queue_tcp(ROTATED_A, "").await;
    wait_for_full_queue(&fixture).await;
    fixture.no_extra_calls();

    let response = fixture
        .chat("")
        .bearer_auth(CALLER_B)
        .body(OTHER_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap(), "{}");
    fixture.call().await;
    drop(queued);
    wait_for_queue_slot(&fixture).await;
    fixture.no_extra_calls();

    let next = tokio::spawn(fixture.chat("").bearer_auth(ROTATED_A).send());
    wait_for_full_queue(&fixture).await;
    running.finish.send(()).unwrap();
    assert_eq!(held.bytes().await.unwrap(), LAST);
    let response = timeout(WAIT, next).await.unwrap().unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap(), "{}");
    fixture.call().await;
    fixture.no_extra_calls();
}

#[cfg(unix)]
#[tokio::test]
async fn pipelined_disconnect_releases_queue_without_spending_provider_credit() {
    const NEXT: &str = "GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let mut fixture = Fixture::start_with_limits("max_in_flight = 2", true).await;
    fixture
        .enable_queue("queue_limit = 1\nmax_wait_ms = 3000\nrequests_per_minute = 2")
        .await;
    let mut held = fixture
        .chat("?hold=stream")
        .bearer_auth(CALLER_A)
        .send()
        .await
        .unwrap();
    first_chunk(&mut held).await;
    let running = fixture.call().await;

    let abandoned = fixture.queue_tcp(ROTATED_A, NEXT).await;
    wait_for_full_queue(&fixture).await;
    drop(abandoned);
    wait_for_queue_slot(&fixture).await;
    fixture.no_extra_calls();

    let mut connected = fixture.queue_tcp(ROTATED_A, NEXT).await;
    wait_for_full_queue(&fixture).await;
    running.finish.send(()).unwrap();
    assert_eq!(held.bytes().await.unwrap(), LAST);
    let mut response = String::new();
    timeout(WAIT, connected.read_to_string(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(
        response.contains("\r\n\r\n{}HTTP/1.1 200 OK\r\n"),
        "{response}"
    );
    fixture.call().await;
    fixture.no_extra_calls();
}

#[cfg(unix)]
async fn wait_for_queue_slot(fixture: &Fixture) {
    let response = timeout(WAIT, async {
        loop {
            let response = fixture
                .chat("")
                .bearer_auth(CALLER_B)
                .header("x-svincolo-max-wait-ms", "25")
                .send()
                .await
                .unwrap();
            if response.headers()["retry-after"] == "5" {
                break response;
            }
            expect_rejection(response, StatusCode::TOO_MANY_REQUESTS, "queue_full").await;
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("disconnected request kept its queue slot");
    expect_rejection_with_retry(
        response,
        StatusCode::TOO_MANY_REQUESTS,
        "queue_timeout",
        "5",
    )
    .await;
}

#[cfg(unix)]
async fn wait_for_full_queue(fixture: &Fixture) {
    // Give the seed request a head start so the observer does not take its slot.
    sleep(Duration::from_millis(75)).await;
    timeout(Duration::from_secs(2), async {
        loop {
            let response = fixture
                .chat("")
                .bearer_auth(CALLER_B)
                .header("x-svincolo-max-wait-ms", "15")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
            let body: serde_json::Value =
                serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
            match body["error"]["code"].as_str().unwrap() {
                "queue_full" => break,
                "queue_timeout" => {}
                code => panic!("unexpected admission result {code}"),
            }
        }
    })
    .await
    .expect("request did not occupy the queue");
}

#[cfg(unix)]
#[tokio::test]
async fn queue_drains_without_dispatching_waiters() {
    let mut fixture =
        Fixture::start_with_limits("max_in_flight = 3\ndrain_delay_s = 0\ngrace_s = 3", true).await;
    fixture
        .enable_queue("queue_limit = 2\nmax_wait_ms = 3000")
        .await;
    let mut held = fixture
        .chat("?hold=stream")
        .bearer_auth(CALLER_A)
        .send()
        .await
        .unwrap();
    first_chunk(&mut held).await;
    let running = fixture.call().await;
    let pending = tokio::spawn(fixture.chat("").bearer_auth(CALLER_B).send());
    let mut prefix = fixture.queued_prefix("mock-model").await;
    wait_for_full_queue(&fixture).await;
    fixture.terminate();
    let response = timeout(WAIT, pending).await.unwrap().unwrap().unwrap();
    expect_rejection(response, StatusCode::SERVICE_UNAVAILABLE, "draining").await;
    let mut error = String::new();
    timeout(WAIT, prefix.read_to_string(&mut error))
        .await
        .unwrap()
        .unwrap();
    assert!(error.starts_with("HTTP/1.1 503 "), "{error}");
    assert!(error.contains("\"code\":\"draining\""), "{error}");
    running.finish.send(()).unwrap();
    assert_eq!(held.bytes().await.unwrap(), LAST);
    assert!(fixture.wait_for_exit().await.success());
    fixture.no_extra_calls();
}

#[cfg(unix)]
#[tokio::test]
async fn caller_rate_waits_only_when_opted_in() {
    let mut fixture = Fixture::start_with_limits("max_in_flight = 3", true).await;
    let config = fs::read_to_string(fixture.directory.path().join("svincolo.toml")).unwrap();
    let config = config
        .replace(
            "key_file = 'provider.key'",
            "queue_limit = 2\nkey_file = 'provider.key'",
        )
        .replace(
            "name = \"alice\"\nkey_file",
            "name = \"alice\"\nover_limit = \"queue\"\nrequests_per_minute = 1\nkey_file",
        )
        .replace(
            "name = \"bob\"\nkey_file",
            "name = \"bob\"\nrequests_per_minute = 1\nkey_file",
        );
    fixture.project_files("rate-queue", &[("svincolo.toml", &config)]);
    fixture.signal("-HUP");
    assert_eq!(
        fixture.reload_log(WAIT).await.trim(),
        "svincolo: configuration reloaded"
    );
    for token in [CALLER_A, CALLER_B] {
        let response = fixture.chat("").bearer_auth(token).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap(), "{}");
        fixture.call().await;
    }
    let response = fixture
        .chat("")
        .bearer_auth(CALLER_A)
        .header("x-svincolo-max-wait-ms", "30")
        .send()
        .await
        .unwrap();
    expect_rejection_with_retry(
        response,
        StatusCode::TOO_MANY_REQUESTS,
        "queue_timeout",
        "5",
    )
    .await;
    let response = fixture
        .chat("")
        .bearer_auth(CALLER_B)
        .header("x-svincolo-max-wait-ms", "30")
        .send()
        .await
        .unwrap();
    expect_rate_limit(response, "caller_rate_limit").await;
    fixture.no_extra_calls();
}

#[cfg(unix)]
#[tokio::test]
async fn rate_limits_share_caller_tokens_and_upstream_aliases_across_reload() {
    let mut fixture = Fixture::start_with_limits("max_in_flight = 4", true).await;
    let config = fs::read_to_string(fixture.directory.path().join("svincolo.toml")).unwrap();
    let config = config
        .replace(
            "key_file = 'provider.key'",
            "key_file = 'provider.key'\nrequests_per_minute = 1",
        )
        .replace(
            "routes = [\"*\"]",
            "routes = [\"*\"]\nrequests_per_minute = 1",
        );
    fixture.project_files("rate-policy", &[("svincolo.toml", &config)]);
    fixture.signal("-HUP");
    assert_eq!(
        fixture.reload_log(WAIT).await.trim(),
        "svincolo: configuration reloaded"
    );

    let response = fixture.chat("").bearer_auth(CALLER_A).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap(), "{}");
    fixture.call().await;
    for (token, body, code) in [
        (ROTATED_A, OTHER_BODY, "caller_rate_limit"),
        (CALLER_B, ALIAS_BODY, "upstream_rate_limit"),
    ] {
        let response = fixture
            .chat("")
            .bearer_auth(token)
            .body(body)
            .send()
            .await
            .unwrap();
        expect_rate_limit(response, code).await;
    }
    fixture.no_extra_calls();
    let response = fixture
        .chat("")
        .bearer_auth(CALLER_B)
        .body(OTHER_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap(), "{}");
    fixture.call().await;

    let next = "test-alice-next";
    fixture.project_files("rate-policy-new-key", &[("alice.key", next)]);
    fixture.signal("-HUP");
    assert_eq!(
        fixture.reload_log(WAIT).await.trim(),
        "svincolo: configuration reloaded"
    );
    let response = fixture
        .chat("")
        .bearer_auth(next)
        .body(OTHER_BODY)
        .send()
        .await
        .unwrap();
    expect_rate_limit(response, "caller_rate_limit").await;
    fixture.no_extra_calls();
}

#[cfg(unix)]
async fn expect_rate_limit(response: reqwest::Response, code: &str) {
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["x-svincolo-provider-reached"], "false");
    let retry_after = response.headers()["retry-after"]
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert!((55..=60).contains(&retry_after));
    let body: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], code);
}

#[cfg(unix)]
#[tokio::test]
async fn projected_rotation_preserves_old_stream_and_applies_new_keys_and_limits() {
    const NEW_CALLER: &str = "test-alice-new";
    let mut fixture = Fixture::start_with_limits("max_in_flight = 4", true).await;
    let config = fs::read_to_string(fixture.directory.path().join("svincolo.toml")).unwrap();
    let mut stream = fixture
        .chat("?hold=stream")
        .bearer_auth(CALLER_A)
        .send()
        .await
        .unwrap();
    first_chunk(&mut stream).await;
    let old_call = fixture.call().await;
    assert_eq!(
        old_call.authorization.as_ref().unwrap(),
        "Bearer test-provider-old"
    );

    fixture.project_files(
        "invalid",
        &[("svincolo.toml", "version = 2"), ("alice.key", NEW_CALLER)],
    );
    fixture.signal("-HUP");
    assert!(
        fixture
            .reload_log(WAIT)
            .await
            .contains("reload rejected: version must be 1")
    );
    let catalog = format!("http://{}/v1/models", fixture.addr);
    // These probes are followed by a wait for the reload poll. Close their
    // connections so the next probe cannot race the idle header deadline.
    assert_eq!(
        fixture
            .client
            .get(&catalog)
            .header("connection", "close")
            .bearer_auth(CALLER_A)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .client
            .get(&catalog)
            .header("connection", "close")
            .bearer_auth(NEW_CALLER)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );

    let config = config.replace(
        "max_concurrent = 2\nkey_file",
        "max_concurrent = 1\nkey_file",
    );
    fixture.project_files(
        "valid",
        &[
            ("svincolo.toml", &config),
            ("alice.key", NEW_CALLER),
            ("provider.key", "test-provider-new"),
        ],
    );
    assert_eq!(
        fixture.reload_log(Duration::from_secs(14)).await.trim(),
        "svincolo: configuration reloaded"
    );
    assert_eq!(
        fixture
            .client
            .get(&catalog)
            .bearer_auth(CALLER_A)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let over_caller = fixture
        .chat("")
        .bearer_auth(NEW_CALLER)
        .send()
        .await
        .unwrap();
    expect_rejection(
        over_caller,
        StatusCode::TOO_MANY_REQUESTS,
        "caller_concurrency_limit",
    )
    .await;
    let over_upstream = fixture.chat("").bearer_auth(CALLER_B).send().await.unwrap();
    expect_rejection(
        over_upstream,
        StatusCode::TOO_MANY_REQUESTS,
        "upstream_concurrency_limit",
    )
    .await;
    fixture.no_extra_calls();

    old_call.finish.send(()).unwrap();
    assert_eq!(stream.bytes().await.unwrap(), LAST);
    let new_response = fixture
        .chat("")
        .bearer_auth(NEW_CALLER)
        .send()
        .await
        .unwrap();
    assert_eq!(new_response.status(), StatusCode::OK);
    assert_eq!(new_response.bytes().await.unwrap(), "{}");
    assert_eq!(
        fixture.call().await.authorization.unwrap(),
        "Bearer test-provider-new"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn local_only_reload_cancels_cloud_work_and_keeps_local_stream() {
    let mut fixture = Fixture::start_with_limits("max_in_flight = 4", true).await;
    let config = fs::read_to_string(fixture.directory.path().join("svincolo.toml"))
        .unwrap()
        .replace(
            "key_file = 'provider.key'",
            "queue_limit = 1\nkey_file = 'provider.key'",
        )
        .replace(
            "name = \"alice\"\nkey_file",
            "name = \"alice\"\nover_limit = \"queue\"\nkey_file",
        );
    fixture.project_files("cloud-queue", &[("svincolo.toml", &config)]);
    fixture.signal("-HUP");
    assert_eq!(
        fixture.reload_log(WAIT).await.trim(),
        "svincolo: configuration reloaded"
    );
    let catalog = fixture
        .client
        .get(format!("http://{}/v1/models", fixture.addr))
        .bearer_auth(CALLER_A);
    let check_catalog = |local_only: bool| {
        let request = catalog.try_clone().unwrap();
        async move {
            let response = request.send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let data: serde_json::Value =
                serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
            assert_eq!(data["svincolo"]["local_only"], local_only);
            let routes = data["data"].as_array().unwrap();
            assert_eq!(
                routes.iter().any(|route| route["id"] == "cloud-route"),
                !local_only
            );
            assert!(routes.iter().any(|route| route["id"] == "same-upstream"));
        }
    };
    check_catalog(false).await;
    let mut cloud = fixture
        .chat("?hold=stream")
        .bearer_auth(CALLER_A)
        .body(CLOUD_BODY)
        .send()
        .await
        .unwrap();
    first_chunk(&mut cloud).await;
    let cloud_call = fixture.call().await;
    let mut local = fixture
        .chat("?hold=stream")
        .bearer_auth(CALLER_B)
        .send()
        .await
        .unwrap();
    first_chunk(&mut local).await;
    let local_call = fixture.call().await;
    // Bob can probe queue occupancy while Alice's provisional cloud upload waits.
    let mut prefix = fixture.queued_prefix("cloud-route").await;
    wait_for_full_queue(&fixture).await;

    let restricted = config.replace("[server]", "[server]\nlocal_only = true");
    fixture.project_files("local-only", &[("svincolo.toml", &restricted)]);
    fixture.signal("-HUP");
    assert_eq!(
        fixture.reload_log(WAIT).await.trim(),
        "svincolo: configuration reloaded"
    );
    check_catalog(true).await;
    let mut error = String::new();
    timeout(WAIT, prefix.read_to_string(&mut error))
        .await
        .unwrap()
        .unwrap();
    assert!(error.starts_with("HTTP/1.1 403 "), "{error}");
    assert!(error.contains("\"code\":\"local_only\""), "{error}");
    assert!(timeout(WAIT, cloud.chunk()).await.unwrap().is_err());
    upstream_dropped(cloud_call).await;
    let response = fixture
        .chat("")
        .bearer_auth(CALLER_A)
        .body(CLOUD_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(response.headers()["x-svincolo-provider-reached"], "false");
    let data: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(data["error"]["code"], "local_only");

    fixture.project_files("restored", &[("svincolo.toml", &config)]);
    fixture.signal("-HUP");
    assert_eq!(
        fixture.reload_log(WAIT).await.trim(),
        "svincolo: configuration reloaded"
    );
    check_catalog(false).await;
    let request = fixture
        .chat("?hold=head")
        .bearer_auth(CALLER_A)
        .body(CLOUD_BODY);
    let pending = tokio::spawn(request.send());
    let cloud_call = fixture.call().await;
    fixture.project_files("restricted-again", &[("svincolo.toml", &restricted)]);
    fixture.signal("-HUP");
    assert_eq!(
        fixture.reload_log(WAIT).await.trim(),
        "svincolo: configuration reloaded"
    );
    let response = pending.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(response.headers()["x-svincolo-provider-reached"], "unknown");
    upstream_dropped(cloud_call).await;
    local_call.finish.send(()).unwrap();
    assert_eq!(local.bytes().await.unwrap(), LAST);
    fixture.no_extra_calls();
}

#[cfg(unix)]
#[tokio::test]
async fn drain_refuses_new_work_and_completes_an_admitted_stream() {
    let mut fixture = Fixture::start("drain_delay_s = 2\ngrace_s = 3").await;
    let mut response = fixture.chat("?hold=stream").send().await.unwrap();
    first_chunk(&mut response).await;
    let call = fixture.call().await;
    let mut upload = fixture.pending_upload(None).await;
    fixture.terminate();

    fixture.expect_draining().await;
    assert_eq!(fixture.health("/healthz").await.status(), StatusCode::OK);
    let mut upload_status = String::new();
    timeout(WAIT, upload.read_line(&mut upload_status))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(upload_status, "HTTP/1.1 503 Service Unavailable\r\n");
    drop(upload);
    expect_rejection(
        fixture.chat("").send().await.unwrap(),
        StatusCode::SERVICE_UNAVAILABLE,
        "draining",
    )
    .await;
    fixture.no_extra_calls();
    timeout(WAIT, async {
        while TcpStream::connect(fixture.addr).await.is_ok() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("router did not close its listener during drain");
    call.finish.send(()).unwrap();
    assert_eq!(response.bytes().await.unwrap(), LAST);
    assert!(fixture.wait_for_exit().await.success());
}

#[cfg(unix)]
#[tokio::test]
async fn drain_grace_expiry_cuts_an_admitted_stream_and_cancels_upstream() {
    let mut fixture = Fixture::start("drain_delay_s = 0\ngrace_s = 1").await;
    let mut response = fixture.chat("?hold=stream").send().await.unwrap();
    first_chunk(&mut response).await;
    let call = fixture.call().await;
    fixture.terminate();

    let error = timeout(WAIT, response.chunk()).await.unwrap().unwrap_err();
    assert!(error.is_body() || error.is_decode(), "{error:?}");
    upstream_dropped(call).await;
    assert!(fixture.wait_for_exit().await.success());
}

#[cfg(unix)]
#[tokio::test]
async fn second_signal_cuts_the_stream_before_the_grace_period() {
    let mut fixture = Fixture::start("drain_delay_s = 30\ngrace_s = 30").await;
    let mut response = fixture.chat("?hold=stream").send().await.unwrap();
    first_chunk(&mut response).await;
    let call = fixture.call().await;
    fixture.terminate();
    fixture.expect_draining().await;
    fixture.terminate();

    let error = timeout(WAIT, response.chunk()).await.unwrap().unwrap_err();
    assert!(error.is_body() || error.is_decode(), "{error:?}");
    upstream_dropped(call).await;
    assert!(fixture.wait_for_exit().await.success());
}
