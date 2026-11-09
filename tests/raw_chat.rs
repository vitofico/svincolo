use std::{
    fs, io,
    net::{Ipv4Addr, SocketAddrV4},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::Response,
};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use tokio::{
    net::TcpListener,
    sync::{Notify, mpsc, oneshot},
    task::JoinHandle,
    time::{sleep, timeout},
};

const CHAT: &str = "/v1/chat/completions";
const CALLER_KEY: &str = "svincolo_test_caller";
const UPSTREAM_KEY: &str = "fake_provider_secret";
const BODY: &str = r#"{"model":"chat:test","messages":[],"unknown": {"model": "nested"}}"#;
const WAIT: Duration = Duration::from_secs(5);

struct Seen {
    uri: String,
    headers: HeaderMap,
    body: oneshot::Receiver<Bytes>,
}

enum Reply {
    Bytes,
    BrokenStream(Arc<Notify>),
    Redirect,
    CredentialError,
}

struct Mock {
    calls: mpsc::UnboundedSender<Seen>,
    reply: Reply,
}

async fn provider(State(mock): State<Arc<Mock>>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let (body_tx, body_rx) = oneshot::channel();
    let _ = mock.calls.send(Seen {
        uri: parts.uri.to_string(),
        headers: parts.headers,
        body: body_rx,
    });
    let _ = body_tx.send(to_bytes(body, 32 * 1024 * 1024).await.unwrap());

    let mut reply = Response::builder();
    let body = match &mock.reply {
        Reply::Bytes => {
            reply = reply
                .header("content-type", "application/json")
                .header("connection", "x-hop")
                .header("x-hop", "response hop")
                .header("set-cookie", "provider_session=fake")
                .header("x-svincolo-origin", "forged")
                .header("x-svincolo-provider-reached", "false")
                .header("x-request-id", "mock-request");
            Body::from("{ \"unknown\": [2, 1] }\n")
        }
        Reply::BrokenStream(release) => {
            reply = reply.header("content-type", "text/event-stream");
            let release = Arc::clone(release);
            let first = stream::once(async { Ok(Bytes::from_static(b"data: first\n\n")) });
            let broken = stream::once(async move {
                release.notified().await;
                Err::<Bytes, _>(io::Error::other("mock stream failure"))
            });
            Body::from_stream(first.chain(broken))
        }
        Reply::Redirect => {
            reply = reply.status(307).header("location", "/redirect-target");
            Body::empty()
        }
        Reply::CredentialError => {
            reply = reply
                .status(401)
                .header("content-type", "application/json")
                .header("x-request-id", format!("id-{UPSTREAM_KEY}"));
            Body::from(format!(r#"{{"error":"Rejected {UPSTREAM_KEY}"}}"#))
        }
    };
    reply.body(body).unwrap()
}

struct Fixture {
    router: Child,
    mock: JoinHandle<()>,
    calls: mpsc::UnboundedReceiver<Seen>,
    client: reqwest::Client,
    base_url: String,
    _directory: tempfile::TempDir,
}

impl Fixture {
    async fn start(reply: Reply) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let (calls_tx, calls) = mpsc::unbounded_channel();
        let app = Router::new().fallback(provider).with_state(Arc::new(Mock {
            calls: calls_tx,
            reply,
        }));
        let mock = tokio::spawn(async { axum::serve(upstream, app).await.unwrap() });

        let directory = tempfile::tempdir().unwrap();
        let caller_key = directory.path().join("caller.key");
        let upstream_key = directory.path().join("upstream.key");
        fs::write(&caller_key, format!("{CALLER_KEY}\n")).unwrap();
        fs::write(&upstream_key, format!("{UPSTREAM_KEY}\n")).unwrap();
        let caller_key = serde_json::to_string(caller_key.to_str().unwrap()).unwrap();
        let upstream_key = serde_json::to_string(upstream_key.to_str().unwrap()).unwrap();
        let reservation = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = reservation.local_addr().unwrap().port();
        let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
        let config = directory.path().join("svincolo.toml");
        fs::write(
            &config,
            format!(
                r#"version = 1

[server]
listen = "{addr}"
max_body_bytes = 1024
max_buffered_bytes = 4096

[[upstream]]
name = "mock"
kind = "openai_compatible"
base_url = "http://{upstream_addr}/v1"
key_file = {upstream_key}
allow_plain_http = true

[[route]]
name = "chat:test"
upstream = "mock"
locality = "local"
surfaces = {{ chat = "raw" }}

[[route]]
name = "chat:restricted"
upstream = "mock"
locality = "local"
surfaces = {{ chat = "raw" }}

[[route]]
name = "chat:cloud"
upstream = "mock"
locality = "cloud"
surfaces = {{ chat = "raw" }}

[[caller]]
name = "test"
key_file = {caller_key}
routes = ["chat:test", "chat:cloud"]
"#
            ),
        )
        .unwrap();
        drop(reservation);
        let router = Command::new(env!("CARGO_BIN_EXE_svincolo"))
            .args(["serve", "--config"])
            .arg(config)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let mut fixture = Self {
            router,
            mock,
            calls,
            client: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(WAIT)
                .build()
                .unwrap(),
            base_url: format!("http://{addr}"),
            _directory: directory,
        };

        timeout(WAIT, async {
            let ready = format!("{}/readyz", fixture.base_url);
            loop {
                if let Some(status) = fixture.router.try_wait().unwrap() {
                    panic!("Svincolo exited during startup: {status}");
                }
                if fixture
                    .client
                    .get(&ready)
                    .send()
                    .await
                    .is_ok_and(|r| r.status() == StatusCode::OK)
                {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Svincolo did not become ready");
        fixture
    }

    fn chat(&self, path: &str) -> reqwest::RequestBuilder {
        self.client
            .post(format!("{}{path}", self.base_url))
            .header("content-type", "application/json")
    }

    async fn seen(&mut self) -> Seen {
        timeout(WAIT, self.calls.recv())
            .await
            .expect("no upstream request")
            .unwrap()
    }

    fn no_more_calls(&mut self) {
        assert!(matches!(
            self.calls.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.router.kill();
        let _ = self.router.wait();
        self.mock.abort();
    }
}

async fn assert_router_error(
    response: reqwest::Response,
    status: StatusCode,
    code: &str,
    reached: &str,
) {
    assert_eq!(response.status(), status);
    assert_eq!(response.headers()["x-svincolo-origin"], "router");
    assert_eq!(response.headers()["x-svincolo-provider-reached"], reached);
    let body: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], code);
}

#[tokio::test]
async fn forwards_raw_bytes_and_isolates_credentials_and_hop_headers() {
    let mut fixture = Fixture::start(Reply::Bytes).await;
    let response = fixture
        .chat("/v1/chat/completions?alpha=%2f&beta=a+b")
        .bearer_auth(CALLER_KEY)
        .header("x-api-key", CALLER_KEY)
        .header("connection", "keep-alive, x-hop")
        .header("x-hop", "request hop")
        .header("accept-encoding", "gzip")
        .header("cookie", "session=fake")
        .header("forwarded", "for=192.0.2.1")
        .header("x-svincolo-tag", "not-for-provider")
        .header("openai-organization", "test-organization")
        .header("openai-project", "test-project")
        .header("openai-beta", "sample-feature=v1")
        .body(BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-request-id"], "mock-request");
    assert_eq!(response.headers()["x-svincolo-origin"], "upstream");
    assert_eq!(response.headers()["x-svincolo-provider-reached"], "true");
    assert_eq!(response.headers()["x-svincolo-locality"], "local");
    assert_eq!(response.headers()["x-svincolo-attempts"], "1");
    assert!(!response.headers().contains_key("x-hop"));
    assert!(!response.headers().contains_key("set-cookie"));
    assert_eq!(response.bytes().await.unwrap(), "{ \"unknown\": [2, 1] }\n");

    let seen = fixture.seen().await;
    assert_eq!(seen.uri, "/v1/chat/completions?alpha=%2f&beta=a+b");
    assert_eq!(timeout(WAIT, seen.body).await.unwrap().unwrap(), BODY);
    assert_eq!(
        seen.headers["authorization"],
        format!("Bearer {UPSTREAM_KEY}")
    );
    assert_eq!(seen.headers["openai-beta"], "sample-feature=v1");
    for name in [
        "x-api-key",
        "x-hop",
        "accept-encoding",
        "cookie",
        "forwarded",
        "x-svincolo-tag",
        "openai-organization",
        "openai-project",
    ] {
        assert!(!seen.headers.contains_key(name), "forwarded {name}");
    }
    fixture.no_more_calls();
}

#[tokio::test]
async fn validates_the_full_body_and_route_before_contacting_upstream() {
    let mut fixture = Fixture::start(Reply::Bytes).await;
    let response = fixture.chat(CHAT).body(BODY).send().await.unwrap();
    assert_router_error(
        response,
        StatusCode::UNAUTHORIZED,
        "caller_key_unknown",
        "false",
    )
    .await;

    let cases: &[(&[u8], StatusCode, &str)] = &[
        (
            br#"{"model":"chat:test","model":"chat:restricted"}"#,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "ambiguous_model",
        ),
        (
            br#"{"model":"chat:test","mod\u0065l":"chat:restricted"}"#,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "ambiguous_model",
        ),
        (
            br#"{"model":"chat:test"} trailing"#,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "body_not_json",
        ),
        (
            b"{\"model\":\"chat:test\",\"unknown\":\"\xff\"}",
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "body_not_json",
        ),
        (
            br#"{"model":"chat:restricted"}"#,
            StatusCode::FORBIDDEN,
            "route_not_permitted",
        ),
    ];
    for &(body, status, code) in cases {
        let response = fixture
            .chat(CHAT)
            .header("x-api-key", CALLER_KEY)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_router_error(response, status, code, "false").await;
    }

    let response = fixture
        .chat(CHAT)
        .bearer_auth(CALLER_KEY)
        .header("x-svincolo-require-locality", "local")
        .body(r#"{"model":"chat:cloud"}"#)
        .send()
        .await
        .unwrap();
    assert_router_error(response, StatusCode::FORBIDDEN, "locality_refused", "false").await;

    let parts = [
        Bytes::from_static(br#"{"model":"chat:test","large":""#),
        Bytes::from(vec![b'x'; 1024]),
        Bytes::from_static(b"\"}"),
    ];
    let response = fixture
        .chat(CHAT)
        .bearer_auth(CALLER_KEY)
        .body(reqwest::Body::wrap_stream(stream::iter(
            parts.map(Ok::<_, io::Error>),
        )))
        .send()
        .await
        .unwrap();
    assert_router_error(
        response,
        StatusCode::PAYLOAD_TOO_LARGE,
        "body_too_large",
        "false",
    )
    .await;
    fixture.no_more_calls();
}

#[tokio::test]
async fn delivers_first_stream_bytes_and_propagates_a_broken_body() {
    let release = Arc::new(Notify::new());
    let mut fixture = Fixture::start(Reply::BrokenStream(Arc::clone(&release))).await;
    let mut response = fixture
        .chat(CHAT)
        .bearer_auth(CALLER_KEY)
        .body(BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let chunk = timeout(WAIT, response.chunk())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(chunk, "data: first\n\n");
    release.notify_one();
    let error = timeout(WAIT, response.chunk()).await.unwrap().unwrap_err();
    assert!(error.is_body() || error.is_decode(), "{error:?}");
    fixture.seen().await;
    fixture.no_more_calls();
}

#[tokio::test]
async fn refuses_redirects_and_redacts_provider_credentials() {
    let mut fixture = Fixture::start(Reply::Redirect).await;
    let response = fixture
        .chat(CHAT)
        .bearer_auth(CALLER_KEY)
        .body(BODY)
        .send()
        .await
        .unwrap();
    assert!(!response.headers().contains_key("location"));
    assert_router_error(
        response,
        StatusCode::BAD_GATEWAY,
        "upstream_redirect",
        "true",
    )
    .await;
    assert_eq!(fixture.seen().await.uri, CHAT);
    fixture.no_more_calls();
    drop(fixture);

    let mut fixture = Fixture::start(Reply::CredentialError).await;
    let response = fixture
        .chat(CHAT)
        .bearer_auth(CALLER_KEY)
        .body(BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()["x-svincolo-origin"], "upstream");
    assert!(!response.headers().contains_key("x-request-id"));
    assert_eq!(
        response.bytes().await.unwrap(),
        r#"{"error":"Rejected [redacted]"}"#
    );
    fixture.seen().await;
    fixture.no_more_calls();
}
