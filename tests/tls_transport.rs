use std::{
    fs,
    io::{BufRead, BufReader},
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, Response, StatusCode, Version},
};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};
use hyper::{body::Incoming, server::conn, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use tokio::{net::TcpListener, sync::watch, task::JoinHandle, time::timeout};
use tokio_rustls::TlsAcceptor;

const BODY: &str = r#"{"model":"local","stream":true,"messages":[]}"#;
const FIRST: &[u8] = b"data: first\n\n";
const LAST: &[u8] = b"data: [DONE]\n\n";
const WAIT: Duration = Duration::from_secs(15);

struct Certificates {
    dir: tempfile::TempDir,
    ca: PathBuf,
}

impl Certificates {
    fn generate() -> Self {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("root.ext"),
            "[req]\ndistinguished_name = dn\n[dn]\n[ca]\n\
             basicConstraints = critical,CA:TRUE\nkeyUsage = critical,keyCertSign,cRLSign\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("leaf.ext"),
            "basicConstraints = critical,CA:FALSE\nextendedKeyUsage = serverAuth\n\
             subjectAltName = IP:127.0.0.1\n",
        )
        .unwrap();
        let openssl = |arguments: &[&str]| {
            let result = Command::new("openssl")
                .current_dir(dir.path())
                .args(arguments)
                .output()
                .expect("openssl is required for synthetic TLS fixtures");
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
        };
        openssl(&[
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-days",
            "2",
            "-subj",
            "/CN=Svincolo test CA",
            "-config",
            "root.ext",
            "-extensions",
            "ca",
            "-keyout",
            "root.key",
            "-out",
            "root.pem",
        ]);
        openssl(&[
            "req",
            "-new",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-subj",
            "/CN=Svincolo test upstream",
            "-keyout",
            "leaf.key",
            "-out",
            "leaf.csr",
        ]);
        openssl(&[
            "x509",
            "-req",
            "-in",
            "leaf.csr",
            "-CA",
            "root.pem",
            "-CAkey",
            "root.key",
            "-CAcreateserial",
            "-out",
            "leaf.pem",
            "-days",
            "2",
            "-extfile",
            "leaf.ext",
        ]);
        let ca = dir.path().join("root.pem");
        Self { dir, ca }
    }

    fn acceptor(&self, protocol: Protocol) -> TlsAcceptor {
        let certificate = CertificateDer::from_pem_file(self.dir.path().join("leaf.pem")).unwrap();
        let key = PrivateKeyDer::from_pem_file(self.dir.path().join("leaf.key")).unwrap();
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![certificate], key)
        .unwrap();
        config.alpn_protocols = vec![protocol.label().as_bytes().to_vec()];
        TlsAcceptor::from(Arc::new(config))
    }
}

#[derive(Clone, Copy)]
enum Protocol {
    Http1,
    Http2,
}

impl Protocol {
    fn label(self) -> &'static str {
        match self {
            Self::Http1 => "http/1.1",
            Self::Http2 => "h2",
        }
    }

    fn version(self) -> Version {
        match self {
            Self::Http1 => Version::HTTP_11,
            Self::Http2 => Version::HTTP_2,
        }
    }
}

struct Mock {
    address: SocketAddr,
    release: watch::Sender<bool>,
    calls: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
    rejected_handshakes: Arc<AtomicUsize>,
    listener: JoinHandle<()>,
}

impl Mock {
    async fn start(certificates: &Certificates, protocol: Protocol) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let acceptor = certificates.acceptor(protocol);
        let release = watch::Sender::new(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let connections = Arc::new(AtomicUsize::new(0));
        let rejected_handshakes = Arc::new(AtomicUsize::new(0));
        let task = {
            let (release, calls, connections, rejected_handshakes) = (
                release.clone(),
                Arc::clone(&calls),
                Arc::clone(&connections),
                Arc::clone(&rejected_handshakes),
            );
            tokio::spawn(async move {
                while let Ok((tcp, _)) = listener.accept().await {
                    let acceptor = acceptor.clone();
                    let (release, calls, connections, rejected_handshakes) = (
                        release.clone(),
                        Arc::clone(&calls),
                        Arc::clone(&connections),
                        Arc::clone(&rejected_handshakes),
                    );
                    tokio::spawn(async move {
                        let Ok(tls) = acceptor.accept(tcp).await else {
                            rejected_handshakes.fetch_add(1, Ordering::Relaxed);
                            return; // The negative tests intentionally reject the handshake.
                        };
                        assert_eq!(
                            tls.get_ref().1.alpn_protocol(),
                            Some(protocol.label().as_bytes())
                        );
                        connections.fetch_add(1, Ordering::Relaxed);
                        let service = service_fn(move |req: Request<Incoming>| {
                            let (release, calls) = (release.clone(), Arc::clone(&calls));
                            async move {
                                assert_eq!(req.version(), protocol.version());
                                assert_eq!(req.uri().path(), "/v1/chat/completions");
                                let hold = req.uri().query() == Some("hold=1");
                                assert_eq!(
                                    to_bytes(Body::new(req.into_body()), 1024).await.unwrap(),
                                    BODY
                                );
                                calls.fetch_add(1, Ordering::Relaxed);
                                let body = if hold {
                                    let mut signal = release.subscribe();
                                    let first = stream::once(async {
                                        Ok::<_, std::io::Error>(Bytes::from_static(FIRST))
                                    });
                                    let last = stream::once(async move {
                                        signal.wait_for(|ready| *ready).await.unwrap();
                                        Ok(Bytes::from_static(LAST))
                                    });
                                    Body::from_stream(first.chain(last))
                                } else {
                                    Body::from("ok")
                                };
                                Ok::<_, std::convert::Infallible>(
                                    Response::builder()
                                        .header("content-type", "text/event-stream")
                                        .header("x-test-protocol", protocol.label())
                                        .body(body)
                                        .unwrap(),
                                )
                            }
                        });
                        let io = TokioIo::new(tls);
                        match protocol {
                            Protocol::Http1 => {
                                let _ = conn::http1::Builder::new()
                                    .serve_connection(io, service)
                                    .await;
                            }
                            Protocol::Http2 => {
                                let _ = conn::http2::Builder::new(TokioExecutor::new())
                                    .max_concurrent_streams(Some(8192))
                                    .serve_connection(io, service)
                                    .await;
                            }
                        }
                    });
                }
            })
        };
        Self {
            address,
            release,
            calls,
            connections,
            rejected_handshakes,
            listener: task,
        }
    }
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.release.send_replace(true);
        self.listener.abort();
    }
}

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Router {
    process: Process,
    url: String,
    _dir: tempfile::TempDir,
    _stderr: BufReader<std::process::ChildStderr>,
}

impl Router {
    async fn start(mock: &Mock, host: &str, ca: Option<&Path>, limit: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("svincolo.toml");
        fs::write(
            &config,
            format!(
                r#"version = 1
[server]
listen = "127.0.0.1:0"
max_in_flight = {limit}
max_body_bytes = 1024
max_buffered_bytes = 8388608
local_only = true
drain_delay_s = 0

[[upstream]]
name = "mock"
kind = "openai_compatible"
base_url = "https://{host}:{}/v1"
max_concurrent = {limit}

[[route]]
name = "local"
upstream = "mock"
locality = "local"
surfaces = {{ chat = "raw" }}
"#,
                mock.address.port(),
            ),
        )
        .unwrap();
        let mut process = Process(router_command(&config, ca).spawn().unwrap());
        let stderr = process.0.stderr.take().unwrap();
        let (line, stderr) = timeout(
            WAIT,
            tokio::task::spawn_blocking(move || {
                let mut stderr = BufReader::new(stderr);
                let mut line = String::new();
                stderr.read_line(&mut line).unwrap();
                (line, stderr)
            }),
        )
        .await
        .expect("router startup timed out")
        .unwrap();
        let addr = line
            .trim()
            .strip_prefix("svincolo: listening on ")
            .unwrap_or_else(|| panic!("router failed to start: {line}"));
        Self {
            process,
            url: format!("http://{addr}/v1/chat/completions"),
            _dir: dir,
            _stderr: stderr,
        }
    }

    fn request(&self, client: &reqwest::Client, hold: bool) -> reqwest::RequestBuilder {
        let suffix = if hold { "?hold=1" } else { "" };
        client
            .post(format!("{}{suffix}", self.url))
            .header("content-type", "application/json")
            .body(BODY)
    }
}

fn router_command(config: &Path, ca: Option<&Path>) -> Command {
    let binary = std::env::var_os("SVINCOLO_TEST_BIN")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_svincolo").into());
    let mut command = Command::new(binary);
    command
        .env_remove("SVINCOLO_EXTRA_CA")
        .arg("serve")
        .arg("--config")
        .arg(config)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if let Some(ca) = ca {
        command.env("SVINCOLO_EXTRA_CA", ca);
    }
    command
}

fn client(wait: Duration) -> reqwest::Client {
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder()
        .no_proxy()
        .timeout(wait)
        .build()
        .unwrap()
}

async fn expect_no_provider(response: reqwest::Response, status: StatusCode, code: &str) {
    assert_eq!(response.status(), status);
    assert_eq!(response.headers()["x-svincolo-provider-reached"], "false");
    let body: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], code);
}

#[tokio::test]
async fn tls_negotiates_each_protocol_and_validates_the_ca_and_hostname() {
    let cert = Certificates::generate();
    let client = client(WAIT);
    for protocol in [Protocol::Http1, Protocol::Http2] {
        let mock = Mock::start(&cert, protocol).await;
        let router = Router::start(&mock, "127.0.0.1", Some(&cert.ca), 4).await;
        let response = router.request(&client, false).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-test-protocol"], protocol.label());
        assert_eq!(response.bytes().await.unwrap(), "ok");
        let router = Router::start(&mock, "127.0.0.1", None, 4).await;
        let response = router.request(&client, false).send().await.unwrap();
        expect_no_provider(response, StatusCode::BAD_GATEWAY, "upstream_unreachable").await;
        let router = Router::start(&mock, "localhost", Some(&cert.ca), 4).await;
        let response = router.request(&client, false).send().await.unwrap();
        expect_no_provider(response, StatusCode::BAD_GATEWAY, "upstream_unreachable").await;
        timeout(Duration::from_secs(2), async {
            while mock.rejected_handshakes.load(Ordering::Relaxed) < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("both rejected requests must reach the mock's TLS handshake");
        assert_eq!(mock.calls.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn invalid_extra_ca_stops_startup() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("svincolo.toml");
    let ca = dir.path().join("bad.pem");
    fs::write(&config, "version = 1").unwrap();
    fs::write(&ca, "synthetic invalid certificate data").unwrap();
    let result = router_command(&config, Some(&ca)).output().unwrap();
    assert!(!result.status.success());
    let stderr = String::from_utf8(result.stderr).unwrap();
    assert!(stderr.contains("SVINCOLO_EXTRA_CA"), "{stderr}");
    assert!(!stderr.contains("listening") && !stderr.contains("synthetic invalid"));
}

async fn first_event(response: &mut reqwest::Response) -> Result<(), reqwest::Error> {
    let mut first = Vec::new();
    while first.len() < FIRST.len() {
        let chunk = response
            .chunk()
            .await?
            .expect("stream ended before first event");
        first.extend_from_slice(&chunk);
    }
    assert_eq!(first, FIRST);
    Ok(())
}

fn memory_kib(pid: u32, field: &str) -> u64 {
    fs::read_to_string(format!("/proc/{pid}/status"))
        .unwrap()
        .lines()
        .find_map(|line| {
            line.strip_prefix(field)
                .and_then(|line| line.split_whitespace().next())
                .and_then(|value| value.parse().ok())
        })
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "release qualification requires Linux, at least 22000 file descriptors, and a release executable"]
async fn tls_release_capacity() {
    const CAPACITY: usize = 5000;
    assert!(std::env::var_os("SVINCOLO_TEST_BIN").is_some());
    let cert = Certificates::generate();
    let client = client(Duration::from_secs(90));
    for protocol in [Protocol::Http1, Protocol::Http2] {
        let mock = Mock::start(&cert, protocol).await;
        let router = Router::start(&mock, "127.0.0.1", Some(&cert.ca), CAPACITY).await;
        let response = router.request(&client, false).send().await.unwrap();
        assert_eq!(response.headers()["x-test-protocol"], protocol.label());
        assert_eq!(response.bytes().await.unwrap(), "ok");
        let idle = memory_kib(router.process.0.id(), "VmRSS:");
        let held: Vec<_> = stream::iter(0..CAPACITY)
            .map(|_| {
                let request = router.request(&client, true);
                async move {
                    let mut response = request.send().await?;
                    assert_eq!(response.status(), StatusCode::OK);
                    assert_eq!(response.headers()["x-test-protocol"], protocol.label());
                    first_event(&mut response).await?;
                    Ok::<_, reqwest::Error>(response)
                }
            })
            .buffer_unordered(128)
            .try_collect()
            .await
            .expect("could not fill 5000 concurrent streams");
        expect_no_provider(
            router.request(&client, false).send().await.unwrap(),
            StatusCode::TOO_MANY_REQUESTS,
            "server_busy",
        )
        .await;
        assert_eq!(mock.calls.load(Ordering::Relaxed), CAPACITY + 1);
        let connections = mock.connections.load(Ordering::Relaxed);
        match protocol {
            Protocol::Http1 => assert!(connections >= CAPACITY),
            Protocol::Http2 => assert!(connections < 32, "HTTP/2 did not multiplex: {connections}"),
        }
        let peak = memory_kib(router.process.0.id(), "VmHWM:");
        let bytes_per_stream = peak.saturating_sub(idle) * 1024 / CAPACITY as u64;
        mock.release.send_replace(true);
        stream::iter(held)
            .for_each_concurrent(128, |response| async move {
                assert_eq!(response.bytes().await.unwrap(), LAST);
            })
            .await;
        let response = router.request(&client, false).send().await.unwrap();
        assert_eq!(response.bytes().await.unwrap(), "ok");
        println!(
            "protocol={} streams={CAPACITY} tls_connections={connections} idle_kib={idle} \
             peak_rss_kib={peak} added_bytes_per_stream={bytes_per_stream}",
            protocol.label(),
        );
        assert!(peak < 512 * 1024, "router peak exceeded 512 MiB");
        assert!(bytes_per_stream <= 80_000, "stream overhead exceeded 80 KB");
    }
}
