use std::{
    collections::VecDeque,
    fs,
    io::{BufRead, BufReader},
    net::Ipv4Addr,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::HeaderMap,
    response::Response,
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle, time::timeout};

const WAIT: Duration = Duration::from_secs(5);
const IMAGES: &str = "/v1/images/generations";
const PROVIDER_KEY: &str = "synthetic_image_provider_key";
const APP_KEY: &str = "svincolo_image_app";
const OTHER_KEY: &str = "svincolo_chat_only_app";
const METERED_KEY: &str = "svincolo_metered_app";
const CAPPED_KEY: &str = "svincolo_capped_app";
const REQUEST: &str =
    r#"{"model":"cover","prompt":"private prompt","size":"1024x1536","quality":"low","n":1}"#;

struct Seen {
    uri: String,
    headers: HeaderMap,
    body: Value,
}

struct Mock {
    calls: mpsc::UnboundedSender<Seen>,
    replies: Mutex<VecDeque<(u16, String)>>,
}

async fn provider(State(mock): State<Arc<Mock>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, 64 * 1024).await.unwrap();
    mock.calls
        .send(Seen {
            uri: parts.uri.to_string(),
            headers: parts.headers,
            body: serde_json::from_slice(&body).unwrap(),
        })
        .unwrap();
    let (status, body) = mock
        .replies
        .lock()
        .unwrap()
        .pop_front()
        .expect("extra attempt");
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

struct Fixture {
    router: Child,
    mock: JoinHandle<()>,
    calls: mpsc::UnboundedReceiver<Seen>,
    client: reqwest::Client,
    base_url: String,
    directory: tempfile::TempDir,
}

impl Fixture {
    async fn start(replies: impl IntoIterator<Item = (u16, String)>) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let (calls_tx, calls) = mpsc::unbounded_channel();
        let app = Router::new().fallback(provider).with_state(Arc::new(Mock {
            calls: calls_tx,
            replies: Mutex::new(replies.into_iter().collect()),
        }));
        let mock = tokio::spawn(async { axum::serve(upstream, app).await.unwrap() });

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        for (file, key) in [
            ("provider.key", PROVIDER_KEY),
            ("app.key", APP_KEY),
            ("other.key", OTHER_KEY),
            ("metered.key", METERED_KEY),
            ("capped.key", CAPPED_KEY),
        ] {
            fs::write(path.join(file), key).unwrap();
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let limits = json!({"app":{"capped":{"hard":1,"period":"month"}},"provider":{}});
        let period = json!({"start":now-60,"end":now+3600,"app":{"capped":5},"provider":{}});
        fs::write(path.join("limits.json"), limits.to_string()).unwrap();
        let usage = json!({"recorded_at":now,"periods":{"month":period}});
        fs::write(path.join("usage.json"), usage.to_string()).unwrap();
        let config = path.join("svincolo.toml");
        fs::write(
            &config,
            format!(
                r#"version = 1

[server]
listen = "127.0.0.1:0"

[[upstream]]
name = "images"
kind = "openai"
base_url = "http://{upstream_addr}/v1"
key_file = "provider.key"
allow_plain_http = true

[[route]]
name = "cover"
upstream = "images"
upstream_model = "image-model-1"
locality = "cloud"
surfaces = {{ images = "raw" }}

[[route]]
name = "chat-only"
upstream = "images"
upstream_model = "chat-model-1"
locality = "cloud"
surfaces = {{ chat = "raw" }}

[[caller]]
name = "app"
key_file = "app.key"
routes = ["cover", "chat-only"]

[[caller]]
name = "other"
key_file = "other.key"
routes = ["chat-only"]

[[caller]]
name = "metered"
key_file = "metered.key"
routes = ["cover"]
requests_per_minute = 1

[[caller]]
name = "capped"
key_file = "capped.key"
routes = ["cover"]
"#
            ),
        )
        .unwrap();
        let router = Command::new(env!("CARGO_BIN_EXE_svincolo"))
            .env("SVINCOLO_SPEND_DIR", path)
            .args(["serve", "--config"])
            .arg(config)
            .stdout(fs::File::create(path.join("router.log")).unwrap())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut fixture = Self {
            router,
            mock,
            calls,
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(WAIT)
                .build()
                .unwrap(),
            base_url: String::new(),
            directory,
        };
        let stderr = fixture.router.stderr.take().unwrap();
        let line = timeout(
            WAIT,
            tokio::task::spawn_blocking(move || {
                let mut line = String::new();
                BufReader::new(stderr).read_line(&mut line).unwrap();
                line
            }),
        )
        .await
        .expect("Svincolo did not report its listener")
        .unwrap();
        let address = line
            .trim()
            .strip_prefix("svincolo: listening on ")
            .unwrap_or_else(|| panic!("Svincolo startup failed: {line}"));
        fixture.base_url = format!("http://{address}");
        fixture
    }

    async fn post(&self, path: &str, key: &str, body: &str) -> reqwest::Response {
        self.client
            .post(format!("{}{path}", self.base_url))
            .bearer_auth(key)
            .header("content-type", "application/json")
            .body(body.to_owned())
            .send()
            .await
            .unwrap()
    }

    async fn seen(&mut self) -> Seen {
        timeout(WAIT, self.calls.recv())
            .await
            .expect("no upstream request")
            .unwrap()
    }

    fn no_more_calls(&mut self) {
        assert_eq!(self.calls.len(), 0, "unexpected upstream request");
    }

    fn events(&self) -> Vec<Value> {
        let log = fs::read_to_string(self.directory.path().join("router.log")).unwrap();
        log.lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|event| event["event"] == "native_usage")
            .collect()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.router.kill();
        let _ = self.router.wait();
        self.mock.abort();
    }
}

fn answer(image: &str, usage: &Value) -> String {
    json!({
        "created": 1_793_491_200,
        "data": [{"b64_json": image, "revised_prompt": "private \"revised\" prompt \u{e9}"}],
        "output_format": "png",
        "usage": usage,
    })
    .to_string()
}

async fn expect_error(response: reqwest::Response, status: u16, code: &str) {
    assert_eq!(response.status().as_u16(), status);
    assert_eq!(response.headers()["x-svincolo-origin"], "router");
    let body: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], code);
}

#[tokio::test]
async fn granted_image_request_forwards_the_pinned_model_and_records_token_usage() {
    // A portrait PNG in base64 is several megabytes, and usage follows it.
    let image = "iVBORw0KGgo".repeat(300_000);
    let usage = json!({
        "total_tokens": 1_100,
        "input_tokens": 50,
        "output_tokens": 1_050,
        "input_tokens_details": {"text_tokens": 50, "image_tokens": 0},
        "output_tokens_details": {"text_tokens": 10, "image_tokens": 1_040},
    });
    let expected = answer(&image, &usage);
    assert!(expected.len() > 3 * 1024 * 1024);
    let mut fixture = Fixture::start([(200, expected.clone())]).await;

    let response = fixture.post(IMAGES, APP_KEY, REQUEST).await;
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.headers()["x-svincolo-route"], "cover");
    assert_eq!(
        response.headers()["x-svincolo-upstream-model"],
        "image-model-1"
    );
    assert_eq!(response.headers()["x-svincolo-origin"], "upstream");
    assert_eq!(response.bytes().await.unwrap(), expected);

    let seen = fixture.seen().await;
    assert_eq!(seen.uri, IMAGES);
    assert_eq!(
        seen.headers["authorization"],
        format!("Bearer {PROVIDER_KEY}")
    );
    assert_eq!(
        seen.body,
        json!({"model":"image-model-1","prompt":"private prompt","size":"1024x1536","quality":"low","n":1})
    );

    let events = fixture.events();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event["surface"], "images");
    assert_eq!(event["caller"], "app");
    assert_eq!(event["upstream_model"], "image-model-1");
    assert_eq!(event["attempts"], 1);
    assert_eq!(event["known_attempts"], 1);
    assert_eq!(event["input_tokens"], 50);
    assert_eq!(event["output_tokens"], 10);
    assert_eq!(event["image_input_tokens"], 0);
    assert_eq!(event["image_output_tokens"], 1_040);
    assert!(!event.to_string().contains("private"));
}

#[tokio::test]
async fn image_answer_without_usage_is_recorded_with_unknown_usage() {
    let mut fixture = Fixture::start([(200, answer("aGVsbG8=", &Value::Null))]).await;
    let response = fixture.post(IMAGES, APP_KEY, REQUEST).await;
    assert_eq!(response.status().as_u16(), 200);
    response.bytes().await.unwrap();
    fixture.seen().await;
    let events = fixture.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["attempts"], 1);
    assert_eq!(events[0]["known_attempts"], 0);
    assert!(events[0].get("image_output_tokens").is_none());
}

#[tokio::test]
async fn ungranted_callers_and_mismatched_routes_never_reach_the_provider() {
    let mut fixture = Fixture::start([]).await;
    expect_error(
        fixture.post(IMAGES, OTHER_KEY, REQUEST).await,
        403,
        "route_not_permitted",
    )
    .await;
    expect_error(
        fixture.post(IMAGES, "unknown", REQUEST).await,
        401,
        "caller_key_unknown",
    )
    .await;
    let chat_model = REQUEST.replace("\"cover\"", "\"chat-only\"");
    expect_error(
        fixture.post(IMAGES, APP_KEY, &chat_model).await,
        404,
        "capability_unavailable",
    )
    .await;
    expect_error(
        fixture
            .post(
                "/v1/chat/completions",
                APP_KEY,
                r#"{"model":"cover","messages":[]}"#,
            )
            .await,
        404,
        "capability_unavailable",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    fixture.no_more_calls();
    assert!(fixture.events().is_empty());
}

#[tokio::test]
async fn streamed_or_partial_image_requests_are_refused_before_dispatch() {
    let mut fixture = Fixture::start([]).await;
    for options in [
        r#""stream":true"#,
        r#""partial_images":2"#,
        r#""stream":true,"partial_images":1"#,
    ] {
        let body = REQUEST.replace("\"n\":1", options);
        expect_error(
            fixture.post(IMAGES, APP_KEY, &body).await,
            400,
            "streaming_unsupported",
        )
        .await;
    }
    let repeated = REQUEST.replace("\"n\":1", r#""stream":false,"stream":true"#);
    expect_error(
        fixture.post(IMAGES, APP_KEY, &repeated).await,
        415,
        "body_not_json",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    fixture.no_more_calls();
    assert!(fixture.events().is_empty());
}

#[tokio::test]
async fn non_streaming_options_are_forwarded_unchanged() {
    let usage = json!({"input_tokens":1,"output_tokens":1});
    let mut fixture = Fixture::start([(200, answer("aGVsbG8=", &usage))]).await;
    let body = REQUEST.replace("\"n\":1", r#""n":1,"stream":false,"partial_images":0"#);
    let response = fixture.post(IMAGES, APP_KEY, &body).await;
    assert_eq!(response.status().as_u16(), 200);
    response.bytes().await.unwrap();
    let seen = fixture.seen().await;
    assert_eq!(seen.body["stream"], false);
    assert_eq!(seen.body["partial_images"], 0);
}

#[tokio::test]
async fn provider_refusal_passes_through_and_counts_an_unknown_attempt() {
    let refusal = json!({"error":{"message":"Rejected by the safety system.","type":"image_generation_user_error","code":"moderation_blocked"}}).to_string();
    let mut fixture = Fixture::start([(400, refusal.clone())]).await;
    let response = fixture.post(IMAGES, APP_KEY, REQUEST).await;
    assert_eq!(response.status().as_u16(), 400);
    assert_eq!(response.headers()["x-svincolo-origin"], "upstream");
    assert_eq!(response.headers()["x-svincolo-provider-reached"], "true");
    assert_eq!(response.bytes().await.unwrap(), refusal);
    fixture.seen().await;
    let events = fixture.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["attempts"], 1);
    assert_eq!(events[0]["known_attempts"], 0);
}

#[tokio::test]
async fn caller_rate_limits_and_spending_caps_apply_to_image_requests() {
    let usage =
        json!({"input_tokens":5,"output_tokens":7,"input_tokens_details":{"text_tokens":5}});
    let mut fixture = Fixture::start([(200, answer("aGVsbG8=", &usage))]).await;
    let response = fixture.post(IMAGES, METERED_KEY, REQUEST).await;
    assert_eq!(response.status().as_u16(), 200);
    response.bytes().await.unwrap();
    fixture.seen().await;
    expect_error(
        fixture.post(IMAGES, METERED_KEY, REQUEST).await,
        429,
        "caller_rate_limit",
    )
    .await;
    expect_error(
        fixture.post(IMAGES, CAPPED_KEY, REQUEST).await,
        402,
        "spend_cap_reached",
    )
    .await;
    fixture.no_more_calls();
    let events = fixture.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["caller"], "metered");
    assert_eq!(events[0]["input_tokens"], 5);
    assert_eq!(events[0]["output_tokens"], 0);
    assert_eq!(events[0]["image_output_tokens"], 7);
}
