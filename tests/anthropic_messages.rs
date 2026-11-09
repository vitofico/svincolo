use std::{
    collections::VecDeque,
    fs,
    io::{BufRead, BufReader},
    net::Ipv4Addr,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
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
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{Notify, mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(5);
const CALLER_KEY: &str = "svincolo_test_caller";
const PROVIDER_KEY: &str = "synthetic_anthropic_key";
const MESSAGES: &str = "/v1/messages";
const BODY: &str = r#"{
    "messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]},
                {"role":"assistant","content":[{"type":"thinking","thinking":"opaque","signature":"keep-this"}]}],
    "max_tokens":1024,
    "model":"claude-test",
    "tools":[{"name":"lookup","input_schema":{"type":"object"}}],
    "thinking":{"type":"enabled","budget_tokens":512},
    "output_config":{"effort":"high"},
    "unknown":{"model":"nested"}
}"#;
const NATIVE_RESPONSE: &str = r#"{ "id":"msg_test", "type":"message", "content":[{"type":"text","text":"hello"}], "future_field":true }"#;
const NATIVE_ERROR: &str = r#"{"type":"error","error":{"type":"overloaded_error","message":"Try later"},"request_id":"req_test"}"#;
const SSE_START: &str = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_test\"}}\n\n";
const SSE_END: &str = "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"signature_delta\",\"signature\":\"kept\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";

struct Seen {
    uri: String,
    headers: HeaderMap,
    body: oneshot::Receiver<Bytes>,
}

enum Reply {
    Message,
    Error,
    Stream(Arc<Notify>),
    Script(Mutex<VecDeque<(u16, Value)>>),
}

struct Mock {
    calls: mpsc::UnboundedSender<Seen>,
    reply: Reply,
}

async fn provider(State(mock): State<Arc<Mock>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let (body_tx, body_rx) = oneshot::channel();
    mock.calls
        .send(Seen {
            uri: parts.uri.to_string(),
            headers: parts.headers,
            body: body_rx,
        })
        .unwrap();
    if let Ok(body) = to_bytes(body, 16 * 1024).await {
        let _ = body_tx.send(body);
    }
    let response = Response::builder().header("request-id", "req_test");
    match &mock.reply {
        Reply::Message => response
            .header("content-type", "application/json")
            .body(Body::from(NATIVE_RESPONSE))
            .unwrap(),
        Reply::Error => response
            .status(529)
            .header("content-type", "application/json")
            .header("retry-after", "7")
            .body(Body::from(NATIVE_ERROR))
            .unwrap(),
        Reply::Stream(gate) => {
            let gate = Arc::clone(gate);
            let start = stream::once(async {
                Ok::<_, std::io::Error>(Bytes::from_static(SSE_START.as_bytes()))
            });
            let end = stream::once(async move {
                gate.notified().await;
                Ok(Bytes::from_static(SSE_END.as_bytes()))
            });
            response
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(start.chain(end)))
                .unwrap()
        }
        Reply::Script(replies) => {
            let (status, body) = replies.lock().unwrap().pop_front().expect("extra attempt");
            response
                .status(status)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        }
    }
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
    async fn start(reply: Reply, version: Option<&str>) -> Self {
        Self::start_with_rate(reply, version, "").await
    }

    async fn start_with_rate(reply: Reply, version: Option<&str>, rate: &str) -> Self {
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
        let config = directory.path().join("svincolo.toml");
        fs::write(directory.path().join("caller.key"), CALLER_KEY).unwrap();
        fs::write(directory.path().join("provider.key"), PROVIDER_KEY).unwrap();
        let version = version.map_or_else(String::new, |version| {
            format!("anthropic_version = {version:?}")
        });
        fs::write(
            &config,
            format!(
                r#"version = 1

[server]
listen = "127.0.0.1:0"
discover_by_default = false

[[upstream]]
name = "claude"
kind = "anthropic"
base_url = "http://{upstream_addr}/v1"
key_file = "provider.key"
allow_plain_http = true
{version}
{rate}

[[upstream]]
name = "chat"
kind = "openai_compatible"
base_url = "http://{upstream_addr}/v1"

[[upstream]]
name = "openai"
kind = "openai"
base_url = "http://{upstream_addr}/v1"
key_file = "provider.key"
allow_plain_http = true

[[upstream]]
name = "ollama"
kind = "ollama"
base_url = "http://{upstream_addr}/v1"
allow_plain_http = true

[[route]]
name = "claude-test"
upstream = "claude"
locality = "cloud"
surfaces = {{ messages = "raw" }}

[[route]]
name = "claude-restricted"
upstream = "claude"
locality = "cloud"
surfaces = {{ messages = "raw" }}

[[route]]
name = "claude-managed"
upstream = "claude"
upstream_model = "claude-test"
locality = "cloud"
surfaces = {{ chat = "managed", messages = "raw" }}
discover = true
validation_retries = 1
default_max_tokens = 1024

[[route]]
name = "chat-test"
upstream = "chat"
locality = "local"
surfaces = {{ chat = "raw" }}

[[route]]
name = "openai-managed"
upstream = "openai"
upstream_model = "synthetic-openai"
locality = "cloud"
surfaces = {{ chat = "managed" }}
discover = true

[[route]]
name = "ollama-managed"
upstream = "ollama"
upstream_model = "synthetic-ollama:cloud"
locality = "cloud"
surfaces = {{ chat = "managed" }}
discover = true
validation_retries = 1

[[route]]
name = "ollama-native"
upstream = "ollama"
upstream_model = "synthetic-ollama:local"
locality = "local"
surfaces = {{ chat = "managed" }}
structured_modes = ["native"]

[[caller]]
name = "test"
key_file = "caller.key"
routes = ["claude-test", "chat-test", "claude-managed", "openai-managed", "ollama-managed", "ollama-native"]
"#
            ),
        )
        .unwrap();
        let router = Command::new(env!("CARGO_BIN_EXE_svincolo"))
            .args(["serve", "--config"])
            .arg(config)
            .stdout(Stdio::null())
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
            _directory: directory,
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

    fn post(&self, path: &str) -> reqwest::RequestBuilder {
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
        assert_eq!(self.calls.len(), 0, "unexpected upstream request");
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.router.kill();
        let _ = self.router.wait();
        self.mock.abort();
    }
}

#[tokio::test]
async fn preserves_native_bytes_and_authenticates_both_messages_paths() {
    let mut fixture = Fixture::start(Reply::Message, Some("2023-06-01")).await;

    let response = fixture
        .post("/anthropic/v1/messages?feature=%2f")
        .bearer_auth(CALLER_KEY)
        .header("anthropic-version", "2026-10-21")
        .header("anthropic-beta", "feature-one")
        .header("anthropic-beta", "feature-two")
        .header("anthropic-workspace-id", "caller-workspace")
        .header("anthropic-user-profile-id", "caller-profile")
        .body(BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["request-id"], "req_test");
    assert_eq!(response.headers()["x-svincolo-origin"], "upstream");
    assert_eq!(response.headers()["x-svincolo-provider-reached"], "true");
    assert_eq!(
        response.headers()["x-svincolo-upstream-model"],
        "claude-test"
    );
    assert_eq!(response.bytes().await.unwrap(), NATIVE_RESPONSE);

    let seen = fixture.seen().await;
    assert_eq!(seen.uri, "/v1/messages?feature=%2f");
    assert_eq!(timeout(WAIT, seen.body).await.unwrap().unwrap(), BODY);
    assert_eq!(seen.headers.get_all("x-api-key").iter().count(), 1);
    assert_eq!(seen.headers["x-api-key"], PROVIDER_KEY);
    assert_eq!(seen.headers["anthropic-version"], "2026-10-21");
    assert_eq!(
        seen.headers
            .get_all("anthropic-beta")
            .iter()
            .collect::<Vec<_>>(),
        ["feature-one", "feature-two"]
    );
    for name in [
        "authorization",
        "anthropic-workspace-id",
        "anthropic-user-profile-id",
    ] {
        assert!(!seen.headers.contains_key(name), "forwarded {name}");
    }

    let response = fixture
        .post(MESSAGES)
        .header("x-api-key", CALLER_KEY)
        .body(BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap(), NATIVE_RESPONSE);
    let seen = fixture.seen().await;
    assert_eq!(seen.uri, MESSAGES);
    assert_eq!(seen.headers["x-api-key"], PROVIDER_KEY);
    assert_eq!(seen.headers["anthropic-version"], "2023-06-01");
    fixture.no_more_calls();
}

#[tokio::test]
async fn streams_native_events_and_supplies_default_version() {
    let gate = Arc::new(Notify::new());
    let mut fixture = Fixture::start(Reply::Stream(Arc::clone(&gate)), None).await;
    let mut response = fixture
        .post(MESSAGES)
        .header("x-api-key", CALLER_KEY)
        .body(BODY.replace("\"max_tokens\":1024", "\"max_tokens\":1024,\"stream\":true"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");

    let seen = fixture.seen().await;
    assert_eq!(seen.headers["anthropic-version"], "2026-10-21");

    let mut body = Vec::new();
    while body.len() < SSE_START.len() {
        let chunk = response.chunk().await.unwrap().expect("stream ended early");
        body.extend_from_slice(&chunk);
    }
    assert_eq!(body, SSE_START.as_bytes());
    gate.notify_one();
    assert_eq!(response.bytes().await.unwrap(), SSE_END);
    fixture.no_more_calls();
}

#[tokio::test]
async fn preserves_native_provider_error() {
    let mut fixture = Fixture::start(Reply::Error, None).await;
    let response = fixture
        .post(MESSAGES)
        .header("x-api-key", CALLER_KEY)
        .body(BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 529);
    assert_eq!(response.headers()["request-id"], "req_test");
    assert_eq!(response.headers()["retry-after"], "7");
    assert_eq!(response.headers()["x-svincolo-origin"], "upstream");
    assert_eq!(response.headers()["x-svincolo-provider-reached"], "true");
    assert_eq!(response.bytes().await.unwrap(), NATIVE_ERROR);
    fixture.seen().await;
    fixture.no_more_calls();
}

#[tokio::test]
async fn rejects_ambiguous_or_forbidden_messages_before_egress() {
    let mut fixture = Fixture::start(Reply::Message, None).await;
    let cases = [
        (
            "/v1/v1/messages",
            BODY,
            StatusCode::NOT_FOUND,
            "unknown_path",
        ),
        (
            "/v1/chat/completions",
            BODY,
            StatusCode::NOT_FOUND,
            "capability_unavailable",
        ),
        (
            MESSAGES,
            r#"{"model":"chat-test"}"#,
            StatusCode::NOT_FOUND,
            "capability_unavailable",
        ),
        (
            MESSAGES,
            r#"{"model":"claude-restricted","messages":[],"max_tokens":1}"#,
            StatusCode::FORBIDDEN,
            "route_not_permitted",
        ),
        (
            MESSAGES,
            r#"{"model":"claude-test","m\u006fdel":"chat-test"}"#,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "ambiguous_model",
        ),
    ];
    for (path, body, status, code) in cases {
        let response = fixture
            .post(path)
            .bearer_auth(CALLER_KEY)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_router_error(response, status, Some(code)).await;
    }

    let response = fixture
        .post(MESSAGES)
        .bearer_auth(CALLER_KEY)
        .header("x-svincolo-require-locality", "local")
        .body(BODY)
        .send()
        .await
        .unwrap();
    assert_router_error(response, StatusCode::FORBIDDEN, Some("locality_refused")).await;

    let response = fixture
        .post(MESSAGES)
        .bearer_auth(CALLER_KEY)
        .header("anthropic-version", "2026-10-21")
        .header("anthropic-version", "2023-06-01")
        .body(BODY)
        .send()
        .await
        .unwrap();
    assert_router_error(response, StatusCode::UNSUPPORTED_MEDIA_TYPE, None).await;
    assert!(
        timeout(Duration::from_millis(100), fixture.calls.recv())
            .await
            .is_err(),
        "an invalid request reached the upstream"
    );
}

async fn assert_router_error(response: reqwest::Response, status: StatusCode, code: Option<&str>) {
    assert_eq!(response.status(), status);
    assert_eq!(response.headers()["x-svincolo-origin"], "router");
    assert_eq!(response.headers()["x-svincolo-provider-reached"], "false");
    if let Some(code) = code {
        let body: serde_json::Value =
            serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert_eq!(body["error"]["code"], code);
    }
}

fn managed_body(schema: Value) -> Value {
    json!({
        "model":"claude-managed",
        "stream":false,
        "temperature":0.2,
        "messages":[{"role":"system","content":"Synthetic Quire prompt."},
                    {"role":"user","content":"Return the requested object."}],
        "response_format":{"type":"json_schema","json_schema":{"name":"Probe","strict":true,"schema":schema}},
    })
}

fn closed_schema() -> Value {
    json!({"type":"object","required":["status"],"additionalProperties":false,
        "properties":{"status":{"enum":["ok"]}}})
}

fn native_text(text: &str) -> Value {
    json!({"id":"msg_test","model":"claude-test","stop_reason":"end_turn",
        "content":[{"type":"thinking","thinking":"kept private"},{"type":"text","text":text}],
        "usage":{"input_tokens":5,"cache_read_input_tokens":2,"cache_creation_input_tokens":1,"output_tokens":3}})
}

fn script(replies: impl IntoIterator<Item = (u16, Value)>) -> Reply {
    Reply::Script(Mutex::new(replies.into_iter().collect()))
}

async fn sent_json(fixture: &mut Fixture) -> Value {
    let seen = fixture.seen().await;
    assert_eq!(seen.uri, "/v1/messages");
    assert_eq!(seen.headers["x-api-key"], PROVIDER_KEY);
    serde_json::from_slice(&seen.body.await.unwrap()).unwrap()
}

#[tokio::test]
async fn managed_native_translates_and_repairs_with_original_schema() {
    let mut fixture = Fixture::start(
        script([
            (200, native_text(r#"{"status":"wrong"}"#)),
            (200, native_text(r#"{"status":"ok"}"#)),
        ]),
        None,
    )
    .await;
    let input = managed_body(closed_schema());
    let response = fixture
        .post("/v1/chat/completions")
        .bearer_auth(CALLER_KEY)
        .body(input.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-svincolo-structured-mode"], "native");
    assert_eq!(response.headers()["x-svincolo-attempts"], "2");
    assert_eq!(
        response.headers()["x-svincolo-dropped-params"],
        "temperature"
    );
    let result: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(
        result["choices"][0]["message"]["content"],
        r#"{"status":"ok"}"#
    );
    assert_eq!(result["usage"]["total_tokens"], 22);
    assert_eq!(result["usage"]["prompt_tokens_details"]["cached_tokens"], 4);
    let first = sent_json(&mut fixture).await;
    assert_eq!(first["model"], "claude-test");
    assert_eq!(first["max_tokens"], 1024);
    assert_eq!(first["system"], "Synthetic Quire prompt.");
    assert!(first.get("temperature").is_none());
    assert_eq!(first["output_config"]["format"]["schema"], closed_schema());
    let repair = sent_json(&mut fixture).await;
    assert_eq!(repair["model"], "claude-test");
    assert_eq!(repair["messages"][1]["content"], r#"{"status":"wrong"}"#);
    assert!(
        repair["messages"][2]["content"]
            .as_str()
            .unwrap()
            .contains("/properties/status/enum")
    );
    fixture.no_more_calls();
}

#[tokio::test]
async fn upstream_rate_limit_counts_managed_repair_as_a_separate_attempt() {
    let mut fixture = Fixture::start_with_rate(
        script([(200, native_text(r#"{"status":"wrong"}"#))]),
        None,
        "requests_per_minute = 1",
    )
    .await;
    let response = fixture
        .post("/v1/chat/completions")
        .bearer_auth(CALLER_KEY)
        .body(managed_body(closed_schema()).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["x-svincolo-attempts"], "1");
    assert_eq!(response.headers()["x-svincolo-provider-reached"], "true");
    assert_eq!(response.headers()["retry-after"], "60");
    let body: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "upstream_rate_limit");
    assert_eq!(sent_json(&mut fixture).await["model"], "claude-test");
    fixture.no_more_calls();
}

#[tokio::test]
async fn managed_free_form_map_uses_prompt_and_no_retry_returns_validation_failure() {
    let mut fixture = Fixture::start(
        script([(200, native_text(r#"{"themes":{"good":42}}"#))]),
        None,
    )
    .await;
    let schema = json!({"type":"object","required":["themes"],"additionalProperties":false,
        "properties":{"themes":{"type":"object","additionalProperties":{"type":"string"}}}});
    let response = fixture
        .post("/v1/chat/completions")
        .bearer_auth(CALLER_KEY)
        .header("x-svincolo-no-retry", "true")
        .body(managed_body(schema.clone()).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(response.headers()["x-svincolo-attempts"], "1");
    assert_eq!(response.headers()["x-svincolo-structured-mode"], "prompt");
    assert_eq!(response.headers()["x-svincolo-provider-reached"], "true");
    assert_eq!(response.headers()["x-should-retry"], "false");
    let failure: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(failure["error"]["code"], "invalid_output");
    assert_eq!(failure["error"]["raw_output"], r#"{"themes":{"good":42}}"#);
    assert_eq!(failure["error"]["details"][0]["path"], "/themes/good");
    let native = sent_json(&mut fixture).await;
    assert!(native.get("output_config").is_none());
    assert!(
        native["system"]
            .as_str()
            .unwrap()
            .contains(&schema.to_string())
    );
    fixture.no_more_calls();
}

#[tokio::test]
async fn managed_only_steps_down_on_format_specific_rejection() {
    let refusal = |message: &str| json!({"type":"error","error":{"type":"invalid_request_error","message":message}});
    let mut fixture = Fixture::start(
        script([
            (
                400,
                refusal("output_config.format schema cannot be compiled"),
            ),
            (200, native_text(r#"{"status":"ok"}"#)),
            (400, refusal("organization spend limit reached")),
        ]),
        None,
    )
    .await;
    for (expected, attempts) in [(StatusCode::OK, "2"), (StatusCode::BAD_REQUEST, "1")] {
        let response = fixture
            .post("/v1/chat/completions")
            .bearer_auth(CALLER_KEY)
            .body(managed_body(closed_schema()).to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        assert_eq!(response.headers()["x-svincolo-attempts"], attempts);
        assert_eq!(response.headers()["x-svincolo-origin"], "upstream");
        let first = sent_json(&mut fixture).await;
        assert_eq!(first["output_config"]["format"]["type"], "json_schema");
        if expected == StatusCode::OK {
            let second = sent_json(&mut fixture).await;
            assert!(second.get("output_config").is_none());
            assert_eq!(response.headers()["x-svincolo-structured-mode"], "prompt");
        } else {
            assert!(
                response
                    .text()
                    .await
                    .unwrap()
                    .contains("organization spend limit reached")
            );
        }
    }
    fixture.no_more_calls();
}

#[tokio::test]
async fn managed_rejects_external_schema_refs_and_tools_without_contacting_provider() {
    let mut fixture = Fixture::start(script([]), None).await;
    let mut tool_call = managed_body(closed_schema());
    tool_call["messages"][1]["tool_calls"] = json!([]);
    for (body, status, code) in [
        (
            managed_body(json!({"$ref":"https://example.invalid/schema.json"})),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "schema_invalid",
        ),
        (
            managed_body(json!({"$ref":"file:///etc/passwd"})),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "schema_invalid",
        ),
        (tool_call, StatusCode::NOT_FOUND, "capability_unavailable"),
    ] {
        let response = fixture
            .post("/v1/chat/completions")
            .bearer_auth(CALLER_KEY)
            .body(body.to_string())
            .send()
            .await
            .unwrap();
        assert_router_error(response, status, Some(code)).await;
    }
    fixture.no_more_calls();
}

fn compatible_answer(model: &str, text: &str) -> Value {
    json!({
        "id":"chatcmpl-test", "object":"chat.completion", "model":model, "created":42,
        "choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":text}}],
        "usage":{"prompt_tokens":5,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":2}}
    })
}

fn managed_chat(fixture: &Fixture, route: &str, schema: Value) -> reqwest::RequestBuilder {
    let mut body = managed_body(schema);
    body["model"] = json!(route);
    fixture
        .post("/v1/chat/completions")
        .bearer_auth(CALLER_KEY)
        .body(body.to_string())
}

async fn sent_chat(fixture: &mut Fixture, authorization: Option<&str>) -> Value {
    let seen = fixture.seen().await;
    assert_eq!(seen.uri, "/v1/chat/completions");
    assert_eq!(
        seen.headers
            .get("authorization")
            .map(|value| value.to_str().unwrap()),
        authorization
    );
    assert!(!seen.headers.contains_key("x-api-key"));
    serde_json::from_slice(&seen.body.await.unwrap()).unwrap()
}

async fn response_json(response: reqwest::Response) -> Value {
    serde_json::from_slice(&response.bytes().await.unwrap()).unwrap()
}

#[tokio::test]
async fn managed_catalog_pins_match_dispatch_on_each_wire_surface() {
    const EXPECT: &str = "x-svincolo-expect-route-revision";
    let mut fixture = Fixture::start(
        script([
            (200, native_text("hello")),
            (200, compatible_answer("synthetic-ollama:cloud", "hello")),
            (200, compatible_answer("synthetic-openai", "hello")),
            (200, native_text("hello")),
        ]),
        None,
    )
    .await;

    let catalog = |path| {
        fixture
            .client
            .get(format!("{}{path}", fixture.base_url))
            .bearer_auth(CALLER_KEY)
    };
    let chat = catalog("/v1/models").send().await.unwrap();
    let messages = catalog("/anthropic/v1/models?limit=1")
        .send()
        .await
        .unwrap();
    assert_eq!(chat.status(), StatusCode::OK);
    assert_eq!(messages.status(), StatusCode::OK);
    let chat = response_json(chat).await;
    let messages = response_json(messages).await;
    fixture.no_more_calls();

    let chat = chat["data"].as_array().unwrap();
    let messages = messages["data"].as_array().unwrap();
    assert_eq!(chat.len(), 3);
    assert_eq!(messages, &chat[..1]);
    assert_eq!(
        chat[0]["surfaces"],
        json!({"chat":"managed","messages":"raw"})
    );

    let auth = format!("Bearer {PROVIDER_KEY}");
    for (entry, alias, pin, authorization) in [
        (&chat[0], "claude-managed", "claude-test", None),
        (&chat[1], "ollama-managed", "synthetic-ollama:cloud", None),
        (
            &chat[2],
            "openai-managed",
            "synthetic-openai",
            Some(auth.as_str()),
        ),
    ] {
        assert_eq!(entry["id"], alias);
        assert_eq!(entry["upstream_model"], pin);
        assert_eq!(entry["surfaces"]["chat"], "managed");
        let revision = entry["route_revision"].as_str().unwrap();
        let response = fixture
            .post("/v1/chat/completions")
            .bearer_auth(CALLER_KEY)
            .header(EXPECT, revision)
            .body(json!({"model":alias,"messages":[{"role":"user","content":"hello"}]}).to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-svincolo-route-revision"], revision);
        let request = if alias == "claude-managed" {
            sent_json(&mut fixture).await
        } else {
            sent_chat(&mut fixture, authorization).await
        };
        assert_eq!(request["model"], pin);
    }

    let revision = messages[0]["route_revision"].as_str().unwrap();
    let response = fixture
        .post("/anthropic/v1/messages")
        .header("x-api-key", CALLER_KEY)
        .header(EXPECT, revision)
        .body(BODY.replace("claude-test", "claude-managed"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-svincolo-route-revision"], revision);
    assert_eq!(sent_json(&mut fixture).await["model"], "claude-test");
    fixture.no_more_calls();
}

#[tokio::test]
async fn managed_openai_makes_optional_fields_nullable_and_restores_the_original_shape() {
    let schema = json!({
        "type":"object", "additionalProperties":false, "required":["items","required_null"],
        "properties":{
            "items":{"type":"array","items":{"type":"object","additionalProperties":false,
                "required":["name"],
                "properties":{"name":{"type":"string"},"note":{"type":"string"},"nullable":{"type":["string","null"]}}
            }},
            "required_null":{"type":["string","null"]},
            "optional_number":{"type":"integer"}
        }
    });
    let native = r#"{"items":[{"name":"a","note":null,"nullable":null}],"required_null":null,"optional_number":null}"#;
    let mut fixture = Fixture::start(
        script([(200, compatible_answer("synthetic-openai", native))]),
        None,
    )
    .await;
    let response = managed_chat(&fixture, "openai-managed", schema)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-svincolo-structured-mode"], "native");
    assert_eq!(response.headers()["x-svincolo-attempts"], "1");
    let data = response_json(response).await;
    assert_eq!(data["created"], 42);
    assert_eq!(data["usage"]["prompt_tokens"], 5);
    assert_eq!(data["usage"]["prompt_tokens_details"]["cached_tokens"], 2);
    let output: Value =
        serde_json::from_str(data["choices"][0]["message"]["content"].as_str().unwrap()).unwrap();
    assert_eq!(
        output,
        json!({"items":[{"name":"a","nullable":null}],"required_null":null})
    );

    let auth = format!("Bearer {PROVIDER_KEY}");
    let request = sent_chat(&mut fixture, Some(&auth)).await;
    assert_eq!(request["model"], "synthetic-openai");
    assert_eq!(request["temperature"], 0.2);
    assert_eq!(request["max_completion_tokens"], 8192);
    let format = &request["response_format"]["json_schema"];
    assert_eq!(format["strict"], true);
    let schema = &format["schema"];
    assert_eq!(
        schema["required"],
        json!(["items", "optional_number", "required_null"])
    );
    assert_eq!(
        schema["properties"]["optional_number"]["anyOf"],
        json!([{"type":"integer"},{"type":"null"}])
    );
    let item = &schema["properties"]["items"]["items"];
    assert_eq!(item["required"], json!(["name", "note", "nullable"]));
    assert_eq!(
        item["properties"]["nullable"]["type"],
        json!(["string", "null"])
    );
    fixture.no_more_calls();
}

#[tokio::test]
async fn managed_openai_falls_back_only_on_a_format_rejection_and_honors_no_retry() {
    let format_error = json!({"error":{"type":"invalid_request_error","param":"response_format.json_schema","message":"format rejected"}});
    let unrelated =
        json!({"error":{"type":"invalid_request_error","param":"model","message":"unavailable"}});
    let mut fixture = Fixture::start(
        script([
            (400, format_error.clone()),
            (
                200,
                compatible_answer("synthetic-openai", r#"{"status":"ok"}"#),
            ),
            (400, format_error.clone()),
            (400, unrelated.clone()),
        ]),
        None,
    )
    .await;
    let auth = format!("Bearer {PROVIDER_KEY}");
    for (no_retry, status, attempts, expected_error) in [
        (false, StatusCode::OK, "2", None),
        (true, StatusCode::BAD_REQUEST, "1", Some(&format_error)),
        (false, StatusCode::BAD_REQUEST, "1", Some(&unrelated)),
    ] {
        let mut schema = closed_schema();
        schema["properties"]["status"]["type"] = json!("string");
        let response = managed_chat(&fixture, "openai-managed", schema)
            .header(
                "x-svincolo-no-retry",
                if no_retry { "true" } else { "false" },
            )
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["x-svincolo-attempts"], attempts);
        let first = sent_chat(&mut fixture, Some(&auth)).await;
        assert_eq!(first["response_format"]["type"], "json_schema");
        if let Some(expected) = expected_error {
            assert_eq!(&response_json(response).await, expected);
        } else {
            assert_eq!(response.headers()["x-svincolo-structured-mode"], "prompt");
            let second = sent_chat(&mut fixture, Some(&auth)).await;
            assert!(second.get("response_format").is_none());
            assert!(
                second["messages"][0]["content"]
                    .as_str()
                    .unwrap()
                    .contains("JSON Schema")
            );
        }
    }
    fixture.no_more_calls();
}

#[tokio::test]
async fn managed_ollama_cloud_validates_successful_responses_and_bounds_repair() {
    let invalid = compatible_answer("synthetic-ollama:cloud", r#"{"status":"wrong"}"#);
    let mut fixture = Fixture::start(
        script([
            (200, invalid.clone()),
            (200, invalid),
            (
                200,
                compatible_answer("synthetic-ollama:cloud", r#"{"status":"ok"}"#),
            ),
        ]),
        None,
    )
    .await;
    let response = managed_chat(&fixture, "ollama-managed", closed_schema())
        .header("x-svincolo-no-retry", "true")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(response.headers()["x-svincolo-structured-mode"], "prompt");
    assert_eq!(response.headers()["x-svincolo-attempts"], "1");
    assert_eq!(response.headers()["x-svincolo-provider-reached"], "true");
    assert_eq!(response.headers()["x-should-retry"], "false");
    let error = response_json(response).await;
    assert_eq!(error["error"]["code"], "invalid_output");
    assert_eq!(error["error"]["raw_output"], r#"{"status":"wrong"}"#);

    let response = managed_chat(&fixture, "ollama-managed", closed_schema())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-svincolo-attempts"], "2");
    let result = response_json(response).await;
    assert_eq!(
        result["choices"][0]["message"]["content"],
        r#"{"status":"ok"}"#
    );
    assert_eq!(result["usage"]["total_tokens"], 16);
    for expected_messages in [3, 3, 5] {
        let request = sent_chat(&mut fixture, None).await;
        assert_eq!(request["model"], "synthetic-ollama:cloud");
        assert_eq!(request["max_tokens"], 8192);
        assert!(request.get("response_format").is_none());
        assert_eq!(
            request["messages"].as_array().unwrap().len(),
            expected_messages
        );
    }
    fixture.no_more_calls();
}

#[tokio::test]
async fn managed_ollama_local_can_use_native_schema_without_openai_rewriting() {
    let schema = json!({"type":"object","properties":{"note":{"type":"string"}}});
    let mut fixture = Fixture::start(
        script([(
            200,
            compatible_answer("synthetic-ollama:local", r#"{"note":null}"#),
        )]),
        None,
    )
    .await;
    let response = managed_chat(&fixture, "ollama-native", schema.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(response.headers()["x-svincolo-structured-mode"], "native");
    assert_eq!(
        response_json(response).await["error"]["code"],
        "invalid_output"
    );
    let request = sent_chat(&mut fixture, None).await;
    assert_eq!(request["response_format"]["json_schema"]["schema"], schema);
    fixture.no_more_calls();
}

#[tokio::test]
async fn managed_openai_uses_prompt_for_references_and_preserves_provider_refusals() {
    let schema = json!({"type":"object", "required":["status"], "additionalProperties":false,
        "properties":{"status":{"$ref":"#/$defs/Status"}},
        "$defs":{"Status":{"type":"string","enum":["ok"]}}
    });
    let mut refusal = compatible_answer("synthetic-openai", "");
    refusal["choices"][0]["message"]["content"] = Value::Null;
    refusal["choices"][0]["message"]["refusal"] = json!("synthetic refusal");
    let mut fixture = Fixture::start(script([(200, refusal)]), None).await;
    let response = managed_chat(&fixture, "openai-managed", schema.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-svincolo-structured-mode"], "prompt");
    assert_eq!(response.headers()["x-svincolo-attempts"], "1");
    let data = response_json(response).await;
    assert_eq!(data["choices"][0]["finish_reason"], "content_filter");
    assert_eq!(
        data["choices"][0]["message"]["refusal"],
        "synthetic refusal"
    );
    assert!(data["choices"][0]["message"]["content"].is_null());
    let auth = format!("Bearer {PROVIDER_KEY}");
    let request = sent_chat(&mut fixture, Some(&auth)).await;
    assert!(request.get("response_format").is_none());
    assert!(
        request["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains(&schema.to_string())
    );
    fixture.no_more_calls();
}
