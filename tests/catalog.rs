use std::{
    fs,
    io::{BufRead, BufReader},
    net::Ipv4Addr,
    process::{Child, ChildStderr, Command, Stdio},
    time::Duration,
};

use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(5);
const KEY: &str = "test-catalog-caller";
const OTHER_KEY: &str = "test-other-caller";
const CLOUD_KEY: &str = "synthetic-catalog-cloud-key";
const NATIVE_KEY: &str = "synthetic-catalog-native-key";
const CHAT: &str = "/v1/chat/completions";
const MESSAGES: &str = "/anthropic/v1/messages";
const RESPONSES: &str = "/v1/responses";
const EMBEDDINGS: &str = "/v1/embeddings";
const TRANSCRIPTIONS: &str = "/v1/audio/transcriptions";
const AUDIO_TYPE: &str = "multipart/form-data; boundary=svincolo-fixture";
const TRANSCRIPT: &str = r#"{"text":"test audio"}"#;
const RESPONSE_EVENTS: &str =
    "event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n";
const EMBEDDING_DATA: &str = r#"{"object":"list","data":[{"index":0,"embedding":[0.5,-0.5]}]}"#;
const CHAT_MODELS: &str = "/v1/models";
const MESSAGE_MODELS: &str = "/anthropic/v1/models";
const MAX_BODY_BYTES: usize = 512;
const CONTENT_DIGEST: &str = "sha-256=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:";

struct Call {
    path: String,
    headers: HeaderMap,
    body: oneshot::Receiver<Bytes>,
}

async fn upstream(State(calls): State<mpsc::UnboundedSender<Call>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let path = parts.uri.path();
    let (body_tx, body_rx) = oneshot::channel();
    calls
        .send(Call {
            path: parts.uri.path().into(),
            headers: parts.headers,
            body: body_rx,
        })
        .expect("report upstream call");
    if let Ok(body) = to_bytes(body, 16 * 1024).await {
        let _ = body_tx.send(body);
    }
    if path.ends_with(RESPONSES) {
        ([("content-type", "text/event-stream")], RESPONSE_EVENTS).into_response()
    } else if path.ends_with(EMBEDDINGS) {
        ([("content-type", "application/json")], EMBEDDING_DATA).into_response()
    } else if path.ends_with(TRANSCRIPTIONS) {
        ([("content-type", "application/json")], TRANSCRIPT).into_response()
    } else {
        StatusCode::OK.into_response()
    }
}

struct Fixture {
    router: Child,
    stderr: Option<BufReader<ChildStderr>>,
    mock: JoinHandle<()>,
    calls: mpsc::UnboundedReceiver<Call>,
    client: reqwest::Client,
    base: String,
    upstream_address: String,
    _dir: tempfile::TempDir,
}

impl Fixture {
    async fn start(local_only: bool) -> Self {
        Self::start_with_catalog_config(local_only, "").await
    }

    async fn start_with_catalog_config(local_only: bool, catalog_config: &str) -> Self {
        Self::start_configured(local_only, catalog_config, |config| config).await
    }

    async fn start_configured(
        local_only: bool,
        catalog_config: &str,
        configure: impl FnOnce(String) -> String,
    ) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let upstream_address = listener.local_addr().unwrap().to_string();
        let (tx, calls) = mpsc::unbounded_channel();
        let app = Router::new().fallback(upstream).with_state(tx);
        let mock = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("caller.key"), KEY).unwrap();
        fs::write(dir.path().join("other.key"), OTHER_KEY).unwrap();
        fs::write(dir.path().join("cloud.key"), CLOUD_KEY).unwrap();
        fs::write(dir.path().join("native.key"), NATIVE_KEY).unwrap();
        let config = dir.path().join("svincolo.toml");
        fs::write(
            &config,
            configure(format!(
                r#"version = 1
[server]
listen = "127.0.0.1:0"
local_only = {local_only}
max_body_bytes = {MAX_BODY_BYTES}
{catalog_config}

[[upstream]]
name = "local-chat"
kind = "openai_compatible"
base_url = "http://{upstream_address}/local/v1"

[[upstream]]
name = "cloud-chat"
kind = "openai_compatible"
base_url = "http://{upstream_address}/cloud/v1"
allow_plain_http = true
key_file = "cloud.key"

[[upstream]]
name = "claude"
kind = "anthropic"
base_url = "http://{upstream_address}/native/v1"
allow_plain_http = true
key_file = "native.key"

[[route]]
name = "middle"
upstream = "cloud-chat"
locality = "cloud"
surfaces = {{ chat = "raw" }}
upstream_model = "middle-v2"
discover = true

[[route]]
name = "high"
upstream = "claude"
locality = "cloud"
surfaces = {{ messages = "raw" }}
upstream_model = "claude-pinned-v3"

[[route]]
name = "legacy"
upstream = "local-chat"
locality = "local"
surfaces = {{ chat = "raw" }}
discover = false

[[route]]
name = "low"
upstream = "local-chat"
locality = "local"
surfaces = {{ chat = "raw", responses = "raw", embeddings = "raw", transcriptions = "raw" }}
upstream_model = "local/pinned:v1"

[[route]]
name = "claude-pinned-v3"
upstream = "claude"
locality = "cloud"
surfaces = {{ messages = "raw" }}
discover = false

[[route]]
name = "restricted"
upstream = "local-chat"
locality = "local"
surfaces = {{ chat = "raw" }}

[[route]]
name = "selected"
upstream = "cloud-chat"
locality = "cloud"
surfaces = {{ responses = "raw" }}
discover = true

[[caller]]
name = "picker"
key_file = "caller.key"
routes = ["low", "middle", "high", "legacy", "selected"]

[[caller]]
name = "other"
key_file = "other.key"
routes = ["restricted", "claude-pinned-v3"]
"#
            )),
        )
        .unwrap();
        let mut router = Command::new(env!("CARGO_BIN_EXE_svincolo"))
            .args(["serve", "--config"])
            .arg(&config)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut fixture = Self {
            stderr: router.stderr.take().map(BufReader::new),
            router,
            mock,
            calls,
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(WAIT)
                .build()
                .unwrap(),
            base: String::new(),
            upstream_address,
            _dir: dir,
        };
        let line = fixture.read_router_log().await;
        let address = line
            .trim()
            .strip_prefix("svincolo: listening on ")
            .unwrap_or_else(|| panic!("router startup failed: {line}"));
        fixture.base = format!("http://{address}");
        fixture
    }

    async fn read_router_log(&mut self) -> String {
        let mut stderr = self.stderr.take().unwrap();
        let (line, stderr) = timeout(
            WAIT,
            tokio::task::spawn_blocking(move || {
                let mut line = String::new();
                stderr.read_line(&mut line).unwrap();
                (line, stderr)
            }),
        )
        .await
        .expect("router did not log its status")
        .unwrap();
        self.stderr = Some(stderr);
        line
    }

    fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.client.get(format!("{}{path}", self.base))
    }

    fn post(&self, path: &str, body: impl Into<reqwest::Body>) -> reqwest::RequestBuilder {
        self.post_as(KEY, path, body)
    }

    fn post_as(
        &self,
        key: &str,
        path: &str,
        body: impl Into<reqwest::Body>,
    ) -> reqwest::RequestBuilder {
        self.client
            .post(format!("{}{path}", self.base))
            .bearer_auth(key)
            .header("content-type", "application/json")
            .body(body)
    }

    async fn take_call(&mut self) -> Call {
        timeout(WAIT, self.calls.recv())
            .await
            .expect("mock did not receive a request")
            .unwrap()
    }

    async fn expect_no_calls(&mut self) {
        assert!(
            timeout(Duration::from_millis(75), self.calls.recv())
                .await
                .is_err(),
            "mock received an unexpected request"
        );
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.router.kill();
        let _ = self.router.wait();
        self.mock.abort();
    }
}

async fn catalog(request: reqwest::RequestBuilder, fixture: &Fixture) -> Value {
    let response = request.send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let body = response.text().await.unwrap();
    assert!(!body.contains(&fixture.upstream_address));
    assert!(!body.contains(CLOUD_KEY));
    assert!(!body.contains(NATIVE_KEY));
    let catalog: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(catalog["object"], "list");
    catalog
}

async fn models(request: reqwest::RequestBuilder, fixture: &Fixture) -> Vec<Value> {
    catalog(request, fixture).await["data"]
        .as_array()
        .unwrap()
        .clone()
}

fn ids(routes: &[Value]) -> Vec<&str> {
    routes
        .iter()
        .map(|route| route["id"].as_str().unwrap())
        .collect()
}

async fn expect_error(request: reqwest::RequestBuilder, status: StatusCode, code: &str) {
    let response = request.send().await.unwrap();
    assert_eq!(response.status(), status);
    assert_eq!(response.headers()["x-svincolo-provider-reached"], "false");
    let value: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(value["error"]["code"], code);
}

#[tokio::test]
async fn catalog_is_curated_by_surface_caller_and_locality() {
    let mut fixture = Fixture::start(false).await;
    for path in [CHAT_MODELS, MESSAGE_MODELS] {
        expect_error(
            fixture.get(path),
            StatusCode::UNAUTHORIZED,
            "caller_key_unknown",
        )
        .await;
    }

    let chat = models(fixture.get(CHAT_MODELS).bearer_auth(KEY), &fixture).await;
    assert_eq!(ids(&chat), ["low", "middle", "selected"]);
    assert_eq!(chat[0]["upstream_model"], "local/pinned:v1");
    assert_eq!(chat[0]["provider"], "local-chat");
    assert_eq!(chat[0]["locality"], "local");
    assert_eq!(
        chat[0]["surfaces"],
        json!({"chat": "raw", "responses": "raw", "embeddings": "raw", "transcriptions": "raw"})
    );
    assert_eq!(chat[0]["type"], "model");
    assert_eq!(chat[0]["display_name"], "low");
    assert_eq!(chat[0]["owned_by"], "local-chat");
    assert_eq!(chat[1]["locality"], "cloud");

    let native = models(
        fixture.get(MESSAGE_MODELS).header("x-api-key", KEY),
        &fixture,
    )
    .await;
    assert_eq!(ids(&native), ["high"]);
    assert_eq!(native[0]["provider"], "claude");
    assert_eq!(native[0]["upstream_model"], "claude-pinned-v3");
    assert_eq!(native[0]["surfaces"], json!({"messages": "raw"}));

    let other = models(fixture.get(CHAT_MODELS).bearer_auth(OTHER_KEY), &fixture).await;
    assert_eq!(ids(&other), ["restricted"]);
    assert_eq!(other[0]["upstream_model"], "restricted");
    for (path, expected) in [(CHAT_MODELS, &["low"][..]), (MESSAGE_MODELS, &[][..])] {
        let local = models(
            fixture
                .get(path)
                .bearer_auth(KEY)
                .header("x-svincolo-require-locality", "local"),
            &fixture,
        )
        .await;
        assert_eq!(ids(&local), expected);
    }
    fixture.expect_no_calls().await;
}

#[tokio::test]
async fn discovery_can_require_explicit_route_opt_in_without_revoking_access() {
    let mut fixture =
        Fixture::start_with_catalog_config(false, "discover_by_default = false").await;
    let routes = models(fixture.get(CHAT_MODELS).bearer_auth(KEY), &fixture).await;
    assert_eq!(ids(&routes), ["middle", "selected"]);
    assert_eq!(routes[1]["surfaces"], json!({"responses": "raw"}));

    for request in [
        fixture.get(CHAT_MODELS).bearer_auth(OTHER_KEY),
        fixture
            .get(CHAT_MODELS)
            .bearer_auth(KEY)
            .header("x-svincolo-require-locality", "local"),
        fixture.get(MESSAGE_MODELS).header("x-api-key", KEY),
    ] {
        assert!(models(request, &fixture).await.is_empty());
    }
    fixture.expect_no_calls().await;

    let selected = r#"{"model":"selected","input":"test"}"#;
    let response = fixture.post(RESPONSES, selected).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let call = fixture.take_call().await;
    assert_eq!(call.path, format!("/cloud{RESPONSES}"));
    assert_eq!(call.body.await.unwrap(), selected);

    let response = fixture
        .post(CHAT, r#"{"model":"low"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let call = fixture.take_call().await;
    assert_eq!(call.path, format!("/local{CHAT}"));
    assert_eq!(call.body.await.unwrap(), r#"{"model":"local/pinned:v1"}"#);
    fixture.expect_no_calls().await;
}

#[tokio::test]
async fn catalog_applies_curation_and_caller_permissions_before_its_limit() {
    let mut fixture = Fixture::start_configured(false, "discover_by_default = false", |config| {
        use std::fmt::Write;

        let mut config = config.replace(
            "routes = [\"restricted\", \"claude-pinned-v3\"]",
            "routes = [\"*\"]",
        );
        for index in (0..130).rev() {
            writeln!(
                config,
                r#"
[[route]]
name = "a-{index:03}"
upstream = "local-chat"
locality = "local"
surfaces = {{ chat = "raw" }}
discover = {}
"#,
                index != 0
            )
            .unwrap();
        }
        config
    })
    .await;
    let wildcard = catalog(fixture.get(CHAT_MODELS).bearer_auth(OTHER_KEY), &fixture).await;
    let expected: Vec<_> = (1..=128).map(|index| format!("a-{index:03}")).collect();
    assert_eq!(ids(wildcard["data"].as_array().unwrap()), expected);
    assert_eq!(wildcard["svincolo"]["partial"], true);

    let restricted = catalog(fixture.get(CHAT_MODELS).bearer_auth(KEY), &fixture).await;
    assert_eq!(
        ids(restricted["data"].as_array().unwrap()),
        ["middle", "selected"]
    );
    assert_eq!(restricted["svincolo"]["partial"], false);
    fixture.expect_no_calls().await;

    let revision = restricted["data"][0]["route_revision"].as_str().unwrap();
    let response = fixture
        .post(CHAT, r#"{"model":"middle"}"#)
        .header("x-svincolo-expect-route-revision", revision)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let call = fixture.take_call().await;
    assert_eq!(call.path, "/cloud/v1/chat/completions");
    assert_eq!(call.body.await.unwrap(), r#"{"model":"middle-v2"}"#);
}

#[cfg(unix)]
#[tokio::test]
async fn curated_tier_reload_updates_the_pin_and_caller_access_together() {
    const BODY: &str = r#"{"model":"middle"}"#;
    const EXPECT: &str = "x-svincolo-expect-route-revision";
    const REQUIRE_LOCAL: &str = "x-svincolo-require-locality";
    let mut fixture =
        Fixture::start_with_catalog_config(false, "discover_by_default = false").await;
    let original = models(fixture.get(CHAT_MODELS).bearer_auth(KEY), &fixture).await;
    assert_eq!(ids(&original), ["middle", "selected"]);
    assert_eq!(original[0]["upstream_model"], "middle-v2");
    let original_revision = original[0]["route_revision"].as_str().unwrap();
    assert!(
        models(fixture.get(CHAT_MODELS).bearer_auth(OTHER_KEY), &fixture)
            .await
            .is_empty()
    );
    expect_error(
        fixture.post_as(OTHER_KEY, CHAT, BODY),
        StatusCode::FORBIDDEN,
        "route_not_permitted",
    )
    .await;

    let path = fixture._dir.path().join("svincolo.toml");
    let updated = fs::read_to_string(&path)
        .unwrap()
        .replace(
            "name = \"middle\"\nupstream = \"cloud-chat\"\nlocality = \"cloud\"",
            "name = \"middle\"\nupstream = \"local-chat\"\nlocality = \"local\"",
        )
        .replace("middle-v2", "middle-v3")
        .replace(
            "routes = [\"restricted\", \"claude-pinned-v3\"]",
            "routes = [\"restricted\", \"claude-pinned-v3\", \"middle\"]",
        );
    let staged = path.with_extension("new");
    fs::write(&staged, updated).unwrap();
    fs::rename(staged, path).unwrap();
    assert!(
        Command::new("kill")
            .args(["-HUP", &fixture.router.id().to_string()])
            .status()
            .unwrap()
            .success()
    );

    assert_eq!(
        fixture.read_router_log().await.trim(),
        "svincolo: configuration reloaded"
    );
    let updated = models(fixture.get(CHAT_MODELS).bearer_auth(OTHER_KEY), &fixture).await;
    assert_eq!(ids(&updated), ["middle"]);
    assert_eq!(updated[0]["provider"], "local-chat");
    assert_eq!(updated[0]["locality"], "local");
    assert_eq!(updated[0]["upstream_model"], "middle-v3");
    let updated_revision = updated[0]["route_revision"].as_str().unwrap();
    assert_ne!(updated_revision, original_revision);
    let local = models(
        fixture
            .get(CHAT_MODELS)
            .bearer_auth(KEY)
            .header(REQUIRE_LOCAL, "local"),
        &fixture,
    )
    .await;
    assert_eq!(local, updated);
    expect_error(
        fixture.post(CHAT, BODY).header(EXPECT, original_revision),
        StatusCode::CONFLICT,
        "route_revision_mismatch",
    )
    .await;
    fixture.expect_no_calls().await;

    let response = fixture
        .post_as(OTHER_KEY, CHAT, BODY)
        .header(EXPECT, updated_revision)
        .header(REQUIRE_LOCAL, "local")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["x-svincolo-route-revision"],
        updated_revision
    );
    let call = fixture.take_call().await;
    assert_eq!(call.path, format!("/local{CHAT}"));
    assert_eq!(call.body.await.unwrap(), r#"{"model":"middle-v3"}"#);
    assert!(!call.headers.contains_key("authorization"));
    fixture.expect_no_calls().await;
}

fn audio_upload(parts: &[&[u8]]) -> Vec<u8> {
    let mut body = Vec::new();
    for part in parts {
        body.extend_from_slice(b"--svincolo-fixture\r\n");
        body.extend_from_slice(part);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(b"--svincolo-fixture--\r\n");
    body
}

const AUDIO: &[u8] = b"Content-Disposition: form-data; name=\"file\"; filename=\"audio\"\r\nContent-Type: audio/wav\r\n\r\nRIFF\xfflow";
const AUDIO_MODEL: &[u8] = b"Content-Disposition: form-data; name=\"model\"\r\n\r\nlow";
const PINNED_AUDIO_MODEL: &[u8] =
    b"Content-Disposition: form-data; name=\"model\"\r\n\r\nlocal/pinned:v1";

#[tokio::test]
async fn audio_alias_rewrites_only_model_and_preserves_binary_file_in_both_orders() {
    let mut fixture = Fixture::start(false).await;
    for (parts, expected) in [
        ([AUDIO, AUDIO_MODEL], [AUDIO, PINNED_AUDIO_MODEL]),
        ([AUDIO_MODEL, AUDIO], [PINNED_AUDIO_MODEL, AUDIO]),
    ] {
        let response = fixture
            .client
            .post(format!("{}{TRANSCRIPTIONS}", fixture.base))
            .bearer_auth(KEY)
            .header("content-type", AUDIO_TYPE)
            .body(audio_upload(&parts))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), TRANSCRIPT);
        let call = fixture.take_call().await;
        assert_eq!(call.path, format!("/local{TRANSCRIPTIONS}"));
        let expected = audio_upload(&expected);
        assert_eq!(call.headers["content-length"], expected.len().to_string());
        assert_eq!(call.headers["content-type"], AUDIO_TYPE);
        assert_eq!(call.body.await.unwrap(), expected);
    }
    fixture.expect_no_calls().await;
}

#[tokio::test]
async fn audio_router_refuses_ambiguous_or_incomplete_multipart_before_egress() {
    let mut fixture = Fixture::start(false).await;
    let malformed =
        b"Content-Disposition: form-data; name=\"other\"; NAME=\"model\"\r\n\r\nrestricted";
    let mut trailing = audio_upload(&[AUDIO, AUDIO_MODEL]);
    trailing.extend_from_slice(b"trailing");
    for (body, status, code) in [
        (
            audio_upload(&[AUDIO_MODEL, AUDIO, AUDIO_MODEL]),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "ambiguous_model",
        ),
        (
            audio_upload(&[AUDIO, malformed]),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "body_not_multipart",
        ),
        (
            trailing,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "body_not_multipart",
        ),
        (
            audio_upload(&[AUDIO]),
            StatusCode::NOT_FOUND,
            "route_not_found",
        ),
    ] {
        let request = fixture
            .client
            .post(format!("{}{TRANSCRIPTIONS}", fixture.base))
            .bearer_auth(KEY)
            .header("content-type", AUDIO_TYPE)
            .body(body);
        expect_error(request, status, code).await;
    }
    fixture.expect_no_calls().await;
}

#[tokio::test]
async fn responses_and_embeddings_preserve_wire_bytes_and_require_enabled_surfaces() {
    let mut fixture = Fixture::start(false).await;
    for (path, body, expected_body, expected_reply) in [
        (
            RESPONSES,
            r#"{"model":"low","input":"test","stream":true}"#,
            r#"{"model":"local/pinned:v1","input":"test","stream":true}"#,
            RESPONSE_EVENTS,
        ),
        (
            EMBEDDINGS,
            r#"{"model":"low","input":["test"],"dimensions":2}"#,
            r#"{"model":"local/pinned:v1","input":["test"],"dimensions":2}"#,
            EMBEDDING_DATA,
        ),
    ] {
        let response = fixture.post(path, body).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), expected_reply);
        let call = fixture.take_call().await;
        assert_eq!(call.path, format!("/local{path}"));
        assert_eq!(call.body.await.unwrap(), expected_body);
        expect_error(
            fixture.post(path, r#"{"model":"middle"}"#),
            StatusCode::NOT_FOUND,
            "capability_unavailable",
        )
        .await;
    }
    fixture.expect_no_calls().await;
}

#[tokio::test]
async fn pinned_tiers_forward_only_a_rewritten_top_level_value() {
    let mut fixture = Fixture::start(false).await;
    let chat = models(fixture.get(CHAT_MODELS).bearer_auth(KEY), &fixture).await;
    let native = models(fixture.get(MESSAGE_MODELS).bearer_auth(KEY), &fixture).await;
    fixture.expect_no_calls().await;

    let original = r#"{ "nested": {"model": "l\u006fw"}, "mo\u0064el" : "l\u006fw", "z":1.0 }"#;
    let rewritten =
        r#"{ "nested": {"model": "l\u006fw"}, "mo\u0064el" : "local/pinned:v1", "z":1.0 }"#;
    let cloud_auth = format!("Bearer {CLOUD_KEY}");
    for (path, upstream_path, body, expected, route, credential) in [
        (
            CHAT,
            "/local/v1/chat/completions",
            original,
            rewritten,
            &chat[0],
            None,
        ),
        (
            CHAT,
            "/cloud/v1/chat/completions",
            r#"{"model":"middle"}"#,
            r#"{"model":"middle-v2"}"#,
            &chat[1],
            Some(("authorization", cloud_auth.as_str())),
        ),
        (
            MESSAGES,
            "/native/v1/messages",
            r#"{"model":"high","max_tokens":100}"#,
            r#"{"model":"claude-pinned-v3","max_tokens":100}"#,
            &native[0],
            Some(("x-api-key", NATIVE_KEY)),
        ),
    ] {
        let response = fixture
            .post(path, body)
            .header("content-digest", CONTENT_DIGEST)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        for (header, field) in [
            ("x-svincolo-route", "id"),
            ("x-svincolo-requested-route", "id"),
            ("x-svincolo-upstream-model", "upstream_model"),
            ("x-svincolo-provider", "provider"),
            ("x-svincolo-route-revision", "route_revision"),
        ] {
            assert_eq!(response.headers()[header], route[field].as_str().unwrap());
        }
        let call = fixture.take_call().await;
        assert_eq!(call.path, upstream_path);
        assert_eq!(call.body.await.unwrap(), expected);
        assert_eq!(call.headers["content-length"], expected.len().to_string());
        assert!(!call.headers.contains_key("content-digest"));
        let credentials: Vec<_> = ["authorization", "x-api-key"]
            .into_iter()
            .flat_map(|name| {
                call.headers
                    .get_all(name)
                    .into_iter()
                    .map(move |value| (name, value.to_str().unwrap()))
            })
            .collect();
        assert_eq!(credentials.as_slice(), credential.as_slice());
    }
    let legacy = fixture
        .post(CHAT, r#"{"model":"legacy"}"#)
        .header("content-digest", CONTENT_DIGEST)
        .send()
        .await
        .unwrap();
    assert_eq!(legacy.status(), StatusCode::OK);
    let call = fixture.take_call().await;
    assert_eq!(call.body.await.unwrap(), r#"{"model":"legacy"}"#);
    assert_eq!(call.headers["content-digest"], CONTENT_DIGEST);
    fixture.expect_no_calls().await;
}

#[tokio::test]
async fn alias_and_concrete_model_permissions_are_independent() {
    let mut fixture = Fixture::start(false).await;
    let alias = r#"{"model":"high"}"#;
    let concrete = r#"{"model":"claude-pinned-v3"}"#;
    for (key, denied) in [(KEY, concrete), (OTHER_KEY, alias)] {
        expect_error(
            fixture.post_as(key, MESSAGES, denied),
            StatusCode::FORBIDDEN,
            "route_not_permitted",
        )
        .await;
    }

    for (key, body, route) in [
        (KEY, alias, "high"),
        (OTHER_KEY, concrete, "claude-pinned-v3"),
    ] {
        let response = fixture.post_as(key, MESSAGES, body).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-svincolo-route"], route);
        let call = fixture.take_call().await;
        assert_eq!(call.path, "/native/v1/messages");
        assert_eq!(call.body.await.unwrap(), concrete);
    }
    fixture.expect_no_calls().await;
}

#[tokio::test]
async fn expected_route_revision_is_checked_before_upstream_dispatch() {
    const EXPECT: &str = "x-svincolo-expect-route-revision";
    let mut fixture = Fixture::start(false).await;
    let routes = models(fixture.get(CHAT_MODELS).bearer_auth(KEY), &fixture).await;
    let current = routes[0]["route_revision"].as_str().unwrap();
    let different = routes[1]["route_revision"].as_str().unwrap();
    let body = r#"{"model":"low"}"#;

    let mismatch = fixture.post(CHAT, body).header(EXPECT, different);
    let response = mismatch.send().await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(response.headers()["x-svincolo-provider-reached"], "false");
    assert_eq!(response.headers()["x-svincolo-route-revision"], current);
    let error: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(error["error"]["code"], "route_revision_mismatch");
    for request in [
        fixture.post(CHAT, body).header(EXPECT, "invalid"),
        fixture
            .post(CHAT, body)
            .header(EXPECT, current)
            .header(EXPECT, current),
    ] {
        expect_error(request, StatusCode::BAD_REQUEST, "invalid_route_revision").await;
    }
    fixture.expect_no_calls().await;

    let response = fixture
        .post(CHAT, body)
        .header(EXPECT, current)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let call = fixture.take_call().await;
    assert!(!call.headers.contains_key(EXPECT));
    assert_eq!(call.body.await.unwrap(), r#"{"model":"local/pinned:v1"}"#);
}

#[tokio::test]
async fn denied_routes_and_ambiguous_model_never_reach_upstream() {
    let mut fixture = Fixture::start(true).await;
    let chat = models(fixture.get(CHAT_MODELS).bearer_auth(KEY), &fixture).await;
    assert_eq!(ids(&chat), ["low"]);
    let native = models(fixture.get(MESSAGE_MODELS).bearer_auth(KEY), &fixture).await;
    assert!(native.is_empty());

    for (path, body, status, code) in [
        (
            CHAT,
            r#"{"model":"restricted"}"#,
            StatusCode::FORBIDDEN,
            "route_not_permitted",
        ),
        (
            CHAT,
            r#"{"model":"middle-v2"}"#,
            StatusCode::NOT_FOUND,
            "route_not_found",
        ),
        (
            CHAT,
            r#"{"model":"high"}"#,
            StatusCode::NOT_FOUND,
            "capability_unavailable",
        ),
        (
            MESSAGES,
            r#"{"model":"high"}"#,
            StatusCode::FORBIDDEN,
            "local_only",
        ),
        (
            CHAT,
            r#"{"model":"low","mo\u0064el":"high"}"#,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "ambiguous_model",
        ),
    ] {
        expect_error(fixture.post(path, body), status, code).await;
    }
    let mut full_body = r#"{"model":"low"}"#.to_owned();
    full_body.push_str(&" ".repeat(MAX_BODY_BYTES - full_body.len()));
    expect_error(
        fixture.post(CHAT, full_body),
        StatusCode::PAYLOAD_TOO_LARGE,
        "body_too_large",
    )
    .await;
    fixture.expect_no_calls().await;
}
