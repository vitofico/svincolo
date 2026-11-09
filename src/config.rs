use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use reqwest::Url;
use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use subtle::{Choice, ConstantTimeEq};

const MIB: usize = 1024 * 1024;
const MAX_CONCURRENCY: usize = 5000;
const MAX_CONNECTIONS: usize = 16_384;
const CONNECTION_HEADROOM: usize = 256;
const MAX_NAME_BYTES: usize = 128;
const MAX_MODEL_ID_BYTES: usize = 256;

pub struct Config {
    pub listen: SocketAddr,
    pub local_only: bool,
    pub max_body_bytes: usize,
    pub max_buffered_bytes: usize,
    pub max_in_flight: usize,
    pub max_connections: usize,
    pub drain_delay_s: u64,
    pub grace_s: u64,
    pub routes: HashMap<String, Route>,
    pub callers: Vec<Caller>,
}

pub struct Route {
    pub upstream: Arc<Upstream>,
    pub surfaces: Vec<Surface>,
    pub locality: Locality,
    pub upstream_model: String,
    pub discover: bool,
    pub revision: String,
    pub managed: Option<ManagedSettings>,
}

#[derive(Debug)]
pub struct ManagedSettings {
    pub dialect: ManagedDialect,
    pub structured_modes: Vec<StructuredMode>,
    pub validation_retries: u8,
    pub default_max_tokens: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedDialect {
    Anthropic,
    Openai,
    Ollama,
}

impl ManagedDialect {
    pub const fn upstream_surface(self) -> Surface {
        match self {
            Self::Anthropic => Surface::Messages,
            Self::Openai | Self::Ollama => Surface::Chat,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StructuredMode {
    Native,
    Prompt,
}

impl StructuredMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Prompt => "prompt",
        }
    }
}

pub struct Upstream {
    pub name: String,
    kind: UpstreamKind,
    base_url: Url,
    pub anthropic_version: Option<HeaderValue>,
    pub provider: String,
    pub credential: Option<(HeaderName, HeaderValue)>,
    pub max_concurrent: Option<usize>,
    pub requests_per_minute: Option<NonZeroU32>,
    pub queue_limit: usize,
    pub max_wait_ms: u64,
}

impl Upstream {
    pub fn endpoint(&self, surface: Surface) -> Url {
        let mut url = self.base_url.clone();
        let segments: &[&str] = match surface {
            Surface::Chat => &["chat", "completions"],
            Surface::Messages => &["messages"],
            Surface::Responses => &["responses"],
            Surface::Embeddings => &["embeddings"],
            Surface::Transcriptions => &["audio", "transcriptions"],
        };
        url.path_segments_mut()
            .expect("validated HTTP base URL")
            .pop_if_empty()
            .extend(segments.iter().copied());
        url
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Surface {
    Chat,
    Messages,
    Responses,
    Embeddings,
    Transcriptions,
}

impl Surface {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Messages => "messages",
            Self::Responses => "responses",
            Self::Embeddings => "embeddings",
            Self::Transcriptions => "transcriptions",
        }
    }

    fn compatible_with(self, kind: UpstreamKind) -> bool {
        (self == Self::Messages) == matches!(kind, UpstreamKind::Anthropic)
    }
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Locality {
    Local,
    Cloud,
}

impl Locality {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Cloud => "cloud",
        }
    }
}

pub struct Caller {
    pub name: String,
    pub max_concurrent: Option<usize>,
    pub requests_per_minute: Option<NonZeroU32>,
    pub over_limit: OverLimit,
    pub priority: Priority,
    token_hashes: Vec<[u8; 32]>,
    routes: HashSet<String>,
}

#[derive(Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OverLimit {
    #[default]
    Refuse,
    Queue,
}

#[derive(Clone, Copy, Default, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    #[default]
    Interactive,
    Background,
}

impl Caller {
    pub fn permits(&self, model: &str) -> bool {
        self.routes.contains("*") || self.routes.contains(model)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    version: u32,
    #[serde(default)]
    server: ServerConfig,
    #[serde(default)]
    upstream: Vec<UpstreamConfig>,
    #[serde(default)]
    route: Vec<RouteConfig>,
    #[serde(default)]
    caller: Vec<CallerConfig>,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ServerConfig {
    listen: SocketAddr,
    local_only: bool,
    discover_by_default: bool,
    max_body_bytes: usize,
    max_buffered_bytes: usize,
    max_in_flight: usize,
    max_connections: Option<usize>,
    drain_delay_s: u64,
    grace_s: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 8080)),
            local_only: false,
            discover_by_default: true,
            max_body_bytes: 16 * MIB,
            max_buffered_bytes: 128 * MIB,
            max_in_flight: 64,
            max_connections: None,
            drain_delay_s: 5,
            grace_s: 300,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpstreamConfig {
    name: String,
    kind: UpstreamKind,
    provider: Option<String>,
    base_url: String,
    auth: Option<Auth>,
    anthropic_version: Option<String>,
    key_file: Option<PathBuf>,
    key_env: Option<String>,
    max_concurrent: Option<usize>,
    requests_per_minute: Option<NonZeroU32>,
    queue_limit: Option<usize>,
    max_wait_ms: Option<u64>,
    #[serde(default)]
    allow_plain_http: bool,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UpstreamKind {
    Openai,
    OpenaiCompatible,
    Ollama,
    Anthropic,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Auth {
    Bearer,
    XApiKey,
    None,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteConfig {
    name: String,
    upstream: String,
    locality: Locality,
    surfaces: Surfaces,
    upstream_model: Option<String>,
    discover: Option<bool>,
    structured_modes: Option<Vec<StructuredMode>>,
    validation_retries: Option<u8>,
    default_max_tokens: Option<u32>,
}

impl RouteConfig {
    fn managed_settings(
        &self,
        kind: UpstreamKind,
        location: &str,
    ) -> Result<Option<ManagedSettings>, String> {
        if !matches!(self.surfaces.chat, Some(ChatMode::Managed)) {
            if self.structured_modes.is_some()
                || self.validation_retries.is_some()
                || self.default_max_tokens.is_some()
            {
                return Err(format!(
                    "{location} managed settings require chat = 'managed'"
                ));
            }
            return Ok(None);
        }
        let dialect = match kind {
            UpstreamKind::Anthropic => ManagedDialect::Anthropic,
            UpstreamKind::Openai => ManagedDialect::Openai,
            UpstreamKind::Ollama => ManagedDialect::Ollama,
            UpstreamKind::OpenaiCompatible => {
                return Err(format!(
                    "{location}.surfaces managed chat requires an Anthropic, OpenAI or Ollama upstream"
                ));
            }
        };
        let structured_modes = self
            .structured_modes
            .clone()
            .unwrap_or_else(|| match dialect {
                ManagedDialect::Ollama => vec![StructuredMode::Prompt],
                _ => vec![StructuredMode::Native, StructuredMode::Prompt],
            });
        if structured_modes.is_empty()
            || structured_modes.len() > 2
            || (structured_modes.len() == 2 && structured_modes[0] == structured_modes[1])
        {
            return Err(format!(
                "{location}.structured_modes must contain distinct native or prompt modes"
            ));
        }
        if dialect == ManagedDialect::Ollama
            && self.locality == Locality::Cloud
            && structured_modes.contains(&StructuredMode::Native)
        {
            return Err(format!(
                "{location}.structured_modes for Ollama cloud must use prompt mode"
            ));
        }
        let validation_retries = self.validation_retries.unwrap_or(0);
        if validation_retries > 1 {
            return Err(format!("{location}.validation_retries must be 0 or 1"));
        }
        let default_max_tokens = self.default_max_tokens.unwrap_or(8192);
        if !(1..=32768).contains(&default_max_tokens) {
            return Err(format!(
                "{location}.default_max_tokens must be between 1 and 32768"
            ));
        }
        Ok(Some(ManagedSettings {
            dialect,
            structured_modes,
            validation_retries,
            default_max_tokens,
        }))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Surfaces {
    chat: Option<ChatMode>,
    messages: Option<RawMode>,
    responses: Option<RawMode>,
    embeddings: Option<RawMode>,
    transcriptions: Option<RawMode>,
}

impl Surfaces {
    fn enabled(&self) -> Vec<Surface> {
        [
            (Surface::Chat, self.chat.is_some()),
            (Surface::Messages, self.messages.is_some()),
            (Surface::Responses, self.responses.is_some()),
            (Surface::Embeddings, self.embeddings.is_some()),
            (Surface::Transcriptions, self.transcriptions.is_some()),
        ]
        .into_iter()
        .filter_map(|(surface, enabled)| enabled.then_some(surface))
        .collect()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum ChatMode {
    Raw,
    Managed,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum RawMode {
    Raw,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CallerConfig {
    name: String,
    key_file: PathBuf,
    routes: Vec<String>,
    max_concurrent: Option<usize>,
    requests_per_minute: Option<NonZeroU32>,
    #[serde(default)]
    over_limit: OverLimit,
    #[serde(default)]
    priority: Priority,
}

impl Config {
    #[cfg(test)]
    pub fn load(path: &Path) -> Result<Self, String> {
        Self::load_snapshot(path).map(|(config, _)| config)
    }

    pub fn load_snapshot(path: &Path) -> Result<(Self, FileInputs), String> {
        let directory = path.parent().unwrap_or_else(|| Path::new(""));
        let mut inputs = FileInputs {
            generation: directory.join("..data"),
            ..FileInputs::default()
        };
        inputs.target = std::fs::read_link(&inputs.generation).ok();
        let text = inputs.read(path).map_err(|_| "could not read config")?;
        let config = Self::compile(&text, directory, &mut inputs)?;
        if inputs.changed() {
            return Err("configuration files changed while loading".into());
        }
        Ok((config, inputs))
    }

    #[cfg(test)]
    fn parse(text: &str, directory: &Path) -> Result<Self, String> {
        Self::compile(text, directory, &mut FileInputs::default())
    }

    fn compile(text: &str, directory: &Path, inputs: &mut FileInputs) -> Result<Self, String> {
        let input: FileConfig = toml::from_str(text).map_err(|error| {
            // Parser diagnostics can quote the contents of a misconfigured secret field.
            error.span().map_or_else(
                || "invalid or unsupported TOML config".to_owned(),
                |span| {
                    let line = text.as_bytes()[..span.start.min(text.len())]
                        .iter()
                        .filter(|&&byte| byte == b'\n')
                        .count()
                        + 1;
                    format!("invalid or unsupported TOML config near line {line}")
                },
            )
        })?;
        let server = input.server;
        if input.version != 1 {
            return Err("version must be 1".into());
        }
        if !(1..=32 * MIB).contains(&server.max_body_bytes) {
            return Err("server.max_body_bytes must be between 1 and 33554432".into());
        }
        if !(server.max_body_bytes..=256 * MIB).contains(&server.max_buffered_bytes) {
            return Err(
                "server.max_buffered_bytes must be at least max_body_bytes and at most 268435456"
                    .into(),
            );
        }
        if !(1..=MAX_CONCURRENCY).contains(&server.max_in_flight) {
            return Err(format!(
                "server.max_in_flight must be between 1 and {MAX_CONCURRENCY}"
            ));
        }
        let max_connections = server
            .max_connections
            .unwrap_or(server.max_in_flight + CONNECTION_HEADROOM);
        if !(server.max_in_flight + 1..=MAX_CONNECTIONS).contains(&max_connections) {
            return Err(format!(
                "server.max_connections must exceed max_in_flight and be at most {MAX_CONNECTIONS}"
            ));
        }
        if server.drain_delay_s > 60 {
            return Err("server.drain_delay_s must be between 0 and 60".into());
        }
        if !(1..=3600).contains(&server.grace_s) {
            return Err("server.grace_s must be between 1 and 3600".into());
        }
        if input.caller.is_empty() && !server.listen.ip().is_loopback() {
            return Err("server.listen must be loopback when no callers are configured".into());
        }

        let mut upstreams = HashMap::new();
        for (index, upstream) in input.upstream.into_iter().enumerate() {
            let location = format!("upstream[{}]", index + 1);
            if !valid_name(&upstream.name) || upstreams.contains_key(&upstream.name) {
                return Err(format!("{location}.name must be a unique identifier"));
            }
            let name = upstream.name.clone();
            let allow_plain_http = upstream.allow_plain_http;
            let compiled = Arc::new(upstream.compile(directory, &location, inputs)?);
            upstreams.insert(name, (compiled, allow_plain_http));
        }

        let mut routes = HashMap::new();
        for (index, route) in input.route.into_iter().enumerate() {
            let location = format!("route[{}]", index + 1);
            if !valid_name(&route.name) || routes.contains_key(&route.name) {
                return Err(format!("{location}.name must be a unique exact model name"));
            }
            let Some((upstream, allow_plain_http)) = upstreams.get(&route.upstream) else {
                return Err(format!(
                    "{location}.upstream must name a configured upstream"
                ));
            };
            let surfaces = route.surfaces.enabled();
            let managed = route.managed_settings(upstream.kind, &location)?;
            if surfaces.is_empty()
                || surfaces.iter().any(|surface| {
                    !surface.compatible_with(upstream.kind)
                        && !(*surface == Surface::Chat && managed.is_some())
                })
            {
                return Err(format!("{location}.surfaces must match the upstream's API"));
            }
            let upstream_model = route.upstream_model.as_deref().unwrap_or(&route.name);
            if !valid_model_id(upstream_model) {
                return Err(format!(
                    "{location}.upstream_model must be an exact model ID of at most {MAX_MODEL_ID_BYTES} bytes"
                ));
            }
            match route.locality {
                Locality::Local
                    if has_cloud_suffix(&route.name) || has_cloud_suffix(upstream_model) =>
                {
                    return Err(format!(
                        "{location}.locality cannot be local for a cloud model"
                    ));
                }
                Locality::Cloud if upstream.base_url.scheme() == "http" && !allow_plain_http => {
                    return Err(format!(
                        "{location}.upstream requires HTTPS or allow_plain_http"
                    ));
                }
                _ => {}
            }
            let revision = route_revision(
                &route,
                upstream,
                upstream_model,
                &surfaces,
                managed.as_ref().map(|settings| settings.dialect),
            );
            let upstream_model = route.upstream_model.unwrap_or_else(|| route.name.clone());
            routes.insert(
                route.name,
                Route {
                    upstream: Arc::clone(upstream),
                    surfaces,
                    locality: route.locality,
                    upstream_model,
                    discover: route.discover.unwrap_or(server.discover_by_default),
                    revision,
                    managed,
                },
            );
        }

        let mut caller_names = HashSet::new();
        let mut all_hashes = HashSet::new();
        let mut callers = Vec::new();
        for (index, caller) in input.caller.into_iter().enumerate() {
            let location = format!("caller[{}]", index + 1);
            if !valid_name(&caller.name) || !caller_names.insert(caller.name.clone()) {
                return Err(format!("{location}.name must be a unique identifier"));
            }
            let permitted: HashSet<_> = caller.routes.into_iter().collect();
            if permitted.is_empty()
                || (permitted.contains("*") && permitted.len() != 1)
                || (!permitted.contains("*") && permitted.iter().any(|r| !routes.contains_key(r)))
            {
                return Err(format!(
                    "{location}.routes must list configured routes or just '*'"
                ));
            }
            let tokens = inputs
                .read(&directory.join(caller.key_file))
                .map_err(|_| format!("could not read {location}.key_file"))?;
            let mut token_hashes = Vec::new();
            for token in tokens
                .lines()
                .map(str::trim_ascii)
                .filter(|s| !s.is_empty())
            {
                if !valid_token(token) {
                    return Err(format!("{location}.key_file contains an invalid token"));
                }
                let hash = hash_token(token.as_bytes());
                if !all_hashes.insert(hash) {
                    return Err(format!("{location}.key_file contains a duplicate token"));
                }
                token_hashes.push(hash);
            }
            if token_hashes.is_empty() {
                return Err(format!(
                    "{location}.key_file must contain at least one token"
                ));
            }
            callers.push(Caller {
                name: caller.name,
                max_concurrent: validate_concurrency(caller.max_concurrent, &location)?,
                requests_per_minute: caller.requests_per_minute,
                over_limit: caller.over_limit,
                priority: caller.priority,
                token_hashes,
                routes: permitted,
            });
        }

        Ok(Self {
            listen: server.listen,
            local_only: server.local_only,
            max_body_bytes: server.max_body_bytes,
            max_buffered_bytes: server.max_buffered_bytes,
            max_in_flight: server.max_in_flight,
            max_connections,
            drain_delay_s: server.drain_delay_s,
            grace_s: server.grace_s,
            routes,
            callers,
        })
    }

    pub fn authenticate(&self, token: &[u8]) -> Option<&Caller> {
        let digest = hash_token(token);
        let mut caller = None;
        for entry in &self.callers {
            let matched = entry
                .token_hashes
                .iter()
                .fold(Choice::from(0), |matched, hash| {
                    matched | hash.ct_eq(&digest)
                });
            if bool::from(matched) {
                caller = Some(entry);
            }
        }
        caller
    }
}

#[derive(Default)]
pub struct FileInputs {
    generation: PathBuf,
    target: Option<PathBuf>,
    digests: HashMap<PathBuf, [u8; 32]>,
    inconsistent: bool,
}

impl FileInputs {
    fn read(&mut self, path: &Path) -> std::io::Result<String> {
        let text = std::fs::read_to_string(path)?;
        let digest = Sha256::digest(text.as_bytes()).into();
        if let Some(previous) = self.digests.insert(path.to_owned(), digest) {
            self.inconsistent |= previous != digest;
        }
        Ok(text)
    }

    pub fn changed(&self) -> bool {
        self.inconsistent
            || std::fs::read_link(&self.generation).ok() != self.target
            || self.digests.iter().any(|(path, digest)| {
                std::fs::read(path)
                    .map(|bytes| Sha256::digest(bytes).as_slice() != digest)
                    .unwrap_or(true)
            })
            || std::fs::read_link(&self.generation).ok() != self.target
    }
}

impl UpstreamConfig {
    fn compile(
        self,
        directory: &Path,
        location: &str,
        inputs: &mut FileInputs,
    ) -> Result<Upstream, String> {
        let invalid_url = || {
            format!(
                "{location}.base_url must be an HTTP(S) base URL without credentials, a query or fragment"
            )
        };
        let authority = self
            .base_url
            .split_once("://")
            .map(|(_, rest)| rest.split('/').next().unwrap_or_default())
            .filter(|authority| !authority.is_empty() && !authority.contains('@'));
        if authority.is_none()
            || self.base_url.contains('\\')
            || self
                .base_url
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            return Err(invalid_url());
        }
        let url = Url::parse(&self.base_url).map_err(|_| invalid_url())?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.has_host()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid_url());
        }
        if matches!(self.kind, UpstreamKind::Ollama)
            && !url.path().trim_end_matches('/').ends_with("/v1")
        {
            return Err(format!("{location}.base_url must end in /v1 for ollama"));
        }
        let anthropic_version = if matches!(self.kind, UpstreamKind::Anthropic) {
            let version = self.anthropic_version.as_deref().unwrap_or("2026-10-21");
            if !valid_anthropic_version(version.as_bytes()) {
                return Err(format!("{location}.anthropic_version must be YYYY-MM-DD"));
            }
            Some(HeaderValue::from_str(version).expect("validated Anthropic version"))
        } else {
            if self.anthropic_version.is_some() {
                return Err(format!(
                    "{location}.anthropic_version is only supported for Anthropic upstreams"
                ));
            }
            None
        };

        let has_key = self.key_file.is_some() || self.key_env.is_some();
        let auth = self.auth.unwrap_or(match self.kind {
            UpstreamKind::Openai => Auth::Bearer,
            UpstreamKind::Anthropic => Auth::XApiKey,
            UpstreamKind::OpenaiCompatible if has_key => Auth::Bearer,
            UpstreamKind::OpenaiCompatible | UpstreamKind::Ollama => Auth::None,
        });
        let credential = match (auth, self.key_file, self.key_env) {
            (_, Some(_), Some(_)) => {
                return Err(format!(
                    "{location} must set at most one of key_file and key_env"
                ));
            }
            (Auth::None, None, None) => None,
            (Auth::None, _, _) => {
                return Err(format!("{location} cannot supply a key with auth = 'none'"));
            }
            (auth, file, env) => {
                let key = if let Some(file) = file {
                    inputs
                        .read(&directory.join(file))
                        .map_err(|_| format!("could not read {location}.key_file"))?
                } else if let Some(env) = env {
                    if env.is_empty() || env.contains(['=', '\0']) {
                        return Err(format!(
                            "{location}.key_env must name an environment variable"
                        ));
                    }
                    std::env::var(env)
                        .map_err(|_| format!("{location}.key_env is unset or unreadable"))?
                } else {
                    return Err(format!("{location}.auth requires key_file or key_env"));
                };
                let token = key.trim_ascii();
                if !valid_token(token) {
                    return Err(format!(
                        "{location} provider key must be a single nonempty token"
                    ));
                }
                let (name, value) = match auth {
                    Auth::Bearer => (
                        AUTHORIZATION,
                        HeaderValue::from_str(&format!("Bearer {token}")),
                    ),
                    Auth::XApiKey => (
                        HeaderName::from_static("x-api-key"),
                        HeaderValue::from_str(token),
                    ),
                    Auth::None => unreachable!("keyless authentication is handled above"),
                };
                let mut value =
                    value.map_err(|_| format!("{location} has an invalid provider key"))?;
                value.set_sensitive(true);
                Some((name, value))
            }
        };

        let provider = self.provider.unwrap_or_else(|| self.name.clone());
        if provider.is_empty()
            || provider.len() > MAX_NAME_BYTES
            || provider.trim_ascii() != provider
            || !provider.bytes().all(|b| b.is_ascii_graphic() || b == b' ')
        {
            return Err(format!(
                "{location}.provider must be a short printable ASCII label"
            ));
        }
        let queue_limit = self.queue_limit.unwrap_or(0);
        if queue_limit > 1000 {
            return Err(format!("{location}.queue_limit must be between 0 and 1000"));
        }
        let max_wait_ms = self.max_wait_ms.unwrap_or(10_000);
        if max_wait_ms > 30_000 {
            return Err(format!(
                "{location}.max_wait_ms must be between 0 and 30000"
            ));
        }
        Ok(Upstream {
            name: self.name,
            kind: self.kind,
            base_url: url,
            anthropic_version,
            provider,
            credential,
            max_concurrent: validate_concurrency(self.max_concurrent, location)?,
            requests_per_minute: self.requests_per_minute,
            queue_limit,
            max_wait_ms,
        })
    }
}

fn validate_concurrency(
    max_concurrent: Option<usize>,
    location: &str,
) -> Result<Option<usize>, String> {
    match max_concurrent {
        Some(limit) if (1..=MAX_CONCURRENCY).contains(&limit) => Ok(Some(limit)),
        Some(_) => Err(format!(
            "{location}.max_concurrent must be between 1 and {MAX_CONCURRENCY}"
        )),
        None => Ok(None),
    }
}

pub fn valid_anthropic_version(value: &[u8]) -> bool {
    value.len() == 10
        && value.iter().enumerate().all(|(index, byte)| match index {
            4 | 7 => *byte == b'-',
            _ => byte.is_ascii_digit(),
        })
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_BYTES
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-:".contains(&byte))
}

fn valid_model_id(model: &str) -> bool {
    !model.is_empty()
        && model.len() <= MAX_MODEL_ID_BYTES
        && model
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-:/".contains(&byte))
}

fn route_revision(
    route: &RouteConfig,
    upstream: &Upstream,
    upstream_model: &str,
    surfaces: &[Surface],
    managed: Option<ManagedDialect>,
) -> String {
    let kind = match upstream.kind {
        UpstreamKind::Openai => "openai",
        UpstreamKind::OpenaiCompatible => "openai_compatible",
        UpstreamKind::Ollama => "ollama",
        UpstreamKind::Anthropic => "anthropic",
    };
    let mut hash = Sha256::new();
    hash.update(b"svincolo-route-v1");
    for field in [
        route.name.as_bytes(),
        route.upstream.as_bytes(),
        upstream.provider.as_bytes(),
        kind.as_bytes(),
        upstream_model.as_bytes(),
        route.locality.as_str().as_bytes(),
        upstream
            .anthropic_version
            .as_ref()
            .map_or(&[][..], HeaderValue::as_bytes),
    ] {
        hash.update((field.len() as u64).to_be_bytes());
        hash.update(field);
    }
    for &surface in surfaces {
        let managed_chat = managed.filter(|_| surface == Surface::Chat);
        let upstream_surface = managed_chat.map_or(surface, ManagedDialect::upstream_surface);
        // Translation already changes the fingerprint. Same-endpoint managed
        // Chat still needs a distinct revision from raw Chat for discovery.
        if managed_chat.is_some() && upstream_surface == surface {
            let mode = b"managed";
            hash.update((mode.len() as u64).to_be_bytes());
            hash.update(mode);
        }
        let endpoint = upstream.endpoint(upstream_surface);
        for field in [surface.as_str(), endpoint.as_str()] {
            hash.update((field.len() as u64).to_be_bytes());
            hash.update(field.as_bytes());
        }
    }
    format_revision(hash.finalize().into())
}

pub(crate) fn format_revision(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut revision = String::with_capacity(64);
    for byte in digest {
        revision.push(char::from(HEX[usize::from(byte >> 4)]));
        revision.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    revision
}

fn has_cloud_suffix(name: &str) -> bool {
    name.as_bytes().last_chunk::<6>().is_some_and(|suffix| {
        suffix.eq_ignore_ascii_case(b":cloud") || suffix.eq_ignore_ascii_case(b"-cloud")
    })
}

fn valid_token(token: &str) -> bool {
    !token.is_empty() && token.bytes().all(|byte| byte.is_ascii_graphic())
}

fn hash_token(token: &[u8]) -> [u8; 32] {
    Sha256::digest(token).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROUTE: &str = r#"
version = 1

[[upstream]]
name = "engine"
kind = "openai_compatible"
base_url = "http://127.0.0.1:9000/custom/v1/"

[[route]]
name = "test:1"
upstream = "engine"
locality = "local"
surfaces = { chat = "raw" }
"#;

    fn invalid(text: &str, directory: &Path) -> String {
        Config::parse(text, directory)
            .err()
            .expect("config should be rejected")
    }

    #[test]
    fn empty_router_defaults_to_loopback_and_safe_limits() {
        let config = Config::parse("version = 1", Path::new("")).unwrap();
        assert_eq!(config.listen, SocketAddr::from(([127, 0, 0, 1], 8080)));
        assert!(!config.local_only);
        assert_eq!(config.max_body_bytes, 16 * MIB);
        assert_eq!(config.max_buffered_bytes, 128 * MIB);
        assert_eq!(config.max_in_flight, 64);
        assert_eq!(config.max_connections, 320);
        assert_eq!(config.drain_delay_s, 5);
        assert_eq!(config.grace_s, 300);
        assert!(config.routes.is_empty());
        assert!(config.callers.is_empty());
    }

    #[test]
    fn startup_fails_closed_on_invalid_schema_listen_and_limits() {
        let directory = Path::new("");
        for text in [
            "",
            "version = 2",
            "version = 1\n[server]\nlisten = '0.0.0.0:8080'",
            "version = 1\n[server]\nmax_body_bytes = 0",
            "version = 1\n[server]\nmax_buffered_bytes = 16777215",
        ] {
            invalid(text, directory);
        }
        let error = invalid(
            "version = 1\n[server]\ninline_key = 'secret-must-not-appear'",
            directory,
        );
        assert!(error.contains("line 3"));
        assert!(!error.contains("secret-must-not-appear"));
    }

    #[test]
    fn validates_transport_limit_overrides() {
        let directory = Path::new("");
        let config = Config::parse(
            "version = 1\n[server]\nmax_in_flight = 5000\ndrain_delay_s = 0\ngrace_s = 3600",
            directory,
        )
        .unwrap();
        assert_eq!(config.max_in_flight, 5000);
        assert_eq!(config.max_connections, 5256);
        assert_eq!(config.drain_delay_s, 0);
        assert_eq!(config.grace_s, 3600);
        for (key, value) in [
            ("max_in_flight", 0),
            ("max_in_flight", 5001),
            ("max_connections", 64),
            ("max_connections", MAX_CONNECTIONS + 1),
            ("drain_delay_s", 61),
            ("grace_s", 0),
            ("grace_s", 3601),
        ] {
            let text = format!("version = 1\n[server]\n{key} = {value}");
            assert!(invalid(&text, directory).contains(key));
        }
    }

    #[test]
    fn validates_caller_and_upstream_concurrency_limits() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("caller.key"), "synthetic-token").unwrap();
        let caller = "\n[[caller]]\nname = 'app'\nkey_file = 'caller.key'\nroutes = ['test:1']\n";
        for limit in [0, MAX_CONCURRENCY + 1] {
            let upstream =
                ROUTE.replace("[[route]]", &format!("max_concurrent = {limit}\n[[route]]"));
            assert!(invalid(&upstream, directory.path()).contains("upstream[1].max_concurrent"));
            let caller = format!("{ROUTE}{caller}max_concurrent = {limit}");
            assert!(invalid(&caller, directory.path()).contains("caller[1].max_concurrent"));
        }
    }

    #[test]
    fn compiles_route_and_reads_relative_credentials_and_rotating_tokens() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("provider"), "provider-secret\n").unwrap();
        std::fs::write(
            directory.path().join("caller"),
            "old-token\r\nnew-token\r\n",
        )
        .unwrap();
        let text = ROUTE.replace(
            "[[route]]",
            "key_file = 'provider'\nauth = 'x-api-key'\n[[route]]",
        ) + r#"
[[caller]]
name = "test"
key_file = "caller"
routes = ["test:1"]
"#;
        let config_path = directory.path().join("svincolo.toml");
        std::fs::write(&config_path, text).unwrap();
        let config = Config::load(&config_path).unwrap();
        let route = &config.routes["test:1"];
        assert_eq!(route.locality.as_str(), "local");
        assert_eq!(
            route.upstream.endpoint(Surface::Chat).as_str(),
            "http://127.0.0.1:9000/custom/v1/chat/completions"
        );
        let (name, credential) = route.upstream.credential.as_ref().unwrap();
        assert_eq!(name, "x-api-key");
        assert_eq!(credential.as_bytes(), b"provider-secret");
        assert!(credential.is_sensitive());
        for token in [b"old-token", b"new-token"] {
            let caller = config.authenticate(token).unwrap();
            assert!(caller.permits("test:1"));
            assert!(!caller.permits("anything-else"));
        }
        assert!(config.authenticate(b"provider-secret").is_none());
    }

    #[test]
    fn rejects_unsafe_or_unsupported_routes_and_urls() {
        let directory = Path::new("");
        for text in [
            ROUTE.replace("test:1", "test:CLOUD"),
            ROUTE.replace("test:1", "test*"),
            ROUTE.replace("locality = \"local\"", "locality = \"cloud\""),
            ROUTE.replace("chat = \"raw\"", "chat = \"managed\""),
            ROUTE.replace("http://127.", "http://hidden-secret@127."),
            ROUTE.replace("http://127.", "http://@127."),
            ROUTE.replace("custom/v1/", "custom/v1/?secret"),
            ROUTE.replace("custom/v1/", "custom/v1/#secret"),
        ] {
            assert!(!invalid(&text, directory).contains("secret"));
        }
    }

    #[test]
    fn defaults_model_to_route_name_and_supports_hidden_pinned_routes() {
        let directory = Path::new("");
        let config = Config::parse(ROUTE, directory).unwrap();
        let exact = &config.routes["test:1"];
        assert_eq!(exact.upstream_model, "test:1");
        assert!(exact.discover);

        let text = ROUTE.replace("test:1", "low")
            + "upstream_model = 'family/model:2026-11-01'\ndiscover = false\n";
        let config = Config::parse(&text, directory).unwrap();
        let alias = &config.routes["low"];
        assert_eq!(alias.upstream_model, "family/model:2026-11-01");
        assert!(!alias.discover);
        assert!(!config.routes.contains_key(&alias.upstream_model));
    }

    #[test]
    fn rejects_invalid_or_cloud_pins_on_local_routes() {
        let directory = Path::new("");
        let too_long = "x".repeat(MAX_MODEL_ID_BYTES + 1);
        for model in ["", "test:*", "two words", "v1?debug", &too_long] {
            let text = format!("{ROUTE}upstream_model = '{model}'\n");
            assert!(invalid(&text, directory).contains(".upstream_model"));
        }
        for model in ["local:CLOUD", "family/test-cloud"] {
            let text = format!("{ROUTE}upstream_model = '{model}'\n");
            assert!(invalid(&text, directory).contains(".locality"));
        }
    }

    #[test]
    fn route_revision_tracks_destination_but_ignores_credentials_and_visibility() {
        let directory = tempfile::tempdir().unwrap();
        let key = directory.path().join("provider");
        std::fs::write(&key, "original-secret").unwrap();
        let text = ROUTE.replace("[[route]]", "key_file = 'provider'\n[[route]]")
            + "upstream_model = 'family/model-v1'\n";
        let revision = |text: &str| {
            Config::parse(text, directory.path()).unwrap().routes["test:1"]
                .revision
                .clone()
        };
        let original = revision(&text);
        assert_eq!(original.len(), 64);
        assert_eq!(original, revision(&text.replace("/v1/", "/v1")));
        assert_ne!(original, revision(&text.replace("model-v1", "model-v2")));
        assert_ne!(original, revision(&text.replace(":9000/", ":9001/")));
        assert_ne!(
            original,
            revision(&text.replace("openai_compatible", "openai"))
        );
        std::fs::write(&key, "rotated-secret").unwrap();
        assert_eq!(original, revision(&text));
        assert_eq!(original, revision(&format!("{text}discover = false\n")));
    }

    #[test]
    fn same_endpoint_managed_chat_has_a_distinct_route_revision() {
        let directory = tempfile::tempdir().unwrap();
        for kind in ["openai", "ollama"] {
            let raw = ROUTE
                .replace("openai_compatible", kind)
                .replace("[[route]]", "auth = \"none\"\n\n[[route]]");
            let managed = raw.replace("chat = \"raw\"", "chat = \"managed\"");
            let revision = |text: &str| {
                Config::parse(text, directory.path()).unwrap().routes["test:1"]
                    .revision
                    .clone()
            };
            assert_ne!(revision(&raw), revision(&managed), "{kind}");
        }
    }

    #[test]
    fn compiles_native_messages_with_version_and_auth_defaults() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("provider"), "provider-secret\n").unwrap();
        let text = ROUTE
            .replace(
                "kind = \"openai_compatible\"",
                "kind = \"anthropic\"\nkey_file = \"provider\"",
            )
            .replace("chat = \"raw\"", "messages = \"raw\"");
        let config = Config::parse(&text, directory.path()).unwrap();
        let upstream = &config.routes["test:1"].upstream;
        let default_revision = &config.routes["test:1"].revision;
        assert_eq!(config.routes["test:1"].surfaces, [Surface::Messages]);
        assert_eq!(
            upstream.endpoint(Surface::Messages).as_str(),
            "http://127.0.0.1:9000/custom/v1/messages"
        );
        assert_eq!(upstream.anthropic_version.as_ref().unwrap(), "2026-10-21");
        let (name, key) = upstream.credential.as_ref().unwrap();
        assert_eq!(name, "x-api-key");
        assert_eq!(key, "provider-secret");
        assert!(key.is_sensitive());

        let text = text.replace("[[route]]", "anthropic_version = '2023-06-01'\n[[route]]");
        let config = Config::parse(&text, directory.path()).unwrap();
        assert_ne!(&config.routes["test:1"].revision, default_revision);
        assert_eq!(
            config.routes["test:1"]
                .upstream
                .anthropic_version
                .as_ref()
                .unwrap(),
            "2023-06-01"
        );
    }

    #[test]
    fn rejects_surface_mismatches_and_invalid_version_config() {
        let directory = Path::new("");
        for surfaces in [
            "",
            "messages = \"raw\"",
            "chat = \"raw\", messages = \"raw\"",
        ] {
            let text = ROUTE.replace("chat = \"raw\"", surfaces);
            assert!(invalid(&text, directory).contains(".surfaces"));
        }
        let wrong_kind = ROUTE.replace("[[route]]", "anthropic_version = '2023-06-01'\n[[route]]");
        assert!(invalid(&wrong_kind, directory).contains(".anthropic_version"));

        let native = ROUTE.replace(
            "kind = \"openai_compatible\"",
            "kind = \"anthropic\"\nauth = \"none\"",
        );
        assert!(invalid(&native, directory).contains(".surfaces"));
        let malformed = native
            .replace("chat = \"raw\"", "messages = \"raw\"")
            .replace("[[route]]", "anthropic_version = '2023-6-01'\n[[route]]");
        assert!(invalid(&malformed, directory).contains(".anthropic_version"));
    }

    #[test]
    fn denies_duplicate_tokens_and_invalid_caller_permissions() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("caller"), "test-token\n").unwrap();
        let text = ROUTE.to_owned()
            + r#"
[[caller]]
name = "test"
key_file = "caller"
routes = ["*"]

[[caller]]
name = "other"
key_file = "caller"
routes = ["test:1"]
"#;
        assert!(invalid(&text, directory.path()).contains("duplicate token"));
        assert!(
            invalid(
                &text.replace("routes = [\"*\"]", "routes = []"),
                directory.path()
            )
            .contains(".routes")
        );
        assert!(
            invalid(
                &text.replace("routes = [\"*\"]", "routes = [\"missing\"]"),
                directory.path()
            )
            .contains(".routes")
        );
    }
}
