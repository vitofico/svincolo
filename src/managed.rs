mod openai_schema;

use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    body::{Body, to_bytes},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
};
use jsonschema::{PatternOptions, Validator};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::{
    config::{ManagedDialect, ManagedSettings, StructuredMode},
    queue::Dispatch,
    upstream::{self, Failure, OutboundBody, Target},
};

const MAX_BODY_BYTES: usize = 1024 * 1024;
const MAX_SCHEMA_BYTES: usize = 64 * 1024;
const MAX_REPAIR_TEXT: usize = 32 * 1024;
pub const WORK_RESERVATION: u32 = 4 * 1024 * 1024;

pub struct Prepared {
    request: Value,
    dialect: ManagedDialect,
    native_schema: Option<Value>,
    validation: Validation,
    modes: Vec<Option<StructuredMode>>,
    validation_retries: u8,
    pub dropped_params: Vec<&'static str>,
}

enum Validation {
    Plain,
    Object,
    Schema {
        original: Value,
        validator: Box<Validator>,
    },
}

#[derive(Default)]
pub struct Report {
    pub attempts: u8,
    pub mode: Option<StructuredMode>,
}

pub struct ManagedError {
    pub failure: Failure,
    pub details: Option<Value>,
}

impl From<Failure> for ManagedError {
    fn from(failure: Failure) -> Self {
        Self {
            failure,
            details: None,
        }
    }
}

pub fn prepare(body: &[u8], model: &str, settings: &ManagedSettings) -> Result<Prepared, Failure> {
    if body.len() > MAX_BODY_BYTES {
        return Err(too_large());
    }
    let mut input: Map<String, Value> = serde_json::from_slice(body).map_err(|_| unsupported())?;
    if input.remove("stream").is_some_and(|stream| stream != false)
        || ["tools", "tool_choice", "functions", "function_call"]
            .iter()
            .any(|key| input.contains_key(*key))
    {
        return Err(capability(
            "Managed chat accepts one non-streaming answer without tools.",
        ));
    }
    if input.remove("n").is_some_and(|n| n != 1) {
        return Err(unsupported());
    }
    input.remove("model");
    let validation = parse_format(input.remove("response_format"))?;
    let dialect = settings.dialect;
    let native_schema = if settings.structured_modes.contains(&StructuredMode::Native)
        && let Validation::Schema { original, .. } = &validation
    {
        match dialect {
            ManagedDialect::Anthropic if native_schema(original) => Some(original.clone()),
            ManagedDialect::Openai => openai_schema::strict(original),
            ManagedDialect::Ollama => Some(original.clone()),
            _ => None,
        }
    } else {
        None
    };
    let modes: Vec<_> = if matches!(&validation, Validation::Plain) {
        vec![None]
    } else {
        settings
            .structured_modes
            .iter()
            .copied()
            .filter(|mode| *mode == StructuredMode::Prompt || native_schema.is_some())
            .map(Some)
            .collect()
    };
    if modes.is_empty() {
        return Err(capability(
            "This route has no mode for the requested output.",
        ));
    }
    let max_tokens = match input
        .remove("max_completion_tokens")
        .or_else(|| input.remove("max_tokens"))
    {
        Some(value) => value
            .as_u64()
            .filter(|value| (1..=32768).contains(value))
            .ok_or_else(unsupported)?,
        None => u64::from(settings.default_max_tokens),
    };
    input.remove("max_tokens");
    let (request, dropped_params) = match dialect {
        ManagedDialect::Anthropic => anthropic_request(input, model, max_tokens)?,
        ManagedDialect::Openai | ManagedDialect::Ollama => {
            (chat_request(input, model, max_tokens, dialect)?, Vec::new())
        }
    };
    Ok(Prepared {
        request,
        dialect,
        native_schema,
        validation,
        modes,
        validation_retries: settings.validation_retries,
        dropped_params,
    })
}

fn anthropic_request(
    mut input: Map<String, Value>,
    model: &str,
    max_tokens: u64,
) -> Result<(Value, Vec<&'static str>), Failure> {
    let (system, messages) = translate_messages(input.remove("messages"))?;
    let mut request = json!({"model": model, "max_tokens": max_tokens, "messages": messages});
    if !system.is_empty() {
        request["system"] = Value::String(system);
    }
    if let Some(stop) = input.remove("stop") {
        request["stop_sequences"] = match stop {
            Value::String(_) => Value::Array(vec![stop]),
            Value::Array(ref values) if values.iter().all(Value::is_string) => stop,
            Value::Null => Value::Array(Vec::new()),
            _ => return Err(unsupported()),
        };
    }
    let mut dropped_params = Vec::new();
    if let Some(effort) = input.remove("reasoning_effort") {
        match effort.as_str() {
            Some("low" | "medium" | "high" | "xhigh" | "max") => {
                request["output_config"] = json!({"effort": effort})
            }
            Some("none" | "minimal") => dropped_params.push("reasoning_effort"),
            _ => return Err(unsupported()),
        }
    }
    for param in ["temperature", "top_p", "user"] {
        if input.remove(param).is_some() {
            dropped_params.push(param);
        }
    }
    if !input.is_empty() {
        return Err(unsupported());
    }
    Ok((request, dropped_params))
}

fn chat_request(
    mut input: Map<String, Value>,
    model: &str,
    max_tokens: u64,
    dialect: ManagedDialect,
) -> Result<Value, Failure> {
    let messages = input
        .get_mut("messages")
        .and_then(Value::as_array_mut)
        .ok_or_else(unsupported)?;
    let mut has_turn = false;
    for message in messages {
        let fields = message.as_object().ok_or_else(unsupported)?;
        if fields
            .keys()
            .any(|key| !matches!(key.as_str(), "role" | "content" | "name"))
        {
            return Err(capability(
                "Managed chat does not accept tool or extended message fields.",
            ));
        }
        let role = message["role"].as_str().ok_or_else(unsupported)?;
        if !matches!(role, "system" | "developer" | "user" | "assistant") {
            return Err(capability("Managed chat does not accept tool messages."));
        }
        has_turn |= matches!(role, "user" | "assistant");
        let content = &message["content"];
        let valid = content.is_string()
            || content.as_array().is_some_and(|parts| {
                parts.iter().all(|part| match part["type"].as_str() {
                    Some("text") => part["text"].is_string(),
                    Some("image_url") if role == "user" => part["image_url"]["url"].is_string(),
                    _ => false,
                })
            });
        if !valid {
            return Err(unsupported());
        }
    }
    if !has_turn
        || input.keys().any(|key| {
            !matches!(
                key.as_str(),
                "messages"
                    | "temperature"
                    | "top_p"
                    | "stop"
                    | "seed"
                    | "frequency_penalty"
                    | "presence_penalty"
                    | "user"
                    | "reasoning_effort"
            )
        })
    {
        return Err(unsupported());
    }
    input.insert("model".into(), model.into());
    let limit = if dialect == ManagedDialect::Openai {
        "max_completion_tokens"
    } else {
        "max_tokens"
    };
    input.insert(limit.into(), max_tokens.into());
    Ok(Value::Object(input))
}

fn translate_messages(input: Option<Value>) -> Result<(String, Vec<Value>), Failure> {
    let Some(Value::Array(input)) = input else {
        return Err(unsupported());
    };
    let mut system = Vec::new();
    let mut messages = Vec::new();
    for mut message in input {
        if message.get("tool_calls").is_some() || message.get("function_call").is_some() {
            return Err(capability("Managed chat does not accept tool messages."));
        }
        let role = message["role"].as_str().ok_or_else(unsupported)?.to_owned();
        let content = message
            .get_mut("content")
            .map(Value::take)
            .ok_or_else(unsupported)?;
        match role.as_str() {
            "system" | "developer" => system.push(match content {
                Value::String(text) => text,
                Value::Array(parts) => parts
                    .iter()
                    .map(|part| {
                        if part["type"] == "text" {
                            part["text"].as_str().ok_or_else(unsupported)
                        } else {
                            Err(unsupported())
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .join("\n"),
                _ => return Err(unsupported()),
            }),
            "user" | "assistant" => messages.push(json!({
                "role": role, "content": translate_content(content, role == "user")?
            })),
            "tool" | "function" => {
                return Err(capability("Managed chat does not accept tool messages."));
            }
            _ => return Err(unsupported()),
        }
    }
    if messages.is_empty() {
        return Err(unsupported());
    }
    Ok((system.join("\n\n"), messages))
}

fn translate_content(content: Value, images_allowed: bool) -> Result<Value, Failure> {
    match content {
        Value::String(_) => Ok(content),
        Value::Array(parts) => parts
            .into_iter()
            .map(|part| match part["type"].as_str() {
                Some("text") if part["text"].is_string() => {
                    Ok(json!({"type":"text", "text":part["text"]}))
                }
                Some("image_url") if images_allowed => {
                    let url = part["image_url"]["url"].as_str().ok_or_else(unsupported)?;
                    let source = if let Some(data) = url.strip_prefix("data:") {
                        let (media, data) = data.split_once(";base64,").ok_or_else(unsupported)?;
                        if !matches!(
                            media,
                            "image/jpeg" | "image/png" | "image/gif" | "image/webp"
                        ) || data.is_empty()
                        {
                            return Err(unsupported());
                        }
                        json!({"type":"base64", "media_type":media, "data":data})
                    } else {
                        let parsed = reqwest::Url::parse(url).map_err(|_| unsupported())?;
                        if !matches!(parsed.scheme(), "http" | "https")
                            || !parsed.username().is_empty()
                            || parsed.password().is_some()
                        {
                            return Err(unsupported());
                        }
                        json!({"type":"url", "url":url})
                    };
                    Ok(json!({"type":"image", "source":source}))
                }
                _ => Err(unsupported()),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        _ => Err(unsupported()),
    }
}

fn parse_format(format: Option<Value>) -> Result<Validation, Failure> {
    let Some(mut format) = format else {
        return Ok(Validation::Plain);
    };
    match format["type"].as_str() {
        Some("text") => Ok(Validation::Plain),
        Some("json_object") => Ok(Validation::Object),
        Some("json_schema") => {
            let schema_invalid = || {
                local(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "schema_invalid",
                    "The requested JSON Schema could not be compiled offline.",
                )
            };
            let original = format
                .pointer_mut("/json_schema/schema")
                .map(Value::take)
                .ok_or_else(schema_invalid)?;
            if original.to_string().len() > MAX_SCHEMA_BYTES {
                return Err(schema_invalid());
            }
            let validator = jsonschema::options()
                .offline()
                .should_validate_formats(true)
                .with_pattern_options(PatternOptions::fancy_regex().backtrack_limit(10_000))
                .build(&original)
                .map_err(|_| schema_invalid())?;
            Ok(Validation::Schema {
                original,
                validator: Box::new(validator),
            })
        }
        _ => Err(unsupported()),
    }
}

fn native_schema(schema: &Value) -> bool {
    let Some(schema) = schema.as_object() else {
        return false;
    };
    let is_object = schema.get("properties").is_some()
        || schema.get("type").is_some_and(|value| {
            value == "object"
                || value
                    .as_array()
                    .is_some_and(|types| types.iter().any(|value| value == "object"))
        });
    if is_object && schema.get("additionalProperties") != Some(&Value::Bool(false)) {
        return false;
    }
    schema.iter().all(|(key, value)| match key.as_str() {
        "properties" | "$defs" | "definitions" => value
            .as_object()
            .is_some_and(|children| children.values().all(native_schema)),
        "items" => native_schema(value),
        "allOf" | "anyOf" | "oneOf" => value
            .as_array()
            .is_some_and(|children| children.iter().all(native_schema)),
        "additionalProperties" => value == false,
        "type" | "required" | "$ref" | "enum" | "const" | "title" | "description" => true,
        _ => false,
    })
}

impl Prepared {
    pub async fn run(
        mut self,
        target: &Target<'_>,
        admission: &mut Dispatch<'_>,
        no_retry: bool,
        report: &mut Report,
    ) -> Result<Response, ManagedError> {
        let mut mode_index = 0;
        let mut repairs = if no_retry { 0 } else { self.validation_retries };
        let mut usage = Usage::default();
        loop {
            let mode = self.modes[mode_index];
            let body = self.encode(mode).map_err(|mut failure| {
                if report.attempts > 0 {
                    failure.reached = "true";
                }
                failure
            })?;
            report.mode = mode;
            let surface = self.dialect.upstream_surface();
            let response = match upstream::forward(target, surface, body, admission).await {
                Ok(response) => {
                    report.attempts += 1;
                    response
                }
                Err(mut failure) => {
                    if report.attempts > 0 && failure.reached == "false" {
                        failure.reached = "true";
                    }
                    report.attempts += u8::from(failure.attempted);
                    return Err(failure.into());
                }
            };
            let (parts, body) = response.into_parts();
            if !parts.status.is_success() {
                if !no_retry
                    && mode == Some(StructuredMode::Native)
                    && mode_index + 1 < self.modes.len()
                    && matches!(
                        parts.status,
                        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
                    )
                {
                    let body = to_bytes(body, MAX_BODY_BYTES)
                        .await
                        .map_err(|_| protocol())?;
                    if format_refusal(self.dialect, &body) {
                        mode_index += 1;
                        continue;
                    }
                    return Ok(Response::from_parts(parts, Body::from(body)));
                }
                return Ok(Response::from_parts(parts, body));
            }

            let raw = to_bytes(body, MAX_BODY_BYTES)
                .await
                .map_err(|_| protocol())?;
            let answer = Answer::decode(self.dialect, &raw)?;
            usage.add(&answer.usage);
            let text = &answer.text;
            let finish_reason = answer.finish_reason;
            if finish_reason == "content_filter" {
                return Ok(answer.response(None, usage, &parts.headers));
            }
            let validated = if finish_reason == "length"
                && !matches!(&self.validation, Validation::Plain)
            {
                Err(json!({"message":"The provider reached its output limit."}))
            } else {
                self.validation.check(
                    text,
                    mode == Some(StructuredMode::Native) && self.dialect == ManagedDialect::Openai,
                )
            };
            match validated {
                Ok(content) => {
                    return Ok(answer.response(Some(&content), usage, &parts.headers));
                }
                Err(problem) if repairs > 0 && finish_reason == "stop" => {
                    repairs -= 1;
                    self.repair(text, &problem);
                }
                Err(problem) => {
                    return Err(ManagedError {
                        failure: reached(
                            StatusCode::BAD_GATEWAY,
                            "invalid_output",
                            "The provider answer did not match the requested output.",
                        ),
                        details: Some(json!({
                            "details": [problem],
                            "raw_output": truncate(text),
                            "finish_reason": finish_reason,
                            "usage": usage.json(),
                            "attempts": report.attempts,
                        })),
                    });
                }
            }
        }
    }

    fn encode(&self, mode: Option<StructuredMode>) -> Result<OutboundBody, Failure> {
        let mut request = self.request.clone();
        match mode {
            Some(StructuredMode::Native) => {
                let schema = self
                    .native_schema
                    .as_ref()
                    .expect("native mode needs a schema");
                match self.dialect {
                    ManagedDialect::Anthropic => {
                        request["output_config"]["format"] =
                            json!({"type":"json_schema", "schema":schema});
                    }
                    ManagedDialect::Openai | ManagedDialect::Ollama => {
                        request["response_format"] = json!({"type":"json_schema",
                            "json_schema":{"name":"svincolo_response", "strict":true, "schema":schema}
                        });
                    }
                }
            }
            Some(StructuredMode::Prompt) => {
                let mut instruction =
                    String::from("Return one JSON value without prose or code fences.");
                match &self.validation {
                    Validation::Schema { original, .. } => {
                        instruction.push_str(" It must match this JSON Schema:\n");
                        instruction.push_str(&original.to_string());
                    }
                    _ => instruction.push_str(" The value must be an object."),
                }
                if self.dialect == ManagedDialect::Anthropic {
                    let system = request["system"].as_str().unwrap_or_default();
                    request["system"] = Value::String(format!("{system}\n\n{instruction}"));
                } else {
                    request["messages"]
                        .as_array_mut()
                        .expect("validated chat messages")
                        .insert(0, json!({"role":"system","content":instruction}));
                }
            }
            None => {}
        }
        let body = serde_json::to_vec(&request).expect("serializing managed request");
        if body.len() > MAX_BODY_BYTES {
            return Err(too_large());
        }
        Ok(OutboundBody {
            len: body.len(),
            body: body.into(),
            rewritten: true,
        })
    }

    fn repair(&mut self, text: &str, problem: &Value) {
        let messages = self.request["messages"]
            .as_array_mut()
            .expect("translated messages");
        if !text.is_empty() {
            messages.push(json!({"role":"assistant", "content":truncate(text)}));
        }
        messages.push(json!({"role":"user","content":format!(
            "Your previous answer failed output validation. Correct this problem and return only valid JSON. Validation problem: {}", problem
        )}));
    }
}

impl Validation {
    fn check(&self, text: &str, restore_optional: bool) -> Result<String, Value> {
        if matches!(self, Self::Plain) {
            return Ok(text.to_owned());
        }
        let text = text.trim();
        let text = text
            .strip_prefix("```json")
            .or_else(|| text.strip_prefix("```"))
            .and_then(|body| body.strip_suffix("```"))
            .map_or(text, str::trim);
        let mut instance: Value = serde_json::from_str(text)
            .map_err(|_| json!({"message":"The answer is not a JSON value."}))?;
        match self {
            Self::Object if !instance.is_object() => {
                return Err(json!({"message":"The answer is not a JSON object."}));
            }
            Self::Schema {
                original,
                validator,
            } => {
                if restore_optional {
                    openai_schema::restore_optional(original, &mut instance);
                }
                validator.validate(&instance).map_err(|error| {
                    json!({
                        "path": error.instance_path().to_string(),
                        "schema": error.schema_path().to_string(),
                        "message": "The value violates a schema constraint.",
                    })
                })?;
            }
            _ => {}
        }
        Ok(instance.to_string())
    }
}

fn format_refusal(dialect: ManagedDialect, bytes: &[u8]) -> bool {
    let Ok(body) = serde_json::from_slice::<Value>(bytes) else {
        return false;
    };
    let error = &body["error"];
    if error["type"] != "invalid_request_error" {
        return false;
    }
    if dialect != ManagedDialect::Anthropic {
        return dialect == ManagedDialect::Openai
            && error["param"].as_str().is_some_and(|param| {
                param == "response_format" || param.starts_with("response_format.")
            });
    }
    if error["param"]
        .as_str()
        .is_some_and(|param| matches!(param, "output_config.format" | "output_format"))
    {
        return true;
    }
    error["message"].as_str().is_some_and(|message| {
        message.contains("output_config.format") || message.contains("output_format")
    })
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct Usage {
    input_tokens: u64,
    cache_read_input_tokens: u64,
    cache_creation_input_tokens: u64,
    output_tokens: u64,
}

impl Usage {
    fn add(&mut self, other: &Self) {
        self.input_tokens = self.input_tokens.saturating_add(other.input_tokens);
        self.cache_read_input_tokens = self
            .cache_read_input_tokens
            .saturating_add(other.cache_read_input_tokens);
        self.cache_creation_input_tokens = self
            .cache_creation_input_tokens
            .saturating_add(other.cache_creation_input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(other.output_tokens);
    }

    fn json(&self) -> Value {
        let prompt = self
            .input_tokens
            .saturating_add(self.cache_read_input_tokens)
            .saturating_add(self.cache_creation_input_tokens);
        json!({
            "prompt_tokens": prompt, "completion_tokens":self.output_tokens,
            "total_tokens":prompt.saturating_add(self.output_tokens),
            "prompt_tokens_details":{"cached_tokens": self.cache_read_input_tokens},
        })
    }
}

#[derive(Deserialize)]
struct NativeAnswer {
    id: String,
    model: String,
    content: Vec<NativeBlock>,
    stop_reason: String,
    usage: Usage,
}

#[derive(Deserialize)]
struct NativeBlock {
    #[serde(rename = "type")]
    kind: String,
    text: Option<String>,
}

#[derive(Deserialize)]
struct ChatAnswer {
    id: String,
    model: String,
    created: Option<u64>,
    choices: Vec<ChatChoice>,
    usage: Option<ChatUsage>,
}

#[derive(Deserialize)]
struct ChatChoice {
    finish_reason: String,
    message: ChatMessage,
}

#[derive(Deserialize)]
struct ChatMessage {
    content: Option<String>,
    refusal: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct ChatUsage {
    prompt_tokens: u64,
    completion_tokens: u64,
    prompt_tokens_details: Option<CachedTokens>,
}

#[derive(Deserialize)]
struct CachedTokens {
    cached_tokens: u64,
}

struct Answer {
    id: String,
    model: String,
    created: Option<u64>,
    text: String,
    refusal: Option<String>,
    finish_reason: &'static str,
    usage: Usage,
}

impl Answer {
    fn decode(dialect: ManagedDialect, raw: &[u8]) -> Result<Self, Failure> {
        if dialect == ManagedDialect::Anthropic {
            let answer: NativeAnswer = serde_json::from_slice(raw).map_err(|_| protocol())?;
            let finish_reason = match answer.stop_reason.as_str() {
                "end_turn" | "stop_sequence" => "stop",
                "max_tokens" | "model_context_window_exceeded" => "length",
                "refusal" => "content_filter",
                _ => return Err(protocol()),
            };
            let text = answer
                .content
                .into_iter()
                .filter(|part| part.kind == "text")
                .filter_map(|part| part.text)
                .collect::<String>();
            let (text, refusal) = if finish_reason == "content_filter" {
                (String::new(), Some(text))
            } else {
                (text, None)
            };
            return Ok(Self {
                id: answer.id,
                model: answer.model,
                created: None,
                text,
                refusal,
                finish_reason,
                usage: answer.usage,
            });
        }

        let mut answer: ChatAnswer = serde_json::from_slice(raw).map_err(|_| protocol())?;
        if answer.choices.len() != 1 {
            return Err(protocol());
        }
        let choice = answer.choices.pop().expect("one managed completion choice");
        let refusal = choice.message.refusal.filter(|text| !text.is_empty());
        let finish_reason = match (refusal.is_some(), choice.finish_reason.as_str()) {
            (true, _) | (_, "content_filter") => "content_filter",
            (false, "stop") => "stop",
            (false, "length") => "length",
            _ => return Err(protocol()),
        };
        let text = if finish_reason == "content_filter" {
            String::new()
        } else {
            choice.message.content.ok_or_else(protocol)?
        };
        let usage = answer.usage.unwrap_or_default();
        let cached = usage
            .prompt_tokens_details
            .map_or(0, |details| details.cached_tokens)
            .min(usage.prompt_tokens);
        Ok(Self {
            id: answer.id,
            model: answer.model,
            created: answer.created,
            text,
            refusal,
            finish_reason,
            usage: Usage {
                input_tokens: usage.prompt_tokens - cached,
                cache_read_input_tokens: cached,
                cache_creation_input_tokens: 0,
                output_tokens: usage.completion_tokens,
            },
        })
    }

    fn response(
        &self,
        content: Option<&str>,
        usage: Usage,
        upstream_headers: &HeaderMap,
    ) -> Response {
        let created = self.created.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        });
        let body = json!({
            "id":self.id, "model":self.model, "object":"chat.completion", "created":created,
            "choices":[{"index":0, "finish_reason":self.finish_reason,
                "message":{"role":"assistant", "content":content, "refusal":self.refusal}}],
            "usage":usage.json(),
        });
        let mut response = Response::new(Body::from(body.to_string()));
        for (name, value) in upstream_headers {
            if matches!(name.as_str(), "request-id" | "x-request-id")
                || name.as_str().starts_with("anthropic-ratelimit-")
                || name.as_str().starts_with("x-ratelimit-")
            {
                response.headers_mut().append(name, value.clone());
            }
        }
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        response
    }
}

fn truncate(text: &str) -> &str {
    &text[..text.floor_char_boundary(MAX_REPAIR_TEXT.min(text.len()))]
}

fn local(status: StatusCode, code: &'static str, message: &'static str) -> Failure {
    Failure {
        status,
        code,
        message,
        reached: "false",
        attempted: false,
        retry_after: None,
    }
}

fn reached(status: StatusCode, code: &'static str, message: &'static str) -> Failure {
    Failure {
        status,
        code,
        message,
        reached: "true",
        attempted: true,
        retry_after: None,
    }
}

fn unsupported() -> Failure {
    local(
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "unsupported_content",
        "The managed chat request uses unsupported content or fields.",
    )
}

fn capability(message: &'static str) -> Failure {
    local(StatusCode::NOT_FOUND, "capability_unavailable", message)
}

fn too_large() -> Failure {
    local(
        StatusCode::PAYLOAD_TOO_LARGE,
        "body_too_large",
        "The managed request exceeds the body limit.",
    )
}

fn protocol() -> Failure {
    reached(
        StatusCode::BAD_GATEWAY,
        "upstream_protocol_error",
        "The native provider returned an unusable response.",
    )
}
