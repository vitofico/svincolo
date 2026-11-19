//! Emission is tied to the lifetime of a dispatch, including cancellation.
use std::{
    io::{self, Write},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{http::header, response::Response};

use crate::{
    config::{Caller, Route, Surface},
    managed,
    native_usage::{MAX_BUFFER_BYTES, NativeUsage, Units},
    state::{Capacity, Reservation},
};

pub struct UsageIdentity {
    request_id: String,
    caller: String,
    provider: String,
    upstream_model: String,
}

impl UsageIdentity {
    pub fn new(request_id: &str, caller: Option<&Caller>, route: &Route) -> Self {
        Self {
            request_id: request_id.to_owned(),
            caller: caller.map_or("anonymous", |c| c.name.as_str()).to_owned(),
            provider: route.upstream.provider.clone(),
            upstream_model: route.upstream_model.clone(),
        }
    }

    fn log(
        &self,
        surface: Surface,
        attempts: u8,
        known: u8,
        units: Units,
        uploaded_ms: Option<u64>,
    ) {
        if attempts == 0 {
            return;
        }
        let tokens = units.tokens;
        let mut event = serde_json::json!({
            "event": if surface == Surface::Chat { "managed_chat_usage" } else { "native_usage" },
            "surface": surface.as_str(),
            "timestamp_s": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            "request_id": self.request_id, "caller": self.caller,
            "provider": self.provider, "upstream_model": self.upstream_model,
            "attempts": attempts, "known_attempts": known,
            "input_tokens": tokens[0], "cache_read_input_tokens": tokens[1],
            "cache_creation_input_tokens": tokens[2], "output_tokens": tokens[3],
        });
        for (name, value) in [
            ("audio_input_tokens", units.audio_input_tokens),
            ("audio_duration_ms", units.audio_duration_ms),
            ("uploaded_audio_duration_ms", uploaded_ms),
        ] {
            if let Some(value) = value {
                event[name] = value.into();
            }
        }
        let _ = writeln!(io::stdout().lock(), "{event}");
    }
}

pub struct ManagedUsageEvent {
    pub identity: UsageIdentity,
    pub report: managed::Report,
}

impl Drop for ManagedUsageEvent {
    fn drop(&mut self) {
        self.identity.log(
            Surface::Chat,
            self.report.attempts,
            self.report.known_attempts,
            self.report.usage_tokens().into(),
            None,
        );
    }
}

pub struct NativeUsageEvent {
    identity: UsageIdentity,
    surface: Surface,
    pub attempts: u8,
    observer: Option<(NativeUsage, Reservation)>,
    remaining: Option<u64>,
    usage: Option<Units>,
    uploaded_ms: Option<u64>,
    complete_transcript: bool,
}

impl NativeUsageEvent {
    pub fn new(identity: UsageIdentity, surface: Surface, uploaded_ms: Option<u64>) -> Self {
        Self {
            identity,
            surface,
            attempts: 0,
            observer: None,
            remaining: None,
            usage: None,
            uploaded_ms,
            complete_transcript: false,
        }
    }

    pub fn observe(&mut self, response: &Response, budget: &Arc<Capacity>, limit: usize) {
        let headers = response.headers();
        if !response.status().is_success()
            || headers
                .get_all(header::CONTENT_ENCODING)
                .iter()
                .any(|v| !v.as_bytes().eq_ignore_ascii_case(b"identity"))
        {
            return;
        }
        // Observation can never exhaust the router's admission budget or prevent forwarding.
        let Some(permit) = budget.reserve(2 * MAX_BUFFER_BYTES, limit.min(8 * 1024 * 1024)) else {
            return;
        };
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        self.observer = Some((NativeUsage::new(self.surface, content_type), permit));
        self.remaining = headers
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok()?.parse().ok());
    }

    pub fn feed(&mut self, bytes: &[u8]) {
        if let Some((observer, _)) = &mut self.observer {
            observer.feed(bytes);
            self.remaining = self
                .remaining
                .and_then(|n| n.checked_sub(bytes.len() as u64));
            // Hyper may drop a fixed-length body immediately after its last data frame.
            if self.remaining == Some(0) {
                self.finish();
            }
        }
    }

    pub fn finish(&mut self) {
        if let Some((mut observer, _permit)) = self.observer.take() {
            self.usage = observer.finish();
            if self.uploaded_ms.is_some()
                && self.usage.is_none_or(|u| u.audio_duration_ms.is_none())
            {
                self.complete_transcript = observer.has_transcript();
            }
        }
    }
}

impl Drop for NativeUsageEvent {
    fn drop(&mut self) {
        let usage = self
            .usage
            .or_else(|| self.observer.as_ref().and_then(|(o, _)| o.terminal()));
        let uploaded = self.uploaded_ms.filter(|_| self.complete_transcript);
        self.identity.log(
            self.surface,
            self.attempts,
            u8::from(usage.is_some() || uploaded.is_some()),
            usage.unwrap_or_default(),
            uploaded,
        );
    }
}
