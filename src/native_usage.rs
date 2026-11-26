use serde::Deserialize;
use serde_json::value::RawValue;

use crate::config::Surface;

// JSON uses one buffer. SSE uses separate buffers for the current line and
// its event's data, each capped independently.
pub const MAX_BUFFER_BYTES: usize = 512 * 1024;

type Tokens = [u64; 4];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Units {
    pub tokens: Tokens,
    pub audio_input_tokens: Option<u64>,
    pub audio_duration_ms: Option<u64>,
    pub image_input_tokens: Option<u64>,
    pub image_output_tokens: Option<u64>,
}

impl From<Tokens> for Units {
    fn from(tokens: Tokens) -> Self {
        Self {
            tokens,
            ..Self::default()
        }
    }
}

pub struct NativeUsage {
    surface: Surface,
    body: Body,
}

enum Body {
    Ignore,
    Json(Vec<u8>),
    // Image answers carry megabytes of base64, so long strings are cut short.
    Images(Vec<u8>, Strings),
    Events(Events),
}

impl NativeUsage {
    pub fn new(surface: Surface, content_type: &str) -> Self {
        let media_type = content_type.split(';').next().unwrap_or_default().trim();
        let json = media_type.eq_ignore_ascii_case("application/json");
        let body = if surface == Surface::Chat {
            Body::Ignore
        } else if surface == Surface::Images {
            if json {
                Body::Images(Vec::new(), Strings::default())
            } else {
                Body::Ignore
            }
        } else if json {
            Body::Json(Vec::new())
        } else if surface != Surface::Embeddings
            && media_type.eq_ignore_ascii_case("text/event-stream")
        {
            Body::Events(Events::default())
        } else {
            Body::Ignore
        };
        Self { surface, body }
    }

    pub fn feed(&mut self, bytes: &[u8]) {
        match &mut self.body {
            Body::Json(buffer) => {
                // A batch of embeddings includes a vector for every input.
                // Its single larger buffer still fits the SSE allocation budget.
                let limit = if self.surface == Surface::Embeddings {
                    2 * MAX_BUFFER_BYTES
                } else {
                    MAX_BUFFER_BYTES
                };
                if bytes.len() <= limit - buffer.len() {
                    append(buffer, bytes, limit);
                } else {
                    self.body = Body::Ignore;
                }
            }
            Body::Images(buffer, strings) => {
                if !strings.feed(buffer, bytes) {
                    self.body = Body::Ignore;
                }
            }
            Body::Events(events) => events.feed(self.surface, bytes),
            Body::Ignore => {}
        }
    }

    pub fn finish(&mut self) -> Option<Units> {
        match &mut self.body {
            Body::Json(buffer) => {
                let envelope: UsageEnvelope<'_> = serde_json::from_slice(buffer).ok()?;
                match self.surface {
                    Surface::Embeddings => decode::<EmbeddingUsage>(envelope.usage)?.units(),
                    Surface::Transcriptions => {
                        decode::<TranscriptionUsage>(envelope.usage)?.units()
                    }
                    _ => usage(self.surface, envelope).map(Units::from),
                }
            }
            Body::Images(buffer, _) => {
                let envelope: UsageEnvelope<'_> = serde_json::from_slice(buffer).ok()?;
                decode::<ImageUsage>(envelope.usage)?.units()
            }
            _ => self.terminal(),
        }
    }

    pub fn terminal(&self) -> Option<Units> {
        match &self.body {
            Body::Events(events) => events.result,
            _ => None,
        }
    }

    pub fn has_transcript(&self) -> bool {
        #[derive(Deserialize)]
        struct Transcript<'a> {
            #[serde(borrow)]
            text: &'a RawValue,
        }

        match (&self.body, self.surface) {
            (Body::Json(buffer), Surface::Transcriptions) => {
                serde_json::from_slice::<Transcript>(buffer)
                    .is_ok_and(|transcript| transcript.text.get().starts_with('"'))
            }
            _ => false,
        }
    }
}

/// Copies JSON while keeping only the first bytes of each long string, so an
/// image answer's usage can be read without holding its base64 data. A string
/// is only cut before a plain ASCII character outside an escape, so the copy
/// stays valid JSON and valid UTF-8.
#[derive(Default)]
struct Strings {
    open: bool,
    escaped: bool,
    hex_left: u8,
    kept: usize,
    cut: bool,
}

const KEPT_STRING_BYTES: usize = 256;

impl Strings {
    fn feed(&mut self, buffer: &mut Vec<u8>, mut bytes: &[u8]) -> bool {
        while !bytes.is_empty() {
            if self.cut && !self.escaped && self.hex_left == 0 {
                // Skip the rest of a cut string without copying it.
                let Some(at) = memchr::memchr2(b'"', b'\\', bytes) else {
                    return true;
                };
                bytes = &bytes[at..];
            }
            let byte = bytes[0];
            bytes = &bytes[1..];
            let keep = if !self.open {
                if byte == b'"' {
                    self.open = true;
                    self.kept = 0;
                }
                true
            } else if self.escaped {
                self.escaped = false;
                if byte == b'u' {
                    self.hex_left = 4;
                }
                !self.cut
            } else if self.hex_left > 0 {
                self.hex_left -= 1;
                !self.cut
            } else if byte == b'"' {
                self.open = false;
                self.cut = false;
                true
            } else if byte == b'\\' {
                self.escaped = true;
                !self.cut
            } else {
                if self.kept >= KEPT_STRING_BYTES && byte.is_ascii() {
                    self.cut = true;
                }
                !self.cut
            };
            if keep {
                if buffer.len() >= MAX_BUFFER_BYTES {
                    return false;
                }
                if self.open {
                    self.kept += 1;
                }
                append(buffer, &[byte], MAX_BUFFER_BYTES);
            }
        }
        true
    }
}

fn fits(buffer: &[u8], bytes: &[u8]) -> bool {
    bytes.len() <= MAX_BUFFER_BYTES - buffer.len()
}

fn append(buffer: &mut Vec<u8>, bytes: &[u8], limit: usize) {
    let needed = buffer.len() + bytes.len();
    if needed > buffer.capacity() {
        let capacity = (buffer.capacity() * 2).max(needed).min(limit);
        buffer.reserve_exact(capacity - buffer.len());
    }
    buffer.extend_from_slice(bytes);
}

#[derive(Default)]
struct Events {
    line: Vec<u8>,
    data: Vec<u8>,
    kind: Option<Kind>,
    has_data: bool,
    cr: bool,
    oversized_line: bool,
    oversized_event: bool,
    done: bool,
    result: Option<Units>,
    anthropic: Option<Tokens>,
    anthropic_delta: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    MessageStart,
    MessageDelta,
    MessageStop,
    ResponseTerminal,
    TranscriptionDone,
    Error,
    Other,
}

impl Kind {
    fn from_bytes(value: &[u8]) -> Self {
        match value {
            b"message_start" => Self::MessageStart,
            b"message_delta" => Self::MessageDelta,
            b"message_stop" => Self::MessageStop,
            b"response.completed" | b"response.incomplete" | b"response.failed" => {
                Self::ResponseTerminal
            }
            b"transcript.text.done" => Self::TranscriptionDone,
            b"error" => Self::Error,
            _ => Self::Other,
        }
    }
}

impl Events {
    fn feed(&mut self, surface: Surface, mut bytes: &[u8]) {
        while !bytes.is_empty() && !self.done {
            if std::mem::take(&mut self.cr) && bytes[0] == b'\n' {
                bytes = &bytes[1..];
                continue;
            }
            let end = memchr::memchr2(b'\r', b'\n', bytes).unwrap_or(bytes.len());
            let fragment = &bytes[..end];
            if !self.oversized_line {
                if fits(&self.line, fragment) {
                    append(&mut self.line, fragment, MAX_BUFFER_BYTES);
                } else {
                    self.oversized_line = true;
                }
            }
            if end == bytes.len() {
                break;
            }
            self.cr = bytes[end] == b'\r';
            bytes = &bytes[end + 1..];
            self.line_ended(surface);
            self.line.clear();
            self.oversized_line = false;
        }
    }

    fn line_ended(&mut self, surface: Surface) {
        if self.oversized_line {
            self.oversized_event = true;
            return;
        }
        if self.line.is_empty() {
            self.dispatch(surface);
            self.data.clear();
            self.kind = None;
            self.has_data = false;
            self.oversized_event = false;
            return;
        }
        let mut parts = self.line.splitn(2, |&byte| byte == b':');
        let field = parts.next().unwrap_or_default();
        let value = parts.next().unwrap_or_default();
        let value = value.strip_prefix(b" ").unwrap_or(value);
        match field {
            b"event" => self.kind = Some(Kind::from_bytes(value)),
            b"data" => {
                let separator_size = usize::from(self.has_data);
                if self.data.len() + separator_size + value.len() > MAX_BUFFER_BYTES {
                    self.oversized_event = true;
                    return;
                }
                if self.has_data {
                    append(&mut self.data, b"\n", MAX_BUFFER_BYTES);
                }
                append(&mut self.data, value, MAX_BUFFER_BYTES);
                self.has_data = true;
            }
            _ => {}
        }
    }

    fn dispatch(&mut self, surface: Surface) {
        if self.kind == Some(Kind::Other) {
            // These are content deltas, pings, and other non-usage events.
            // Their size does not affect whether we can read later usage.
            return;
        }
        if self.oversized_event || self.kind == Some(Kind::Error) {
            self.done = true;
            return;
        }
        if !self.has_data {
            return;
        }
        let Ok(event) = serde_json::from_slice::<Event>(&self.data) else {
            self.done = true;
            return;
        };
        let kind = Kind::from_bytes(event.kind.as_bytes());
        if self.kind.is_some_and(|expected| expected != kind) {
            self.done = true;
            return;
        }
        match (surface, kind) {
            (Surface::Messages, Kind::MessageStart) => {
                if self.anthropic.is_some() {
                    self.done = true;
                    return;
                }
                self.anthropic = event
                    .message
                    .and_then(decode)
                    .and_then(|message| usage(surface, message));
                self.done = self.anthropic.is_none();
            }
            (Surface::Messages, Kind::MessageDelta) => {
                self.anthropic = self
                    .anthropic
                    .zip(event.usage.and_then(decode::<MessageDelta>))
                    .and_then(|(previous, delta)| delta.apply(previous));
                self.anthropic_delta = true;
                self.done = self.anthropic.is_none();
            }
            (Surface::Messages, Kind::MessageStop) => {
                self.done = true;
                self.result = self
                    .anthropic
                    .filter(|_| self.anthropic_delta)
                    .map(Units::from);
            }
            (Surface::Responses, Kind::ResponseTerminal) => {
                self.done = true;
                self.result = event
                    .response
                    .and_then(decode)
                    .and_then(|response| usage(surface, response))
                    .map(Units::from);
            }
            (Surface::Transcriptions, Kind::TranscriptionDone) => {
                self.done = true;
                self.result = event
                    .usage
                    .and_then(decode::<TranscriptionUsage>)
                    .and_then(TranscriptionUsage::units);
            }
            (_, Kind::Error) => self.done = true,
            _ => {}
        }
    }
}

#[derive(Deserialize)]
struct Event<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    message: Option<&'a RawValue>,
    response: Option<&'a RawValue>,
    usage: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct UsageEnvelope<'a> {
    #[serde(borrow)]
    usage: &'a RawValue,
}

fn usage(surface: Surface, envelope: UsageEnvelope<'_>) -> Option<Tokens> {
    match surface {
        Surface::Messages => decode::<MessageUsage>(envelope.usage).map(MessageUsage::tokens),
        Surface::Responses => decode::<ResponseUsage>(envelope.usage)?.tokens(),
        _ => None,
    }
}

fn decode<'a, T: Deserialize<'a>>(raw: &'a RawValue) -> Option<T> {
    serde_json::from_str(raw.get()).ok()
}

#[derive(Deserialize)]
struct MessageUsage {
    input_tokens: u64,
    #[serde(default)]
    cache_read_input_tokens: u64,
    #[serde(default)]
    cache_creation_input_tokens: u64,
    output_tokens: u64,
}

impl MessageUsage {
    fn tokens(self) -> Tokens {
        [
            self.input_tokens,
            self.cache_read_input_tokens,
            self.cache_creation_input_tokens,
            self.output_tokens,
        ]
    }
}

#[derive(Deserialize)]
struct MessageDelta {
    input_tokens: Option<u64>,
    cache_read_input_tokens: Option<u64>,
    cache_creation_input_tokens: Option<u64>,
    output_tokens: u64,
}

impl MessageDelta {
    fn apply(self, previous: Tokens) -> Option<Tokens> {
        let next = [
            self.input_tokens.unwrap_or(previous[0]),
            self.cache_read_input_tokens.unwrap_or(previous[1]),
            self.cache_creation_input_tokens.unwrap_or(previous[2]),
            self.output_tokens,
        ];
        next.iter()
            .zip(previous)
            .all(|(&current, prior)| current >= prior)
            .then_some(next)
    }
}

#[derive(Deserialize)]
struct ResponseUsage {
    input_tokens: u64,
    input_tokens_details: Option<InputDetails>,
    output_tokens: u64,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct InputDetails {
    cached_tokens: u64,
    cache_write_tokens: u64,
}

impl ResponseUsage {
    fn tokens(self) -> Option<Tokens> {
        let details = self.input_tokens_details.unwrap_or_default();
        Some([
            self.input_tokens
                .checked_sub(details.cached_tokens)?
                .checked_sub(details.cache_write_tokens)?,
            details.cached_tokens,
            details.cache_write_tokens,
            self.output_tokens,
        ])
    }
}

#[derive(Deserialize)]
struct EmbeddingUsage {
    prompt_tokens: Option<u64>,
    input_tokens: Option<u64>,
    total_tokens: Option<u64>,
}

impl EmbeddingUsage {
    fn units(self) -> Option<Units> {
        // An embeddings response contains vectors, so all usage is input.
        let input = self
            .prompt_tokens
            .or(self.input_tokens)
            .or(self.total_tokens)?;
        if self.input_tokens.is_some_and(|count| count != input)
            || self.total_tokens.is_some_and(|count| count != input)
        {
            return None;
        }
        Some(Units::from([input, 0, 0, 0]))
    }
}

#[derive(Deserialize)]
struct ImageUsage {
    input_tokens: u64,
    output_tokens: u64,
    total_tokens: Option<u64>,
    input_tokens_details: Modalities,
    output_tokens_details: Option<Modalities>,
}

#[derive(Deserialize)]
struct Modalities {
    #[serde(default)]
    text_tokens: u64,
    #[serde(default)]
    image_tokens: u64,
}

impl ImageUsage {
    fn units(self) -> Option<Units> {
        // Without a breakdown, an image model's output is the image itself.
        let output = self.output_tokens_details.unwrap_or(Modalities {
            text_tokens: 0,
            image_tokens: self.output_tokens,
        });
        let input = self.input_tokens_details;
        if input.text_tokens.checked_add(input.image_tokens) != Some(self.input_tokens)
            || output.text_tokens.checked_add(output.image_tokens) != Some(self.output_tokens)
            || self.total_tokens.is_some_and(|total| {
                self.input_tokens.checked_add(self.output_tokens) != Some(total)
            })
        {
            return None;
        }
        Some(Units {
            tokens: [input.text_tokens, 0, 0, output.text_tokens],
            image_input_tokens: Some(input.image_tokens),
            image_output_tokens: Some(output.image_tokens),
            ..Units::default()
        })
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TranscriptionUsage {
    Duration {
        seconds: f64,
    },
    Tokens {
        input_tokens: u64,
        output_tokens: u64,
        total_tokens: Option<u64>,
        input_token_details: TranscriptionInput,
    },
}

#[derive(Deserialize)]
struct TranscriptionInput {
    text_tokens: Option<u64>,
    audio_tokens: Option<u64>,
}

impl TranscriptionUsage {
    fn units(self) -> Option<Units> {
        match self {
            Self::Duration { seconds } => {
                let milliseconds = (seconds * 1000.0).round();
                if !seconds.is_finite() || seconds < 0.0 || milliseconds >= u64::MAX as f64 {
                    return None;
                }
                Some(Units {
                    audio_duration_ms: Some(milliseconds as u64),
                    ..Units::default()
                })
            }
            Self::Tokens {
                input_tokens,
                output_tokens,
                total_tokens,
                input_token_details,
            } => {
                if let Some(total) = total_tokens
                    && input_tokens.checked_add(output_tokens) != Some(total)
                {
                    return None;
                }
                let audio = match input_token_details.audio_tokens {
                    Some(audio) => audio,
                    None => input_tokens.checked_sub(input_token_details.text_tokens?)?,
                };
                let text = input_tokens.checked_sub(audio)?;
                if input_token_details
                    .text_tokens
                    .is_some_and(|reported| reported != text)
                {
                    return None;
                }
                Some(Units {
                    tokens: [text, 0, 0, output_tokens],
                    audio_input_tokens: Some(audio),
                    ..Units::default()
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MESSAGE_EVENTS: &str = concat!(
        "event: message_start\r\n",
        "data: {\"type\":\"message_start\",\"message\":{\"usage\":{",
        "\"input_tokens\":8,\"cache_read_input_tokens\":4,\"output_tokens\":1}}}\r\n\r\n",
        "event: message_delta\r\n",
        "data: {\"type\":\"message_delta\",\r\n",
        "data: \"usage\":{\"output_tokens\":5}}\r\n\r\n",
        "event: message_delta\r\n",
        "data: {\"type\":\"message_delta\",\"usage\":{\"input_tokens\":9,",
        "\"cache_creation_input_tokens\":2,\"output_tokens\":6}}\r\n\r\n",
    );

    const RESPONSE: &str = concat!(
        r#"{"usage":{"input_tokens":12,"output_tokens":5,"input_tokens_details":{"#,
        r#""cached_tokens":5,"cache_write_tokens":3}}}"#,
    );

    #[test]
    fn observes_anthropic_cumulative_stream_usage_at_message_stop() {
        let mut observer = NativeUsage::new(Surface::Messages, "text/event-stream; charset=utf-8");
        for byte in MESSAGE_EVENTS.as_bytes() {
            observer.feed(std::slice::from_ref(byte));
        }
        assert_eq!(observer.terminal(), None);
        observer.feed(b"event: message_stop\r\ndata: {\"type\":\"message_stop\"}\r\n");
        assert_eq!(observer.finish(), None);
        observer.feed(b"\r");
        assert_eq!(observer.terminal(), Some(Units::from([9, 4, 2, 6])));
        observer.feed(b"\n");
        assert_eq!(observer.finish(), Some(Units::from([9, 4, 2, 6])));
    }

    #[test]
    fn observes_responses_terminal_usage_after_oversized_content_event() {
        for kind in [
            "response.completed",
            "response.incomplete",
            "response.failed",
        ] {
            let mut observer = NativeUsage::new(Surface::Responses, "text/event-stream");
            observer.feed(b"event: response.output_text.delta\ndata: ");
            observer.feed(&vec![b'x'; MAX_BUFFER_BYTES]);
            observer.feed(b"\n\n");
            let events = format!(
                "data: {{\"type\":\"response.created\",\"response\":{{}}}}\n\n\
                 data: {{\"type\":\"{kind}\",\"response\":{RESPONSE}}}\n\n"
            );
            for chunk in events.as_bytes().chunks(3) {
                observer.feed(chunk);
            }
            assert_eq!(observer.terminal(), Some(Units::from([4, 5, 3, 5])));
        }
    }

    #[test]
    fn reads_buffered_json_only_at_clean_eof() {
        let mut observer = NativeUsage::new(Surface::Responses, "Application/JSON");
        for chunk in RESPONSE.as_bytes().chunks(3) {
            observer.feed(chunk);
        }
        assert_eq!(observer.terminal(), None);
        assert_eq!(observer.finish(), Some(Units::from([4, 5, 3, 5])));

        let mut observer = NativeUsage::new(Surface::Messages, "application/json");
        observer.feed(br#"{"usage":{"input_tokens":5,"output_tokens":3}}"#);
        assert_eq!(observer.finish(), Some(Units::from([5, 0, 0, 3])));
    }

    #[test]
    fn drops_usage_for_missing_invalid_or_oversized_data() {
        for raw in [
            br#"{"usage":null}"#.as_slice(),
            br#"{"usage":{"input_tokens":5}}"#,
            br#"{"usage":{"input_tokens":5,"output_tokens":1,"input_tokens_details":{"cached_tokens":4,"cache_write_tokens":2}}}"#,
        ] {
            let mut observer = NativeUsage::new(Surface::Responses, "application/json");
            observer.feed(raw);
            assert_eq!(observer.finish(), None);
        }

        let mut observer = NativeUsage::new(Surface::Responses, "application/json");
        observer.feed(&vec![b' '; MAX_BUFFER_BYTES]);
        observer.feed(RESPONSE.as_bytes());
        assert_eq!(observer.finish(), None);

        let mut observer = NativeUsage::new(Surface::Messages, "text/event-stream");
        observer.feed(MESSAGE_EVENTS.as_bytes());
        observer.feed(b"event: error\ndata: {\"type\":\"error\"}\n\n");
        observer.feed(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n");
        assert_eq!(observer.finish(), None);
    }

    #[test]
    fn reads_embedding_usage_from_a_large_batch() {
        let mut observer = NativeUsage::new(Surface::Embeddings, "application/json");
        observer.feed(br#"{"data":[{"embedding":["#);
        let vector = "0.1234567890123456,".repeat(32 * 1024);
        assert!(vector.len() > MAX_BUFFER_BYTES);
        for chunk in vector.as_bytes().chunks(64 * 1024) {
            observer.feed(chunk);
        }
        observer.feed(br#"0]}],"usage":{"prompt_tokens":53,"total_tokens":53}}"#);
        assert_eq!(observer.finish(), Some(Units::from([53, 0, 0, 0])));

        let mut observer = NativeUsage::new(Surface::Embeddings, "application/json");
        observer.feed(br#"{"usage":{"prompt_tokens":8,"input_tokens":9}}"#);
        assert_eq!(observer.finish(), None);

        let mut observer = NativeUsage::new(Surface::Embeddings, "application/json");
        observer.feed(br#"{"usage":{"total_tokens":53}}"#);
        assert_eq!(observer.finish(), Some(Units::from([53, 0, 0, 0])));
    }

    #[test]
    fn reads_transcription_duration_without_allocating_transcript_text() {
        let mut observer = NativeUsage::new(Surface::Transcriptions, "application/json");
        observer.feed(br#"{"text":"hello\nworld","usage":{"type":"duration","seconds":1.147813}}"#);
        assert!(observer.has_transcript());
        assert_eq!(
            observer.finish(),
            Some(Units {
                audio_duration_ms: Some(1148),
                ..Units::default()
            })
        );
        assert_eq!(observer.terminal(), None);

        let mut observer = NativeUsage::new(Surface::Transcriptions, "application/json");
        observer.feed(br#"{"text":"hello""#);
        assert!(!observer.has_transcript());
        observer.feed(b"}");
        assert!(observer.has_transcript());
        assert_eq!(observer.finish(), None);

        let mut observer = NativeUsage::new(Surface::Transcriptions, "application/json");
        observer.feed(br#"{"text":42}"#);
        assert!(!observer.has_transcript());
    }

    #[test]
    fn reads_image_usage_after_megabytes_of_base64_and_escaped_text() {
        let mut observer = NativeUsage::new(Surface::Images, "application/json");
        let prompt = format!("{}\\\"\\u00e9\\ud83d\\ude00 é ", "\\\"x".repeat(200));
        observer.feed(
            format!(r#"{{"data":[{{"revised_prompt":"{prompt}{prompt}","b64_json":""#).as_bytes(),
        );
        for _ in 0..64 {
            observer.feed(&[b'A'; 64 * 1024]);
        }
        let usage = r#"{"input_tokens":12,"output_tokens":20,"total_tokens":32,"input_tokens_details":{"text_tokens":9,"image_tokens":3}}"#;
        for chunk in format!(r#"=="}}],"usage":{usage}}}"#).as_bytes().chunks(5) {
            observer.feed(chunk);
        }
        assert_eq!(
            observer.finish(),
            Some(Units {
                tokens: [9, 0, 0, 0],
                image_input_tokens: Some(3),
                image_output_tokens: Some(20),
                ..Units::default()
            })
        );
    }

    #[test]
    fn drops_inconsistent_or_incomplete_image_usage() {
        for usage in [
            r#"{"input_tokens":5,"output_tokens":2}"#,
            r#"{"input_tokens":5,"output_tokens":2,"input_tokens_details":{"text_tokens":4}}"#,
            r#"{"input_tokens":5,"output_tokens":2,"total_tokens":8,"input_tokens_details":{"text_tokens":5}}"#,
            r#"{"input_tokens":5,"output_tokens":2,"input_tokens_details":{"text_tokens":5},"output_tokens_details":{"image_tokens":1}}"#,
        ] {
            let mut observer = NativeUsage::new(Surface::Images, "application/json");
            observer.feed(format!(r#"{{"data":[],"usage":{usage}}}"#).as_bytes());
            assert_eq!(observer.finish(), None, "{usage}");
        }
        let mut observer = NativeUsage::new(Surface::Images, "text/event-stream");
        observer
            .feed(br#"{"usage":{"input_tokens":0,"output_tokens":0,"input_tokens_details":{}}}"#);
        assert_eq!(observer.finish(), None);
    }

    #[test]
    fn reads_transcription_token_modalities_at_stream_end() {
        let mut observer = NativeUsage::new(Surface::Transcriptions, "text/event-stream");
        let data = concat!(
            "data: {\"type\":\"transcript.text.delta\",\"delta\":\"hello\"}\n\n",
            "data: {\"type\":\"transcript.text.done\",\"usage\":{\"type\":\"tokens\",",
            "\"input_tokens\":14,\"output_tokens\":5,\"total_tokens\":19,",
            "\"input_token_details\":{\"text_tokens\":2,\"audio_tokens\":12}}}\n\n",
        );
        for chunk in data.as_bytes().chunks(7) {
            observer.feed(chunk);
        }
        assert_eq!(
            observer.terminal(),
            Some(Units {
                tokens: [2, 0, 0, 5],
                audio_input_tokens: Some(12),
                ..Units::default()
            })
        );

        let mut observer = NativeUsage::new(Surface::Transcriptions, "application/json");
        observer.feed(br#"{"usage":{"type":"tokens","input_tokens":5,"output_tokens":2}}"#);
        assert_eq!(observer.finish(), None);
    }
}
