use crate::config::{Surface, Upstream, valid_anthropic_version};
use crate::{queue::Dispatch, rate::RateLimited};
use axum::body::Body;
use axum::http::StatusCode;
use axum::http::header::{
    AUTHORIZATION, CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HeaderMap,
    HeaderName, TRANSFER_ENCODING,
};
use axum::response::Response;
use bytes::Bytes;
use futures_util::stream;
use memchr::memmem::Finder;
use reqwest::Client;
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, timeout_at};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_ERROR_BYTES: usize = 1024 * 1024;
const REDACTED: &[u8] = b"[redacted]";
const STALE_BODY_METADATA: &[&str] = &[
    "content-length",
    "content-encoding",
    "content-range",
    "content-location",
    "etag",
    "last-modified",
    "content-md5",
    "digest",
    "content-digest",
    "repr-digest",
    "signature",
    "signature-input",
];

pub struct Failure {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: &'static str,
    pub reached: &'static str,
    pub attempted: bool,
    pub retry_after: Option<u64>,
}

impl Failure {
    fn input(message: &'static str) -> Self {
        Self {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            code: "unsupported_content",
            message,
            reached: "false",
            attempted: false,
            retry_after: None,
        }
    }

    fn timeout(reached: &'static str) -> Self {
        Self {
            status: StatusCode::GATEWAY_TIMEOUT,
            code: "upstream_timeout",
            message: "The upstream response timed out.",
            reached,
            attempted: true,
            retry_after: None,
        }
    }

    fn protocol(reached: &'static str) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            code: "upstream_protocol_error",
            message: "The upstream returned an unusable response.",
            reached,
            attempted: true,
            retry_after: None,
        }
    }

    fn send(error: reqwest::Error) -> Self {
        if error.is_connect() {
            Self {
                status: StatusCode::BAD_GATEWAY,
                code: "upstream_unreachable",
                message: "The upstream could not be reached.",
                reached: "false",
                attempted: true,
                retry_after: None,
            }
        } else if error.is_timeout() {
            Self::timeout("unknown")
        } else {
            Self::protocol("unknown")
        }
    }
}

impl From<RateLimited> for Failure {
    fn from(error: RateLimited) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            code: error.code,
            message: error.message,
            reached: "false",
            attempted: false,
            retry_after: Some(error.retry_after()),
        }
    }
}

pub fn client(extra_ca: Option<&Path>) -> Result<Client, &'static str> {
    let mut roots =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = extra_ca {
        let certificates =
            CertificateDer::pem_file_iter(path).map_err(|_| "could not open SVINCOLO_EXTRA_CA")?;
        let before = roots.len();
        for certificate in certificates {
            let certificate = certificate.map_err(|_| "invalid PEM in SVINCOLO_EXTRA_CA")?;
            roots
                .add(certificate)
                .map_err(|_| "invalid certificate in SVINCOLO_EXTRA_CA")?;
        }
        if roots.len() == before {
            return Err("SVINCOLO_EXTRA_CA must contain at least one certificate");
        }
    }
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("the ring provider supports rustls default protocol versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Client::builder()
        .tls_backend_preconfigured(tls)
        .connect_timeout(CONNECT_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .no_gzip()
        .no_brotli()
        .no_zstd()
        .no_deflate()
        .build()
        .map_err(|_| "TLS client initialisation failed")
}

pub struct OutboundBody {
    pub body: reqwest::Body,
    pub len: usize,
    pub rewritten: bool,
}

pub struct Target<'a> {
    pub client: &'a Client,
    pub upstream: &'a Upstream,
    pub headers: &'a HeaderMap,
    pub query: Option<&'a str>,
}

pub async fn forward(
    target: &Target<'_>,
    surface: Surface,
    body: OutboundBody,
    admission: &mut Dispatch<'_>,
) -> Result<Response, Failure> {
    let Target {
        client,
        upstream,
        headers,
        query,
    } = *target;
    let mut url = upstream.endpoint(surface);
    url.set_query(query);
    if url.query() != query {
        return Err(Failure::input(
            "Percent-encode special characters in the query string.",
        ));
    }

    let mut request_headers = relay_headers(headers, None);
    if body.rewritten {
        for name in STALE_BODY_METADATA {
            request_headers.remove(*name);
        }
    }
    request_headers.insert(CONTENT_LENGTH, body.len.into());
    if let Some(default) = &upstream.anthropic_version {
        const VERSION: &str = "anthropic-version";
        let all = headers.get_all(VERSION);
        let mut values = all.iter();
        if let Some(value) = values.next()
            && (!valid_anthropic_version(value.as_bytes()) || values.next().is_some())
        {
            return Err(Failure::input(
                "Supply one Anthropic API version as YYYY-MM-DD.",
            ));
        }
        request_headers
            .entry(VERSION)
            .or_insert_with(|| default.clone());
    }
    if let Some((name, value)) = &upstream.credential {
        request_headers.insert(name.clone(), value.clone());
    }

    admission.admit().await?;
    let deadline = Instant::now() + REQUEST_TIMEOUT;
    let mut upstream_response = timeout_at(
        deadline,
        client
            .post(url)
            .headers(request_headers)
            .body(body.body)
            .send(),
    )
    .await
    .map_err(|_| Failure::timeout("unknown"))?
    .map_err(Failure::send)?;
    let status = upstream_response.status();
    if status.is_redirection() {
        return Err(Failure {
            status: StatusCode::BAD_GATEWAY,
            code: "upstream_redirect",
            message: "The upstream returned a redirect.",
            reached: "true",
            attempted: true,
            retry_after: None,
        });
    }
    if status.is_informational() {
        return Err(Failure::protocol("true"));
    }

    let secret = upstream.credential.as_ref().map(|(name, value)| {
        let bytes = value.as_bytes();
        if name == AUTHORIZATION {
            bytes.strip_prefix(b"Bearer ").unwrap_or(bytes)
        } else {
            bytes
        }
    });
    let finder = secret.filter(|secret| !secret.is_empty()).map(Finder::new);
    let mut response_headers = relay_headers(upstream_response.headers(), finder.as_ref());
    let body = if status.is_success() {
        let stream_deadline = (!is_streaming(upstream_response.headers())).then_some(deadline);
        streaming_body(upstream_response, stream_deadline)
    } else {
        let (body, changed) = error_body(&mut upstream_response, finder.as_ref(), deadline).await?;
        if changed {
            for name in STALE_BODY_METADATA {
                response_headers.remove(*name);
            }
        }
        Body::from(body)
    };

    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = response_headers;
    Ok(response)
}

fn relay_headers(source: &HeaderMap, secret: Option<&Finder<'_>>) -> HeaderMap {
    let connection_fields = source
        .get_all(CONNECTION)
        .iter()
        .flat_map(|value| value.as_bytes().split(|byte| *byte == b','))
        .filter_map(|field| HeaderName::from_bytes(field.trim_ascii()).ok())
        .collect::<HashSet<_>>();
    source
        .iter()
        .filter(|(name, value)| {
            let header = name.as_str();
            !matches!(
                header,
                "connection"
                    | "keep-alive"
                    | "te"
                    | "trailer"
                    | "transfer-encoding"
                    | "upgrade"
                    | "host"
                    | "authorization"
                    | "x-api-key"
                    | "api-key"
                    | "openai-organization"
                    | "openai-project"
                    | "anthropic-workspace-id"
                    | "anthropic-user-profile-id"
                    | "cookie"
                    | "cookie2"
                    | "set-cookie"
                    | "set-cookie2"
                    | "referer"
                    | "forwarded"
                    | "accept-encoding"
                    | "expect"
            ) && !header.starts_with("proxy-")
                && !header.starts_with("x-forwarded-")
                && !header.starts_with("x-svincolo-")
                && !connection_fields.contains(*name)
                && secret.is_none_or(|secret| secret.find(value.as_bytes()).is_none())
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

fn is_streaming(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .is_some_and(|kind| {
            kind.eq_ignore_ascii_case("text/event-stream")
                || kind.eq_ignore_ascii_case("application/x-ndjson")
        })
}

fn streaming_body(response: reqwest::Response, deadline: Option<Instant>) -> Body {
    Body::from_stream(stream::unfold(Some(response), move |response| async move {
        let mut response = response?;
        let next_deadline = deadline.unwrap_or_else(|| Instant::now() + STREAM_IDLE_TIMEOUT);
        match next_chunk(&mut response, next_deadline).await {
            Ok(Some(chunk)) => Some((Ok(chunk), Some(response))),
            Ok(None) => None,
            Err(failure) => Some((Err(io::Error::other(failure.message)), None)),
        }
    }))
}

async fn next_chunk(
    response: &mut reqwest::Response,
    deadline: Instant,
) -> Result<Option<Bytes>, Failure> {
    if Instant::now() >= deadline {
        return Err(Failure::timeout("true"));
    }
    timeout_at(deadline, response.chunk())
        .await
        .map_err(|_| Failure::timeout("true"))?
        .map_err(|_| Failure::protocol("true"))
}

async fn error_body(
    response: &mut reqwest::Response,
    secret: Option<&Finder<'_>>,
    deadline: Instant,
) -> Result<(Bytes, bool), Failure> {
    let headers = response.headers();
    if !has_only_coding(headers, CONTENT_ENCODING, b"identity")
        || !has_only_coding(headers, TRANSFER_ENCODING, b"chunked")
    {
        return Err(Failure::protocol("true"));
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_ERROR_BYTES as u64)
    {
        return Ok((Bytes::new(), true));
    }
    let mut body = Vec::new();
    while let Some(chunk) = next_chunk(response, deadline).await? {
        if chunk.len() > MAX_ERROR_BYTES - body.len() {
            // A cutoff could leave the prefix of an echoed credential in the body.
            return Ok((Bytes::new(), true));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(redact(body, secret))
}

fn has_only_coding(headers: &HeaderMap, header: HeaderName, coding: &[u8]) -> bool {
    headers
        .get_all(header)
        .iter()
        .flat_map(|value| value.as_bytes().split(|byte| *byte == b','))
        .all(|value| value.trim_ascii().eq_ignore_ascii_case(coding))
}

fn redact(body: Vec<u8>, secret: Option<&Finder<'_>>) -> (Bytes, bool) {
    let Some(secret) = secret.filter(|secret| secret.find(&body).is_some()) else {
        return (Bytes::from(body), false);
    };
    let mut output = Vec::new();
    let mut offset = 0;
    for index in secret.find_iter(&body) {
        if output.len() + index - offset + REDACTED.len() > MAX_ERROR_BYTES {
            return (Bytes::new(), true);
        }
        output.extend_from_slice(&body[offset..index]);
        output.extend_from_slice(REDACTED);
        offset = index + secret.needle().len();
    }
    if output.len() + body.len() - offset > MAX_ERROR_BYTES {
        return (Bytes::new(), true);
    }
    output.extend_from_slice(&body[offset..]);
    if secret.find(&output).is_some() {
        return (Bytes::new(), true);
    }
    (Bytes::from(output), true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[tokio::test]
    async fn client_accepts_the_preconfigured_tls_backend() {
        client(None).unwrap();
    }

    #[test]
    fn drops_all_connection_options_and_keeps_repeated_provider_headers() {
        let mut headers = HeaderMap::new();
        headers.append(CONNECTION, HeaderValue::from_static(" x-first, X-SECOND"));
        headers.append(CONNECTION, HeaderValue::from_static("x-third"));
        for name in [
            "x-first",
            "x-second",
            "x-third",
            "x-svincolo-origin",
            "cookie",
            "api-key",
            "openai-organization",
            "openai-project",
        ] {
            headers.insert(name, HeaderValue::from_static("blocked"));
        }
        headers.append("anthropic-beta", HeaderValue::from_static("one"));
        headers.append("anthropic-beta", HeaderValue::from_static("two"));
        headers.insert("x-echo", HeaderValue::from_static("key is secret"));
        let result = relay_headers(&headers, Some(&Finder::new(b"secret")));
        assert_eq!(result.len(), 2);
        let mut values = result.get_all("anthropic-beta").iter();
        assert_eq!(values.next().unwrap(), "one");
        assert_eq!(values.next().unwrap(), "two");
    }

    #[test]
    fn redacts_each_literal_without_decoding_or_expanding_past_limit() {
        let finder = Finder::new(b"secret");
        let (bytes, changed) = redact(b"\xffsecret secret\x00".to_vec(), Some(&finder));
        assert!(changed);
        assert_eq!(bytes, b"\xff[redacted] [redacted]\x00".as_slice());
        let (bytes, changed) = redact(vec![b'x'; MAX_ERROR_BYTES], Some(&Finder::new(b"x")));
        assert!(changed);
        assert!(bytes.is_empty());
    }
}
