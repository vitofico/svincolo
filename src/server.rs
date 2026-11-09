use std::{
    convert::Infallible,
    future::Future,
    io::{self, Write},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use axum::{
    body::{Body, BodyDataStream, Bytes},
    extract::Request,
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
    serve::Listener,
};
use futures_util::StreamExt;
use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use sha2::{Digest, Sha256};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{oneshot, watch},
    task::JoinSet,
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use uuid::Uuid;

use crate::{
    client_socket::ClientSocket,
    config::{
        Caller, Config, FileInputs, Locality, OverLimit, Priority, Route, Surface, format_revision,
    },
    managed,
    model::{Model, ModelError, read_model, read_model_prefix},
    multipart::Boundary,
    queue::{self, CallerLimit, Dispatch, Limit, Slots, Wait},
    rate::{AttemptBudget, Rate},
    state::{self, Admission, Capacity, ReloadSignal, Reservation, Snapshot},
    upstream,
};

struct App {
    current: watch::Sender<Arc<Snapshot>>,
    client: reqwest::Client,
    budget: Arc<Capacity>,
    active: Arc<Capacity>,
    queue_bytes: Arc<Capacity>,
    draining: watch::Sender<bool>,
}

pub async fn serve(config: Config, path: PathBuf, inputs: FileInputs) -> Result<(), String> {
    let ca = std::env::var_os("SVINCOLO_EXTRA_CA");
    let client = upstream::client(ca.as_deref().map(std::path::Path::new))?;
    let mut signals =
        ShutdownSignals::new().map_err(|error| format!("signal setup failed: {error}"))?;
    let reload_signal =
        ReloadSignal::new().map_err(|error| format!("reload signal setup failed: {error}"))?;
    let listener = TcpListener::bind(config.listen)
        .await
        .map_err(|error| format!("bind failed: {error}"))?;
    eprintln!(
        "svincolo: listening on {}",
        listener.local_addr().map_err(|error| error.to_string())?
    );
    let mut admission = Admission::default();
    let cloud = CancellationToken::new();
    if config.local_only {
        cloud.cancel();
    }
    let app = Arc::new(App {
        client,
        budget: Arc::new(Capacity::default()),
        active: Arc::new(Capacity::default()),
        queue_bytes: Arc::new(Capacity::default()),
        current: watch::Sender::new(admission.snapshot(config, cloud)),
        draining: watch::Sender::new(false),
    });
    let (stop_accepting, stop) = oneshot::channel();
    let mut server = std::pin::pin!(serve_connections(listener, Arc::clone(&app), stop));

    tokio::select! {
        () = &mut server => return Ok(()),
        () = signals.recv() => {},
        () = state::reload(path, Arc::new(inputs), admission, &app.current, reload_signal) => unreachable!(),
    }
    app.draining.send_replace(true);
    let (drain_delay, grace) = {
        let snapshot = app.current.borrow();
        (snapshot.config.drain_delay_s, snapshot.config.grace_s)
    };
    tokio::select! {
        () = &mut server => return Ok(()),
        () = signals.recv() => return Ok(()),
        () = tokio::time::sleep(Duration::from_secs(drain_delay)) => {},
    }
    let _ = stop_accepting.send(());

    let result = tokio::select! {
        () = signals.recv() => return Ok(()),
        result = tokio::time::timeout(Duration::from_secs(grace), server) => result,
    };
    match result {
        Ok(()) => Ok(()),
        Err(_) => {
            eprintln!("svincolo: shutdown grace period expired");
            Ok(())
        }
    }
}

const HEADER_TIMEOUT: Duration = Duration::from_secs(10);

async fn serve_connections(
    mut listener: TcpListener,
    app: Arc<App>,
    mut stop: oneshot::Receiver<()>,
) {
    let shutdown = CancellationToken::new();
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = &mut stop => break,
            _ = connections.join_next(), if !connections.is_empty() => {},
            (stream, _) = Listener::accept(&mut listener) => {
                let limit = app.current.borrow().config.max_connections;
                if connections.len() < limit {
                    connections.spawn(serve_connection(stream, Arc::clone(&app), shutdown.clone()));
                }
            }
        }
    }
    drop(listener);
    shutdown.cancel();
    while connections.join_next().await.is_some() {}
}

async fn serve_connection(stream: TcpStream, app: Arc<App>, shutdown: CancellationToken) {
    let stream = ClientSocket::new(stream);
    let peer = stream.clone();
    let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
        let app = Arc::clone(&app);
        let peer = peer.clone();
        async move { Ok::<_, Infallible>(dispatch(app, request.map(Body::new), &peer).await) }
    });
    let mut http = http1::Builder::new();
    http.timer(TokioTimer::new())
        .header_read_timeout(HEADER_TIMEOUT);
    let mut connection = std::pin::pin!(http.serve_connection(TokioIo::new(stream), service));
    tokio::select! {
        _ = &mut connection => {},
        () = shutdown.cancelled() => {
            connection.as_mut().graceful_shutdown();
            let _ = connection.await;
        }
    }
}

struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    fn new() -> io::Result<Self> {
        #[cfg(unix)]
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            #[cfg(unix)]
            interrupt: signal(SignalKind::interrupt())?,
            #[cfg(unix)]
            terminate: signal(SignalKind::terminate())?,
        })
    }

    #[cfg(unix)]
    async fn recv(&mut self) {
        tokio::select! {
            _ = self.interrupt.recv() => {},
            _ = self.terminate.recv() => {},
        }
    }

    #[cfg(not(unix))]
    async fn recv(&mut self) {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn dispatch(app: Arc<App>, request: Request, peer: &ClientSocket) -> Response {
    let mut drain = app.draining.subscribe();
    let draining = *drain.borrow_and_update();
    let path = request.uri().path();
    if request.method() == Method::GET && path == "/healthz" {
        return StatusCode::OK.into_response();
    }
    if request.method() == Method::GET && path == "/readyz" && !draining {
        return StatusCode::OK.into_response();
    }
    let mut meta = Metadata::new();
    let (surface, discover) = match (request.method(), path) {
        (&Method::POST, "/v1/chat/completions") => (Surface::Chat, false),
        (&Method::POST, "/v1/responses") => (Surface::Responses, false),
        (&Method::POST, "/v1/embeddings") => (Surface::Embeddings, false),
        (&Method::POST, "/v1/audio/transcriptions") => (Surface::Transcriptions, false),
        (&Method::POST, "/v1/messages" | "/anthropic/v1/messages") => (Surface::Messages, false),
        (&Method::GET, "/v1/models") => (Surface::Chat, true),
        (&Method::GET, "/anthropic/v1/models") => (Surface::Messages, true),
        (&Method::GET, "/readyz") => return meta.error(DRAINING),
        _ => return meta.error(Fault::local(404, "unknown_path", "Endpoint unavailable.")),
    };

    let (parts, body) = request.into_parts();
    let headers = &parts.headers;
    let snapshot = Arc::clone(&app.current.borrow());
    let config = &snapshot.config;
    let caller = if config.callers.is_empty() {
        None
    } else {
        let Some(caller) = caller_token(headers).and_then(|token| config.authenticate(token))
        else {
            return meta.error(Fault::local(
                401,
                "caller_key_unknown",
                "Supply a valid router token.",
            ));
        };
        Some(caller)
    };
    if draining {
        return meta.error(DRAINING);
    }
    if headers.contains_key("x-svincolo-upstream-key") {
        return meta.error(Fault::local(
            403,
            "caller_upstream_key_forbidden",
            "Caller-supplied upstream keys are forbidden.",
        ));
    }
    let require_local = match single_header(headers, "x-svincolo-require-locality") {
        Ok(None) => false,
        Ok(Some(b"local")) => true,
        _ => {
            return meta.error(Fault::local(
                415,
                "unsupported_content",
                "Invalid locality requirement.",
            ));
        }
    };
    if discover {
        return catalog(&snapshot, caller, surface, require_local, &meta);
    }
    let content_type = single_header(headers, "content-type")
        .ok()
        .flatten()
        .and_then(|value| std::str::from_utf8(value).ok())
        .unwrap_or_default();
    let boundary = if surface == Surface::Transcriptions {
        let Some(boundary) = Boundary::from_content_type(content_type) else {
            return meta.error(BAD_MULTIPART);
        };
        Some(boundary)
    } else {
        if !content_type
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .eq_ignore_ascii_case("application/json")
        {
            return meta.error(Fault::local(
                415,
                "unsupported_content",
                "Expected an application/json body.",
            ));
        }
        None
    };
    let bad_body = if boundary.is_some() {
        BAD_MULTIPART
    } else {
        BAD_BODY
    };
    let identity_encoding = match single_header(headers, "content-encoding") {
        Ok(None) => true,
        Ok(Some(value)) => value.eq_ignore_ascii_case(b"identity"),
        Err(()) => false,
    };
    if !identity_encoding {
        return meta.error(Fault::local(
            415,
            "unsupported_content",
            "Expected an uncompressed request body.",
        ));
    }
    let caller_slot = match caller {
        Some(caller) => {
            let max = caller.max_concurrent.unwrap_or(usize::MAX);
            let slot = snapshot.callers[&caller.name].reserve(1, max);
            if slot.is_none() && caller.over_limit == OverLimit::Refuse {
                return meta.error(queue::CALLER_BUSY.into());
            }
            slot
        }
        None => None,
    };
    let Some(active) = app.active.reserve(1, config.max_in_flight) else {
        return meta.error(Fault::local(
            429,
            "server_busy",
            "The server is at capacity.",
        ));
    };
    let length = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let mut upload = match Upload::new(config, &app.budget, body, length, bad_body) {
        Ok(upload) => upload,
        Err(fault) => return meta.error(fault),
    };
    let admission_drain = drain.clone();
    let admission_for = |route: &Route, slots, priority, wait_ms| {
        let upstream = &snapshot.upstreams[&route.upstream.name];
        let rate = |per_minute| Rate {
            per_minute,
            generation: snapshot.generation,
            effective_at: snapshot.published_at,
        };
        Dispatch {
            active: Limit {
                capacity: &app.active,
                max: config.max_in_flight,
            },
            caller: caller.map(|caller| CallerLimit {
                name: &caller.name,
                limit: Limit {
                    capacity: &snapshot.callers[&caller.name],
                    max: caller.max_concurrent.unwrap_or(usize::MAX),
                },
                over_limit: caller.over_limit,
            }),
            upstream: Limit {
                capacity: upstream,
                max: route.upstream.max_concurrent.unwrap_or(usize::MAX),
            },
            wait: Wait {
                limit: route.upstream.queue_limit,
                duration: Duration::from_millis(wait_ms),
                priority,
                budget: &app.queue_bytes,
                bytes: 0,
                max_bytes: (config.max_buffered_bytes / 2)
                    .min(config.max_buffered_bytes - config.max_body_bytes),
                resumed: None,
            },
            slots,
            rate: AttemptBudget::new(
                caller.map(|caller| {
                    (
                        &snapshot.callers[&caller.name].rate,
                        rate(caller.requests_per_minute),
                    )
                }),
                (&upstream.rate, rate(route.upstream.requests_per_minute)),
            ),
            drain: admission_drain.clone(),
            peer,
        }
    };
    let mut slots = Slots::upload(active, caller_slot);
    let mut provisional = None;
    let mut upload_cloud = None;
    if snapshot.upload_queue {
        let prefix = tokio::select! {
            biased;
            _ = drain.wait_for(|draining| *draining) => return meta.error(DRAINING),
            result = upload.prefix(boundary.as_ref()) => result,
        };
        let name = match prefix {
            Ok(name) => name,
            Err(fault) => return meta.error(fault),
        };
        if let Some(name) = name
            && let Some(route) = config.routes.get(&name)
            && route.upstream.queue_limit > 0
            && caller.is_none_or(|caller| caller.permits(&name))
            && route_policy(
                route,
                surface,
                headers,
                snapshot.cloud.is_cancelled(),
                require_local,
            )
            .is_ok()
            && let Ok((priority, wait_ms)) =
                queue_preferences(headers, caller, route.upstream.max_wait_ms)
            && wait_ms > 0
        {
            let mut admission = admission_for(route, std::mem::take(&mut slots), priority, wait_ms);
            admission.wait.bytes = upload.retained();
            upload_cloud = (route.locality == Locality::Cloud).then_some(&snapshot.cloud);
            let wait_started = tokio::time::Instant::now();
            let admitted = tokio::select! {
                biased;
                () = cloud_cancelled(upload_cloud) => Err(LOCAL_ONLY),
                result = admission.resume_upload(|| upload.release_unused()) => result.map_err(Into::into),
            };
            if let Err(fault) = admitted {
                meta.route = Some((&name, route));
                return meta.error(fault);
            }
            upload.deadline += wait_started.elapsed();
            provisional = Some((name, admission));
        }
    }
    let body = tokio::select! {
        biased;
        _ = drain.wait_for(|draining| *draining) => return meta.error(DRAINING),
        () = cloud_cancelled(upload_cloud) => return meta.error(LOCAL_ONLY),
        result = upload.finish() => result,
    };
    let (body, retained_bytes) = match body {
        Ok(body) => body,
        Err(fault) => return meta.error(fault),
    };
    let parsed = match &boundary {
        Some(boundary) => boundary.read_model(&body),
        None => read_model(&body),
    };
    let model = match parsed {
        Ok(model) => model,
        Err(ModelError::Invalid) => return meta.error(bad_body),
        Err(ModelError::Missing) => return meta.error(NO_ROUTE),
        Err(ModelError::Duplicate) => {
            return meta.error(Fault::local(
                415,
                "ambiguous_model",
                "More than one model was supplied.",
            ));
        }
    };
    let Some(route) = config.routes.get(&model.name) else {
        return meta.error(NO_ROUTE);
    };
    if caller.is_some_and(|caller| !caller.permits(&model.name)) {
        return meta.error(Fault::local(
            403,
            "route_not_permitted",
            "This caller cannot use the route.",
        ));
    }
    meta.route = Some((&model.name, route));
    if let Err(fault) = route_policy(
        route,
        surface,
        headers,
        snapshot.cloud.is_cancelled(),
        require_local,
    ) {
        return meta.error(fault);
    }
    let cloud = (route.locality == Locality::Cloud).then_some(&snapshot.cloud);
    if *drain.borrow() {
        return meta.error(DRAINING);
    }
    let mut admission = if let Some((name, admission)) = provisional {
        if name != model.name {
            return meta.error(bad_body);
        }
        admission
    } else {
        let (priority, wait_ms) =
            match queue_preferences(headers, caller, route.upstream.max_wait_ms) {
                Ok(preferences) => preferences,
                Err(fault) => return meta.error(fault),
            };
        admission_for(route, slots, priority, wait_ms)
    };
    let settings = route.managed.as_ref().filter(|_| surface == Surface::Chat);
    admission.wait.bytes =
        retained_bytes + settings.map_or(0, |_| managed::WORK_RESERVATION as usize);
    let started = Instant::now();
    let mut working = None;
    let target = upstream::Target {
        client: &app.client,
        upstream: &route.upstream,
        headers,
        query: parts.uri.query(),
    };
    let dispatch = async {
        if let Some(settings) = settings {
            let no_retry = match single_header(headers, "x-svincolo-no-retry") {
                Ok(None | Some(b"false")) => false,
                Ok(Some(b"true")) => true,
                _ => {
                    return meta.error(Fault::local(
                        415,
                        "unsupported_content",
                        "Invalid retry preference.",
                    ));
                }
            };
            let Some(permit) = app.budget.reserve(
                managed::WORK_RESERVATION as usize,
                config.max_buffered_bytes,
            ) else {
                return meta.error(BODY_BUDGET);
            };
            working = Some(permit);
            let prepared = match managed::prepare(&body, &route.upstream_model, settings) {
                Ok(prepared) => prepared,
                Err(error) => return meta.error(error.into()),
            };
            drop(body);
            meta.dropped_params = prepared.dropped_params.join(",");
            let mut report = managed::Report::default();
            let result = prepared
                .run(&target, &mut admission, no_retry, &mut report)
                .await;
            meta.attempts = report.attempts;
            meta.structured_mode = report.mode.map(|mode| mode.as_str());
            match result {
                Ok(mut response) => {
                    meta.write_headers(response.headers_mut(), "upstream", "true");
                    response
                }
                Err(error) => meta.error_with_details(error.failure.into(), error.details),
            }
        } else {
            let body = match outbound_body(
                body,
                &model,
                &route.upstream_model,
                config.max_body_bytes,
                boundary.as_ref(),
            ) {
                Ok(outbound) => outbound,
                Err(fault) => return meta.error(fault),
            };
            match upstream::forward(&target, surface, body, &mut admission).await {
                Ok(mut response) => {
                    meta.attempts = 1;
                    meta.write_headers(response.headers_mut(), "upstream", "true");
                    response
                }
                Err(error) => {
                    meta.attempts = u8::from(error.attempted);
                    meta.error(error.into())
                }
            }
        }
    };
    let response = if let Some(cloud) = cloud {
        let response = tokio::select! {
            biased;
            () = cloud.cancelled() => None,
            response = dispatch => Some(response),
        };
        let Some(response) = response else {
            let reached = if admission.slots.dispatched() {
                log_locality_cut(&meta.id);
                "unknown"
            } else {
                "false"
            };
            return meta.error(Fault {
                reached,
                ..LOCAL_ONLY
            });
        };
        response
    } else {
        dispatch.await
    };
    if meta.attempts > 0 {
        let headers = response.headers();
        let event = serde_json::json!({
            "event": "destination",
            "stage": "response_head",
            "request_id": meta.id,
            "caller": caller.map_or("anonymous", |caller| caller.name.as_str()),
            "route": model.name,
            "route_revision": route.revision,
            "provider": route.upstream.provider,
            "upstream_model": route.upstream_model,
            "locality": route.locality.as_str(),
            "surface": surface.as_str(),
            "structured_mode": meta.structured_mode,
            "attempts": meta.attempts,
            "status": response.status().as_u16(),
            "origin": headers["x-svincolo-origin"].to_str().expect("origin metadata"),
            "provider_reached": headers["x-svincolo-provider-reached"].to_str().expect("reach metadata"),
            "duration_ms": started.elapsed().as_millis(),
        });
        let _ = writeln!(io::stdout().lock(), "{event}");
    }
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(ActiveResponse {
            body,
            _slots: admission.slots,
            _working: working,
            cloud_cancel: cloud.map(|token| Box::pin(token.clone().cancelled_owned())),
            request_id: meta.id,
        }),
    )
}

fn outbound_body(
    body: Bytes,
    model: &Model,
    target: &str,
    limit: usize,
    boundary: Option<&Boundary>,
) -> Result<upstream::OutboundBody, Fault> {
    if target == model.name {
        return Ok(upstream::OutboundBody {
            len: body.len(),
            body: body.into(),
            rewritten: false,
        });
    }

    let replacement = match boundary {
        Some(boundary) if boundary.allows_value(target) => target.to_owned(),
        Some(_) => return Err(BAD_MULTIPART),
        None => serde_json::to_string(target).expect("serializing a model string"),
    };
    let len = body.len() - model.span.len() + replacement.len();
    if len > limit {
        return Err(TOO_LARGE);
    }
    // Both slices retain the original buffer and its ingress budget reservation.
    let chunks = [
        body.slice(..model.span.start),
        Bytes::from(replacement),
        body.slice(model.span.end..),
    ];
    Ok(upstream::OutboundBody {
        body: reqwest::Body::wrap_stream(futures_util::stream::iter(
            chunks.into_iter().map(Ok::<_, Infallible>),
        )),
        len,
        rewritten: true,
    })
}

async fn cloud_cancelled(cloud: Option<&CancellationToken>) {
    if let Some(cloud) = cloud {
        cloud.cancelled().await;
    } else {
        std::future::pending::<()>().await;
    }
}

fn route_policy(
    route: &Route,
    surface: Surface,
    headers: &HeaderMap,
    local_only: bool,
    require_local: bool,
) -> Result<(), Fault> {
    if !route.surfaces.contains(&surface) {
        return Err(Fault::local(
            404,
            "capability_unavailable",
            "This API surface is unavailable on the requested route.",
        ));
    }
    if let Some(fault) = locality_fault(route, local_only, require_local) {
        return Err(fault);
    }
    match single_header(headers, "x-svincolo-expect-route-revision") {
        Ok(None) => {}
        Ok(Some(expected)) if expected == route.revision.as_bytes() => {}
        Ok(Some(expected))
            if expected.len() == route.revision.len()
                && expected
                    .iter()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte)) =>
        {
            return Err(Fault::local(
                409,
                "route_revision_mismatch",
                "The route has changed. Refresh discovery before resubmitting.",
            ));
        }
        _ => {
            return Err(Fault::local(
                400,
                "invalid_route_revision",
                "Supply one lowercase hexadecimal route revision.",
            ));
        }
    }
    Ok(())
}

fn locality_fault(route: &Route, local_only: bool, require_local: bool) -> Option<Fault> {
    match (route.locality, local_only, require_local) {
        (Locality::Cloud, true, _) => Some(LOCAL_ONLY),
        (Locality::Cloud, false, true) => Some(Fault::local(
            403,
            "locality_refused",
            "This request requires a local route.",
        )),
        _ => None,
    }
}

fn log_locality_cut(request_id: &str) {
    let event = serde_json::json!({
        "event": "request_cut",
        "request_id": request_id,
        "outcome": "cut_by_local_only",
    });
    let _ = writeln!(io::stdout().lock(), "{event}");
}

fn catalog(
    snapshot: &Snapshot,
    caller: Option<&Caller>,
    surface: Surface,
    require_local: bool,
    meta: &Metadata<'_>,
) -> Response {
    const MAX_MODELS: usize = 128;
    // Reload can cancel cloud dispatch before it publishes the local-only config.
    let local_only = snapshot.cloud.is_cancelled();
    let mut routes: Vec<_> = snapshot
        .config
        .routes
        .iter()
        .filter(|(name, route)| {
            route.discover
                && route.surfaces.iter().any(|offered| {
                    (*offered == Surface::Messages) == (surface == Surface::Messages)
                })
                && caller.is_none_or(|caller| caller.permits(name))
                && locality_fault(route, local_only, require_local).is_none()
        })
        .collect();
    routes.sort_unstable_by_key(|(name, _)| *name);

    let mut digest = Sha256::new();
    let surface_name = surface.as_str();
    digest.update(surface_name);
    digest.update([u8::from(local_only)]);
    for (_, route) in &routes {
        digest.update(route.revision.as_bytes());
    }
    let revision = format_revision(digest.finalize().into());
    let data: Vec<_> = routes
        .iter()
        .take(MAX_MODELS)
        .map(|(name, route)| {
            let surfaces: serde_json::Map<_, _> = route
                .surfaces
                .iter()
                .map(|surface| {
                    let mode = if *surface == Surface::Chat && route.managed.is_some() {
                        "managed"
                    } else {
                        "raw"
                    };
                    (surface.as_str().to_owned(), mode.into())
                })
                .collect();
            serde_json::json!({
                "id": name,
                "object": "model",
                "type": "model",
                "created": 0,
                "created_at": "1970-01-01T00:00:00Z",
                "display_name": name,
                "owned_by": route.upstream.provider,
                "provider": route.upstream.provider,
                "upstream_model": route.upstream_model,
                "locality": route.locality.as_str(),
                "surfaces": surfaces,
                "route_revision": route.revision,
            })
        })
        .collect();
    let mut body = serde_json::json!({
        "object": "list",
        "data": data,
        "svincolo": {
            "version": env!("CARGO_PKG_VERSION"),
            "api_version": 1,
            "revision": revision,
            "local_only": local_only,
            "partial": routes.len() > MAX_MODELS,
        },
    });
    if surface == Surface::Messages {
        body["first_id"] = data
            .first()
            .map_or(serde_json::Value::Null, |model| model["id"].clone());
        body["last_id"] = data
            .last()
            .map_or(serde_json::Value::Null, |model| model["id"].clone());
        body["has_more"] = serde_json::Value::Bool(false);
    }
    let mut response = Response::new(Body::from(body.to_string()));
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        "x-svincolo-revision",
        HeaderValue::from_str(&revision).expect("hex digest header"),
    );
    meta.write_headers(headers, "router", "false");
    response
}

struct ActiveResponse {
    body: Body,
    _slots: Slots,
    _working: Option<Reservation>,
    cloud_cancel: Option<Pin<Box<WaitForCancellationFutureOwned>>>,
    request_id: String,
}

impl http_body::Body for ActiveResponse {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, axum::Error>>> {
        if self
            .cloud_cancel
            .as_mut()
            .is_some_and(|cancel| cancel.as_mut().poll(cx).is_ready())
        {
            self.cloud_cancel = None;
            self.body = Body::empty();
            log_locality_cut(&self.request_id);
            return Poll::Ready(Some(Err(axum::Error::new(io::Error::other(
                "stream cut by local-only policy",
            )))));
        }
        Pin::new(&mut self.body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}

fn single_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a [u8]>, ()> {
    let values = headers.get_all(name);
    let mut values = values.iter();
    let first = values.next().map(HeaderValue::as_bytes);
    if values.next().is_some() {
        Err(())
    } else {
        Ok(first)
    }
}

fn queue_preferences(
    headers: &HeaderMap,
    caller: Option<&Caller>,
    upstream_wait: u64,
) -> Result<(Priority, u64), Fault> {
    let priority = match single_header(headers, "x-svincolo-priority") {
        Ok(None | Some(b"interactive")) => {
            caller.map_or(Priority::Interactive, |caller| caller.priority)
        }
        Ok(Some(b"background")) => Priority::Background,
        _ => return Err(INVALID_QUEUE_PREFERENCE),
    };
    let wait = match single_header(headers, "x-svincolo-max-wait-ms") {
        Ok(None) => upstream_wait,
        Ok(Some(bytes)) if bytes.iter().all(u8::is_ascii_digit) => std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| text.parse::<u64>().ok())
            .ok_or(INVALID_QUEUE_PREFERENCE)?
            .min(upstream_wait),
        _ => return Err(INVALID_QUEUE_PREFERENCE),
    };
    Ok((priority, wait))
}

fn caller_token(headers: &HeaderMap) -> Option<&[u8]> {
    let bearer = match single_header(headers, "authorization").ok()? {
        None => None,
        Some(value) => {
            let separator = value.iter().position(|byte| *byte == b' ')?;
            let (scheme, token) = value.split_at(separator);
            if !scheme.eq_ignore_ascii_case(b"bearer") {
                return None;
            }
            Some(token.trim_ascii_start())
        }
    };
    let api_key = single_header(headers, "x-api-key").ok()?;
    match (bearer, api_key) {
        (Some(left), Some(right)) if left != right => None,
        _ => bearer.or(api_key),
    }
}

struct BufferedRequest {
    bytes: Vec<u8>,
    _permit: Reservation,
}

impl AsRef<[u8]> for BufferedRequest {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

const PREFIX_LIMIT: usize = 8192;
const UPLOAD_TIMEOUT: Duration = Duration::from_secs(30);

struct Upload {
    stream: BodyDataStream,
    bytes: Vec<u8>,
    permit: Reservation,
    capacity: usize,
    buffer_limit: usize,
    declared: bool,
    eof: bool,
    bad_body: Fault,
    deadline: tokio::time::Instant,
}

impl Upload {
    fn new(
        config: &Config,
        budget: &Arc<Capacity>,
        body: Body,
        declared: Option<u64>,
        bad_body: Fault,
    ) -> Result<Self, Fault> {
        let max = config.max_body_bytes;
        if declared.is_some_and(|length| length > max as u64) {
            return Err(TOO_LARGE);
        }
        let capacity = declared.map_or(max, |length| length as usize);
        let permit = budget
            .reserve(capacity.max(1), config.max_buffered_bytes)
            .ok_or(BODY_BUDGET)?;
        Ok(Self {
            stream: body.into_data_stream(),
            bytes: Vec::new(),
            permit,
            capacity,
            buffer_limit: config.max_buffered_bytes,
            declared: declared.is_some(),
            eof: false,
            bad_body,
            deadline: tokio::time::Instant::now() + UPLOAD_TIMEOUT,
        })
    }

    async fn next(&mut self) -> Result<Option<Bytes>, Fault> {
        if self.eof {
            return Ok(None);
        }
        let frame = self.stream.next().await;
        match frame {
            Some(Ok(chunk)) if chunk.len() <= self.capacity - self.bytes.len() => Ok(Some(chunk)),
            Some(Ok(_)) => Err(TOO_LARGE),
            Some(Err(_)) => Err(self.bad_body),
            None => {
                self.eof = true;
                Ok(None)
            }
        }
    }

    async fn prefix(&mut self, boundary: Option<&Boundary>) -> Result<Option<String>, Fault> {
        tokio::time::timeout_at(self.deadline, self.read_prefix(boundary))
            .await
            .map_err(|_| UPLOAD_TIMED_OUT)?
    }

    async fn read_prefix(&mut self, boundary: Option<&Boundary>) -> Result<Option<String>, Fault> {
        // Limit both the retained prefix and the cost of rescanning tiny frames.
        // A frame crossing the limit goes straight to full-body validation.
        let limit = PREFIX_LIMIT.min(self.capacity / 2);
        let mut scan_at = 1;
        while self.bytes.len() < limit {
            let Some(chunk) = self.next().await? else {
                return Ok(None);
            };
            let len = self.bytes.len() + chunk.len();
            let reserve = if len <= limit {
                len.next_power_of_two().min(limit)
            } else {
                self.capacity
            };
            self.bytes.reserve_exact(reserve - self.bytes.len());
            self.bytes.extend_from_slice(&chunk);
            if len > limit {
                break;
            }
            if len >= scan_at {
                let found = match boundary {
                    Some(boundary) => boundary.read_model_prefix(&self.bytes),
                    None => read_model_prefix(&self.bytes),
                };
                if found.is_some() {
                    return Ok(found);
                }
                scan_at = len.saturating_mul(2).min(limit);
            }
        }
        Ok(None)
    }

    fn retained(&self) -> usize {
        self.bytes.capacity().min(self.capacity).max(1)
    }

    fn release_unused(&mut self) {
        self.permit.shrink_to(self.retained());
    }

    async fn finish(mut self) -> Result<(Bytes, usize), Fault> {
        if !self.permit.grow_to(self.capacity.max(1), self.buffer_limit) {
            return Err(BODY_BUDGET);
        }
        if !self.eof {
            self.bytes.reserve_exact(self.capacity - self.bytes.len());
        }
        tokio::time::timeout_at(self.deadline, self.read_remaining())
            .await
            .map_err(|_| UPLOAD_TIMED_OUT)??;
        if self.declared && self.bytes.len() != self.capacity {
            return Err(self.bad_body);
        }
        // Compact short, unknown-length bodies while both initialized copies
        // still fit inside the original full-body reservation.
        if !self.declared && self.bytes.len() <= self.capacity / 2 {
            self.bytes.shrink_to_fit();
        }
        let retained = self.retained();
        self.release_unused();
        Ok((
            Bytes::from_owner(BufferedRequest {
                bytes: self.bytes,
                _permit: self.permit,
            }),
            retained,
        ))
    }

    async fn read_remaining(&mut self) -> Result<(), Fault> {
        while let Some(chunk) = self.next().await? {
            self.bytes.extend_from_slice(&chunk);
        }
        Ok(())
    }
}

#[cfg(test)]
async fn read_body(
    config: &Config,
    budget: &Arc<Capacity>,
    body: Body,
    declared: Option<u64>,
    bad_body: Fault,
) -> Result<(Bytes, usize), Fault> {
    Upload::new(config, budget, body, declared, bad_body)?
        .finish()
        .await
}

#[derive(Clone, Copy)]
struct Fault {
    status: u16,
    code: &'static str,
    message: &'static str,
    reached: &'static str,
    retry_after: Option<u64>,
}

impl Fault {
    const fn local(status: u16, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
            reached: "false",
            retry_after: None,
        }
    }
}

impl From<upstream::Failure> for Fault {
    fn from(error: upstream::Failure) -> Self {
        Self {
            status: error.status.as_u16(),
            code: error.code,
            message: error.message,
            reached: error.reached,
            retry_after: error.retry_after,
        }
    }
}

const NO_ROUTE: Fault = Fault::local(
    404,
    "route_not_found",
    "No route matches the requested model.",
);
const BAD_BODY: Fault = Fault::local(
    415,
    "body_not_json",
    "Expected a valid JSON object with a string model.",
);
const BAD_MULTIPART: Fault = Fault::local(
    415,
    "body_not_multipart",
    "Expected a multipart/form-data audio upload with a file and a model.",
);
const TOO_LARGE: Fault = Fault::local(413, "body_too_large", "The request exceeds the body limit.");
const UPLOAD_TIMED_OUT: Fault = Fault::local(
    408,
    "request_body_timeout",
    "The request body took too long to arrive.",
);
const BODY_BUDGET: Fault = Fault::local(
    429,
    "body_budget_exhausted",
    "The request buffer is at capacity.",
);
const DRAINING: Fault = Fault::local(503, "draining", "The router is shutting down.");
const INVALID_QUEUE_PREFERENCE: Fault = Fault::local(
    400,
    "invalid_queue_preference",
    "Invalid priority or queue wait preference.",
);
const LOCAL_ONLY: Fault = Fault::local(
    403,
    "local_only",
    "The router is restricted to local routes.",
);

struct Metadata<'a> {
    id: String,
    route: Option<(&'a str, &'a Route)>,
    attempts: u8,
    structured_mode: Option<&'static str>,
    dropped_params: String,
}

impl Metadata<'_> {
    fn new() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            route: None,
            attempts: 0,
            structured_mode: None,
            dropped_params: String::new(),
        }
    }

    fn write_headers(&self, headers: &mut HeaderMap, origin: &'static str, reached: &'static str) {
        headers.insert("x-svincolo-origin", HeaderValue::from_static(origin));
        headers.insert(
            "x-svincolo-provider-reached",
            HeaderValue::from_static(reached),
        );
        headers.insert(
            "x-svincolo-version",
            HeaderValue::from_static(env!("CARGO_PKG_VERSION")),
        );
        headers.insert(
            "x-svincolo-request-id",
            HeaderValue::from_str(&self.id).expect("UUID header"),
        );
        if let Some((name, route)) = self.route {
            let name = HeaderValue::from_str(name).expect("validated route name");
            headers.insert("x-svincolo-route", name.clone());
            headers.insert("x-svincolo-requested-route", name);
            headers.insert(
                "x-svincolo-upstream-model",
                HeaderValue::from_str(&route.upstream_model).expect("validated model ID"),
            );
            headers.insert(
                "x-svincolo-route-revision",
                HeaderValue::from_str(&route.revision).expect("hex digest header"),
            );
            headers.insert(
                "x-svincolo-provider",
                HeaderValue::from_str(&route.upstream.provider).expect("validated provider label"),
            );
            headers.insert(
                "x-svincolo-locality",
                HeaderValue::from_static(route.locality.as_str()),
            );
            headers.insert("x-svincolo-attempts", u16::from(self.attempts).into());
        }
        if let Some(mode) = self.structured_mode {
            headers.insert("x-svincolo-structured-mode", HeaderValue::from_static(mode));
        }
        if !self.dropped_params.is_empty() {
            headers.insert(
                "x-svincolo-dropped-params",
                HeaderValue::from_str(&self.dropped_params).expect("known parameter names"),
            );
        }
    }

    fn error(&self, fault: Fault) -> Response {
        self.error_with_details(fault, None)
    }

    fn error_with_details(&self, fault: Fault, details: Option<serde_json::Value>) -> Response {
        let kind = match fault.status {
            401 => "authentication_error",
            403 => "permission_error",
            404 => "not_found_error",
            408 | 504 => "timeout_error",
            413 => "request_too_large",
            400 | 409 | 415 => "invalid_request_error",
            429 => "rate_limit_error",
            503 => "overloaded_error",
            _ => "api_error",
        };
        let mut body = serde_json::json!({
            "type": "error",
            "error": { "type": kind, "code": fault.code, "message": fault.message, "param": null },
            "request_id": self.id,
        });
        if let Some(serde_json::Value::Object(details)) = details {
            body["error"]
                .as_object_mut()
                .expect("error object")
                .extend(details);
        }
        let mut response = Response::new(Body::from(body.to_string()));
        *response.status_mut() = StatusCode::from_u16(fault.status).expect("valid fault status");
        let headers = response.headers_mut();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert("x-should-retry", HeaderValue::from_static("false"));
        if matches!(fault.status, 429 | 503) {
            headers.insert(header::RETRY_AFTER, fault.retry_after.unwrap_or(1).into());
        }
        self.write_headers(headers, "router", fault.reached);
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn catalog_hides_cancelled_cloud_routes_before_snapshot_publication() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("svincolo.toml");
        std::fs::write(
            &path,
            r#"version = 1
[server]
discover_by_default = false
[[upstream]]
name = "mock"
kind = "openai_compatible"
base_url = "https://mock.invalid/v1"
[[route]]
name = "high"
upstream = "mock"
upstream_model = "model-snapshot"
locality = "cloud"
surfaces = { chat = "raw" }
discover = true
"#,
        )
        .unwrap();
        let cloud = CancellationToken::new();
        let snapshot = Admission::default().snapshot(Config::load(&path).unwrap(), cloud.clone());
        assert!(!snapshot.config.local_only);
        let meta = Metadata::new();
        for local_only in [false, true] {
            if local_only {
                cloud.cancel();
            }
            let response = catalog(&snapshot, None, Surface::Chat, false, &meta);
            let body = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            let data: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(data["svincolo"]["local_only"], local_only);
            let routes = data["data"].as_array().unwrap();
            assert_eq!(routes.len(), usize::from(!local_only));
        }
    }

    #[tokio::test]
    async fn body_budget_stays_reserved_until_the_last_reference_drops() {
        let config = Config {
            listen: "127.0.0.1:0".parse().unwrap(),
            max_in_flight: 1,
            max_connections: 2,
            drain_delay_s: 0,
            grace_s: 1,
            local_only: true,
            max_body_bytes: 4,
            max_buffered_bytes: 4,
            routes: Default::default(),
            callers: Vec::new(),
        };
        let budget = Arc::new(Capacity::default());
        let Ok((body, retained)) =
            read_body(&config, &budget, Body::from("xy"), None, BAD_BODY).await
        else {
            panic!("the first body should fit");
        };
        assert_eq!(retained, 2);
        assert!(
            read_body(&config, &budget, Body::from("ab"), Some(2), BAD_BODY)
                .await
                .is_ok()
        );
        let outbound = body.clone();
        drop(body);
        let Err(fault) = read_body(&config, &budget, Body::from("abc"), Some(3), BAD_BODY).await
        else {
            panic!("the outbound buffer still holds the reservation");
        };
        assert_eq!(fault.code, "body_budget_exhausted");
        drop(outbound);
        assert!(
            read_body(&config, &budget, Body::from("abc"), Some(3), BAD_BODY)
                .await
                .is_ok()
        );
    }
}
