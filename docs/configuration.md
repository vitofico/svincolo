# Svincolo configuration and operations

Start with the [README](../README.md) for a runnable local example.

## Caller authentication

To authenticate callers, add a `[[caller]]` with `name`, `key_file` and an
explicit list of permitted `routes`. Put one caller token per line in the file.
Clients can present it as a bearer token or an `x-api-key`. Configure provider
credentials separately using `key_file` or `key_env` on the upstream. Relative
key-file paths resolve from the config directory.

## Native Anthropic Messages

Configure an Anthropic upstream and a route whose name is the provider's exact
model ID. For example, add the following to a config with cloud routes enabled.

```toml
[[upstream]]
name = "anthropic"
kind = "anthropic"
base_url = "https://api.anthropic.com/v1"
key_env = "ANTHROPIC_API_KEY"
anthropic_version = "2026-10-21"

[[route]]
name = "claude-sonnet-5-5"
upstream = "anthropic"
locality = "cloud"
surfaces = { messages = "raw" }
```

Use the router origin or the origin ending in `/anthropic` as the Anthropic SDK
base URL. The two accepted paths are `/v1/messages` and
`/anthropic/v1/messages`. The router injects the configured provider key. It
preserves the caller's Anthropic API version when supplied and uses the upstream
default otherwise. If using caller tokens, permit the route explicitly.

## Model tiers and discovery

An alias is an ordinary route with an explicit `upstream_model`. To publish
an explicit allowlist, set `discover_by_default = false` in your existing
`[server]` table. Then add the tiers you want to publish with `discover = true`.
This example uses the Anthropic upstream above and the Ollama upstream from
the [quickstart](../README.md#try-it-locally). Replace the quickstart's `low`
route with the one here. The cloud tier requires `local_only = false`.

```toml
[[route]]
name = "low"
upstream = "local-models"
upstream_model = "llama3.2:1b"
locality = "local"
surfaces = { chat = "raw" }
discover = true

[[route]]
name = "middle"
upstream = "local-models"
upstream_model = "llama3.2:3b"
locality = "local"
surfaces = { chat = "raw" }
discover = true

[[route]]
name = "high"
upstream = "anthropic"
upstream_model = "claude-sonnet-5-5"
locality = "cloud"
surfaces = { messages = "raw" }
discover = true

[[caller]]
name = "example-app"
key_file = "example-app.key"
routes = ["low", "high"]
```

Each tier has its own upstream, model ID, and supported API surfaces. Use
immutable provider snapshot IDs where available. To offer only selected model
families, review provider model IDs and update the pins and visible routes in
your config. If a client switches tiers on one API, configure every tier it
can select for that API. With the token in `example-app.key`, this caller
sees `low` in the OpenAI-style catalog and `high` in the Anthropic catalog.
A caller sends `"model": "low"` and the provider receives `llama3.2:1b`.
Raw routes return the provider-native response, with router headers identifying
the requested route and upstream model.

`GET /v1/models` lists Chat Completions, Responses, embedding, and transcription routes.
`GET /anthropic/v1/models` lists native Messages routes. For Anthropic SDK
discovery, use the router origin ending in `/anthropic` as the base URL. Send
the usual caller token. Discovery filters configured routes by permissions and
locality, including `x-svincolo-require-locality: local`. If you omit
`discover_by_default`, eligible routes are listed unless they set
`discover = false`. To block use of a route,
remove it or revoke the caller's permission. The catalog reads local
configuration and contacts no provider.
It lists up to 128 eligible routes in ID order and marks a truncated list
with `svincolo.partial`.

Catalog entries include the resolved model, provider, locality and a
`route_revision`. That revision is also returned as
`x-svincolo-route-revision` on requests that select the route. A changed
model pin or upstream destination changes the revision.

Clients that bind user consent to a destination can send the approved revision
as `x-svincolo-expect-route-revision` on the model request. A mismatch returns
409 `route_revision_mismatch` before contacting the provider. Refresh
discovery and check consent before resubmitting. Require support for this
header on every serving instance before relying on the precondition.

## Audio transcriptions

Enable `surfaces = { transcriptions = "raw" }` on an OpenAI-compatible route.
Clients send a multipart form with one `model` field and one nonempty
`file` part with a filename. The router validates the complete upload before
forwarding, accepts either part order, and preserves the audio bytes. Set
`max_body_bytes = 33554432` under `[server]` if your uploads need more than
the default 16 MiB. The limit includes multipart framing.

## Managed chat

Set `chat = "managed"` in the route's `surfaces` table for an Anthropic,
OpenAI or Ollama upstream. Managed Chat accepts a single non-streaming
Chat Completions answer without tools. Anthropic routes send native Messages
upstream and can also expose `messages = "raw"` for native callers.

Anthropic and OpenAI default to `structured_modes = ["native", "prompt"]`.
For eligible OpenAI schemas, the router makes optional properties required
and nullable upstream. It removes nulls introduced by the rewrite before
checking the original schema. Schemas outside the supported native subset,
such as OpenAI schemas with free-form maps or references, use prompt mode.
[OpenAI Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs?api-mode=chat)

Ollama defaults to prompt mode. For a local server that supports structured
outputs on its `/v1` API, opt in with `structured_modes = ["native", "prompt"]`.
Cloud Ollama routes require prompt mode.
[Ollama structured outputs](https://docs.ollama.com/capabilities/structured-outputs)

Successful structured answers pass offline validation against the original
caller schema in either mode. The router refuses external schema references.
Managed requests and responses have a 1 MiB limit, schemas have a 64 KiB
limit, and each managed call reserves 4 MiB of additional working capacity.

Set `validation_retries = 1` to permit one repair after an invalid answer.
The default is zero. Send `x-svincolo-no-retry: true` to prevent both repair
and provider format fallback for a request. Response headers show the
chosen structured mode, attempt count and dropped fields such as
`temperature`. The destination event on stdout contains routing metadata
and the HTTP status at response head. It omits content and credentials.

## Transport limits

Under `[server]`, `max_in_flight` defaults to 64 and covers active uploads,
upstream waits, and response streams, with a maximum of 5000.
`max_buffered_bytes` separately caps
reserved request bytes. Exhausting either limit returns 429 before egress.

Set `max_concurrent` from 1 to 5000 on a `[[caller]]` or `[[upstream]]` for
an additional per-process limit. All tokens for a caller share its limit,
which covers uploads through response completion. All routes targeting an
upstream share its limit, which covers provider dispatch through response
completion, including managed repair attempts. Exhaustion returns 429
with `Retry-After: 1`. Omitted limits leave admission to the other applicable
limits. Keep a caller's cap below the global and shared upstream caps to
leave slots for other callers, and size the shared byte budget for concurrent
uploads.

Set `requests_per_minute` to a positive integer on a caller or upstream to
add a token bucket. Its burst capacity is one minute's allowance. A caller
uses one token per dispatched request. An upstream uses one per dispatch
attempt, including managed repairs. A refusal before dispatch leaves both
buckets intact and returns 429 with the time to the next token in
`Retry-After`. If an upstream rate limit stops a managed retry, the
response still reports the attempts already sent. Balances carry over
when the config reloads for the same caller or upstream.

Set `queue_limit` on an upstream to enable waiting for its concurrency or
rate limit. The default is zero, which preserves immediate refusals.
`max_wait_ms` defaults to 10000 and is capped at 30000. Callers can opt in to
waiting on their own limits with `over_limit = "queue"`. The queue serves
interactive work before background work, then favors callers with fewer active
requests on that upstream, using arrival order to break ties. Set
`priority = "background"` on a caller or send `x-svincolo-priority: background`
on one request. `x-svincolo-max-wait-ms` can shorten the upstream's wait.

An upload can wait with just its buffered prefix when the router finds a
usable model in the first 8 KiB and within half the declared body length.
For an unknown length, it uses half the configured body limit.
Requests with a late model or a frame crossing that window go through
full-body buffering. A provisional wait releases the upload's active slots
and unused byte reservation. Resuming the upload takes no provider slot or
rate credit. The router validates the complete body before dispatch. Already
validated requests take precedence over provisional uploads, and a resumed
request keeps its original queue deadline if it must wait again.

Retained prefixes, validated bodies and managed preparation count toward
a separate cap of at most half the buffer budget, with room for one
maximum-size request outside the queue. A full queue returns
429 `queue_full`. A deadline returns 429 `queue_timeout` with
`Retry-After: 5`. The router watches the client socket while a request waits,
including when pipelined bytes remain unread. A detected disconnect removes
the request before provider dispatch and leaves its rate credit intact.
Drain also removes waiting requests. Managed retries keep the original
dispatch slot and check the upstream rate without entering the queue again.

The HTTP/1.1 listener closes a connection if the next request header takes
more than ten seconds to arrive. The deadline covers the header read phase.
Set `max_connections` under `[server]` to bound open client connections,
including idle keep-alives and incomplete headers. It defaults to
`max_in_flight + 256`. An explicit limit must exceed `max_in_flight` and
can be at most 16384. The listener closes excess connections before reading
HTTP. A config reload applies the new cap to future connections and lets
open connections finish.

On SIGTERM or SIGINT, readiness drops immediately. `drain_delay_s` defaults
to five seconds before closing the listener. `grace_s` defaults to 300
seconds after that for admitted work to finish.

## Reload configuration and keys

Svincolo checks the config and referenced key files every ten seconds.
Send `SIGHUP` on Unix to check immediately. A valid update takes effect as
one snapshot for new requests. An invalid update leaves the last accepted
snapshot in service and logs a diagnostic. A listener address change requires
a restart. Environment credentials are read from the process environment.

Replace key files atomically. To rotate a caller token, add a second line,
switch the client, then remove the old line. Mount a whole Kubernetes Secret
directory for coordinated changes to routes and keys. In-flight work retains
its snapshot and reservations. Enabling `local_only` also cancels cloud work
already in flight and keeps local work running.

## Upstream TLS

The router negotiates HTTP/1.1 or HTTP/2 with HTTPS upstreams. Set
`SVINCOLO_EXTRA_CA` to a PEM file to trust additional private CA
certificates alongside the compiled-in public roots. The router checks
hostnames and loads this file at startup. An invalid or empty file stops
startup. Restart the router to change its trust bundle.

## Tests

The conformance suite starts a local mock and uses synthetic credentials.

```sh
cargo test --locked
```

The verification workflow runs the Rust suite, Cargo dependency checks, and
native Linux amd64 and arm64 builds. Each image is checked for static linkage,
non-root startup, clean shutdown and the transport budgets. The daily job
reruns the dependency checks against the current advisory database.

The mixed-load check holds chat streams and slow readers while four audio
uploads approach 8 MiB each. It checks process peak RSS, pre-egress byte-budget
refusal, reuse after a cancelled upload and the exact multipart bytes sent to
the pinned model. Pod and node capacity require separate measurements.

Manually dispatch `Verify Svincolo` on `main` to retain the checked binaries,
OCI archives, image SBOMs, provenance and checksums for review. Manual runs
also check full TLS capacity with both upstream protocols. The workflow
uses read-only repository permissions and has no registry credentials.
