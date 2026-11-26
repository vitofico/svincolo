# Svincolo

**One endpoint for your apps. You choose which models they can use.**

Svincolo is a self-hosted LLM router written in Rust. Give each app its own
token and control which routes it can discover and use. An app can keep
asking for `low` while you change the model behind that alias. Provider
credentials stay in Svincolo.

![A writing app can use low for local chat or high for cloud chat. A search app can use local embeddings. Svincolo checks each app's token and sends the request to the model pinned to the route.](docs/assets/routing.svg)

For example, you can offer a writing app local chat under `low` and cloud chat
under `high`, while a search app gets only a local `embeddings` route. Names
like `low`, `middle`, and `high` are yours to define. Each pins one upstream
and model ID. If an app switches tiers through a single API, give those routes
the same API surface. [Configure model tiers and discovery](docs/configuration.md#model-tiers-and-discovery).

- **Native API forwarding.** Use Chat Completions, Responses, embeddings, audio
  transcriptions, image generation, and Anthropic Messages. Raw routes send
  the pinned model ID upstream and return provider-native responses.
  Streaming chat, Responses, and Messages can pass through as streams.
- **Validated structured output.** Managed Chat supports one non-streaming
  answer without tools on Anthropic, OpenAI, and Ollama. Structured answers
  are checked against the caller's original schema.
- **Bounded transport.** Configure caller and upstream limits, opt-in queues,
  and graceful drain. Reload routes and credential files as one snapshot.
- **Per-app cost reporting.** Attribute usage to each caller and upstream model,
  track estimated provider charges, and export a monthly breakdown from the
  [operator dashboard](#operator-cost-dashboard).

## Releases

After the checks on `main` pass, CI publishes a multi-architecture Docker image
and the same static Linux binaries tested inside it. The image and GitHub
Release share a UTC CalVer tag, `YYYY.MM.MICRO`, starting at `0` each month.
Use a release tag to pin a version, or pull the current release with:

```sh
docker pull ghcr.io/vitofico/svincolo:stable
```

For a standalone installation, download `svincolo-linux-amd64` (x86-64) or
`svincolo-linux-arm64` (ARM64) from [GitHub Releases](https://github.com/vitofico/svincolo/releases).
With the binary, `SHA256SUMS`, and `SHA256SUMS.sigstore.json` in the same
directory, verify and install it (shown for amd64):

```sh
cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity https://github.com/vitofico/svincolo/.github/workflows/verify.yml@refs/heads/main \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS
install -Dm755 svincolo-linux-amd64 ~/.local/bin/svincolo
```

## Try it locally

With Rust installed through rustup and Ollama running locally, save the
following as `svincolo.toml`. Set `upstream_model` to a chat model already
installed on your Ollama server.

```toml
version = 1

[server]
listen = "127.0.0.1:8080"
local_only = true
discover_by_default = false

[[upstream]]
name = "local-models"
kind = "ollama"
base_url = "http://127.0.0.1:11434/v1"

[[route]]
name = "low"
upstream = "local-models"
upstream_model = "llama3.2:3b"
locality = "local"
surfaces = { chat = "raw" }
discover = true
```

```sh
cargo run --locked -- serve --config svincolo.toml
```

In a second terminal, discover the alias and send a request.

```sh
curl -fsS http://127.0.0.1:8080/v1/models
curl -fsS http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"low","messages":[{"role":"user","content":"Say hello."}]}'
```

`/v1/models` should list `low`, and the chat request should return a reply
from your local model.

## Connect an application

| Client | SDK base URL | Discovery |
| --- | --- | --- |
| OpenAI-style | `http://127.0.0.1:8080/v1` | `GET /v1/models` |
| Anthropic Messages | `http://127.0.0.1:8080/anthropic` | `GET /anthropic/v1/models` |

Give each application a `[[caller]]` entry with its own `key_file` and a list
of permitted `routes`. The token goes in `Authorization: Bearer` or
`x-api-key`. Keep upstream credentials in the upstream's `key_file` or
`key_env`. The catalog reads configured routes and filters by caller, API
surface, visibility, and locality.

Clients that record consent for a destination can send the catalog entry's
`route_revision` as `x-svincolo-expect-route-revision`. A stale revision
returns HTTP 409 before a provider is contacted.

## Operator cost dashboard

See which apps drive spend and which models they actually use, even when an
app requests an alias such as `low`. The dashboard ranks callers by estimated
monthly spend. Expand an app to see its provider, upstream model, API, usage,
and cost. The coverage figures show attempts with missing usage or prices.
**Export TSV** downloads the same monthly breakdown as the CLI.

![Sample dashboard showing estimated monthly spend, cost by API, and apps ranked by spend.](docs/assets/cost-dashboard.jpg)

Estimates use dated provider rates for managed Chat Completions, native
Anthropic Messages and OpenAI Responses, embeddings, audio transcriptions,
and token-billed image generation.
Transcription charges can use audio duration or tokens, depending on the
model. Raw Chat Completions are outside reporting coverage.

To generate a standalone dashboard from router logs and your price table:

```sh
python3 scripts/cost-report.py --html --month 2026-11 --prices prices.json router.jsonl > cost-report.html
```

Open `cost-report.html` in a browser. For a live view, the optional Kubernetes
collector updates the current UTC month every 30 seconds. The Theficos
deployment is available on its LAN at [svincolo.theficos.local](http://svincolo.theficos.local/)
on devices with that hostname mapping. See the [operator guide](docs/cost-report.md)
for the price table format, usage coverage, and collector setup.

## Operate and contribute

The listener serves HTTP and can connect to HTTPS upstreams using HTTP/1.1
or HTTP/2. Keep it on loopback or an internal network. Use caller tokens and
network policy for shared deployments. The route's `locality` is an operator
declaration. Use network policy to enforce an egress boundary.

See the [configuration and operations guide](docs/configuration.md) for
provider examples, managed output, limits, key rotation, and TLS. The
[Kubernetes canary guide](deploy/k8s/README.md) shows an isolated deployment.

Run the mock-provider suite with `cargo test --locked` to check routing,
caller-scoped discovery, native Messages forwarding, and transport behavior.
Native Linux CI also checks both image targets and runs the transport load test.

[Contributing](CONTRIBUTING.md) · [Security](SECURITY.md) · [Apache 2.0 license](LICENSE)
