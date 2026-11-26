# Monthly API cost estimate

Export the router's stdout as JSONL and run the report locally with Python 3.
It reads `managed_chat_usage` events for managed Chat Completions and
`native_usage` events for Anthropic Messages, OpenAI Responses, embeddings,
audio transcriptions, and image generation. Raw Chat Completions and requests rejected before
upstream dispatch are outside its coverage.

Managed Chat Completions may make several attempts, such as structured output
repairs. Native usage comes from successful uncompressed JSON or SSE responses.
A native attempt counts as unknown when a failed, encoded, or interrupted
response prevents observation of final usage, or when observation memory is
unavailable. JSON inspection is capped at 512 KiB, or 1 MiB for embeddings.
SSE inspection caps each line and each event's data at 512 KiB. These limits
apply to usage inspection. The router continues forwarding the upstream body
to the client. For image generation, inspection keeps only the start of each
long string, so base64 image data does not count toward the cap.

Estimates cover token and duration charges. Provider-hosted tools, storage,
and other per-use fees are billed separately.

The event timestamp determines the UTC month and price for the request.
Keep all router log partitions for the month to capture every recorded request.
Historical managed events continue to work. Older builds only contribute
events they emitted, so an empty report means the input contained no matching
events for that month.

Create a price table using the exact `provider` and `upstream_model` labels
shown in the usage events. Under each model, key each set of prices by its
first effective UTC day. The report applies the latest entry effective on
or before the event's UTC day. The four rates are USD per million tokens
and accept decimal strings or JSON numbers. These rates are examples.

```json
{
  "cloud": {
    "example-model": {
      "2026-11-01": {
        "input_tokens": "2",
        "cache_read_input_tokens": "0.2",
        "cache_creation_input_tokens": "2.5",
        "output_tokens": "8"
      }
    }
  },
  "local": {
    "example-local-model": {
      "2026-11-01": {
        "input_tokens": "0",
        "cache_read_input_tokens": "0",
        "cache_creation_input_tokens": "0",
        "output_tokens": "0"
      }
    }
  }
}
```

Price uncached input, cache reads, cache creation, and output separately.
For a model with a higher rate above a prompt length threshold, add
`long_context` to the dated price row. It takes a positive integer
`input_tokens_over` threshold and the same four token rates. For example,
Claude Haiku 5.5 uses `"input_tokens_over": 100000`. The report compares
the threshold with uncached input, cache reads, and cache writes combined.
It uses the standard rates at or below the threshold. Above the threshold
it uses the higher rates when exactly one attempt has known usage. When
multiple attempts have known usage and their aggregate prompt exceeds the
threshold, the report counts all those known attempts as unpriced because
the log has no per-attempt breakdown. It still includes their token totals.

The report accepts one cache-write rate per prompt length tier. Use the
rate for the cache lifetime your requests use, or a blended estimate when
they use different lifetimes. Each dated price row also assumes one
processing tier and region. Traffic billed at a different speed, region,
or cache lifetime can differ from the estimate. Enter zero rates explicitly for a model with no provider
fee. A zero provider rate for local execution excludes compute, storage,
and operating costs.

Save that table as `prices.json`. For example, a November router log might
contain the following usage event. One of the two attempts returned usage.

```json
{"event":"managed_chat_usage","request_id":"ae6df818-8483-4506-ab32-f99b743f4d77","timestamp_s":1793491200,"caller":"editor","provider":"cloud","upstream_model":"example-model","attempts":2,"known_attempts":1,"input_tokens":100,"cache_read_input_tokens":80,"cache_creation_input_tokens":20,"output_tokens":25}
```

```sh
python3 scripts/cost-report.py --month 2026-11 --prices prices.json router.jsonl
```

Native events use the same accounting fields, with `"event":"native_usage"`
and a `surface` of `messages`, `responses`, `embeddings`, or
`transcriptions`. Embeddings record input tokens. Token-billed
transcriptions record text input and output separately from
`audio_input_tokens`. Add an `audio_input_tokens` price in USD per million
audio input tokens to the standard four-rate row.

Use a dated row containing only `audio_duration_usd_per_minute` for a
duration-billed transcription model. The report uses provider-reported
`audio_duration_ms` when available. When a successful transcription lacks
provider duration, it can use `uploaded_audio_duration_ms` measured from a
validated WAV or Ogg Opus upload. Rates are prorated to milliseconds. The
dashboard identifies models for which the estimate used uploaded duration.
If the only measurement is token usage, a duration-priced attempt is
unpriced. OpenAI lists GPT-Transcribe at $0.0045 per minute. [OpenAI model
pricing](https://developers.openai.com/api/docs/models/gpt-transcribe)

Image generation events with `"surface":"images"` record the text input and
text output tokens from the provider's `usage` in `input_tokens` and
`output_tokens`, and its image tokens in `image_input_tokens` and
`image_output_tokens`. When the provider does not split output tokens, the
router counts them all as image output. Add `image_input_tokens` and
`image_output_tokens` rates in USD per million tokens to the standard
four-rate row. An image attempt is unpriced when its model has no image
rates. An answer without token usage, such as one from a model billed per
image, counts as unknown usage.

Pass zero or more log paths. With no paths, the report reads stdin, and
`-` reads stdin alongside files. Input can contain other router events.
Matching usage events are counted once per request ID across all input
files. Conflicting usage records for a request in the selected month stop
the report. Skipped non-JSON lines and duplicates are reported on stderr.

The tab-separated output has one row per caller, provider, and upstream
model across all covered APIs, followed by a total. The example yields one request, two attempts,
one unknown usage attempt, zero unpriced attempts, and a
`known_cost_usd` subtotal of `0.000466`.

- `unknown_usage_attempts` counts attempts without usable measurements.
  Explicit zero measurements count as known.
- `unpriced_attempts` counts attempts without an applicable dated price,
  recorded attempts whose prompt-length tier is ambiguous, and measured
  usage incompatible with the rate's billing unit. It can overlap with
  `unknown_usage_attempts` when there is no applicable dated price.
- `known_cost_usd` sums priced usage. Treat it as a partial estimate whenever
  either count is positive, even when it displays zero.
- The appended audio fields show separate totals and attempt counts for
  audio input tokens, provider-reported duration, and uploaded duration.
  `upload_duration_priced_attempts` counts attempts priced using measured
  uploaded duration.
- The final `image_input_tokens` and `image_output_tokens` fields total the
  image token counts.

The report uses operator-supplied rates and usage seen by the router.
Provider invoices may include additional charges, discounts, or requests
whose usage was unavailable in the logs.

## View in a browser

Generate a standalone HTML snapshot from the same logs and price table.

```sh
python3 scripts/cost-report.py --html --month 2026-11 --prices prices.json router.jsonl > cost-report.html
```

Open `cost-report.html` in a browser to see monthly totals for known spend,
recorded usage, and coverage. The API breakdown shows recorded managed and
native traffic. Callers are ranked by known spend. Expand a caller to see
its usage and coverage by model and API. Generate it again to incorporate new log events or
rate changes. The dashboard’s Export TSV link downloads the same monthly totals as the CLI. Keep the sibling cost-dashboard.css file next to the report script when installing it. The exported HTML is self-contained. The file includes caller and model labels, so give it the same
access controls as your usage logs.

## Live operator view

The optional [Kubernetes collector](../deploy/cost-ui/collector.py) renders the current UTC month every 30 seconds. It reads router pod logs, persists managed and native usage events by request ID in SQLite, and serves the dashboard on port 8081. The listener defaults to pod loopback. Set `DASHBOARD_BIND_ADDRESS` to the pod IP to serve it through a Service with restricted ingress. Expand a caller to see its provider, models, token counts, audio duration, and estimated costs. Refresh the browser for the latest snapshot.

Run the collector with the report script at `/app/cost-report.py`, its stylesheet and favicon at `/app/cost-dashboard.css` and `/app/favicon.ico`, rates at `/config/prices.json`, and persistent storage at `/data`. Its service account needs pod list and pod log read permissions in the router namespace. Connect through an authorized Kubernetes port-forward. The page shows the last refresh time and returns HTTP 503 when collection is unavailable or stale.


The dashboard date fields select an inclusive UTC range. The same range can be exported from the CLI using `--dates YYYY-MM-DD YYYY-MM-DD` in place of `--month`.

To enable spending limits, install `deploy/cost-ui/spend.py` beside the collector and mount a persistent spending directory. Set `SVINCOLO_SPEND_DIR` on the collector, operator editor and router. Mount the directory read-only on the router. Each application and provider may have a soft dashboard warning and a hard threshold for a UTC day, week or month. The router checks recorded, priced spend before dispatch and returns HTTP 402 at the hard threshold. In-flight work can finish and usage collection can lag. Keep the operator editor behind authenticated HTTPS on a restricted network.
