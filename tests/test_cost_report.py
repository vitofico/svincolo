"""Run with python3 -m unittest discover -s tests -p test_cost_report.py."""

import base64
import csv
import re
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "cost-report.py"
TOKEN_FIELDS = (
    "input_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
    "output_tokens",
)
NOVEMBER_1 = 1793491200
NOVEMBER_2 = NOVEMBER_1 + 86400
DECEMBER_1 = NOVEMBER_1 + 30 * 86400


def usage(request_id, timestamp=NOVEMBER_1, **changes):
    return {
        "event": "managed_chat_usage",
        "timestamp_s": timestamp,
        "request_id": request_id,
        "caller": "editor",
        "provider": "cloud",
        "upstream_model": "model",
        "attempts": 1,
        "known_attempts": 1,
        **dict.fromkeys(TOKEN_FIELDS, 0),
        **changes,
    }


def jsonl(*events):
    return "".join(json.dumps(event) + "\n" for event in events)


class CostReportTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.directory = Path(temp.name)
        self.prices = self.directory / "prices.json"
        self.prices.write_text(json.dumps({
            "cloud": {"model": {
                "2026-11-02": dict(zip(TOKEN_FIELDS, ["2.2", "0.2", "2.5", "10"])),
                "2026-11-01": dict(zip(TOKEN_FIELDS, ["1.1", "0.1", "1.25", "5"])),
            }},
            "local": {"model": {"2026-11-01": dict.fromkeys(TOKEN_FIELDS, 0)}},
        }), encoding="utf-8")

    def test_date_range_crosses_month_and_includes_both_dates(self):
        report = subprocess.run(
            [sys.executable, SCRIPT, "--dates", "2026-11-30", "2026-12-01", "--prices", self.prices],
            input=jsonl(usage("early"), usage("last-november", DECEMBER_1 - 1),
                        usage("first-december", DECEMBER_1), usage("late", DECEMBER_1 + 86400)),
            text=True, capture_output=True, check=True,
        )
        rows = list(csv.DictReader(io.StringIO(report.stdout), delimiter="\t"))
        self.assertEqual(next(row["requests"] for row in rows if row["scope"] == "total"), "2")

    def run_report(self, stdin="", *logs, html=False):
        options = ["--html"] if html else []
        return subprocess.run(
            [sys.executable, SCRIPT, "--month", "2026-11", "--prices", self.prices, *options, *logs],
            input=stdin, text=True, capture_output=True, check=False,
        )

    def test_utc_dates_grouping_partial_usage_and_missing_prices(self):
        result = self.run_report("startup\n" + jsonl(
            {"event": "destination"},
            usage("before", NOVEMBER_1 - 1, input_tokens=1_000_000),
            usage("first", attempts=2, input_tokens=1000, cache_read_input_tokens=1000,
                  cache_creation_input_tokens=1000, output_tokens=1000),
            usage("after-price-change", NOVEMBER_2, input_tokens=1000),
            usage("after-month", DECEMBER_1, input_tokens=1_000_000),
            usage("different-caller", caller="batch", input_tokens=1000),
            usage("no-price", upstream_model="unlisted", input_tokens=1_000_000),
            usage("free", provider="local", input_tokens=1_000_000),
            usage("unknown", NOVEMBER_2, known_attempts=0),
        ))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("skipped 1 non-JSON log lines", result.stderr)
        groups = list(csv.DictReader(io.StringIO(result.stdout), delimiter="\t"))
        self.assertEqual([
            (row["caller"], row["provider"], row["upstream_model"], row["requests"],
             row["attempts"], row["known_attempts"], row["unknown_usage_attempts"],
             row["unpriced_attempts"], row["known_cost_usd"]) for row in groups
        ], [
            ("batch", "cloud", "model", "1", "1", "1", "0", "0", "0.0011"),
            ("editor", "cloud", "model", "3", "4", "2", "2", "0", "0.00965"),
            ("editor", "cloud", "unlisted", "1", "1", "1", "0", "1", "0"),
            ("editor", "local", "model", "1", "1", "1", "0", "0", "0"),
            ("", "", "", "6", "7", "5", "2", "1", "0.01075"),
        ])
        self.assertEqual(groups[-1]["input_tokens"], "2003000")
        self.assertEqual(groups[-1]["scope"], "total")

    def test_identical_exports_deduplicate_and_conflicts_fail(self):
        path = self.directory / "usage.jsonl"
        original = usage("duplicate", input_tokens=5)
        path.write_text(jsonl(original), encoding="utf-8")
        duplicate = self.run_report(jsonl(original), path, "-")
        self.assertEqual(duplicate.returncode, 0, duplicate.stderr)
        self.assertIn("skipped 1 duplicate usage events", duplicate.stderr)
        self.assertIn("0.0000055", duplicate.stdout)
        conflict = self.run_report(jsonl(usage("duplicate", input_tokens=6)), path, "-")
        self.assertEqual(conflict.returncode, 2)
        self.assertIn("conflicting events", conflict.stderr)
        self.assertEqual(conflict.stdout, "")

    def test_html_shares_totals_and_escapes_labels(self):
        events = jsonl(
            usage("priced", caller="app <script>", input_tokens=1000),
            usage("unpriced", caller="app <script>", upstream_model="unlisted & model",
                  known_attempts=0),
            usage("other", caller="second app", input_tokens=1000),
            usage("outside", DECEMBER_1, caller="outside the month", input_tokens=1000),
        )
        html = self.run_report(events, html=True)
        self.assertEqual(html.returncode, 0, html.stderr)
        self.assertIn('<time datetime="2026-11">November 2026</time>', html.stdout)
        self.assertIn('class="amount">$0.0022</div>', html.stdout)
        self.assertIn('<strong>$0.0011</strong>', html.stdout)
        self.assertEqual(html.stdout.count("<details>"), 2)
        self.assertIn("app &lt;script&gt;", html.stdout)
        self.assertIn("unlisted &amp; model", html.stdout)
        self.assertIn("Partial estimate", html.stdout)
        self.assertIn("1 missing usage · 1 unpriced", html.stdout)
        self.assertNotIn("<script>", html.stdout)
        self.assertNotIn("outside the month", html.stdout)

    def test_native_usage_groups_with_legacy_tsv_and_ranks_apps_in_html(self):
        events = jsonl(
            usage("managed", caller="zebra", input_tokens=500),
            usage("messages", event="native_usage", surface="messages", caller="zebra",
                  input_tokens=1500, cache_read_input_tokens=100),
            usage("responses", event="native_usage", surface="responses", caller="ant",
                  known_attempts=0),
        )
        report = self.run_report(events)
        self.assertEqual(report.returncode, 0, report.stderr)
        rows = list(csv.DictReader(io.StringIO(report.stdout), delimiter="\t"))
        self.assertNotIn("surface", rows[0])
        self.assertEqual([row["caller"] for row in rows], ["ant", "zebra", ""])
        self.assertEqual(rows[1]["requests"], "2")
        self.assertEqual(rows[1]["input_tokens"], "2000")
        self.assertEqual(rows[-1]["known_cost_usd"], "0.00221")
        self.assertEqual(rows[-1]["unknown_usage_attempts"], "1")

        page = self.run_report(events, html=True)
        self.assertEqual(page.returncode, 0, page.stderr)
        export = re.search(r'download="svincolo-2026-11.tsv" href="data:text/tab-separated-values;base64,([^"]+)"', page.stdout)
        self.assertIsNotNone(export)
        self.assertEqual(base64.b64decode(export.group(1)).decode(), report.stdout)
        self.assertIn("2,100 input", page.stdout)
        self.assertIn('100.0%<span class="sr"> of known spend', page.stdout)
        for label in ("Anthropic Messages", "OpenAI Responses", "Managed Chat Completions"):
            self.assertIn(f"<h3>{label}</h3>", page.stdout)
            self.assertIn(f'<td class="api-detail">{label}<small>', page.stdout)
        self.assertLess(page.stdout.index('class="caller">zebra'),
                        page.stdout.index('class="caller">ant'))

    def test_audio_rates_and_zero_versus_unknown_embeddings(self):
        table = json.loads(self.prices.read_text())
        table["cloud"].update({
            "duration": {"2026-11-01": {"audio_duration_usd_per_minute": "0.0045"}},
            "audio-token": {"2026-11-01": {
                **dict(zip(TOKEN_FIELDS, ("2", "0", "0", "8"))),
                "audio_input_tokens": "40",
            }},
        })
        self.prices.write_text(json.dumps(table))

        def transcribe(request, model="duration", **counts):
            return usage(request, event="native_usage", surface="transcriptions",
                         upstream_model=model, **counts)

        events = jsonl(
            transcribe("provider-duration", audio_duration_ms=60_000),
            transcribe("uploaded-duration", uploaded_audio_duration_ms=30_000),
            transcribe("uploaded-with-tokens", input_tokens=1, output_tokens=2,
                       audio_input_tokens=0, uploaded_audio_duration_ms=10_000),
            transcribe("measured-zero", audio_duration_ms=0),
            transcribe("unpriced-token-usage", audio_input_tokens=0),
            transcribe("token-fee", "audio-token", input_tokens=500, output_tokens=1000,
                       audio_input_tokens=2000, uploaded_audio_duration_ms=5_000),
            usage("free-embedding", event="native_usage", surface="embeddings",
                  caller="spunto", provider="local", input_tokens=0),
            usage("unknown-embedding", event="native_usage", surface="embeddings",
                  caller="unknown", provider="local", known_attempts=0),
        )
        report = self.run_report(events)
        self.assertEqual(report.returncode, 0, report.stderr)
        rows = list(csv.DictReader(io.StringIO(report.stdout), delimiter="\t"))
        durations = next(row for row in rows if row["upstream_model"] == "duration")
        self.assertEqual(durations["known_cost_usd"], "0.0075")
        self.assertEqual(durations["audio_duration_ms"], "60000")
        self.assertEqual(durations["audio_duration_attempts"], "2")
        self.assertEqual(durations["uploaded_audio_duration_ms"], "40000")
        self.assertEqual(durations["uploaded_duration_attempts"], "2")
        self.assertEqual(durations["upload_duration_priced_attempts"], "2")
        self.assertEqual(durations["unpriced_attempts"], "1")
        token = next(row for row in rows if row["upstream_model"] == "audio-token")
        self.assertEqual(token["known_cost_usd"], "0.089")
        self.assertEqual(token["audio_input_tokens"], "2000")
        self.assertEqual(token["upload_duration_priced_attempts"], "0")
        self.assertEqual(rows[-1]["known_cost_usd"], "0.0965")
        self.assertEqual(rows[-1]["unknown_usage_attempts"], "1")

        html = self.run_report(events, html=True)
        self.assertEqual(html.returncode, 0, html.stderr)
        self.assertIn("<h3>Audio transcriptions</h3>", html.stdout)
        self.assertIn("<h3>Embeddings</h3>", html.stdout)
        self.assertIn("2 attempts priced by upload duration", html.stdout)
        self.assertIn('<td class="tokens">0 input tokens</td>', html.stdout)
        self.assertIn('<td class="tokens">Unknown</td>', html.stdout)

    def test_image_token_rates_and_unpriced_image_usage(self):
        table = json.loads(self.prices.read_text())
        table["cloud"]["image-model"] = {"2026-11-01": {
            **dict(zip(TOKEN_FIELDS, ("5", "1.25", "0", "10"))),
            "image_input_tokens": "8",
            "image_output_tokens": "32",
        }}
        self.prices.write_text(json.dumps(table))

        def paint(request, model="image-model", **counts):
            return usage(request, event="native_usage", surface="images",
                         upstream_model=model, **counts)

        image_counts = {"image_input_tokens": 0, "image_output_tokens": 1000}
        events = jsonl(
            paint("painted", input_tokens=100, output_tokens=0, **image_counts),
            paint("no-usage", known_attempts=0),
            paint("text-rates-only", "model", input_tokens=100, output_tokens=0, **image_counts),
        )
        report = self.run_report(events)
        self.assertEqual(report.returncode, 0, report.stderr)
        rows = list(csv.DictReader(io.StringIO(report.stdout), delimiter="\t"))
        painted = next(row for row in rows if row["upstream_model"] == "image-model")
        self.assertEqual(painted["known_cost_usd"], "0.0325")
        self.assertEqual(painted["image_output_tokens"], "1000")
        self.assertEqual(painted["unknown_usage_attempts"], "1")
        unpriced = next(row for row in rows if row["upstream_model"] == "model")
        self.assertEqual(unpriced["unpriced_attempts"], "1")

        html = self.run_report(events, html=True)
        self.assertEqual(html.returncode, 0, html.stderr)
        self.assertIn("<h3>Image generation</h3>", html.stdout)
        self.assertIn("1,000 image output tokens", html.stdout)

        for event in (paint("missing", input_tokens=1, output_tokens=0),
                      usage("misplaced", event="native_usage", surface="embeddings", **image_counts)):
            invalid = self.run_report(jsonl(event))
            self.assertEqual(invalid.returncode, 2)
        self.prices.write_text(json.dumps({"cloud": {"model": {"2026-11-01": {
            **dict.fromkeys(TOKEN_FIELDS, "1"), "image_output_tokens": "1"}}}}))
        self.assertIn("invalid rates", self.run_report().stderr)

    def test_html_empty_month(self):
        html = self.run_report("ignored startup line\n", html=True)
        self.assertEqual(html.returncode, 0, html.stderr)
        self.assertIn("No API usage records for this month", html.stdout)
        self.assertIn("Skipped 1 non-JSON log lines.", html.stdout)
        self.assertNotIn("<details>", html.stdout)

    def test_malformed_usage_and_invalid_prices_fail(self):
        invalid = self.run_report(jsonl(usage("invalid", known_attempts=0, input_tokens=5)))
        self.assertEqual(invalid.returncode, 2)
        self.assertIn("attempt counts are inconsistent", invalid.stderr)
        self.prices.write_text(json.dumps({
            "cloud": {"model": {"2026-11-01": dict.fromkeys(TOKEN_FIELDS, "-1")}}
        }), encoding="utf-8")
        invalid = self.run_report()
        self.assertEqual(invalid.returncode, 2)
        self.assertIn("invalid rates for cloud/model on 2026-11-01", invalid.stderr)
