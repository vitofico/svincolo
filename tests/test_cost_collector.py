"""Exercise usage collection, replay, and local rendering without a cluster."""

from datetime import datetime, timedelta, timezone
from http.client import IncompleteRead
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.error import HTTPError


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("collector", ROOT / "deploy/cost-ui/collector.py")
collector = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = collector
SPEC.loader.exec_module(collector)
NOW = datetime(2026, 11, 19, tzinfo=timezone.utc)


def line(request_id, stamp=NOW, **fields):
    event = {
        "event": "managed_chat_usage",
        "timestamp_s": int(stamp.timestamp()),
        "request_id": request_id,
        "caller": "editor",
        "provider": "cloud",
        "upstream_model": "model",
        "attempts": 1,
        "known_attempts": 1,
        "input_tokens": 1000,
        "cache_read_input_tokens": 0,
        "cache_creation_input_tokens": 0,
        "output_tokens": 0,
        "unexpected": "this field is never persisted",
        **fields,
    }
    return f"{stamp.isoformat()} {json.dumps(event)}\n".encode()


class FakeAPI:
    container = "svincolo"

    def __init__(self):
        self.restart = 0
        self.current = b""
        self.previous = b""
        self.calls = []

    def pods(self):
        return [{
            "metadata": {"uid": "uid", "name": "router"},
            "status": {"containerStatuses": [{
                "name": self.container,
                "state": {"running": {}},
                "restartCount": self.restart,
            }]},
        }]

    def logs(self, name, previous, since):
        self.calls.append((name, previous, since))
        body = self.previous if previous else self.current
        if isinstance(body, Exception):
            raise body
        return io.BytesIO(body)


class CollectorTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.directory = Path(temp.name)
        self.database = self.directory / "usage.sqlite3"
        self.db = collector.connect(self.database)
        self.addCleanup(self.db.close)

    def test_overlap_restart_replay_and_minimal_storage(self):
        api = FakeAPI()
        api.current = b"startup\n" + line("one")
        self.assertEqual(collector.collect(self.db, api), [])
        api.current += line("two", NOW + timedelta(seconds=25))
        with collector.connect(self.database) as reopened:
            self.assertEqual(collector.collect(reopened, api), [])
        api.restart = 1
        api.previous = api.current + line("last-before-restart")
        api.current = line("one") + line("one-after-restart")
        self.assertEqual(collector.collect(self.db, api), [])
        self.assertEqual(collector.collect(self.db, api), [])
        self.assertEqual(api.calls, [
            ("router", False, None),
            ("router", False, "2026-11-18T23:58:00+00:00"),
            ("router", True, None),
            ("router", False, None),
            ("router", False, "2026-11-18T23:58:00+00:00"),
        ])
        rows = [json.loads(value) for value, in self.db.execute("SELECT event FROM usage")]
        self.assertCountEqual([event["request_id"] for event in rows],
                              ["one", "two", "last-before-restart", "one-after-restart"])
        self.assertTrue(all("unexpected" not in event for event in rows))

    def test_collect_keeps_native_surface_and_legacy_history(self):
        api = FakeAPI()
        api.current = line("old")
        self.assertEqual(collector.collect(self.db, api), [])
        api.current += (
            line("messages", event="native_usage", surface="messages")
            + line("responses", event="native_usage", surface="responses")
            + line("invalid", event="native_usage", surface="raw_chat")
        )
        self.assertEqual(collector.collect(self.db, api), [])
        rows = {request: json.loads(event) for request, event in
                self.db.execute("SELECT request_id, event FROM usage")}
        self.assertEqual(set(rows), {"old", "messages", "responses"})
        self.assertEqual(rows["old"]["event"], "managed_chat_usage")
        self.assertNotIn("surface", rows["old"])
        self.assertEqual(rows["messages"]["surface"], "messages")
        self.assertEqual(rows["responses"]["surface"], "responses")
        self.assertNotIn("unexpected", rows["responses"])

    def test_collect_persists_allowed_audio_measurements_and_explicit_zero(self):
        api = FakeAPI()
        api.current = (
            line("embed", event="native_usage", surface="embeddings")
            + line("tokens", event="native_usage", surface="transcriptions",
                   audio_input_tokens=0, uploaded_audio_duration_ms=1000)
            + line("seconds", event="native_usage", surface="transcriptions",
                   input_tokens=0, audio_duration_ms=0)
            + line("unknown", event="native_usage", surface="transcriptions",
                   input_tokens=0, known_attempts=0)
            + line("invalid", event="native_usage", surface="embeddings",
                   audio_input_tokens=0)
            + line("image", event="native_usage", surface="images",
                   image_input_tokens=0, image_output_tokens=1040)
            + line("image-missing", event="native_usage", surface="images",
                   image_input_tokens=0)
            + line("image-misplaced", event="native_usage", surface="embeddings",
                   image_output_tokens=1)
        )
        self.assertEqual(collector.collect(self.db, api), [])
        rows = {request: json.loads(event) for request, event in
                self.db.execute("SELECT request_id, event FROM usage")}
        self.assertEqual(set(rows), {"embed", "tokens", "seconds", "unknown", "image"})
        self.assertEqual(rows["image"]["image_output_tokens"], 1040)
        self.assertEqual(rows["tokens"]["audio_input_tokens"], 0)
        self.assertEqual(rows["tokens"]["uploaded_audio_duration_ms"], 1000)
        self.assertEqual(rows["seconds"]["audio_duration_ms"], 0)
        self.assertNotIn("audio_duration_ms", rows["unknown"])
        self.assertNotIn("unexpected", rows["tokens"])

    def test_transient_error_keeps_previous_successes_and_retries(self):
        api = FakeAPI()
        api.restart = 1
        api.current = line("current")
        api.previous = HTTPError("https://irrelevant", 503, "private details", None, None)
        self.assertEqual(collector.collect(self.db, api), ["previous log read: HTTP 503"])
        api.previous = line("previous")
        api.current = IncompleteRead(b"incomplete")
        self.assertEqual(
            collector.collect(self.db, api),
            ["current log read: IncompleteRead", "no current router logs"],
        )
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM usage").fetchone(), (2,))
        api.current = b""
        self.assertEqual(collector.collect(self.db, api), [])

    def test_render_filters_month_and_preserves_snapshot_on_failure(self):
        api = FakeAPI()
        api.current = line("current") + line("last-month", NOW - timedelta(days=30))
        self.assertEqual(collector.collect(self.db, api), [])
        report = self.directory / "index.html"
        rate_path = self.directory / "prices.json"
        rate_path.write_text(json.dumps({"cloud": {"model": {
            "2026-11-01": dict.fromkeys(collector.COUNTS[3:], 2),
        }}}))
        with patch.multiple(collector, REPORT=report, PRICE_TABLE=rate_path,
                            RENDERER=ROOT / "scripts/cost-report.py"):
            collector.render(self.db, NOW)
            first = report.read_text()
            self.assertIn('<div class="amount">$0.002</div>', first)
            self.assertIn("1 request · 1 attempt", first)
            rate_path.write_text("invalid prices")
            with self.assertRaises(collector.subprocess.CalledProcessError):
                collector.render(self.db, NOW)
            self.assertEqual(report.read_text(), first)
            self.assertEqual(sorted(path.name for path in self.directory.iterdir()),
                             ["index.html", "prices.json", "usage.sqlite3"])

    def test_http_distinguishes_ready_incomplete_stale_and_unavailable(self):
        report = self.directory / "index.html"
        report.write_text("<main>Report</main>")

        def fetch(refresh):
            handler = object.__new__(collector.Handler)
            handler.server = SimpleNamespace(refresh=refresh)
            handler.path = "/"
            handler.command = "GET"
            handler.wfile = io.BytesIO()
            response = []
            handler.send_response = response.append
            handler.send_header = lambda *_: None
            handler.end_headers = lambda: None
            with patch.object(collector, "REPORT", report):
                handler.do_GET()
            return response[0], handler.wfile.getvalue().decode()

        now = datetime.now(timezone.utc)
        ready = fetch(collector.Refresh(now, True))
        self.assertEqual(ready[0], 200)
        self.assertIn("Last refreshed", ready[1])
        for refresh in (collector.Refresh(now, False),
                        collector.Refresh(now - collector.STALE_AFTER - timedelta(seconds=1), True)):
            response, body = fetch(refresh)
            self.assertEqual(response, 503)
            self.assertIn("Collection is stale or incomplete", body)
            self.assertIn("Report", body)
        response, body = fetch(collector.Refresh())
        self.assertEqual(response, 503)
        self.assertIn("API costs unavailable", body)
