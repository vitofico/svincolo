#!/usr/bin/env python3
"""Collect API usage from router pods and serve the monthly report locally."""

import json
import importlib.util
from contextlib import closing
from threading import BoundedSemaphore
import os
import sqlite3
import ssl
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
from http.client import HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from urllib.error import HTTPError
from urllib.parse import parse_qs, quote, urlencode, urlsplit
from urllib.request import Request, urlopen


SA = Path("/var/run/secrets/kubernetes.io/serviceaccount")
DATA = Path("/data")
REPORT = DATA / "index.html"
PRICE_TABLE = Path("/config/prices.json")
RENDERER = Path("/app/cost-report.py")
POLL_SECONDS = 30
OVERLAP = timedelta(minutes=2)
STALE_AFTER = timedelta(seconds=90)
EVENTS = ("managed_chat_usage", "native_usage")
EVENT_MARKERS = tuple(f'"{event}"'.encode() for event in EVENTS)
IDENTIFIERS = ("request_id", "caller", "provider", "upstream_model")
COUNTS = (
    "timestamp_s", "attempts", "known_attempts", "input_tokens",
    "cache_read_input_tokens", "cache_creation_input_tokens", "output_tokens",
)
UTC = timezone.utc


class Kubernetes:
    def __init__(self, namespace, selector, container):
        self.namespace = quote(namespace, safe="")
        self.selector = selector
        self.container = container
        host = os.environ["KUBERNETES_SERVICE_HOST"]
        if ":" in host:
            host = f"[{host}]"
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        self.endpoint = f"https://{host}:{port}"
        self.tls = ssl.create_default_context(cafile=SA / "ca.crt")

    def get(self, path, **params):
        token = (SA / "token").read_text().strip()
        url = f"{self.endpoint}/api/v1/namespaces/{self.namespace}/{path}"
        request = Request(f"{url}?{urlencode(params)}",
                          headers={"Authorization": f"Bearer {token}"})
        return urlopen(request, context=self.tls, timeout=10)

    def pods(self):
        cursor = ""
        while True:
            query = {"labelSelector": self.selector, "limit": 100}
            if cursor:
                query["continue"] = cursor
            with self.get("pods", **query) as response:
                page = json.load(response)
            yield from page["items"]
            cursor = page.get("metadata", {}).get("continue")
            if not cursor:
                return

    def logs(self, name, previous, since):
        query = {
            "container": self.container,
            "timestamps": "true",
            "previous": "true" if previous else "false",
        }
        if since:
            query["sinceTime"] = since
        return self.get(f"pods/{quote(name, safe='')}/log", **query)


def connect(path):
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS usage (
            request_id TEXT PRIMARY KEY,
            timestamp_s INTEGER NOT NULL,
            event TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS usage_timestamp ON usage(timestamp_s);
        CREATE TABLE IF NOT EXISTS streams (
            uid TEXT NOT NULL,
            generation INTEGER NOT NULL,
            latest TEXT,
            previous_done INTEGER NOT NULL,
            PRIMARY KEY (uid, generation)
        );
    """)
    return db


def usage_event(body):
    if not any(marker in body for marker in EVENT_MARKERS):
        return None
    try:
        source = json.loads(body)
    except (ValueError, RecursionError):
        return None
    if not isinstance(source, dict) or source.get("event") not in EVENTS:
        return None
    event = {"event": source["event"]}
    if event["event"] == "native_usage":
        surface = source.get("surface")
        if surface not in ("messages", "responses", "embeddings", "transcriptions", "images"):
            return None
        event["surface"] = surface
    for name in IDENTIFIERS:
        value = source.get(name)
        if not isinstance(value, str) or not value or len(value) > 1024:
            return None
        event[name] = value
    for name in COUNTS:
        value = source.get(name)
        if type(value) is not int or value < 0:
            return None
        event[name] = value
    if (event["timestamp_s"] > 253402300799
            or event["known_attempts"] > event["attempts"]
            or (not event["known_attempts"] and any(event[name] for name in COUNTS[3:]))):
        return None
    audio_fields = ("audio_input_tokens", "audio_duration_ms", "uploaded_audio_duration_ms")
    for name in audio_fields:
        if name in source:
            value = source[name]
            if type(value) is not int or value < 0:
                return None
            event[name] = value
    audio_input, duration, uploaded = [event.get(name) for name in audio_fields]
    if event.get("surface") == "transcriptions" and event["known_attempts"]:
        if (all(value is None for value in (audio_input, duration, uploaded))
                or (duration is not None and (audio_input is not None or uploaded is not None))
                or (audio_input is None and any(event[name] for name in COUNTS[3:]))):
            return None
    elif any(name in event for name in audio_fields):
        return None
    image_fields = ("image_input_tokens", "image_output_tokens")
    for name in image_fields:
        if name in source:
            value = source[name]
            if type(value) is not int or value < 0:
                return None
            event[name] = value
    present = [name in event for name in image_fields]
    if event.get("surface") == "images" and event["known_attempts"]:
        if not all(present):
            return None
    elif any(present):
        return None
    return event


def collect_stream(db, api, name, uid, generation, previous):
    cursor, done = db.execute(
        "SELECT latest, previous_done FROM streams WHERE uid = ? AND generation = ?",
        (uid, generation),
    ).fetchone() or (None, False)
    if previous and done:
        return
    latest = datetime.fromisoformat(cursor) if cursor else None
    since = (latest - OVERLAP).isoformat() if latest and not previous else None
    with db, api.logs(name, previous, since) as response:
        for line in response:
            stamp, separator, body = line.partition(b" ")
            if not separator:
                continue
            try:
                logged = datetime.fromisoformat(stamp.decode("ascii").replace("Z", "+00:00"))
                if logged.tzinfo is None:
                    continue
            except ValueError:
                continue
            if latest is None or logged > latest:
                latest = logged
            event = usage_event(body)
            if event is not None:
                db.execute("INSERT OR IGNORE INTO usage VALUES (?, ?, ?)", (
                    event["request_id"], event["timestamp_s"], json.dumps(event),
                ))
        db.execute("""
            INSERT INTO streams VALUES (?, ?, ?, ?)
            ON CONFLICT(uid, generation) DO UPDATE SET
                latest = excluded.latest,
                previous_done = MAX(previous_done, excluded.previous_done)
        """, (uid, generation, latest.isoformat() if latest else None, int(previous)))


def collect(db, api):
    errors = set()
    observed = False
    for pod in api.pods():
        metadata = pod["metadata"]
        statuses = pod.get("status", {}).get("containerStatuses", [])
        for status in statuses:
            if status["name"] != api.container:
                continue
            generation = status["restartCount"]
            state = status.get("state", {})
            streams = []
            if generation:
                streams.append((generation - 1, True))
            if "running" in state or "terminated" in state:
                streams.append((generation, False))
            for ordinal, previous in streams:
                try:
                    collect_stream(db, api, metadata["name"], metadata["uid"], ordinal, previous)
                    observed = observed or not previous
                except (OSError, HTTPException) as error:
                    stream = "previous" if previous else "current"
                    errors.add(f"{stream} log read: {error_kind(error)}")
    if not observed:
        errors.add("no current router logs")
    return sorted(errors)


def events_between(db, start, end):
    return (row[0] for row in db.execute(
        "SELECT event FROM usage WHERE timestamp_s >= ? AND timestamp_s < ? ORDER BY timestamp_s",
        (int(start.timestamp()), int(end.timestamp()))))


def render_to(db, start, end, period_args, output):
    with tempfile.TemporaryFile(dir=REPORT.parent, mode="w+", encoding="utf-8") as events:
        for line in events_between(db, start, end):
            events.write(line + "\n")
        events.seek(0)
        subprocess.run([sys.executable, "-B", RENDERER, "--html", *period_args, "--prices", PRICE_TABLE],
                       stdin=events, stdout=output, stderr=subprocess.DEVNULL, check=True, timeout=60)


def render(db, now):
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    following = (start + timedelta(days=32)).replace(day=1)
    with tempfile.NamedTemporaryFile(dir=REPORT.parent, delete=False) as output:
        temporary = Path(output.name)
        try:
            render_to(db, start, following, ["--month", now.strftime("%Y-%m")], output)
            os.replace(temporary, REPORT)
        finally:
            temporary.unlink(missing_ok=True)


def publish_usage(db, now, pricing):
    import spend
    rates = pricing.read_prices(PRICE_TABLE)
    periods = {}
    for period, (start, end) in spend.windows(now).items():
        groups, _, _ = pricing.summarize([events_between(db, start, end)], None, rates)
        totals = {"provider": {}, "app": {}}
        for (caller, provider, _, _surface), row in groups.items():
            for scope, name in [("app", caller), ("provider", provider)]:
                totals[scope][name] = totals[scope].get(name, 0) + row.known_cost_usd
        periods[period] = {scope: {name: int(value * spend.UNIT) for name, value in amounts.items()}
                           for scope, amounts in totals.items()}
        periods[period].update(start=int(start.timestamp()), end=int(end.timestamp()))
    spend.atomic("usage.json", dict(recorded_at=int(now.timestamp()), periods=periods))


RANGE_SLOTS = BoundedSemaphore(2)


@dataclass(frozen=True)
class Refresh:
    rendered: datetime = None
    complete: bool = False


class Dashboard(ThreadingHTTPServer):
    refresh = Refresh()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        url = urlsplit(self.path)
        if url.path == "/favicon.ico":
            icon = RENDERER.with_name("favicon.ico").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "image/x-icon")
            self.send_header("Content-Length", str(len(icon)))
            self.end_headers()
            if self.command == "GET":
                self.wfile.write(icon)
            return
        params = parse_qs(url.query, keep_blank_values=True)
        if url.path not in ("/", "/index.html"):
            self.send_error(404)
            return
        refresh = self.server.refresh
        now = datetime.now(UTC)
        status = 503
        page = None
        if refresh.rendered:
            try:
                page = REPORT.read_text(encoding="utf-8")
            except OSError:
                pass
        first = now.replace(day=1).date()
        last = now.date()
        if params:
            try:
                if set(params) != {"start", "end"} or any(len(v) != 1 for v in params.values()):
                    raise ValueError
                first, last = (datetime.strptime(params[k][0], "%Y-%m-%d").date() for k in ("start", "end"))
                if first > last:
                    raise ValueError
                start = datetime.combine(first, datetime.min.time(), UTC)
                end = datetime.combine(last + timedelta(days=1), datetime.min.time(), UTC)
            except (ValueError, OverflowError):
                self.send_error(400, "Choose a valid start and end date")
                return
            if not RANGE_SLOTS.acquire(blocking=False):
                self.send_error(503, "Report generation is busy")
                return
            try:
                with closing(sqlite3.connect(f"file:{DATA}/usage.sqlite3?mode=ro", uri=True)) as reader, tempfile.TemporaryFile(dir=REPORT.parent) as output:
                    render_to(reader, start, end, ["--dates", first.isoformat(), last.isoformat()], output)
                    output.seek(0)
                    page = output.read().decode()
            except (OSError, sqlite3.Error, subprocess.SubprocessError):
                self.send_error(503, "Report generation unavailable")
                return
            finally:
                RANGE_SLOTS.release()
        if page is None:
            page = (
                "<!doctype html><html lang=\"en\"><meta charset=\"utf-8\">"
                "<link rel=icon href='favicon.ico?v=2'><title>API costs unavailable</title><main>"
                "<h1>API costs unavailable</h1>"
                "<p>The collector has yet to produce a report.</p></main></html>"
            )
        else:
            age = now - refresh.rendered if refresh.rendered else timedelta.max
            clock = refresh.rendered.strftime("%Y-%m-%d %H:%M:%S UTC") if refresh.rendered else "pending"
            if refresh.complete and age <= STALE_AFTER:
                status = 200
                message = f"Last refreshed {clock}."
            else:
                age_seconds = max(0, int(age.total_seconds()))
                message = (
                    f"Collection is stale or incomplete. Snapshot refreshed {clock} "
                    f"({age_seconds:,} seconds ago)."
                )
            page = page.replace("<main>", f'<main><p class="period">{message}</p>', 1)
        selector = (f'<form class="report-dates" method="get"><label>From <input type="date" name="start" value="{first}" required></label> '
                    f'<label>Through <input type="date" name="end" value="{last}" required></label> '
                    '<button>Show period</button></form>')
        page = page.replace("</header>", "</header>" + selector, 1)
        if os.environ.get("SVINCOLO_SPEND_DIR"):
            import spend
            try:
                alerts = list(spend.alerts(spend.read("limits.json"), spend.read("usage.json")))
            except (ValueError, OSError, KeyError):
                alerts = ["Current budget status unavailable."]
            if alerts:
                page = page.replace("<main>", "<main><aside>" + " ".join(escape(a) for a in alerts) + "</aside>", 1)
        editor = os.environ.get("ALIAS_EDITOR_URL")
        if editor:
            link = f'<a href="{escape(editor, quote=True)}">Aliases &amp; caps</a>'
            page = page.replace("</nav>", link + "</nav>", 1) if "</nav>" in page else page.replace("<main>", "<main>" + link, 1)
        body = page.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'none'; img-src 'self'; style-src 'unsafe-inline'")
        self.end_headers()
        if self.command == "GET":
            self.wfile.write(body)


def error_kind(error):
    if isinstance(error, HTTPError):
        return f"HTTP {error.code}"
    if isinstance(error, subprocess.CalledProcessError):
        return f"renderer exit {error.returncode}"
    if isinstance(error, OSError) and error.errno is not None:
        return f"{type(error).__name__} (errno {error.errno})"
    return type(error).__name__


def main():
    namespace = os.environ.get("POD_NAMESPACE") or (SA / "namespace").read_text().strip()
    api = Kubernetes(
        namespace,
        os.environ.get("ROUTER_SELECTOR", "app.kubernetes.io/name=svincolo"),
        os.environ.get("ROUTER_CONTAINER", "svincolo"),
    )
    db = connect(DATA / "usage.sqlite3")
    dashboard = Dashboard((os.environ.get("DASHBOARD_BIND_ADDRESS", "127.0.0.1"), 8081), Handler)
    if REPORT.is_file():
        dashboard.refresh = Refresh(datetime.fromtimestamp(REPORT.stat().st_mtime, UTC))
    Thread(target=dashboard.serve_forever, daemon=True).start()
    pricing = None
    if os.environ.get("SVINCOLO_SPEND_DIR"):
        import spend
        spend.initialize()
        spec = importlib.util.spec_from_file_location("svincolo_pricing", RENDERER)
        pricing = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = pricing
        spec.loader.exec_module(pricing)
    last_log = None
    while True:
        begin = time.monotonic()
        try:
            errors = collect(db, api)
            now = datetime.now(UTC)
            if pricing and not errors:
                publish_usage(db, now, pricing)
            render(db, now)
            dashboard.refresh = Refresh(datetime.now(UTC), not errors)
            state = "incomplete (" + ", ".join(errors) + ")" if errors else "ready"
        except (OSError, HTTPException, KeyError, ValueError,
                sqlite3.Error, subprocess.SubprocessError) as error:
            dashboard.refresh = Refresh(dashboard.refresh.rendered)
            state = f"unavailable ({error_kind(error)})"
        if last_log != state:
            print(f"cost-ui: {state}", file=sys.stderr, flush=True)
            last_log = state
        time.sleep(max(0, POLL_SECONDS - (time.monotonic() - begin)))


if __name__ == "__main__":
    main()
