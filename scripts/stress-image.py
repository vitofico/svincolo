#!/usr/bin/env python3
"""Run the exact Linux release executable against a local HTTP mock."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing
from hashlib import sha256
from http.client import HTTPConnection, HTTPResponse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from queue import Queue
import select
import socket
import subprocess
import sys
import tempfile
from threading import Event, Thread
from time import monotonic, sleep

CHAT = "/v1/chat/completions"
REQUEST = b'{"model":"local","messages":[]}'
FIRST = b"data: first\n\n"
LAST = b"data: [DONE]\n\n"
STREAMS = 128
MIB = 1024 * 1024
BODY_LIMIT = 8 * MIB
BODY_BUDGET = 4 * BODY_LIMIT
AUDIO = "/v1/audio/transcriptions"
AUDIO_TYPE = "multipart/form-data; boundary=svincolo-stress"
AUDIO_DATA = b"\x00\xff" * ((BODY_LIMIT - 65536) // 2)
AUDIO_END = b"\r\n--svincolo-stress--\r\n"


def audio_prefix(model):
    return (b'--svincolo-stress\r\nContent-Disposition: form-data; name="model"\r\n\r\n'
            + model + b'\r\n--svincolo-stress\r\nContent-Disposition: form-data; '
            b'name="file"; filename="sample.wav"\r\nContent-Type: audio/wav\r\n\r\n')


AUDIO_PREFIX = audio_prefix(b"audio")
PINNED_PREFIX = audio_prefix(b"audio-pinned-v1")
AUDIO_LENGTH = len(AUDIO_PREFIX) + len(AUDIO_DATA) + len(AUDIO_END)
PINNED_LENGTH = len(PINNED_PREFIX) + len(AUDIO_DATA) + len(AUDIO_END)
PINNED_HASH = sha256(PINNED_PREFIX + AUDIO_DATA + AUDIO_END).digest()


class Mock(ThreadingHTTPServer):
    request_queue_size = 256
    daemon_threads = True


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_POST(self):
        if self.path == AUDIO:
            self.server.audio_requests.put(None)
            length = int(self.headers["Content-Length"])
            if length != PINNED_LENGTH or self.headers["Content-Type"] != AUDIO_TYPE:
                self.send_error(400)
                return
            digest = sha256()
            while length:
                chunk = self.rfile.read(min(length, 65536))
                if not chunk:
                    self.send_error(400)
                    return
                digest.update(chunk)
                length -= len(chunk)
            if digest.digest() != PINNED_HASH:
                self.send_error(400)
                return
            self.send_json()
            return
        if self.rfile.read(int(self.headers["Content-Length"])) != REQUEST:
            self.send_error(400)
            return
        if self.path == CHAT + "?stream=1":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            self.wfile.write(FIRST)
            self.wfile.flush()
            if release.wait(12):
                try:
                    self.wfile.write(LAST)
                except (BrokenPipeError, ConnectionResetError):
                    pass
            return
        if self.path == CHAT + "?slow-reader=1":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            self.wfile.write(FIRST)
            self.wfile.flush()
            if slow_release.wait(12):
                try:
                    chunk = (b"data: " + b"x" * 4088 + b"\n\n") * 256
                    for _ in range(4):
                        self.wfile.write(chunk)
                    self.wfile.write(LAST)
                except (BrokenPipeError, ConnectionResetError):
                    pass
            return
        self.send_json()

    def send_json(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")


def rss(pid, field="VmRSS"):
    lines = Path(f"/proc/{pid}/status").read_text().splitlines()
    return 1024 * int(next(line.split()[1] for line in lines if line.startswith(field + ":")))


def request(port, suffix="", path=CHAT):
    conn = HTTPConnection("127.0.0.1", port, timeout=8)
    method = "POST" if path == CHAT else "GET"
    conn.request(method, path + suffix, REQUEST if method == "POST" else None,
                 {"Content-Type": "application/json"})
    return conn, conn.getresponse()


def status(port, expected, code=None, path=CHAT):
    conn, reply = request(port, path=path)
    with closing(conn), reply:
        assert reply.status == expected, (reply.status, reply.read())
        if code:
            assert reply.getheader("x-svincolo-provider-reached") == "false"
            assert json.load(reply)["error"]["code"] == code
        else:
            reply.read()


def held_stream(port, suffix="?stream=1"):
    conn, reply = request(port, suffix)
    assert reply.status == 200 and reply.readline() == b"data: first\n"
    return conn, reply


def stalled_upload(port):
    conn = socket.create_connection(("127.0.0.1", port), timeout=8)
    conn.sendall(
        f"POST {CHAT} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n"
        "Transfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n".encode()
    )
    with conn.makefile("rb") as reader:
        assert reader.readline() == b"HTTP/1.1 100 Continue\r\n"
        assert reader.readline() == b"\r\n"
    conn.sendall(b"8000\r\n" + b"x" * 32768 + b"\r\n")
    return conn


def exercise(port, pid):
    status(port, 200)
    idle = rss(pid)
    assert idle < 20 * MIB, idle
    with ThreadPoolExecutor(max_workers=32) as pool:
        streams = list(pool.map(lambda _: held_stream(port), range(STREAMS)))
    try:
        status(port, 429, "server_busy")
        peak = max(rss(pid) for _ in range(4))
        per_stream = (peak - idle) / STREAMS
        assert per_stream <= 80_000, (idle, peak, per_stream)
    finally:
        release.set()
        for conn, reply in streams:
            with closing(conn), reply:
                assert reply.read() == b"\n" + LAST
    status(port, 200)

    idle_sockets = [socket.create_connection(("127.0.0.1", port), timeout=5) for _ in range(128)]
    try:
        for conn in idle_sockets:
            conn.sendall(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n")
        status(port, 200, path="/readyz")
        uploads = [stalled_upload(port) for _ in range(4)]
        try:
            status(port, 429, "body_budget_exhausted")
        finally:
            for conn in uploads:
                conn.close()
    finally:
        for conn in idle_sockets:
            conn.close()
    # After cancellation the buffer and active slots must become reusable.
    deadline = monotonic() + 3
    while True:
        conn, reply = request(port)
        with closing(conn), reply:
            reply.read()
            if reply.status == 200:
                break
            assert reply.status == 429 and monotonic() < deadline
        sleep(0.01)

    slow = [held_stream(port, "?slow-reader=1") for _ in range(8)]
    try:
        slow_release.set()
        sleep(0.2)
        status(port, 200)
    finally:
        for conn, reply in slow:
            reply.close()
            conn.close()
    return {"idle_rss_bytes": idle, "stream_rss_bytes": peak, "bytes_per_stream": per_stream,
            "streams": STREAMS, "idle_connections": 128, "stalled_uploads": 4, "slow_readers": 8}


def audio_headers():
    return {"Content-Type": AUDIO_TYPE, "Content-Length": str(AUDIO_LENGTH)}


def send_audio_prefix(conn):
    conn.sendall(AUDIO_PREFIX)
    audio = memoryview(AUDIO_DATA)
    for offset in range(0, len(audio), 256 * 1024):
        conn.sendall(audio[offset:offset + 256 * 1024])
        sleep(0.005)


def start_audio(port):
    conn = socket.create_connection(("127.0.0.1", port), timeout=8)
    try:
        conn.sendall((f"POST {AUDIO} HTTP/1.1\r\nHost: localhost\r\n"
                      f"Content-Type: {AUDIO_TYPE}\r\nContent-Length: {AUDIO_LENGTH}\r\n"
                      "Expect: 100-continue\r\n\r\n").encode())
        with conn.makefile("rb") as reader:
            line = reader.readline()
            if line == b"HTTP/1.1 100 Continue\r\n":
                assert reader.readline() == b"\r\n"
                return conn
            assert line.startswith(b"HTTP/1.1 429 "), line
    except BaseException:
        conn.close()
        raise
    conn.close()
    return None


def exercise_mixed(port, pid, mock):
    release.clear()
    slow_release.clear()
    with ExitStack() as connections:
        def hold(suffix):
            conn, reply = held_stream(port, suffix)
            connections.enter_context(closing(conn))
            connections.enter_context(reply)
            return reply

        streams = [hold("?stream=1") for _ in range(16)]
        slow = [hold("?slow-reader=1") for _ in range(4)]
        slow_release.set()
        uploads = [start_audio(port) for _ in range(4)]
        assert all(uploads)
        for conn in uploads:
            connections.enter_context(conn)
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(send_audio_prefix, uploads))

        assert mock.audio_requests.empty(), "incomplete audio reached upstream"
        with closing(HTTPConnection("127.0.0.1", port, timeout=8)) as excess:
            excess.request("POST", AUDIO, headers=audio_headers())
            with excess.getresponse() as reply:
                assert reply.status == 429, reply.status
                assert reply.getheader("x-svincolo-provider-reached") == "false"
                assert json.load(reply)["error"]["code"] == "body_budget_exhausted"
        status(port, 200, path="/readyz")
        status(port, 200)

        uploads.pop().close()
        deadline = monotonic() + 3
        while (replacement := start_audio(port)) is None:
            assert monotonic() < deadline, "cancelled audio reservation was retained"
            sleep(0.01)
        uploads.append(connections.enter_context(replacement))
        send_audio_prefix(replacement)
        assert mock.audio_requests.empty(), "an incomplete or refused upload reached upstream"

        for conn in uploads:
            conn.sendall(AUDIO_END)
        for conn in uploads:
            with HTTPResponse(conn) as reply:
                reply.begin()
                assert reply.status == 200, reply.status
                assert reply.read() == b"{}"
        assert mock.audio_requests.qsize() == 4
        peak = rss(pid, "VmHWM")
        assert peak < 96 * MIB, peak

        release.set()
        for reply in streams:
            assert reply.read() == b"\n" + LAST
        for reply in slow:
            assert reply.read().endswith(LAST)

    return {"active_streams": 16, "slow_readers": 4, "audio_uploads": 4,
            "audio_bytes_per_request": AUDIO_LENGTH, "buffer_budget_bytes": BODY_BUDGET,
            "peak_process_rss_bytes": peak, "budget_rejections": 1, "upstream_audio_calls": 4}


def main(binary):
    with Mock(("127.0.0.1", 0), Handler) as mock, tempfile.TemporaryDirectory() as tmp:
        mock.audio_requests = Queue()
        Thread(target=mock.serve_forever, daemon=True).start()
        path = Path(tmp, "svincolo.toml")
        path.write_text(f"""version = 1
[server]
listen = "127.0.0.1:0"
local_only = true
max_in_flight = {STREAMS}
max_body_bytes = {BODY_LIMIT}
max_buffered_bytes = {BODY_BUDGET}
drain_delay_s = 0
grace_s = 2

[[upstream]]
name = "mock"
kind = "openai_compatible"
base_url = "http://127.0.0.1:{mock.server_port}/v1"

[[route]]
name = "local"
upstream = "mock"
locality = "local"
surfaces = {{ chat = "raw" }}

[[route]]
name = "audio"
upstream = "mock"
upstream_model = "audio-pinned-v1"
locality = "local"
surfaces = {{ transcriptions = "raw" }}
""")
        started = monotonic()
        with subprocess.Popen(
            [str(Path(binary).resolve()), "serve", "--config", str(path)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        ) as process:
            try:
                assert select.select([process.stderr], [], [], 3)[0], "no listening message"
                line = process.stderr.readline().decode().strip()
                assert line.startswith("svincolo: listening on 127.0.0.1:"), line
                port = int(line.rsplit(":", 1)[1])
                status(port, 200, path="/readyz")
                start_ms = (monotonic() - started) * 1000
                assert start_ms < 100, start_ms
                evidence = exercise(port, process.pid)
                evidence["mixed_load"] = exercise_mixed(port, process.pid, mock)
                evidence["startup_ms"] = round(start_ms, 2)
                print(json.dumps(evidence, indent=2))
            finally:
                release.set()
                slow_release.set()
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                mock.shutdown()
            assert process.returncode == 0, process.stderr.read().decode()


if __name__ == "__main__":
    release = Event()
    slow_release = Event()
    main(sys.argv[1])
