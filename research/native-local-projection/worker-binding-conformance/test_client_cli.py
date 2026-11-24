"""Focused CLI validation tests for the standalone zero-JVM Node and Python clients."""

from http.server import BaseHTTPRequestHandler
import json
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import unittest

from recovery_proxies import _FastThreadingHTTPServer

HERE = Path(__file__).resolve().parent


class StandaloneClientCliTest(unittest.TestCase):
    def test_submit_validates_complete_signed_int32_argument(self):
        received: list[dict] = []
        lock = threading.Lock()

        class FakeCoordinator(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format, *args):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                with lock:
                    received.append(body)
                resp = b'{"executionId":"exec-1","clientKey":"ck"}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                self.wfile.write(resp)

        server = _FastThreadingHTTPServer(("127.0.0.1", 0), FakeCoordinator)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(lambda: (server.shutdown(), server.server_close(), thread.join(timeout=5)))
        base_url = f"http://127.0.0.1:{server.server_port}"

        node = shutil.which("node")
        self.assertIsNotNone(node, "node must be on PATH")
        runners = (
            ("node", [node, str(HERE / "client.mjs")]),
            ("python", [sys.executable, "-B", str(HERE / "client.py")]),
        )

        for label, prefix in runners:
            for valid in ("3", "-42", "0", "2147483647", "-2147483648"):
                proc = subprocess.run(
                    [*prefix, "submit", base_url, "tok", "tenant-7", "pipe", "ck", "v1", "rel-1", "InType", valid],
                    capture_output=True, text=True, check=False, timeout=10,
                )
                self.assertEqual(0, proc.returncode, f"{label} rejected valid int32 {valid!r}: {proc.stderr}")
                with lock:
                    self.assertEqual(json.dumps({"value": int(valid)}, separators=(",", ":")),
                                     received[-1]["inputPayload"]["payload"])

            with lock:
                before_invalid = len(received)
            for invalid in ("3.9", "3invalid", " 3", "3 ", "", "+3", "2147483648", "-2147483649"):
                proc = subprocess.run(
                    [*prefix, "submit", base_url, "tok", "tenant-7", "pipe", "ck", "v1", "rel-1", "InType", invalid],
                    capture_output=True, text=True, check=False, timeout=10,
                )
                self.assertNotEqual(0, proc.returncode, f"{label} accepted invalid int32 {invalid!r}")
            with lock:
                self.assertEqual(before_invalid, len(received), f"{label} sent HTTP request for invalid input")


if __name__ == "__main__":
    unittest.main()
