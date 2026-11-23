"""Focused unit tests for the packaged recovery DynamoDB barrier and worker observer proxies."""

from http.client import HTTPConnection, HTTPSConnection
from http.server import BaseHTTPRequestHandler
import json
from pathlib import Path
import ssl
import tempfile
import threading
import unittest

from recovery_proxies import (
    CAPABILITIES_PATH,
    EXECUTE_PATH,
    DynamoBarrierProxy,
    WorkerHttpsObserverProxy,
    _FastThreadingHTTPServer,
)
from run_hosted_worker import certificates


def _dynamo_post(endpoint_port: int, target: str, payload: dict, timeout: float = 5.0):
    conn = HTTPConnection("127.0.0.1", endpoint_port, timeout=timeout)
    body = json.dumps(payload).encode("utf-8")
    try:
        conn.request("POST", "/", body=body, headers={
            "Content-Type": "application/x-amz-json-1.0",
            "X-Amz-Target": target,
        })
        resp = conn.getresponse()
        return resp.status, json.loads(resp.read() or b"{}")
    finally:
        conn.close()


class RecoveryProxiesTest(unittest.TestCase):
    def test_dynamo_barrier_holds_admission_ack_and_fences_claims_before_upstream(self):
        seen_upstream: list[tuple[str, dict]] = []
        lock = threading.Lock()

        class FakeDynamo(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                target = self.headers.get("X-Amz-Target", "")
                with lock:
                    seen_upstream.append((target, body))
                response = b"{}"
                self.send_response(200)
                self.send_header("Content-Type", "application/x-amz-json-1.0")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)

        upstream = _FastThreadingHTTPServer(("127.0.0.1", 0), FakeDynamo)
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(lambda: (upstream.shutdown(), upstream.server_close(), thread.join(timeout=5)))

        with DynamoBarrierProxy(f"http://127.0.0.1:{upstream.server_port}") as proxy:
            proxy_port = int(proxy.endpoint.rsplit(":", 1)[1])
            proxy.arm_admission_before_intent("t_exec", "t_key")

            admission_result: list[object] = []
            claim_result: list[object] = []

            def send_admission():
                try:
                    admission_result.append(_dynamo_post(proxy_port, "DynamoDB_20120810.TransactWriteItems", {
                        "TransactItems": [
                            {"Put": {"TableName": "t_exec", "Item": {"status": {"S": "QUEUED"}}}},
                            {"Put": {"TableName": "t_key", "Item": {"execution_id": {"S": "exec-1"}}}},
                        ],
                    }))
                except BaseException as error:
                    admission_result.append(error)

            def send_claim():
                try:
                    claim_result.append(_dynamo_post(proxy_port, "DynamoDB_20120810.UpdateItem", {
                        "TableName": "t_exec",
                        "UpdateExpression": "SET #status = :running",
                    }))
                except BaseException as error:
                    claim_result.append(error)

            t_adm = threading.Thread(target=send_admission)
            t_adm.start()
            self.assertTrue(proxy.admission_committed.wait(timeout=5))
            # Upstream committed the admission transaction, but the caller is still blocked on the held ack.
            self.assertTrue(t_adm.is_alive())
            with lock:
                self.assertEqual(1, len(seen_upstream))
                self.assertEqual("DynamoDB_20120810.TransactWriteItems", seen_upstream[0][0])

            # Ordinary scan still passes through while armed.
            scan_status, _ = _dynamo_post(proxy_port, "DynamoDB_20120810.Scan", {"TableName": "t_exec"})
            self.assertEqual(200, scan_status)

            # Claim UpdateItem is fenced before reaching upstream.
            t_claim = threading.Thread(target=send_claim)
            t_claim.start()
            self.assertTrue(proxy.claim_held.wait(timeout=5))
            self.assertTrue(t_claim.is_alive())
            self.assertEqual(1, proxy.held_claims)
            with lock:
                self.assertFalse(any(t == "DynamoDB_20120810.UpdateItem" for t, _ in seen_upstream))

            # Discarding held requests closes old connections without forwarding the claim and disarms the proxy.
            proxy.discard_held_and_disarm()
            t_adm.join(timeout=5)
            t_claim.join(timeout=5)
            self.assertFalse(t_adm.is_alive())
            self.assertFalse(t_claim.is_alive())
            self.assertIsInstance(admission_result[0], BaseException)
            self.assertIsInstance(claim_result[0], BaseException)
            with lock:
                self.assertFalse(any(t == "DynamoDB_20120810.UpdateItem" for t, _ in seen_upstream))

            # After disarming, a restarted process's claim UpdateItem passes through normally.
            claim_status, _ = _dynamo_post(proxy_port, "DynamoDB_20120810.UpdateItem", {
                "TableName": "t_exec",
                "UpdateExpression": "SET #status = :running",
            })
            self.assertEqual(200, claim_status)
            with lock:
                self.assertTrue(any(t == "DynamoDB_20120810.UpdateItem" for t, _ in seen_upstream))

    def test_worker_https_proxy_verifies_tls_preserves_headers_and_runs_pre_forward_check(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td).resolve()
            certificates(work)

            observed_worker: list[tuple[str, str, str, bytes]] = []
            pre_forward_seen: list[bytes] = []

            server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_tls.load_cert_chain(str(work / "worker.pem"), str(work / "worker.key"))

            class FakeWorkerServer(_FastThreadingHTTPServer):
                def process_request_thread(self, request, client_address):
                    with server_tls.wrap_socket(request, server_side=True) as secured:
                        super().process_request_thread(secured, client_address)

            class FakeWorker(BaseHTTPRequestHandler):
                protocol_version = "HTTP/1.1"

                def log_message(self, format, *args):
                    pass

                def do_GET(self):
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("X-TPF-Worker-Projection-Digest", "sha256:" + "a" * 64)
                    body = b'{"ok":true}'
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                def do_POST(self):
                    length = int(self.headers.get("Content-Length", "0"))
                    raw = self.rfile.read(length)
                    observed_worker.append((
                        self.command,
                        self.path,
                        self.headers.get("X-TPF-Worker-Signature", ""),
                        raw,
                    ))
                    body = b'{"outcome":"COMPLETED"}'
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

            upstream = FakeWorkerServer(("127.0.0.1", 0), FakeWorker)
            thread = threading.Thread(target=upstream.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(lambda: (upstream.shutdown(), upstream.server_close(), thread.join(timeout=5)))

            def before_forward(raw_body: bytes):
                # Must execute before upstream worker receives the POST.
                self.assertEqual([], observed_worker)
                pre_forward_seen.append(raw_body)

            with WorkerHttpsObserverProxy(
                upstream_port=upstream.server_port,
                cafile=work / "ca.pem",
                certfile=work / "worker.pem",
                keyfile=work / "worker.key",
                before_execute_forward=before_forward,
            ) as proxy:
                client_tls = ssl.create_default_context(cafile=str(work / "ca.pem"))
                conn = HTTPSConnection("localhost", proxy.port, context=client_tls, timeout=5)
                try:
                    conn.request("GET", CAPABILITIES_PATH, headers={"Accept": "application/json"})
                    resp = conn.getresponse()
                    self.assertEqual(200, resp.status)
                    self.assertEqual(["sha256:" + "a" * 64], resp.headers.get_all("X-TPF-Worker-Projection-Digest"))
                    self.assertEqual(b'{"ok":true}', resp.read())
                finally:
                    conn.close()

                payload = b'{"executionId":"exec-9"}'
                conn = HTTPSConnection("localhost", proxy.port, context=client_tls, timeout=5)
                try:
                    conn.request("POST", EXECUTE_PATH, body=payload, headers={
                        "Content-Type": "application/json",
                        "X-TPF-Worker-Signature": "sig-exact",
                    })
                    resp = conn.getresponse()
                    self.assertEqual(200, resp.status)
                    self.assertEqual(b'{"outcome":"COMPLETED"}', resp.read())
                finally:
                    conn.close()

                self.assertEqual(1, proxy.capability_gets)
                self.assertEqual(1, proxy.execute_posts)
                self.assertEqual(1, proxy.pre_forward_checks)
                self.assertEqual([], proxy.callback_errors)
                self.assertEqual([payload], pre_forward_seen)
                self.assertEqual([("POST", EXECUTE_PATH, "sig-exact", payload)], observed_worker)


if __name__ == "__main__":
    unittest.main()
