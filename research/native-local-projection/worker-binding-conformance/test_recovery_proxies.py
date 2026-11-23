"""Focused unit tests for the packaged recovery DynamoDB barrier and worker observer proxies."""

from http.client import HTTPConnection, HTTPSConnection, RemoteDisconnected
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
    IngressHttpBarrierProxy,
    WorkerHttpsObserverProxy,
    _FastThreadingHTTPServer,
)
from run_hosted_worker import certificates


def _dynamo_post(endpoint_port: int, target: str, payload: dict, timeout: float = 5.0, extra_headers=None):
    conn = HTTPConnection("127.0.0.1", endpoint_port, timeout=timeout)
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": target,
    } | (extra_headers or {})
    try:
        conn.request("POST", "/", body=body, headers=headers)
        resp = conn.getresponse()
        return resp.status, dict(resp.getheaders()), json.loads(resp.read() or b"{}")
    finally:
        conn.close()


class RecoveryProxiesTest(unittest.TestCase):
    def test_dynamo_barrier_holds_admission_ack_fences_claims_and_closes_held_connections(self):
        seen_upstream: list[tuple[str, dict, dict]] = []
        lock = threading.Lock()

        class FakeDynamo(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                target = self.headers.get("X-Amz-Target", "")
                with lock:
                    seen_upstream.append((target, {k.lower(): v for k, v in self.headers.raw_items()}, body))
                response = b"{}"
                self.send_response(200)
                self.send_header("Content-Type", "application/x-amz-json-1.0")
                self.send_header("Connection", "X-Resp-Hop")
                self.send_header("X-Resp-Hop", "strip-me")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)

        upstream = _FastThreadingHTTPServer(("127.0.0.1", 0), FakeDynamo)
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(lambda: (upstream.shutdown(), upstream.server_close(), thread.join(timeout=5)))
        upstream_url = f"http://127.0.0.1:{upstream.server_port}"

        with DynamoBarrierProxy(upstream_url) as proxy:
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
                    }, timeout=20.0))
                except BaseException as error:
                    admission_result.append(error)

            def send_claim():
                try:
                    claim_result.append(_dynamo_post(proxy_port, "DynamoDB_20120810.UpdateItem", {
                        "TableName": "t_exec",
                        "UpdateExpression": "SET #status = :running",
                    }, timeout=20.0))
                except BaseException as error:
                    claim_result.append(error)

            t_adm = threading.Thread(target=send_admission)
            t_adm.start()
            self.assertTrue(proxy.admission_committed.wait(timeout=5))
            self.assertTrue(t_adm.is_alive())
            with lock:
                self.assertEqual(1, len(seen_upstream))
                self.assertEqual("DynamoDB_20120810.TransactWriteItems", seen_upstream[0][0])

            # Ordinary scan passes through while armed and strips Connection-nominated hop headers in both directions.
            scan_status, scan_headers, _ = _dynamo_post(
                proxy_port, "DynamoDB_20120810.Scan", {"TableName": "t_exec"},
                extra_headers={"Connection": "X-Req-Hop", "X-Req-Hop": "strip-me"},
            )
            self.assertEqual(200, scan_status)
            self.assertNotIn("X-Resp-Hop", scan_headers)
            with lock:
                self.assertNotIn("x-req-hop", seen_upstream[-1][1])

            # Claim UpdateItem is fenced before reaching upstream.
            t_claim = threading.Thread(target=send_claim)
            t_claim.start()
            self.assertTrue(proxy.claim_held.wait(timeout=5))
            self.assertTrue(t_claim.is_alive())
            self.assertEqual(1, proxy.held_claims)
            with lock:
                self.assertFalse(any(t == "DynamoDB_20120810.UpdateItem" for t, _, _ in seen_upstream))

            # Fencing permanently closes both held connections (RemoteDisconnected, not timeout) and blocks new relays.
            proxy.fence_and_discard()
            t_adm.join(timeout=5)
            t_claim.join(timeout=5)
            self.assertFalse(t_adm.is_alive())
            self.assertFalse(t_claim.is_alive())
            self.assertIsInstance(admission_result[0], RemoteDisconnected)
            self.assertIsInstance(claim_result[0], RemoteDisconnected)
            with self.assertRaises((RemoteDisconnected, ConnectionResetError)):
                _dynamo_post(proxy_port, "DynamoDB_20120810.UpdateItem", {"TableName": "t_exec"})
            with lock:
                self.assertFalse(any(t == "DynamoDB_20120810.UpdateItem" for t, _, _ in seen_upstream))

        # A fresh proxy for a restarted process forwards claims normally.
        with DynamoBarrierProxy(upstream_url) as restarted_proxy:
            restarted_port = int(restarted_proxy.endpoint.rsplit(":", 1)[1])
            claim_status, _, _ = _dynamo_post(restarted_port, "DynamoDB_20120810.UpdateItem", {
                "TableName": "t_exec",
                "UpdateExpression": "SET #status = :running",
            })
            self.assertEqual(200, claim_status)
            with lock:
                self.assertTrue(any(t == "DynamoDB_20120810.UpdateItem" for t, _, _ in seen_upstream))

    def test_worker_https_proxy_verifies_tls_preserves_headers_and_runs_pre_forward_check(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td).resolve()
            certificates(work)

            observed_worker: list[tuple[str, str, str, dict, bytes]] = []
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
                    self.send_header("Connection", "X-Worker-Hop")
                    self.send_header("X-Worker-Hop", "drop")
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
                        {k.lower(): v for k, v in self.headers.raw_items()},
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
                self.assertEqual([], observed_worker)
                pre_forward_seen.append(raw_body)

            with WorkerHttpsObserverProxy(
                cafile=work / "ca.pem",
                certfile=work / "worker.pem",
                keyfile=work / "worker.key",
                before_execute_forward=before_forward,
            ) as proxy:
                proxy.set_upstream_port(upstream.server_port)
                client_tls = ssl.create_default_context(cafile=str(work / "ca.pem"))
                conn = HTTPSConnection("localhost", proxy.port, context=client_tls, timeout=5)
                try:
                    conn.request("GET", CAPABILITIES_PATH, headers={"Accept": "application/json"})
                    resp = conn.getresponse()
                    self.assertEqual(200, resp.status)
                    self.assertEqual(["sha256:" + "a" * 64], resp.headers.get_all("X-TPF-Worker-Projection-Digest"))
                    self.assertIsNone(resp.headers.get("X-Worker-Hop"))
                    self.assertEqual(b'{"ok":true}', resp.read())
                finally:
                    conn.close()

                payload = b'{"executionId":"exec-9"}'
                conn = HTTPSConnection("localhost", proxy.port, context=client_tls, timeout=5)
                try:
                    conn.request("POST", EXECUTE_PATH, body=payload, headers={
                        "Content-Type": "application/json",
                        "X-TPF-Worker-Signature": "sig-exact",
                        "Connection": "X-Client-Hop",
                        "X-Client-Hop": "drop",
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
                self.assertEqual(1, len(observed_worker))
                self.assertEqual(("POST", EXECUTE_PATH, "sig-exact", payload),
                                 (observed_worker[0][0], observed_worker[0][1], observed_worker[0][2], observed_worker[0][4]))
                self.assertNotIn("x-client-hop", observed_worker[0][3])

    def test_dynamo_barrier_stale_settlement_holds_renewals_and_captures_rejected_late_settlement(self):
        seen_upstream: list[tuple[str, str]] = []
        lock = threading.Lock()

        class FakeDynamo(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                target = self.headers.get("X-Amz-Target", "")
                expr = str(body.get("UpdateExpression") or "")
                with lock:
                    seen_upstream.append((target, expr))
                if "#status = :succeeded" in expr:
                    if body.get("ReturnValues") == "VALIDATION_ERROR_CONTROL":
                        resp = (b'{"__type":"com.amazonaws.dynamodb.v20120810#ValidationException",'
                                b'"message":"mentions ConditionalCheckFailedException in text"}')
                    else:
                        resp = b'{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException"}'
                    status = 400
                else:
                    resp = b"{}"
                    status = 200
                self.send_response(status)
                self.send_header("Content-Type", "application/x-amz-json-1.0")
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                self.wfile.write(resp)

        upstream = _FastThreadingHTTPServer(("127.0.0.1", 0), FakeDynamo)
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(lambda: (upstream.shutdown(), upstream.server_close(), thread.join(timeout=5)))

        with DynamoBarrierProxy(f"http://127.0.0.1:{upstream.server_port}") as proxy:
            proxy_port = int(proxy.endpoint.rsplit(":", 1)[1])
            proxy.arm_stale_settlement("t_exec")

            renewal_outcome: list[object] = []

            def send_renewal():
                try:
                    renewal_outcome.append(_dynamo_post(proxy_port, "DynamoDB_20120810.UpdateItem", {
                        "TableName": "t_exec",
                        "UpdateExpression": "SET #leaseExpires = :leaseExpires, #updated = :now",
                    }, timeout=20.0))
                except BaseException as error:
                    renewal_outcome.append(error)

            t_renew = threading.Thread(target=send_renewal)
            t_renew.start()
            self.assertTrue(proxy.renewal_held.wait(timeout=5))
            self.assertEqual(1, proxy.held_renewals)
            with lock:
                self.assertEqual([], seen_upstream)

            # Ordinary sweep unknown-settlement passes through while renewals remain held.
            unknown_status, _, _ = _dynamo_post(proxy_port, "DynamoDB_20120810.UpdateItem", {
                "TableName": "t_exec",
                "UpdateExpression": "SET #status = :unknown, #version = #version + :one",
            })
            self.assertEqual(200, unknown_status)

            # A 400 ValidationException mentioning ConditionalCheckFailedException in its text must NOT count.
            validation_status, _, _ = _dynamo_post(proxy_port, "DynamoDB_20120810.UpdateItem", {
                "TableName": "t_exec",
                "UpdateExpression": "SET #status = :succeeded, #version = #version + :one",
                "ReturnValues": "VALIDATION_ERROR_CONTROL",
            })
            self.assertEqual(400, validation_status)
            self.assertFalse(proxy.late_settlement_rejected.is_set())
            self.assertEqual(0, proxy.confirmed_late_settlement_rejections)

            # Confirmed ConditionalCheckFailedException reaches upstream and sets the rejection event.
            late_status, _, _ = _dynamo_post(proxy_port, "DynamoDB_20120810.UpdateItem", {
                "TableName": "t_exec",
                "UpdateExpression": "SET #status = :succeeded, #version = #version + :one",
            })
            self.assertEqual(400, late_status)
            self.assertTrue(proxy.late_settlement_rejected.wait(timeout=5))
            self.assertEqual(1, proxy.confirmed_late_settlement_rejections)
            self.assertEqual(2, len(proxy.late_settlement_observations))

            # Verify upstream has NOT received any lease renewal immediately before explicit release.
            with lock:
                self.assertFalse(any("#leaseExpires = :leaseExpires" in expr for _, expr in seen_upstream))

            # Releasing held renewals forwards them cleanly to upstream after late settlement was observed.
            proxy.release_held_renewals()
            t_renew.join(timeout=5)
            self.assertFalse(t_renew.is_alive())
            self.assertEqual(200, renewal_outcome[0][0])
            with lock:
                self.assertTrue(any("#leaseExpires = :leaseExpires" in expr for _, expr in seen_upstream))

    def test_worker_https_proxy_holds_and_releases_or_discards_execute_response(self):
        with tempfile.TemporaryDirectory() as td:
            work = Path(td).resolve()
            certificates(work)

            server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_tls.load_cert_chain(str(work / "worker.pem"), str(work / "worker.key"))

            class FakeWorkerServer(_FastThreadingHTTPServer):
                def process_request_thread(self, request, client_address):
                    with server_tls.wrap_socket(request, server_side=True) as secured:
                        super().process_request_thread(secured, client_address)

            full_bodies_received: list[bytes] = []
            body_lock = threading.Lock()

            class FakeWorker(BaseHTTPRequestHandler):
                protocol_version = "HTTP/1.1"

                def log_message(self, format, *args):
                    pass

                def do_POST(self):
                    length = int(self.headers.get("Content-Length", "0"))
                    try:
                        data = self.rfile.read(length)
                    except OSError:
                        return
                    if len(data) < length:
                        return
                    with body_lock:
                        full_bodies_received.append(data)
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

            with WorkerHttpsObserverProxy(
                cafile=work / "ca.pem",
                certfile=work / "worker.pem",
                keyfile=work / "worker.key",
            ) as proxy:
                proxy.set_upstream_port(upstream.server_port)
                client_tls = ssl.create_default_context(cafile=str(work / "ca.pem"))

                def send_exec(out: list[object]):
                    conn = HTTPSConnection("localhost", proxy.port, context=client_tls, timeout=5)
                    try:
                        conn.request("POST", EXECUTE_PATH, body=b'{"x":1}', headers={"Content-Type": "application/json"})
                        resp = conn.getresponse()
                        out.append((resp.status, resp.read()))
                    except BaseException as error:
                        out.append(error)
                    finally:
                        conn.close()

                # Release path delivers the exact captured completion.
                proxy.arm_hold_execute_response()
                released: list[object] = []
                t1 = threading.Thread(target=send_exec, args=(released,))
                t1.start()
                self.assertTrue(proxy.execute_response_captured.wait(timeout=5))
                self.assertTrue(t1.is_alive())
                self.assertEqual(200, proxy.captured_execute_response[0])
                proxy.release_execute_response()
                t1.join(timeout=5)
                self.assertEqual([(200, b'{"outcome":"COMPLETED"}')], released)

                # Discard path closes the held response channel without delivering bytes.
                proxy.arm_hold_execute_response()
                discarded: list[object] = []
                t2 = threading.Thread(target=send_exec, args=(discarded,))
                t2.start()
                self.assertTrue(proxy.execute_response_captured.wait(timeout=5))
                self.assertTrue(t2.is_alive())
                proxy.discard_execute_response()
                t2.join(timeout=5)
                self.assertIsInstance(discarded[0], RemoteDisconnected)

                # Replace-response mode captures valid upstream completion and returns corrupted bytes on same channel.
                proxy.arm_replace_execute_response(200, b'{"outcome":"COMPLETED"')
                replaced: list[object] = []
                send_exec(replaced)
                self.assertTrue(proxy.execute_response_captured.wait(timeout=5))
                self.assertEqual((200, b'{"outcome":"COMPLETED"}'),
                                 (proxy.captured_execute_response[0], proxy.captured_execute_response[2]))
                self.assertEqual([(200, b'{"outcome":"COMPLETED"')], replaced)
                proxy.clear_execute_policy()

                # Short-circuit 503 returns plain 503 without contacting upstream.
                with body_lock:
                    before_sc = len(full_bodies_received)
                proxy.arm_short_circuit_execute(503, b"Service Unavailable")
                sc_out: list[object] = []
                send_exec(sc_out)
                self.assertEqual([(503, b"Service Unavailable")], sc_out)
                self.assertEqual(1, proxy.short_circuited_executes)
                self.assertEqual([(503, b"Service Unavailable")], proxy.delivered_short_circuit_responses)
                with body_lock:
                    self.assertEqual(before_sc, len(full_bodies_received))
                proxy.clear_execute_policy()

                # Withhold-body opens verified TLS and sends headers only until released, then closes exchange.
                proxy.arm_withhold_execute_body()
                withheld_out: list[object] = []
                t3 = threading.Thread(target=send_exec, args=(withheld_out,))
                t3.start()
                self.assertTrue(proxy.execute_body_withheld.wait(timeout=5))
                self.assertTrue(t3.is_alive())
                with body_lock:
                    self.assertEqual(before_sc, len(full_bodies_received))
                proxy.release_withheld_exchange()
                t3.join(timeout=5)
                self.assertFalse(t3.is_alive())
                self.assertIsInstance(withheld_out[0], (RemoteDisconnected, ConnectionResetError))
                with body_lock:
                    self.assertEqual(before_sc, len(full_bodies_received))

    def test_dynamo_barrier_duplicate_cdi_delivery_holds_ack_and_forwards_both_claims(self):
        claim_calls = 0
        lock = threading.Lock()

        class FakeDynamo(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass

            def do_POST(self):
                nonlocal claim_calls
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                target = self.headers.get("X-Amz-Target", "")
                status = 200
                resp = b"{}"
                if target == "DynamoDB_20120810.UpdateItem":
                    with lock:
                        claim_calls += 1
                        num = claim_calls
                    if num > 1:
                        status = 400
                        resp = b'{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException"}'
                self.send_response(status)
                self.send_header("Content-Type", "application/x-amz-json-1.0")
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                self.wfile.write(resp)

        upstream = _FastThreadingHTTPServer(("127.0.0.1", 0), FakeDynamo)
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(lambda: (upstream.shutdown(), upstream.server_close(), thread.join(timeout=5)))

        with DynamoBarrierProxy(f"http://127.0.0.1:{upstream.server_port}") as proxy:
            proxy_port = int(proxy.endpoint.rsplit(":", 1)[1])
            proxy.arm_duplicate_cdi_delivery("t_exec", "t_key")

            adm_out, c1_out, c2_out = [], [], []
            claim_body = {
                "TableName": "t_exec",
                "UpdateExpression": "SET #status = :running, #leaseOwner = :leaseOwner",
            }
            t_adm = threading.Thread(target=lambda: adm_out.append(_dynamo_post(
                proxy_port, "DynamoDB_20120810.TransactWriteItems", {
                    "TransactItems": [
                        {"Put": {"TableName": "t_exec", "Item": {"status": {"S": "QUEUED"}}}},
                        {"Put": {"TableName": "t_key", "Item": {"execution_id": {"S": "exec-1"}}}},
                    ],
                }, timeout=20.0)))
            t_adm.start()
            self.assertTrue(proxy.admission_committed.wait(timeout=5))

            t_c1 = threading.Thread(target=lambda: c1_out.append(_dynamo_post(
                proxy_port, "DynamoDB_20120810.UpdateItem", claim_body, timeout=20.0)))
            t_c1.start()
            self.assertTrue(proxy.claim_held.wait(timeout=5))
            with lock:
                self.assertEqual(0, claim_calls)

            proxy.release_admission_ack()
            t_adm.join(timeout=5)
            self.assertEqual(200, adm_out[0][0])

            t_c2 = threading.Thread(target=lambda: c2_out.append(_dynamo_post(
                proxy_port, "DynamoDB_20120810.UpdateItem", claim_body, timeout=20.0)))
            t_c2.start()
            self.assertTrue(proxy.second_claim_held.wait(timeout=5))
            with lock:
                self.assertEqual(0, claim_calls)

            proxy.release_held_claims()
            t_c1.join(timeout=5)
            t_c2.join(timeout=5)
            self.assertEqual(2, claim_calls)
            self.assertEqual(sorted([(200, False), (400, True)]), sorted(proxy.claim_outcomes))

    def test_dynamo_barrier_death_after_settlement_commits_and_holds_ack(self):
        commits: list[str] = []
        lock = threading.Lock()

        class FakeDynamo(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                with lock:
                    commits.append(str(body.get("UpdateExpression") or ""))
                self.send_response(200)
                self.send_header("Content-Type", "application/x-amz-json-1.0")
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

        upstream = _FastThreadingHTTPServer(("127.0.0.1", 0), FakeDynamo)
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(lambda: (upstream.shutdown(), upstream.server_close(), thread.join(timeout=5)))

        with DynamoBarrierProxy(f"http://127.0.0.1:{upstream.server_port}") as proxy:
            proxy_port = int(proxy.endpoint.rsplit(":", 1)[1])
            proxy.arm_death_after_settlement("t_exec")
            outcome: list[object] = []

            def send_settlement():
                try:
                    outcome.append(_dynamo_post(proxy_port, "DynamoDB_20120810.UpdateItem", {
                        "TableName": "t_exec",
                        "UpdateExpression": "SET #status = :succeeded, #version = #version + :one",
                    }, timeout=20.0))
                except BaseException as error:
                    outcome.append(error)

            t = threading.Thread(target=send_settlement)
            t.start()
            self.assertTrue(proxy.settlement_committed.wait(timeout=5))
            self.assertTrue(t.is_alive())
            with lock:
                self.assertEqual(1, len(commits))
            proxy.fence_and_discard()
            t.join(timeout=5)
            self.assertFalse(t.is_alive())
            self.assertIsInstance(outcome[0], (RemoteDisconnected, ConnectionResetError))

    def test_ingress_http_barrier_drops_client_response_after_commit(self):
        class FakeCoordinator(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format, *args):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(length)
                body = b'{"executionId":"exec-42","clientKey":"ck-1"}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        upstream = _FastThreadingHTTPServer(("127.0.0.1", 0), FakeCoordinator)
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(lambda: (upstream.shutdown(), upstream.server_close(), thread.join(timeout=5)))

        with IngressHttpBarrierProxy(upstream.server_port) as proxy:
            proxy.arm_disconnect_before_receipt()
            client_out: list[object] = []

            def submit():
                conn = HTTPConnection("127.0.0.1", proxy.port, timeout=10)
                try:
                    conn.request("POST", "/tpf/control-plane/tenants/t/pipelines/p/execution-admissions",
                                 body=b'{"clientKey":"ck-1"}', headers={"Content-Type": "application/json"})
                    resp = conn.getresponse()
                    client_out.append((resp.status, resp.read()))
                except BaseException as error:
                    client_out.append(error)
                finally:
                    conn.close()

            t = threading.Thread(target=submit)
            t.start()
            self.assertTrue(proxy.admission_response_captured.wait(timeout=5))
            self.assertTrue(t.is_alive())
            self.assertEqual(200, proxy.captured_admission_response[0])
            self.assertEqual(b'{"executionId":"exec-42","clientKey":"ck-1"}', proxy.captured_admission_response[2])
            proxy.drop_client_response()
            t.join(timeout=5)
            self.assertFalse(t.is_alive())
            self.assertEqual(1, proxy.dropped_client_responses)
            self.assertIsInstance(client_out[0], (RemoteDisconnected, ConnectionResetError))


if __name__ == "__main__":
    unittest.main()
