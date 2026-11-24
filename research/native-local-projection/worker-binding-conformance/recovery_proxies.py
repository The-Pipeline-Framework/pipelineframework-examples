"""Loopback HTTP/HTTPS barrier and observer proxies for packaged recovery proofs."""

from __future__ import annotations

from collections.abc import Callable
from http.client import HTTPConnection, HTTPSConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from socketserver import TCPServer
import ssl
import threading
from urllib.parse import urlsplit

CAPABILITIES_PATH = "/pipeline/worker/capabilities"
EXECUTE_PATH = "/pipeline/worker/transitions/execute"
_HOP_BY_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "proxy-connection", "te", "trailers", "transfer-encoding", "upgrade",
})


class _FastThreadingHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer that avoids blocking reverse DNS lookups in server_bind."""

    allow_reuse_address = True
    daemon_threads = True

    def server_bind(self) -> None:
        TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name = str(host)
        self.server_port = int(port)


def _read_request_body(handler: BaseHTTPRequestHandler) -> bytes:
    transfer = (handler.headers.get("Transfer-Encoding") or "").lower()
    if "chunked" in transfer:
        chunks: list[bytes] = []
        while True:
            size_line = handler.rfile.readline()
            if not size_line:
                break
            size = int(size_line.split(b";", 1)[0].strip(), 16)
            if size == 0:
                while True:
                    trailer = handler.rfile.readline()
                    if trailer in (b"", b"\r\n", b"\n"):
                        break
                break
            chunks.append(handler.rfile.read(size))
            handler.rfile.read(2)  # trailing CRLF
        return b"".join(chunks)
    length = handler.headers.get("Content-Length")
    if length is None:
        return b""
    return handler.rfile.read(int(length))


def _connection_nominated(raw_items: list[tuple[str, str]]) -> set[str]:
    nominated: set[str] = set()
    for name, value in raw_items:
        if name.lower() == "connection":
            for token in value.split(","):
                stripped = token.strip().lower()
                if stripped:
                    nominated.add(stripped)
    return nominated


def _forward_headers(raw_items: list[tuple[str, str]], extra_skip: frozenset[str]) -> list[tuple[str, str]]:
    skip = _HOP_BY_HOP | extra_skip | _connection_nominated(raw_items)
    seen: set[str] = set()
    forwarded: list[tuple[str, str]] = []
    for name, value in raw_items:
        lower = name.lower()
        if lower in skip or lower in seen:
            continue
        seen.add(lower)
        forwarded.append((name, value))
    return forwarded


def _write_response(handler: BaseHTTPRequestHandler, status: int, headers: list[tuple[str, str]], body: bytes) -> None:
    handler.close_connection = True
    skip = _HOP_BY_HOP | {"content-length"} | _connection_nominated(headers)
    try:
        handler.send_response(status)
        for name, value in headers:
            if name.lower() in skip:
                continue
            handler.send_header(name, value)
        handler.send_header("Content-Length", str(len(body)))
        handler.send_header("Connection", "close")
        handler.end_headers()
        if body:
            handler.wfile.write(body)
    except OSError:
        pass


def _is_admission_transaction(payload: dict, execution_table: str, key_table: str) -> bool:
    items = payload.get("TransactItems")
    if not isinstance(items, list):
        return False
    put_tables: set[str] = set()
    for item in items:
        if not isinstance(item, dict) or "Put" not in item:
            continue
        put = item["Put"]
        if not isinstance(put, dict):
            continue
        if "paired_dispatch_intent" in (put.get("Item") or {}):
            return False
        table = put.get("TableName")
        if isinstance(table, str):
            put_tables.add(table)
    return execution_table in put_tables and key_table in put_tables


def _is_intent_transaction(payload: dict, execution_table: str, key_table: str) -> bool:
    items = payload.get("TransactItems")
    if not isinstance(items, list):
        return False
    for item in items:
        if not isinstance(item, dict):
            continue
        put = item.get("Put")
        if isinstance(put, dict) and put.get("TableName") == key_table:
            if "paired_dispatch_intent" in (put.get("Item") or {}):
                return True
        update = item.get("Update")
        if isinstance(update, dict) and update.get("TableName") == execution_table:
            if "paired_dispatch_intent" in str(update.get("UpdateExpression") or ""):
                return True
    return False


def _is_lease_renewal_update(payload: dict, execution_table: str) -> bool:
    if payload.get("TableName") != execution_table:
        return False
    expression = str(payload.get("UpdateExpression") or "")
    return "#leaseExpires = :leaseExpires" in expression and "#status =" not in expression


def _is_claim_update(payload: dict, execution_table: str) -> bool:
    if payload.get("TableName") != execution_table:
        return False
    expression = str(payload.get("UpdateExpression") or "")
    return "#status = :running" in expression and "#leaseOwner = :leaseOwner" in expression


def _is_succeeded_settlement_update(payload: dict, execution_table: str) -> bool:
    if payload.get("TableName") != execution_table:
        return False
    expression = str(payload.get("UpdateExpression") or "")
    return "#status = :succeeded" in expression


def _is_conditional_check_failed(status: int, body: bytes) -> bool:
    if status != 400:
        return False
    try:
        envelope = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(envelope, dict):
        return False
    error_type = envelope.get("__type")
    return isinstance(error_type, str) and error_type.rsplit("#", 1)[-1] == "ConditionalCheckFailedException"


class _ExecuteGate:
    """Gate-scoped state for one execute-exchange barrier, isolating stale relays from rearmed gates."""

    def __init__(self, kind: str, *, replacement: tuple[int, str, bytes] | None = None) -> None:
        self.kind = kind
        self.replacement = replacement
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.gate = threading.Event()
        self.captured = threading.Event()
        self.withheld = threading.Event()
        self.action: str | None = None
        self.captured_response: tuple[int, list[tuple[str, str]], bytes] | None = None
        self.active_relays = 0


class DynamoBarrierProxy:
    """Loopback HTTP proxy in front of DynamoDB Local for deterministic kill, renewal, and claim-race barriers.

    Use a dedicated proxy instance for a coordinator process that will be killed:
    calling fence_and_discard() permanently rejects new forwarding, drains registered
    relays, and closes held ones.
    """

    def __init__(self, upstream_endpoint: str) -> None:
        parsed = urlsplit(upstream_endpoint)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.port is None:
            raise ValueError(f"Expected local http DynamoDB endpoint, got {upstream_endpoint!r}")
        self._upstream_host = parsed.hostname
        self._upstream_port = parsed.port
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._mode = "passthrough"
        self._fenced_closed = False
        self._execution_table = ""
        self._key_table = ""
        self._discard_event = threading.Event()
        self._renewal_release_event = threading.Event()
        self._admission_ack_release_event = threading.Event()
        self._claims_release_event = threading.Event()
        self._active_relays = 0
        self._active_held_renewals = 0
        self._active_held_ack = 0
        self._active_held_claims = 0
        self.admission_committed = threading.Event()
        self.claim_held = threading.Event()
        self.second_claim_held = threading.Event()
        self.held_claims = 0
        self.claim_outcomes: list[tuple[int, bool]] = []
        self.unexpected_intent_writes = 0
        self.renewal_held = threading.Event()
        self.held_renewals = 0
        self.late_settlement_rejected = threading.Event()
        self.confirmed_late_settlement_rejections = 0
        self.late_settlement_observations: list[tuple[int, bytes]] = []
        self._server: _FastThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def endpoint(self) -> str:
        assert self._server is not None
        return f"http://127.0.0.1:{self._server.server_port}"

    def arm_admission_before_intent(self, execution_table: str, key_table: str) -> None:
        with self._lock:
            if self._fenced_closed:
                raise RuntimeError("Cannot re-arm a permanently fenced DynamoBarrierProxy")
            self._execution_table = execution_table
            self._key_table = key_table
            self._discard_event = threading.Event()
            self.admission_committed.clear()
            self.claim_held.clear()
            self.held_claims = 0
            self.unexpected_intent_writes = 0
            self._mode = "admission_before_intent"

    def arm_duplicate_cdi_delivery(self, execution_table: str, key_table: str) -> None:
        """Hold committed admission ack and both sweep and immediate-event claims, then forward both claims unchanged."""
        with self._lock:
            if self._fenced_closed:
                raise RuntimeError("Cannot re-arm a permanently fenced DynamoBarrierProxy")
            self._execution_table = execution_table
            self._key_table = key_table
            self._admission_ack_release_event = threading.Event()
            self._claims_release_event = threading.Event()
            self.admission_committed.clear()
            self.claim_held.clear()
            self.second_claim_held.clear()
            self.held_claims = 0
            self.claim_outcomes = []
            self._mode = "duplicate_cdi_delivery"

    def release_admission_ack(self, timeout: float = 10.0) -> None:
        """Deliver the held committed admission response back to the coordinator."""
        with self._condition:
            self._admission_ack_release_event.set()
            if not self._condition.wait_for(lambda: self._active_held_ack == 0, timeout=timeout):
                raise TimeoutError(f"Timed out waiting for {self._active_held_ack} held admission ack(s) to release")

    def release_held_claims(self, timeout: float = 10.0) -> None:
        """Forward all held claim UpdateItem requests unchanged to DynamoDB and record their outcomes."""
        with self._condition:
            if self._mode == "duplicate_cdi_delivery":
                self._mode = "passthrough"
            self._claims_release_event.set()
            if not self._condition.wait_for(lambda: self._active_held_claims == 0, timeout=timeout):
                raise TimeoutError(f"Timed out waiting for {self._active_held_claims} held claim(s) to finish")

    def arm_stale_settlement(self, execution_table: str) -> None:
        """Hold lease renewals before DynamoDB and observe late succeeded settlements failing their condition."""
        with self._lock:
            if self._fenced_closed:
                raise RuntimeError("Cannot re-arm a permanently fenced DynamoBarrierProxy")
            self._execution_table = execution_table
            self._renewal_release_event = threading.Event()
            self.renewal_held.clear()
            self.held_renewals = 0
            self.late_settlement_rejected.clear()
            self.confirmed_late_settlement_rejections = 0
            self.late_settlement_observations = []
            self._mode = "stale_settlement"

    def release_held_renewals(self, timeout: float = 10.0) -> None:
        """Release held lease renewals so they forward after late settlement has been observed."""
        with self._condition:
            if self._mode == "stale_settlement":
                self._mode = "passthrough"
            self._renewal_release_event.set()
            if not self._condition.wait_for(lambda: self._active_held_renewals == 0, timeout=timeout):
                raise TimeoutError(f"Timed out waiting for {self._active_held_renewals} held renewal(s) to finish")

    def fence_and_discard(self, timeout: float = 10.0) -> None:
        """Permanently reject new forwarding, drain registered relays, and close held ones."""
        with self._condition:
            self._fenced_closed = True
            self._mode = "passthrough"
            self._discard_event.set()
            self._renewal_release_event.set()
            self._admission_ack_release_event.set()
            self._claims_release_event.set()
            if not self._condition.wait_for(lambda: self._active_relays == 0, timeout=timeout):
                raise TimeoutError(f"Timed out waiting for {self._active_relays} DynamoDB proxy handler(s) to exit")

    def _forward(self, method: str, path: str, headers: list[tuple[str, str]], body: bytes) -> tuple[int, list[tuple[str, str]], bytes]:
        connection = HTTPConnection(self._upstream_host, self._upstream_port, timeout=30)
        try:
            connection.putrequest(method, path, skip_host=False, skip_accept_encoding=True)
            for name, value in headers:
                connection.putheader(name, value)
            if body or method == "POST":
                connection.putheader("Content-Length", str(len(body)))
            connection.endheaders(body if body else None)
            response = connection.getresponse()
            return response.status, list(response.getheaders()), response.read()
        finally:
            connection.close()

    def __enter__(self) -> DynamoBarrierProxy:
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: object) -> None:
                pass

            def do_POST(self) -> None:
                self._relay("POST")

            def do_GET(self) -> None:
                self._relay("GET")

            def _relay(self, method: str) -> None:
                with proxy._condition:
                    if proxy._fenced_closed:
                        self.close_connection = True
                        return
                    proxy._active_relays += 1
                    arrival_mode = proxy._mode
                    exec_table = proxy._execution_table
                    key_table = proxy._key_table
                    discard_event = proxy._discard_event
                    renewal_release = proxy._renewal_release_event
                    ack_release = proxy._admission_ack_release_event
                    claims_release = proxy._claims_release_event
                try:
                    try:
                        body = _read_request_body(self)
                    except OSError:
                        self.close_connection = True
                        return
                    headers = _forward_headers(list(self.headers.raw_items()), frozenset({"host", "content-length"}))
                    target = self.headers.get("X-Amz-Target") or ""

                    with proxy._lock:
                        if proxy._fenced_closed:
                            self.close_connection = True
                            return
                        mode = proxy._mode if proxy._mode != "passthrough" else arrival_mode
                        exec_table = proxy._execution_table or exec_table
                        key_table = proxy._key_table or key_table
                        discard_event = proxy._discard_event
                        renewal_release = proxy._renewal_release_event
                        ack_release = proxy._admission_ack_release_event
                        claims_release = proxy._claims_release_event

                    if mode == "admission_before_intent" and method == "POST" and target in {
                        "DynamoDB_20120810.TransactWriteItems",
                        "DynamoDB_20120810.UpdateItem",
                    }:
                        try:
                            payload = json.loads(body.decode("utf-8"))
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            payload = {}
                        if target == "DynamoDB_20120810.UpdateItem" and payload.get("TableName") == exec_table:
                            with proxy._lock:
                                proxy.held_claims += 1
                            proxy.claim_held.set()
                            self.close_connection = True
                            discard_event.wait()
                            return
                        if target == "DynamoDB_20120810.TransactWriteItems":
                            if _is_intent_transaction(payload, exec_table, key_table):
                                with proxy._lock:
                                    proxy.unexpected_intent_writes += 1
                                self.close_connection = True
                                discard_event.wait()
                                return
                            if _is_admission_transaction(payload, exec_table, key_table):
                                status, resp_headers, resp_body = proxy._forward(method, self.path, headers, body)
                                if status == 200:
                                    proxy.admission_committed.set()
                                    self.close_connection = True
                                    discard_event.wait()
                                    return
                                _write_response(self, status, resp_headers, resp_body)
                                return

                    if mode == "duplicate_cdi_delivery" and method == "POST" and target in {
                        "DynamoDB_20120810.TransactWriteItems",
                        "DynamoDB_20120810.UpdateItem",
                    }:
                        try:
                            payload = json.loads(body.decode("utf-8"))
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            payload = {}
                        if target == "DynamoDB_20120810.TransactWriteItems" and _is_admission_transaction(
                            payload, exec_table, key_table
                        ):
                            status, resp_headers, resp_body = proxy._forward(method, self.path, headers, body)
                            if status == 200:
                                with proxy._condition:
                                    proxy._active_held_ack += 1
                                proxy.admission_committed.set()
                                try:
                                    ack_release.wait()
                                    with proxy._lock:
                                        if proxy._fenced_closed:
                                            self.close_connection = True
                                            return
                                    _write_response(self, status, resp_headers, resp_body)
                                    return
                                finally:
                                    with proxy._condition:
                                        proxy._active_held_ack -= 1
                                        if proxy._active_held_ack == 0:
                                            proxy._condition.notify_all()
                            _write_response(self, status, resp_headers, resp_body)
                            return
                        if target == "DynamoDB_20120810.UpdateItem" and _is_claim_update(payload, exec_table):
                            with proxy._condition:
                                proxy.held_claims += 1
                                count = proxy.held_claims
                                proxy._active_held_claims += 1
                            proxy.claim_held.set()
                            if count >= 2:
                                proxy.second_claim_held.set()
                            try:
                                claims_release.wait()
                                with proxy._lock:
                                    if proxy._fenced_closed:
                                        self.close_connection = True
                                        return
                                status, resp_headers, resp_body = proxy._forward(method, self.path, headers, body)
                                with proxy._lock:
                                    proxy.claim_outcomes.append((status, _is_conditional_check_failed(status, resp_body)))
                                _write_response(self, status, resp_headers, resp_body)
                                return
                            finally:
                                with proxy._condition:
                                    proxy._active_held_claims -= 1
                                    if proxy._active_held_claims == 0:
                                        proxy._condition.notify_all()

                    if mode == "stale_settlement" and method == "POST" and target == "DynamoDB_20120810.UpdateItem":
                        try:
                            payload = json.loads(body.decode("utf-8"))
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            payload = {}
                        if _is_lease_renewal_update(payload, exec_table):
                            with proxy._condition:
                                proxy.held_renewals += 1
                                proxy._active_held_renewals += 1
                            proxy.renewal_held.set()
                            try:
                                renewal_release.wait()
                                with proxy._lock:
                                    if proxy._fenced_closed:
                                        self.close_connection = True
                                        return
                                status, resp_headers, resp_body = proxy._forward(method, self.path, headers, body)
                                _write_response(self, status, resp_headers, resp_body)
                                return
                            finally:
                                with proxy._condition:
                                    proxy._active_held_renewals -= 1
                                    if proxy._active_held_renewals == 0:
                                        proxy._condition.notify_all()
                        if _is_succeeded_settlement_update(payload, exec_table):
                            status, resp_headers, resp_body = proxy._forward(method, self.path, headers, body)
                            is_conditional_failure = _is_conditional_check_failed(status, resp_body)
                            with proxy._lock:
                                proxy.late_settlement_observations.append((status, resp_body))
                                if is_conditional_failure:
                                    proxy.confirmed_late_settlement_rejections += 1
                            if is_conditional_failure:
                                proxy.late_settlement_rejected.set()
                            _write_response(self, status, resp_headers, resp_body)
                            return

                    with proxy._lock:
                        if proxy._fenced_closed:
                            self.close_connection = True
                            return
                    status, resp_headers, resp_body = proxy._forward(method, self.path, headers, body)
                    _write_response(self, status, resp_headers, resp_body)
                finally:
                    with proxy._condition:
                        proxy._active_relays -= 1
                        if proxy._active_relays == 0:
                            proxy._condition.notify_all()

        self._server = _FastThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            self.fence_and_discard()
        finally:
            if self._server is not None:
                self._server.shutdown()
                self._server.server_close()
            if self._thread is not None:
                self._thread.join(timeout=5)


class WorkerHttpsObserverProxy:
    """Loopback TLS proxy terminating at the pinned worker origin and verifying upstream TLS."""

    def __init__(
        self,
        *,
        upstream_port: int | None = None,
        cafile: Path,
        certfile: Path,
        keyfile: Path,
        before_execute_forward: Callable[[bytes], None] | None = None,
    ) -> None:
        self._upstream_port = upstream_port
        self._cafile = cafile
        self._certfile = certfile
        self._keyfile = keyfile
        self._before_execute_forward = before_execute_forward
        self._lock = threading.Lock()
        self._active_gate: _ExecuteGate | None = None
        self._last_gate: _ExecuteGate | None = None
        self.capability_gets = 0
        self.execute_posts = 0
        self.pre_forward_checks = 0
        self.short_circuited_executes = 0
        self.delivered_short_circuit_responses: list[tuple[int, bytes]] = []
        self.captured_execute_request: tuple[str, str, list[tuple[str, str]], bytes] | None = None
        self.callback_errors: list[BaseException] = []
        self._server: _FastThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        assert self._server is not None
        return int(self._server.server_port)

    @property
    def execute_response_captured(self) -> threading.Event:
        with self._lock:
            gate = self._active_gate or self._last_gate
        if gate is None:
            return threading.Event()
        return gate.captured

    @property
    def execute_body_withheld(self) -> threading.Event:
        with self._lock:
            gate = self._active_gate or self._last_gate
        if gate is None:
            return threading.Event()
        return gate.withheld

    @property
    def captured_execute_response(self) -> tuple[int, list[tuple[str, str]], bytes] | None:
        with self._lock:
            gate = self._active_gate or self._last_gate
        if gate is None:
            return None
        with gate.lock:
            return gate.captured_response

    def set_upstream_port(self, upstream_port: int) -> None:
        with self._lock:
            self._upstream_port = upstream_port

    def set_before_execute_forward(self, callback: Callable[[bytes], None] | None) -> None:
        with self._lock:
            self._before_execute_forward = callback

    def reset_counters(self) -> None:
        with self._lock:
            self.capability_gets = 0
            self.execute_posts = 0
            self.pre_forward_checks = 0
            self.short_circuited_executes = 0
            self.delivered_short_circuit_responses = []
            self.captured_execute_request = None
            self.callback_errors = []

    def _arm_gate(self, kind: str, *, replacement: tuple[int, str, bytes] | None = None) -> None:
        with self._lock:
            if self._active_gate is not None:
                raise RuntimeError("Previous execute gate is still armed; clear, release, or discard it first")
            gate = _ExecuteGate(kind, replacement=replacement)
            self._active_gate = gate
            self._last_gate = gate

    def arm_hold_execute_response(self) -> None:
        self._arm_gate("hold_response")

    def arm_withhold_execute_body(self) -> None:
        """Open verified upstream TLS and send execute headers only, withholding body bytes until closed."""
        self._arm_gate("withhold_body")

    def arm_replace_execute_response(self, status: int, body: bytes, *, content_type: str = "application/json") -> None:
        """Forward execute to worker, capture valid completion, and return corrupted replacement on same channel."""
        self._arm_gate("replace_response", replacement=(status, content_type, body))

    def arm_short_circuit_execute(self, status: int, body: bytes, *, content_type: str = "text/plain") -> None:
        """Return synthetic HTTP status on execute after pre-forward check without contacting the worker."""
        self._arm_gate("short_circuit", replacement=(status, content_type, body))

    def _finish_gate(self, action: str, timeout: float = 10.0) -> None:
        with self._lock:
            gate = self._active_gate
            self._active_gate = None
        if gate is None:
            return
        with gate.condition:
            gate.action = action
            gate.gate.set()
            if not gate.condition.wait_for(lambda: gate.active_relays == 0, timeout=timeout):
                raise TimeoutError(f"Timed out waiting for {gate.active_relays} execute relay(s) to {action}")

    def release_execute_response(self, timeout: float = 10.0) -> None:
        """Deliver the held upstream worker response to the waiting coordinator."""
        self._finish_gate("release", timeout=timeout)

    def discard_execute_response(self, timeout: float = 10.0) -> None:
        """Close the held execute response channel without delivering response bytes."""
        self._finish_gate("discard", timeout=timeout)

    def release_withheld_exchange(self, timeout: float = 10.0) -> None:
        """Close the withheld upstream TLS connection and coordinator exchange."""
        self._finish_gate("discard", timeout=timeout)

    def clear_execute_policy(self, timeout: float = 10.0) -> None:
        """Clear any active execute policy and drain active relays."""
        self._finish_gate("discard", timeout=timeout)

    def __enter__(self) -> WorkerHttpsObserverProxy:
        proxy = self
        upstream_tls = ssl.create_default_context(cafile=str(self._cafile))
        upstream_tls.check_hostname = True
        upstream_tls.verify_mode = ssl.CERT_REQUIRED

        server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_tls.minimum_version = ssl.TLSVersion.TLSv1_2
        server_tls.load_cert_chain(str(self._certfile), str(self._keyfile))
        server_tls.set_alpn_protocols(["http/1.1"])

        class _TlsServer(_FastThreadingHTTPServer):
            def process_request_thread(self, request, client_address) -> None:
                try:
                    with server_tls.wrap_socket(request, server_side=True) as secured:
                        super().process_request_thread(secured, client_address)
                except OSError:
                    try:
                        request.close()
                    except OSError:
                        pass

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, format: str, *args: object) -> None:
                pass

            def do_GET(self) -> None:
                self._relay("GET")

            def do_POST(self) -> None:
                self._relay("POST")

            def _relay(self, method: str) -> None:
                body = _read_request_body(self)
                raw_path = self.requestline.split()[1]
                is_execute = method == "POST" and raw_path == EXECUTE_PATH
                headers = _forward_headers(list(self.headers.raw_items()), frozenset({"host", "content-length"}))
                if method == "GET" and raw_path == CAPABILITIES_PATH:
                    with proxy._lock:
                        proxy.capability_gets += 1
                elif is_execute:
                    with proxy._lock:
                        proxy.execute_posts += 1
                        proxy.captured_execute_request = (method, raw_path, list(headers), body)
                        before_cb = proxy._before_execute_forward
                    if before_cb is not None:
                        try:
                            before_cb(body)
                            with proxy._lock:
                                proxy.pre_forward_checks += 1
                        except BaseException as error:
                            with proxy._lock:
                                proxy.callback_errors.append(error)
                            _write_response(self, 500, [("Content-Type", "text/plain")], b"pre-forward assertion failed")
                            return

                with proxy._lock:
                    upstream_port = proxy._upstream_port
                    gate = proxy._active_gate if is_execute else None

                # Register against the gate BEFORE short-circuit or upstream I/O so clear/discard always drains this relay.
                if gate is not None:
                    with gate.condition:
                        gate.active_relays += 1

                try:
                    if gate is not None and gate.kind == "short_circuit":
                        assert gate.replacement is not None
                        rep_status, rep_ct, rep_body = gate.replacement
                        with proxy._lock:
                            proxy.short_circuited_executes += 1
                            proxy.delivered_short_circuit_responses.append((rep_status, rep_body))
                        _write_response(self, rep_status, [("Content-Type", rep_ct)], rep_body)
                        return

                    if upstream_port is None:
                        _write_response(self, 502, [("Content-Type", "text/plain")], b"upstream worker port not configured")
                        return

                    if gate is not None and gate.kind == "withhold_body":
                        connection = HTTPSConnection("localhost", upstream_port, context=upstream_tls, timeout=30)
                        try:
                            connection.connect()
                            connection.putrequest(method, raw_path, skip_host=False, skip_accept_encoding=True)
                            for name, value in headers:
                                connection.putheader(name, value)
                            connection.putheader("Content-Length", str(len(body)))
                            connection.endheaders(None)
                            gate.withheld.set()
                            gate.gate.wait()
                        finally:
                            connection.close()
                        self.close_connection = True
                        return

                    connection = HTTPSConnection("localhost", upstream_port, context=upstream_tls, timeout=30)
                    try:
                        connection.putrequest(method, raw_path, skip_host=False, skip_accept_encoding=True)
                        for name, value in headers:
                            connection.putheader(name, value)
                        if method == "POST" or body:
                            connection.putheader("Content-Length", str(len(body)))
                        connection.endheaders(body if body else None)
                        response = connection.getresponse()
                        resp_headers = list(response.getheaders())
                        resp_body = response.read()
                        resp_status = response.status
                    except OSError:
                        self.close_connection = True
                        return
                    finally:
                        connection.close()

                    if gate is not None and gate.kind == "hold_response":
                        with gate.lock:
                            gate.captured_response = (resp_status, resp_headers, resp_body)
                        gate.captured.set()
                        gate.gate.wait()
                        with gate.lock:
                            action = gate.action
                        if action != "release":
                            self.close_connection = True
                            return
                        _write_response(self, resp_status, resp_headers, resp_body)
                        return

                    if gate is not None and gate.kind == "replace_response":
                        assert gate.replacement is not None
                        rep_status, rep_ct, rep_body = gate.replacement
                        with gate.lock:
                            gate.captured_response = (resp_status, resp_headers, resp_body)
                        gate.captured.set()
                        _write_response(self, rep_status, [("Content-Type", rep_ct)], rep_body)
                        return

                    _write_response(self, resp_status, resp_headers, resp_body)
                finally:
                    if gate is not None:
                        with gate.condition:
                            gate.active_relays -= 1
                            if gate.active_relays == 0:
                                gate.condition.notify_all()

        self._server = _TlsServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            self.clear_execute_policy()
        finally:
            if self._server is not None:
                self._server.shutdown()
                self._server.server_close()
            if self._thread is not None:
                self._thread.join(timeout=5)
