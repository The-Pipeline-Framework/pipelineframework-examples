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


class DynamoBarrierProxy:
    """Loopback HTTP proxy in front of DynamoDB Local for deterministic kill barriers.

    Use a dedicated proxy instance for a coordinator process that will be killed:
    calling fence_and_discard() permanently stops forwarding on this instance and
    waits for every accepted relay handler to exit before returning.
    """

    def __init__(self, upstream_endpoint: str) -> None:
        parsed = urlsplit(upstream_endpoint)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.port is None:
            raise ValueError(f"Expected local http DynamoDB endpoint, got {upstream_endpoint!r}")
        self._upstream_host = parsed.hostname
        self._upstream_port = parsed.port
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._armed = False
        self._fenced_closed = False
        self._execution_table = ""
        self._key_table = ""
        self._discard_event = threading.Event()
        self._active_relays = 0
        self.admission_committed = threading.Event()
        self.claim_held = threading.Event()
        self.held_claims = 0
        self.unexpected_intent_writes = 0
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
            self._armed = True

    def fence_and_discard(self, timeout: float = 10.0) -> None:
        """Permanently stop forwarding on this proxy instance and close every accepted or held handler."""
        with self._condition:
            self._fenced_closed = True
            self._armed = False
            self._discard_event.set()
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
                    armed = proxy._armed
                    exec_table = proxy._execution_table
                    key_table = proxy._key_table
                    discard_event = proxy._discard_event
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
                        armed = armed or proxy._armed
                        exec_table = proxy._execution_table or exec_table
                        key_table = proxy._key_table or key_table
                        discard_event = proxy._discard_event

                    if armed and method == "POST" and target in {
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
        self.capability_gets = 0
        self.execute_posts = 0
        self.pre_forward_checks = 0
        self.callback_errors: list[BaseException] = []
        self._server: _FastThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        assert self._server is not None
        return int(self._server.server_port)

    def set_upstream_port(self, upstream_port: int) -> None:
        with self._lock:
            self._upstream_port = upstream_port

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
                if method == "GET" and raw_path == CAPABILITIES_PATH:
                    with proxy._lock:
                        proxy.capability_gets += 1
                elif method == "POST" and raw_path == EXECUTE_PATH:
                    with proxy._lock:
                        proxy.execute_posts += 1
                    if proxy._before_execute_forward is not None:
                        try:
                            proxy._before_execute_forward(body)
                            with proxy._lock:
                                proxy.pre_forward_checks += 1
                        except BaseException as error:
                            with proxy._lock:
                                proxy.callback_errors.append(error)
                            _write_response(self, 500, [("Content-Type", "text/plain")], b"pre-forward assertion failed")
                            return

                with proxy._lock:
                    upstream_port = proxy._upstream_port
                if upstream_port is None:
                    _write_response(self, 502, [("Content-Type", "text/plain")], b"upstream worker port not configured")
                    return

                headers = _forward_headers(list(self.headers.raw_items()), frozenset({"host", "content-length"}))
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
                    _write_response(self, response.status, resp_headers, resp_body)
                finally:
                    connection.close()

        self._server = _TlsServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
