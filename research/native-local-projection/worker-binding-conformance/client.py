#!/usr/bin/env python3
"""Standalone zero-JVM Python client for the packaged hosted native coordinator.

Commands:
  submit <baseUrl> <token> <tenant> <pipeline> <clientKey> <contractVersion> <releaseVersion> <inputType> <value>
  lookup <baseUrl> <token> <tenant> <pipeline> <clientKey>
  wait   <baseUrl> <token> <tenant> <executionId> [timeoutSeconds]
"""

from http.client import HTTPConnection
import json
import re
import sys
from time import monotonic, sleep
from urllib.parse import urlsplit


def request(method: str, url: str, token: str, body=None):
    parsed = urlsplit(url)
    if parsed.scheme != "http" or parsed.hostname is None:
        raise ValueError(f"Expected local http coordinator URL, got {url!r}")
    port = parsed.port or 80
    path = parsed.path + (f"?{parsed.query}" if parsed.query else "")
    headers = {"Authorization": f"Bearer {token}"}
    raw = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        raw = json.dumps(body).encode("utf-8")
    conn = HTTPConnection(parsed.hostname, port, timeout=30)
    try:
        conn.request(method, path, body=raw, headers=headers)
        resp = conn.getresponse()
        text = resp.read().decode("utf-8")
        if resp.status < 200 or resp.status >= 300:
            raise RuntimeError(f"{method} {url} failed with HTTP {resp.status}: {text}")
        return json.loads(text) if text else None
    finally:
        conn.close()


def main(argv: list[str]) -> int:
    if len(argv) < 4:
        print("Usage: client.py <submit|lookup|wait> <baseUrl> <token> ...", file=sys.stderr)
        return 2
    cmd, base_url, token, *rest = argv[1:]
    root = base_url.rstrip("/")

    if cmd == "submit":
        tenant, pipeline, client_key, contract_ver, release_ver, input_type, raw_value = rest
        if re.fullmatch(r"-?\d+", raw_value) is None:
            raise ValueError(f"Expected decimal int32 value, got {raw_value!r}")
        value = int(raw_value, 10)
        if value < -2147483648 or value > 2147483647:
            raise ValueError(f"Expected signed int32 value, got {raw_value!r}")
        payload = {
            "clientKey": client_key,
            "contractVersion": contract_ver,
            "releaseVersion": release_ver,
            "inputShape": "UNI",
            "outputStreaming": False,
            "inputPayload": {
                "payloadTypeId": input_type,
                "payloadEncoding": "tpf-polyglot-int32-json/1",
                "payload": json.dumps({"value": value}, separators=(",", ":")),
            },
        }
        receipt = request(
            "POST",
            f"{root}/tpf/control-plane/tenants/{tenant}/pipelines/{pipeline}/execution-admissions",
            token,
            payload,
        )
        print(json.dumps(receipt))
        return 0

    if cmd == "lookup":
        tenant, pipeline, client_key = rest
        receipt = request(
            "GET",
            f"{root}/tpf/control-plane/tenants/{tenant}/pipelines/{pipeline}/execution-admissions/{client_key}",
            token,
        )
        print(json.dumps(receipt))
        return 0

    if cmd == "wait":
        tenant, execution_id = rest[0], rest[1]
        timeout_s = float(rest[2]) if len(rest) > 2 else 30.0
        status_url = f"{root}/tpf/control-plane/tenants/{tenant}/executions/{execution_id}"
        deadline = monotonic() + timeout_s
        status = request("GET", status_url, token)
        while status["status"] in {"QUEUED", "RUNNING", "WAIT_RETRY"} and monotonic() < deadline:
            sleep(0.1)
            status = request("GET", status_url, token)
        result = request("GET", status_url + "/result", token)
        print(json.dumps({"status": status, "result": result}))
        return 0

    print(f"Unknown command: {cmd}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
