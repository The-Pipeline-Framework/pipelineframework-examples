"""Packaged worker-fault, network-fault, and duplicate-delivery recovery proofs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import hmac
from http.client import HTTPSConnection
import json
import os
from pathlib import Path
import secrets
import ssl
import threading
from time import monotonic, sleep
from typing import Any, Callable

from recovery_proxies import CAPABILITIES_PATH, DynamoBarrierProxy, WorkerHttpsObserverProxy
from run_hosted_worker import command, host, observations, require, write_launch


@dataclass(frozen=True)
class PackagedHarness:
    """Shared coordinator, HTTP, and DynamoDB helpers supplied by run_packaged_ingress."""

    http: Callable[..., Any]
    tables: Callable[[str, str], None]
    coordinator: Callable[..., Any]
    consistent_execution: Callable[[str, str, str, str], dict | None]
    consistent_scan: Callable[[str, str], list[dict]]
    verify_durable_in_flight_intent: Callable[[str, str, bytes], dict]
    expected_completed_wire: Callable[[str], dict]
    assert_unknown_terminal_durable: Callable[[str, str, str, str, dict], dict]
    assert_zero_dlq_publications: Callable[..., None]
    distinct_port: Callable[[int], int]


def _prepare_launch(h: PackagedHarness, work: Path, classes: Path, java: str, language: str,
                    generated: Path, bundle: Path, worker_proxy: WorkerHttpsObserverProxy,
                    calls: Path, secret: str, slug: str, *, pause_after_normalize: Path | None = None):
    worker_port = h.distinct_port(worker_proxy.port)
    worker_proxy.set_upstream_port(worker_port)
    launch = json.loads(command([
        "java", "-cp", os.pathsep.join((str(classes), java)), "NativeWorkerReleaseConformance",
        language, str(generated), str(bundle), "-", str(work / f"{language}-{slug}-release"),
        f"https://localhost:{worker_proxy.port}",
    ], work))
    target = launch["target"]
    target["sharedSecretRef"] = "env:TPF_NATIVE_WORKER_SECRET"
    launch.update(port=worker_port, calls=str(calls), secret=secret,
                  key=str(work / "worker.key"), cert=str(work / "worker.pem"))
    if pause_after_normalize is not None:
        launch["pauseAfterNormalize"] = str(pause_after_normalize)
    launch_file = write_launch(work, f"{language}-{slug}",
                               {"target": target, "descriptor": launch["descriptor"]})
    tenant = target["tenantId"]
    pipeline = target["coordinatorContract"]["pipelineId"]
    release = target["releaseVersion"]
    admin = f"/tpf/admin/tenants/{tenant}/pipelines/{pipeline}/releases"
    control = f"/tpf/control-plane/tenants/{tenant}"
    admission = f"{control}/pipelines/{pipeline}/execution-admissions"
    return worker_port, launch, launch_file, tenant, release, admin, control, admission


def _intent_for(launch: dict, release: str, client_key: str) -> dict:
    return {
        "clientKey": client_key,
        "contractVersion": launch["command"]["contractVersion"],
        "releaseVersion": release,
        "inputShape": "UNI",
        "outputStreaming": False,
        "inputPayload": {
            "payloadTypeId": launch["inputType"],
            "payloadEncoding": "tpf-polyglot-int32-json/1",
            "payload": '{"value":3}',
        },
    }


def _wait_for_terminal(h: PackagedHarness, port: int, execution_path: str, timeout: float = 30.0) -> dict:
    deadline = monotonic() + timeout
    status = h.http(port, execution_path, 200)
    while status["status"] in {"QUEUED", "RUNNING", "WAIT_RETRY"} and monotonic() < deadline:
        sleep(0.1)
        status = h.http(port, execution_path, 200)
    return status


def _assert_unknown_http_and_sweep(h: PackagedHarness, port: int, admission: str, execution_path: str,
                                   intent: dict, receipt: dict) -> tuple[dict, dict]:
    execution_id = receipt["executionId"]
    status = _wait_for_terminal(h, port, execution_path)
    require(status["status"] == "REMOTE_OUTCOME_UNKNOWN"
            and status["executionId"] == execution_id
            and status.get("errorCode") == "REMOTE_OUTCOME_UNKNOWN",
            f"Expected REMOTE_OUTCOME_UNKNOWN, got {status}")
    result = h.http(port, execution_path + "/result", 200)
    require(result["status"] == status and result.get("resultPayload") is None,
            f"Expected no typed payload for REMOTE_OUTCOME_UNKNOWN: {result}")
    require(h.http(port, f"{admission}/{intent['clientKey']}", 200) == receipt,
            "Original receipt changed after unknown settlement")
    require(h.http(port, admission, 200, intent) == receipt,
            "Same-key replay changed original receipt after unknown settlement")
    h.http(port, admission, 409, intent | {"inputPayload": intent["inputPayload"] | {"payload": '{"value":4}'}})
    sleep(2.2)
    require(h.http(port, execution_path, 200) == status,
            "Subsequent ordinary sweep after replay/conflict changed REMOTE_OUTCOME_UNKNOWN status")
    require(h.http(port, execution_path + "/result", 200) == result,
            "Subsequent ordinary sweep after replay/conflict changed REMOTE_OUTCOME_UNKNOWN result")
    return status, result


def _assert_known_terminal_durable(h: PackagedHarness, endpoint: str, prefix: str, tenant: str,
                                   execution_id: str, observed_in_flight: dict, expected_receipt: dict) -> dict:
    row = h.consistent_execution(endpoint, prefix + "_execution", tenant, execution_id)
    require(row is not None and row["status"]["S"] == "SUCCEEDED",
            f"Expected SUCCEEDED execution row, got {row}")
    require(int(row["version"]["N"]) == 2, f"Expected terminal version 2, got {row.get('version')}")
    require(int(row["attempt"]["N"]) == 0, f"Expected terminal attempt 0, got {row.get('attempt')}")
    require("lease_owner" not in row, f"Lease owner was not cleared on known settlement: {row}")
    require("paired_dispatch_intent" not in row,
            f"Active intent locator was not cleared on known settlement: {row}")
    require(row.get("last_transition_key", {}).get("S") == f"{execution_id}:0:0",
            f"Unexpected terminal transition key: {row}")

    final_keys = h.consistent_scan(endpoint, prefix + "_key")
    final_intents = [item for item in final_keys
                     if item.get("tenant_execution_key", {}).get("S", "").startswith("paired-dispatch:v1:")]
    final_receipts = [item for item in final_keys if "admission_metadata" in item]
    final_indexes = [item for item in final_keys if "execution_id" in item and "admission_metadata" not in item]
    require(len(final_intents) == 1, f"Expected 1 retained immutable intent fact, got {len(final_intents)}")
    require(final_intents[0]["tenant_execution_key"]["S"] == observed_in_flight["factKey"]
            and final_intents[0].get("paired_dispatch_intent", {}).get("S") == observed_in_flight["evidenceRaw"],
            f"Terminal immutable intent fact changed: {final_intents[0]}")
    require(len(final_receipts) == 1
            and json.loads(final_receipts[0]["admission_metadata"]["S"])["receipt"] == expected_receipt,
            f"Terminal native receipt changed: {final_receipts}")
    require(len(final_indexes) == 1 and final_indexes[0]["execution_id"]["S"] == execution_id,
            f"Terminal execution-key index changed: {final_indexes}")
    require(len(final_keys) == 3,
            f"Expected 1 receipt + 1 execution-key + 1 immutable intent fact row, got {len(final_keys)}")
    return row


def _signed_worker_headers(secret: str, method: str, raw_path: str, body: bytes) -> list[tuple[str, str]]:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    nonce = secrets.token_hex(16)
    digest = sha256(body).hexdigest()
    canonical = f"{method}\n{raw_path}\n{timestamp}\n{nonce}\n{digest}".encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), canonical, "sha256").hexdigest()
    return [
        ("X-TPF-Worker-Timestamp", timestamp),
        ("X-TPF-Worker-Nonce", nonce),
        ("X-TPF-Worker-Signature", signature),
    ]


def _direct_https_request(cafile: Path, worker_port: int, method: str, raw_path: str,
                          headers: list[tuple[str, str]], body: bytes) -> tuple[int, bytes]:
    tls_context = ssl.create_default_context(cafile=str(cafile))
    tls_context.check_hostname = True
    tls_context.verify_mode = ssl.CERT_REQUIRED
    connection = HTTPSConnection("localhost", worker_port, context=tls_context, timeout=10)
    try:
        connection.putrequest(method, raw_path, skip_host=False, skip_accept_encoding=True)
        for name, value in headers:
            connection.putheader(name, value)
        if method == "POST" or body:
            connection.putheader("Content-Length", str(len(body)))
        connection.endheaders(body if body else None)
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def _verify_worker_death_before_entry(h: PackagedHarness, work: Path, jar: Path, classes: Path, java: str,
                                      node: str, language: str, generated: Path, bundle: Path, endpoint: str) -> None:
    prefix = f"native_wdeath_pre_{language}"
    h.tables(endpoint, prefix)
    calls = work / f"{language}-wdeath-pre-calls.jsonl"
    secret = secrets.token_urlsafe(32)
    observed_in_flight: dict = {}

    with WorkerHttpsObserverProxy(
        cafile=work / "ca.pem",
        certfile=work / "worker.pem",
        keyfile=work / "worker.key",
        before_execute_forward=lambda raw: observed_in_flight.update(
            h.verify_durable_in_flight_intent(endpoint, prefix, raw)),
    ) as worker_proxy:
        worker_port, launch, launch_file, tenant, release, admin, control, admission = _prepare_launch(
            h, work, classes, java, language, generated, bundle, worker_proxy, calls, secret, "wdeath-pre")
        intent = _intent_for(launch, release, "worker-death-before-entry")

        with coordinator(h, work, jar, launch_file, endpoint, prefix, secret, "wdeath-pre-live") as (port, _proc):
            h.http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
            with host(language, work, node, launch) as worker_proc:
                first_worker_pid = worker_proc.pid
                worker_proxy.arm_withhold_execute_body()
                receipt = h.http(port, admission, 200, intent)
                execution_id = receipt["executionId"]
                execution_path = f"{control}/executions/{execution_id}"

                require(worker_proxy.execute_body_withheld.wait(timeout=20),
                        "Worker proxy did not establish verified TLS and withhold execute body")
                require(worker_proxy.callback_errors == [],
                        f"Pre-forward intent check failed: {worker_proxy.callback_errors}")
                require(worker_proxy.pre_forward_checks == 1 and worker_proxy.execute_posts == 1,
                        f"Expected 1 checked execute POST, got checks={worker_proxy.pre_forward_checks} posts={worker_proxy.execute_posts}")
                require(observations(calls) == [],
                        f"Worker entered business handlers before body delivery: {observations(calls)}")

                worker_proc.kill()
                require(worker_proc.wait(timeout=8) == -9, "Worker process did not exit on SIGKILL")
                worker_proxy.release_withheld_exchange()

            status, result = _assert_unknown_http_and_sweep(h, port, admission, execution_path, intent, receipt)

            # Restart the worker after observing REMOTE_OUTCOME_UNKNOWN and confirm an ordinary sweep does not resend.
            with host(language, work, node, launch) as restarted_worker:
                restarted_worker_pid = restarted_worker.pid
                require(restarted_worker_pid != first_worker_pid, "Expected fresh worker PID after restart")
                cap_headers = _signed_worker_headers(secret, "GET", CAPABILITIES_PATH, b"")
                cap_status, _ = _direct_https_request(work / "ca.pem", worker_port, "GET", CAPABILITIES_PATH, cap_headers, b"")
                require(cap_status == 200, f"Restarted worker did not answer capability probe: HTTP {cap_status}")
                sleep(1.5)
                require(h.http(port, execution_path, 200) == status,
                        "Worker restart changed REMOTE_OUTCOME_UNKNOWN status")
                require(h.http(port, execution_path + "/result", 200) == result,
                        "Worker restart changed REMOTE_OUTCOME_UNKNOWN result")

        terminal_row = h.assert_unknown_terminal_durable(endpoint, prefix, tenant, execution_id, observed_in_flight)
        h.assert_zero_dlq_publications(work, prefix, "wdeath-pre-live")
        require(worker_proxy.execute_posts == 1,
                f"Expected exactly 1 coordinator execute POST, got {worker_proxy.execute_posts}")
        require(observations(calls) == [], f"Expected 0 worker business calls, got {observations(calls)}")
        print(json.dumps({
            "case": "worker-death-before-entry",
            "language": language,
            "executionId": execution_id,
            "killedWorkerPid": first_worker_pid,
            "restartedWorkerPid": restarted_worker_pid,
            "executePostsTotal": worker_proxy.execute_posts,
            "terminalStatus": terminal_row["status"]["S"],
            "terminalVersion": int(terminal_row["version"]["N"]),
            "terminalAttempt": int(terminal_row["attempt"]["N"]),
            "businessCalls": 0,
        }), flush=True)
        print(f"PACKAGED_WORKER_DEATH_BEFORE_ENTRY_PASS:{language}", flush=True)


def _verify_worker_death_after_entry(h: PackagedHarness, work: Path, jar: Path, classes: Path, java: str,
                                     node: str, language: str, generated: Path, bundle: Path, endpoint: str) -> None:
    prefix = f"native_wdeath_post_{language}"
    h.tables(endpoint, prefix)
    calls = work / f"{language}-wdeath-post-calls.jsonl"
    witness = work / f"{language}-normalize-witness.json"
    secret = secrets.token_urlsafe(32)
    observed_in_flight: dict = {}

    with WorkerHttpsObserverProxy(
        cafile=work / "ca.pem",
        certfile=work / "worker.pem",
        keyfile=work / "worker.key",
        before_execute_forward=lambda raw: observed_in_flight.update(
            h.verify_durable_in_flight_intent(endpoint, prefix, raw)),
    ) as worker_proxy:
        _, launch, launch_file, tenant, release, admin, control, admission = _prepare_launch(
            h, work, classes, java, language, generated, bundle, worker_proxy, calls, secret, "wdeath-post",
            pause_after_normalize=witness)
        intent = _intent_for(launch, release, "worker-death-after-entry")

        with coordinator(h, work, jar, launch_file, endpoint, prefix, secret, "wdeath-post-live") as (port, _proc):
            h.http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
            with host(language, work, node, launch) as worker_proc:
                worker_pid = worker_proc.pid
                receipt = h.http(port, admission, 200, intent)
                execution_id = receipt["executionId"]
                execution_path = f"{control}/executions/{execution_id}"
                context = {"executionId": execution_id, "traceId": execution_id}
                expected_partial = [{"handler": "normalize", "value": 3, "context": context}]

                deadline = monotonic() + 20
                while not witness.exists() and monotonic() < deadline:
                    sleep(0.05)
                require(witness.exists(), "Worker did not flush post-normalize witness before hold")
                witness_record = json.loads(witness.read_text(encoding="utf-8"))
                require(witness_record == expected_partial[0],
                        f"Unexpected post-normalize witness: {witness_record}")
                require(observations(calls) == expected_partial,
                        f"Expected exact [normalize(3)] before SIGKILL, got {observations(calls)}")
                require(worker_proxy.callback_errors == [],
                        f"Pre-forward intent check failed: {worker_proxy.callback_errors}")

                worker_proc.kill()
                require(worker_proc.wait(timeout=8) == -9, "Worker process did not exit on SIGKILL")

            _assert_unknown_http_and_sweep(h, port, admission, execution_path, intent, receipt)

        terminal_row = h.assert_unknown_terminal_durable(endpoint, prefix, tenant, execution_id, observed_in_flight)
        h.assert_zero_dlq_publications(work, prefix, "wdeath-post-live")
        require(worker_proxy.execute_posts == 1,
                f"Expected exactly 1 coordinator execute POST, got {worker_proxy.execute_posts}")
        require(observations(calls) == expected_partial,
                f"Expected exact [normalize(3)] after settlement, got {observations(calls)}")
        print(json.dumps({
            "case": "worker-death-after-entry",
            "language": language,
            "executionId": execution_id,
            "killedWorkerPid": worker_pid,
            "executePostsTotal": worker_proxy.execute_posts,
            "terminalStatus": terminal_row["status"]["S"],
            "terminalVersion": int(terminal_row["version"]["N"]),
            "terminalAttempt": int(terminal_row["attempt"]["N"]),
            "businessCalls": 1,
        }), flush=True)
        print(f"PACKAGED_WORKER_DEATH_AFTER_ENTRY_PASS:{language}", flush=True)


def coordinator(h: PackagedHarness, work: Path, jar: Path, launch_file: Path, endpoint: str, prefix: str,
                secret: str, suffix: str, extra_config: dict | None = None):
    return h.coordinator(work, jar, launch_file, endpoint, prefix, secret, suffix, extra_config=extra_config)


def _verify_shared_live_worker_cases(h: PackagedHarness, work: Path, jar: Path, classes: Path, java: str,
                                     node: str, language: str, generated: Path, bundle: Path, endpoint: str) -> None:
    """Exercise dropped response, corrupted responses, generic 503, duplicate CDI delivery, and duplicate worker request."""
    calls = work / f"{language}-fault-shared-calls.jsonl"
    secret = secrets.token_urlsafe(32)

    with WorkerHttpsObserverProxy(
        cafile=work / "ca.pem",
        certfile=work / "worker.pem",
        keyfile=work / "worker.key",
    ) as worker_proxy:
        worker_port, launch, launch_file, tenant, release, admin, control, admission = _prepare_launch(
            h, work, classes, java, language, generated, bundle, worker_proxy, calls, secret, "fault-shared")

        with host(language, work, node, launch) as worker_proc:
            worker_pid = worker_proc.pid

            # Case 3: Dropped response after execution.
            prefix = f"native_drop_resp_{language}"
            h.tables(endpoint, prefix)
            observed_in_flight: dict = {}
            worker_proxy.reset_counters()
            worker_proxy.set_before_execute_forward(
                lambda raw, p=prefix, obs=observed_in_flight: obs.update(
                    h.verify_durable_in_flight_intent(endpoint, p, raw)))
            before_calls = len(observations(calls))
            intent = _intent_for(launch, release, "dropped-response-after-execution")
            with coordinator(h, work, jar, launch_file, endpoint, prefix, secret, "drop-resp-live") as (port, _proc):
                h.http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
                worker_proxy.arm_hold_execute_response()
                receipt = h.http(port, admission, 200, intent)
                execution_id = receipt["executionId"]
                execution_path = f"{control}/executions/{execution_id}"
                context = {"executionId": execution_id, "traceId": execution_id}
                expected_calls = [{"handler": name, "value": value, "context": context}
                                  for name, value in (("normalize", 3), ("apply", 4), ("finish", 8))]

                require(worker_proxy.execute_response_captured.wait(timeout=20),
                        "Worker completion was not captured before response drop")
                require(worker_proxy.callback_errors == [],
                        f"Pre-forward intent check failed: {worker_proxy.callback_errors}")
                captured_status, _, captured_body = worker_proxy.captured_execute_response
                require(captured_status == 200
                        and json.loads(captured_body.decode("utf-8")) == h.expected_completed_wire(launch["outputType"]),
                        f"Unexpected captured completion before drop: status={captured_status} body={captured_body!r}")
                require(observations(calls)[before_calls:] == expected_calls,
                        f"Expected 3 completed worker calls before drop: {observations(calls)[before_calls:]}")

                worker_proxy.discard_execute_response()
                _assert_unknown_http_and_sweep(h, port, admission, execution_path, intent, receipt)

            terminal_row = h.assert_unknown_terminal_durable(endpoint, prefix, tenant, execution_id, observed_in_flight)
            h.assert_zero_dlq_publications(work, prefix, "drop-resp-live")
            require(worker_proxy.execute_posts == 1,
                    f"Dropped response triggered resend: {worker_proxy.execute_posts}")
            require(observations(calls)[before_calls:] == expected_calls,
                    f"Dropped response ran extra worker calls: {observations(calls)[before_calls:]}")
            print(json.dumps({
                "case": "dropped-response-after-execution",
                "language": language,
                "executionId": execution_id,
                "workerPid": worker_pid,
                "executePostsTotal": worker_proxy.execute_posts,
                "terminalStatus": terminal_row["status"]["S"],
                "terminalVersion": int(terminal_row["version"]["N"]),
                "terminalAttempt": int(terminal_row["attempt"]["N"]),
                "businessCalls": len(expected_calls),
            }), flush=True)
            print(f"PACKAGED_DROPPED_RESPONSE_PASS:{language}", flush=True)

            # Case 4a & 4b: Corrupted active response (invalid JSON + incompatible typed output on original channel).
            incompatible_typed_body = json.dumps({
                "outcome": "COMPLETED",
                "outputPayloads": [{
                    "payloadTypeId": launch["inputType"],
                    "payloadEncoding": "tpf-polyglot-int32-json/1",
                    "payload": '{"value":5}',
                }],
                "terminalOutputPublished": False,
                "terminalInputPassthrough": False,
            }).encode("utf-8")
            for variant, table_slug, replacement_body in (
                ("invalid-json", "corrupt_json", b'{"outcome":"COMPLETED"'),
                ("incompatible-typed", "corrupt_type", incompatible_typed_body),
            ):
                prefix = f"native_{table_slug}_{language}"
                h.tables(endpoint, prefix)
                observed_in_flight = {}
                worker_proxy.reset_counters()
                worker_proxy.set_before_execute_forward(
                    lambda raw, p=prefix, obs=observed_in_flight: obs.update(
                        h.verify_durable_in_flight_intent(endpoint, p, raw)))
                before_calls = len(observations(calls))
                intent = _intent_for(launch, release, f"corrupted-response-{variant}")
                with coordinator(h, work, jar, launch_file, endpoint, prefix, secret, f"{table_slug}-live") as (port, _proc):
                    h.http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
                    worker_proxy.arm_replace_execute_response(200, replacement_body)
                    try:
                        receipt = h.http(port, admission, 200, intent)
                        execution_id = receipt["executionId"]
                        execution_path = f"{control}/executions/{execution_id}"
                        context = {"executionId": execution_id, "traceId": execution_id}
                        expected_calls = [{"handler": name, "value": value, "context": context}
                                          for name, value in (("normalize", 3), ("apply", 4), ("finish", 8))]

                        require(worker_proxy.execute_response_captured.wait(timeout=20),
                                f"Worker completion was not captured before {variant} corruption")
                        require(worker_proxy.callback_errors == [],
                                f"Pre-forward intent check failed: {worker_proxy.callback_errors}")
                        captured_status, _, captured_body = worker_proxy.captured_execute_response
                        require(captured_status == 200
                                and json.loads(captured_body.decode("utf-8")) == h.expected_completed_wire(launch["outputType"]),
                                f"Unexpected captured completion before {variant}: status={captured_status} body={captured_body!r}")
                        _assert_unknown_http_and_sweep(h, port, admission, execution_path, intent, receipt)
                    finally:
                        worker_proxy.clear_execute_policy()

                terminal_row = h.assert_unknown_terminal_durable(endpoint, prefix, tenant, execution_id, observed_in_flight)
                h.assert_zero_dlq_publications(work, prefix, f"{table_slug}-live")
                require(worker_proxy.execute_posts == 1,
                        f"Corrupted response ({variant}) triggered resend: {worker_proxy.execute_posts}")
                require(observations(calls)[before_calls:] == expected_calls,
                        f"Corrupted response ({variant}) ran unexpected calls: {observations(calls)[before_calls:]}")
                print(json.dumps({
                    "case": f"corrupted-active-response-{variant}",
                    "language": language,
                    "executionId": execution_id,
                    "workerPid": worker_pid,
                    "executePostsTotal": worker_proxy.execute_posts,
                    "terminalStatus": terminal_row["status"]["S"],
                    "terminalVersion": int(terminal_row["version"]["N"]),
                    "terminalAttempt": int(terminal_row["attempt"]["N"]),
                    "businessCalls": len(expected_calls),
                }), flush=True)
                print(f"PACKAGED_CORRUPTED_RESPONSE_PASS:{language}:{variant}", flush=True)

            # Case 5: Generic execute 503 after readiness and durable intent without forwarding to worker.
            prefix = f"native_exec_503_{language}"
            h.tables(endpoint, prefix)
            observed_in_flight = {}
            worker_proxy.reset_counters()
            worker_proxy.set_before_execute_forward(
                lambda raw, p=prefix, obs=observed_in_flight: obs.update(
                    h.verify_durable_in_flight_intent(endpoint, p, raw)))
            before_calls = len(observations(calls))
            intent = _intent_for(launch, release, "generic-execute-503")
            with coordinator(h, work, jar, launch_file, endpoint, prefix, secret, "exec-503-live") as (port, _proc):
                h.http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
                worker_proxy.arm_short_circuit_execute(503, b"Service Unavailable")
                try:
                    receipt = h.http(port, admission, 200, intent)
                    execution_id = receipt["executionId"]
                    execution_path = f"{control}/executions/{execution_id}"
                    _assert_unknown_http_and_sweep(h, port, admission, execution_path, intent, receipt)
                finally:
                    worker_proxy.clear_execute_policy()

            terminal_row = h.assert_unknown_terminal_durable(endpoint, prefix, tenant, execution_id, observed_in_flight)
            h.assert_zero_dlq_publications(work, prefix, "exec-503-live")
            require(worker_proxy.callback_errors == [],
                    f"Pre-forward intent check failed on 503: {worker_proxy.callback_errors}")
            require(worker_proxy.pre_forward_checks == 1 and worker_proxy.execute_posts == 1,
                    f"Expected 1 checked execute POST on 503, got checks={worker_proxy.pre_forward_checks} posts={worker_proxy.execute_posts}")
            require(worker_proxy.short_circuited_executes == 1
                    and worker_proxy.delivered_short_circuit_responses == [(503, b"Service Unavailable")],
                    f"Unexpected short-circuit 503 delivery: {worker_proxy.delivered_short_circuit_responses}")
            require(observations(calls)[before_calls:] == [],
                    f"Generic 503 case entered worker handlers: {observations(calls)[before_calls:]}")
            print(json.dumps({
                "case": "generic-execute-503",
                "language": language,
                "executionId": execution_id,
                "workerPid": worker_pid,
                "executePostsTotal": worker_proxy.execute_posts,
                "shortCircuitedExecutes": worker_proxy.short_circuited_executes,
                "terminalStatus": terminal_row["status"]["S"],
                "terminalVersion": int(terminal_row["version"]["N"]),
                "terminalAttempt": int(terminal_row["attempt"]["N"]),
                "businessCalls": 0,
            }), flush=True)
            print(f"PACKAGED_GENERIC_EXECUTE_503_PASS:{language}", flush=True)

            # Case 6: Duplicate CDI delivery (sweep claim races immediate event claim) +
            # Case 7: Duplicate worker request (direct signed POST replay with captured nonce).
            prefix = f"native_dup_cdi_{language}"
            h.tables(endpoint, prefix)
            observed_in_flight = {}
            worker_proxy.reset_counters()
            worker_proxy.set_before_execute_forward(
                lambda raw, p=prefix, obs=observed_in_flight: obs.update(
                    h.verify_durable_in_flight_intent(endpoint, p, raw)))
            before_calls = len(observations(calls))
            intent = _intent_for(launch, release, "duplicate-cdi-delivery")
            sweep_config = {"pipeline.orchestrator.sweep-interval": "PT3S"}

            with DynamoBarrierProxy(endpoint) as dynamo_proxy:
                with coordinator(h, work, jar, launch_file, dynamo_proxy.endpoint, prefix, secret,
                                 "dup-cdi-live", extra_config=sweep_config) as (port, _proc):
                    h.http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
                    dynamo_proxy.arm_duplicate_cdi_delivery(prefix + "_execution", prefix + "_key")

                    submit_outcome: list[tuple[str, Any]] = []

                    def submit_admission():
                        try:
                            submit_outcome.append(("ok", h.http(port, admission, 200, intent, timeout=30)))
                        except BaseException as error:
                            submit_outcome.append(("err", error))

                    client_thread = threading.Thread(target=submit_admission, daemon=True)
                    client_thread.start()

                    require(dynamo_proxy.admission_committed.wait(timeout=20),
                            "Admission transaction did not commit while ack barrier was armed")
                    require(dynamo_proxy.claim_held.wait(timeout=20),
                            "Ordinary sweep did not attempt a claim while admission ack was held")
                    first_claim_at = monotonic()
                    require(worker_proxy.execute_posts == 0 and observations(calls)[before_calls:] == [],
                            "Worker was called while both claims were held")

                    dynamo_proxy.release_admission_ack()
                    client_thread.join(timeout=10)
                    require(not client_thread.is_alive() and submit_outcome and submit_outcome[0][0] == "ok",
                            f"Admission client did not receive 200 after ack release: {submit_outcome}")
                    receipt = submit_outcome[0][1]
                    execution_id = receipt["executionId"]
                    execution_path = f"{control}/executions/{execution_id}"

                    require(dynamo_proxy.second_claim_held.wait(timeout=2.5),
                            "Immediate CDI event did not attempt a second claim before the next 3s sweep")
                    require(monotonic() - first_claim_at < 2.9,
                            "Second claim did not arrive within the immediate-event window before the next sweep tick")
                    require(dynamo_proxy.held_claims == 2,
                            f"Expected exactly 2 held competing claims, got {dynamo_proxy.held_claims}")
                    require(worker_proxy.execute_posts == 0 and observations(calls)[before_calls:] == [],
                            "Worker was called before competing claims were released")

                    dynamo_proxy.release_held_claims()
                    require(len(dynamo_proxy.claim_outcomes) == 2
                            and sorted(dynamo_proxy.claim_outcomes) == [(200, False), (400, True)],
                            f"Expected 1 successful claim and 1 ConditionalCheckFailedException, got {dynamo_proxy.claim_outcomes}")

                    status = _wait_for_terminal(h, port, execution_path)
                    require(status["status"] == "SUCCEEDED" and status["executionId"] == execution_id,
                            f"Expected SUCCEEDED after duplicate CDI delivery, got {status}")
                    result = h.http(port, execution_path + "/result", 200)
                    require(result["status"] == status and result["resultPayload"] == {
                        "payloadTypeId": launch["outputType"],
                        "payloadEncoding": "tpf-polyglot-int32-json/1",
                        "payload": '{"value":5}',
                    }, f"Unexpected typed result after duplicate CDI delivery: {result}")
                    require(h.http(port, f"{admission}/{intent['clientKey']}", 200) == receipt,
                            "Receipt changed after duplicate CDI delivery")

                    context = {"executionId": execution_id, "traceId": execution_id}
                    expected_calls = [{"handler": name, "value": value, "context": context}
                                      for name, value in (("normalize", 3), ("apply", 4), ("finish", 8))]
                    require(worker_proxy.callback_errors == [],
                            f"Pre-forward intent check failed: {worker_proxy.callback_errors}")
                    require(worker_proxy.execute_posts == 1 and worker_proxy.pre_forward_checks == 1,
                            f"Expected exactly 1 execute POST on duplicate CDI delivery, got {worker_proxy.execute_posts}")
                    require(observations(calls)[before_calls:] == expected_calls,
                            f"Unexpected worker calls on duplicate CDI delivery: {observations(calls)[before_calls:]}")

                    terminal_row = _assert_known_terminal_durable(
                        h, endpoint, prefix, tenant, execution_id, observed_in_flight, receipt)
                    print(json.dumps({
                        "case": "duplicate-cdi-delivery",
                        "language": language,
                        "executionId": execution_id,
                        "workerPid": worker_pid,
                        "competingClaims": dynamo_proxy.held_claims,
                        "claimOutcomes": dynamo_proxy.claim_outcomes,
                        "executePostsTotal": worker_proxy.execute_posts,
                        "terminalStatus": terminal_row["status"]["S"],
                        "terminalVersion": int(terminal_row["version"]["N"]),
                        "terminalAttempt": int(terminal_row["attempt"]["N"]),
                        "businessCalls": len(expected_calls),
                    }), flush=True)
                    print(f"PACKAGED_DUPLICATE_CDI_DELIVERY_PASS:{language}", flush=True)

                    # Case 7: Replay exact captured signed POST + nonce directly against the same running worker.
                    captured = worker_proxy.captured_execute_request
                    require(captured is not None, "Worker observer proxy did not capture the signed execute POST")
                    cap_method, cap_path, cap_headers, cap_body = captured
                    replay_status, _ = _direct_https_request(
                        work / "ca.pem", worker_port, cap_method, cap_path, cap_headers, cap_body)
                    require(replay_status == 401,
                            f"Expected worker to reject replayed signed POST/nonce with 401, got {replay_status}")
                    require(worker_proxy.execute_posts == 1,
                            f"Fixture direct replay must not increment coordinator execute count: {worker_proxy.execute_posts}")
                    require(observations(calls)[before_calls:] == expected_calls,
                            f"Duplicate worker request triggered additional calls: {observations(calls)[before_calls:]}")
                    require(h.http(port, execution_path + "/result", 200) == result,
                            "Duplicate worker request changed settled execution result")

            post_replay_row = _assert_known_terminal_durable(
                h, endpoint, prefix, tenant, execution_id, observed_in_flight, receipt)
            h.assert_zero_dlq_publications(work, prefix, "dup-cdi-live")
            print(json.dumps({
                "case": "duplicate-worker-request",
                "language": language,
                "executionId": execution_id,
                "workerPid": worker_pid,
                "coordinatorExecutePosts": worker_proxy.execute_posts,
                "fixtureReplays": 1,
                "replayHttpStatus": replay_status,
                "terminalStatus": post_replay_row["status"]["S"],
                "terminalVersion": int(post_replay_row["version"]["N"]),
                "terminalAttempt": int(post_replay_row["attempt"]["N"]),
                "businessCalls": len(expected_calls),
            }), flush=True)
            print(f"PACKAGED_DUPLICATE_WORKER_REQUEST_PASS:{language}", flush=True)


def run_worker_faults_and_duplicates(h: PackagedHarness, work: Path, jar: Path, classes: Path, java: str,
                                     node: str, language: str, generated: Path, bundle: Path, endpoint: str) -> None:
    """Run all seven worker-fault, network-fault, and duplicate-delivery cases for one language."""
    _verify_worker_death_before_entry(h, work, jar, classes, java, node, language, generated, bundle, endpoint)
    _verify_worker_death_after_entry(h, work, jar, classes, java, node, language, generated, bundle, endpoint)
    _verify_shared_live_worker_cases(h, work, jar, classes, java, node, language, generated, bundle, endpoint)
