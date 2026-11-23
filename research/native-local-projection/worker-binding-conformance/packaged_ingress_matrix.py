"""Packaged ingress validation, worker-readiness rejection, portable-failure, and distinct-key proofs."""

from __future__ import annotations

from http.client import HTTPConnection
import json
import os
from pathlib import Path
import secrets
from time import sleep

from packaged_isolation_recovery import generic_dispatch_count, observation_config
from packaged_worker_faults import (
    PackagedHarness,
    _assert_known_terminal_durable,
    _intent_for,
    _prepare_launch,
    _wait_for_terminal,
)
from recovery_proxies import WorkerHttpsObserverProxy
from run_hosted_worker import command, host, observations, require, write_launch


def _raw_http_post(port: int, path: str, raw_body: bytes, *, token: str | None = "fixture-admin-token") -> int:
    conn = HTTPConnection("127.0.0.1", port, timeout=10)
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = "Bearer " + token
    try:
        conn.request("POST", path, body=raw_body, headers=headers)
        resp = conn.getresponse()
        resp.read()
        return resp.status
    finally:
        conn.close()


def _assert_zero_durable_rows(h: PackagedHarness, endpoint: str, prefix: str, context_label: str) -> None:
    exec_rows = h.consistent_scan(endpoint, prefix + "_execution")
    key_rows = h.consistent_scan(endpoint, prefix + "_key")
    require(exec_rows == [] and key_rows == [],
            f"{context_label} wrote unexpected durable rows: exec={exec_rows} key={key_rows}")


def _assert_portable_failure_durable(h: PackagedHarness, endpoint: str, prefix: str, tenant: str,
                                     execution_id: str, expected_code: str, expected_diagnostic: str,
                                     expected_step_index: int) -> dict:
    row = h.consistent_execution(endpoint, prefix + "_execution", tenant, execution_id)
    require(row is not None and row["status"]["S"] == "FAILED",
            f"Expected FAILED execution row for {expected_code}, got {row}")
    require(int(row["version"]["N"]) == 2, f"Expected portable FAILED version 2, got {row.get('version')}")
    require(int(row["attempt"]["N"]) == 0, f"Expected portable FAILED attempt 0, got {row.get('attempt')}")
    require(row.get("error_code", {}).get("S") == expected_code,
            f"Expected error_code {expected_code}, got {row.get('error_code')}")
    require(row.get("error_message", {}).get("S") == expected_diagnostic,
            f"Expected diagnostic {expected_diagnostic!r}, got {row.get('error_message')}")
    require(int(row.get("failed_step_index", {}).get("N", "-1")) == expected_step_index,
            f"Expected failed_step_index={expected_step_index}, got {row.get('failed_step_index')}")
    require("lease_owner" not in row and "paired_dispatch_intent" not in row,
            f"Active lease or intent locator not cleared on known portable failure: {row}")
    for forbidden in ("result_payload_json", "result_payload_reference", "result_payload_digest"):
        require(forbidden not in row, f"Unexpected {forbidden} on portable failure row: {row}")
    return row


def run_ingress_matrix_and_portable_outcomes(h: PackagedHarness, work: Path, jar: Path, classes: Path, java: str,
                                             node: str, language: str, generated: Path, alternate: Path,
                                             bundle: Path, endpoint: str, native_dispatch_log: Path) -> None:
    """Exercise auth/scope, invalid/oversized inputs, worker readiness rejections, portable failures, and distinct keys."""
    calls = work / f"{language}-matrix-calls.jsonl"
    secret = secrets.token_urlsafe(32)

    with WorkerHttpsObserverProxy(
        cafile=work / "ca.pem",
        certfile=work / "worker.pem",
        keyfile=work / "worker.key",
    ) as worker_proxy:
        worker_port, launch, launch_file, tenant, release, admin, control, admission = _prepare_launch(
            h, work, classes, java, language, generated, bundle, worker_proxy, calls, secret, "matrix")
        pipeline = launch["target"]["coordinatorContract"]["pipelineId"]

        # Prepare mismatched-projection binding (same Contract/Release/runnable, different compiled projection)
        # and wrong-hostname binding (https://127.0.0.1:<proxy_port> against DNS:localhost cert).
        different_truth = json.loads(command([
            "java", "-cp", os.pathsep.join((str(classes), java)), "NativeWorkerReleaseConformance",
            language, str(alternate), str(bundle), "-", str(work / f"{language}-matrix-alt-truth"),
            f"https://localhost:{worker_proxy.port}",
        ], work))
        different_truth["target"]["sharedSecretRef"] = "env:TPF_NATIVE_WORKER_SECRET"
        require(different_truth["projection"] != launch["projection"],
                "Expected distinct compiled projection for mismatched-projection check")
        alt_truth_file = write_launch(
            work, f"{language}-matrix-alt-truth",
            {"target": different_truth["target"], "descriptor": different_truth["descriptor"]},
        )

        wrong_host = json.loads(command([
            "java", "-cp", os.pathsep.join((str(classes), java)), "NativeWorkerReleaseConformance",
            language, str(generated), str(bundle), "-", str(work / f"{language}-matrix-wrong-host"),
            f"https://127.0.0.1:{worker_proxy.port}",
        ], work))
        wrong_host["target"]["sharedSecretRef"] = "env:TPF_NATIVE_WORKER_SECRET"
        wrong_host_file = write_launch(
            work, f"{language}-matrix-wrong-host",
            {"target": wrong_host["target"], "descriptor": wrong_host["descriptor"]},
        )

        # ---- 1. Unavailable worker before admission: offline worker ----
        prefix_unavail = f"native_unavail_{language}"
        h.tables(endpoint, prefix_unavail)
        worker_proxy.reset_counters()
        with h.coordinator(work, jar, launch_file, endpoint, prefix_unavail, secret, "unavail-offline",
                           extra_config=observation_config(native_dispatch_log)) as (port, _proc):
            h.http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
            h.http(port, admission, 503, _intent_for(launch, release, "readiness-offline"))
        _assert_zero_durable_rows(h, endpoint, prefix_unavail, "offline worker readiness rejection")
        require(worker_proxy.execute_posts == 0 and observations(calls) == [],
                "Offline worker readiness rejection sent execute POST or ran handlers")

        # ---- 2. Incompatible worker before admission: mismatched Release/artifact digest ----
        altered_worker = launch | {"target": launch["target"] | {"runnableArtifactDigest": "sha256:" + "1" * 64}}
        worker_proxy.reset_counters()
        with host(language, work, node, altered_worker):
            with h.coordinator(work, jar, launch_file, endpoint, prefix_unavail, secret, "unavail-artifact",
                               extra_config=observation_config(native_dispatch_log)) as (port, _proc):
                h.http(port, admission, 503, _intent_for(launch, release, "readiness-wrong-artifact"))
        require(worker_proxy.capability_gets >= 1 and worker_proxy.execute_posts == 0 and observations(calls) == [],
                "Mismatched artifact readiness check did not probe cleanly or dispatched")
        _assert_zero_durable_rows(h, endpoint, prefix_unavail, "mismatched artifact readiness rejection")

        # ---- Live valid worker: remaining readiness negatives + auth/scope + invalid inputs + portable outcomes ----
        with host(language, work, node, launch) as worker_proc:
            worker_pid = worker_proc.pid

            # 2b. Mismatched projection identity (same Contract/Release/runnable, different projection digest).
            prefix_proj = f"native_unavail_proj_{language}"
            h.tables(endpoint, prefix_proj)
            worker_proxy.reset_counters()
            with h.coordinator(work, jar, alt_truth_file, endpoint, prefix_proj, secret, "unavail-projection",
                               extra_config=observation_config(native_dispatch_log)) as (port, _proc):
                h.http(port, admin + "/register", 200, {"releaseDescriptorPath": different_truth["descriptor"]})
                h.http(port, admission, 503, _intent_for(different_truth, release, "readiness-wrong-projection"))
            require(worker_proxy.capability_gets >= 1 and worker_proxy.execute_posts == 0 and observations(calls) == [],
                    "Mismatched projection readiness check did not probe cleanly or dispatched")
            _assert_zero_durable_rows(h, endpoint, prefix_proj, "mismatched projection readiness rejection")

            # 2c. Untrusted CA (omit fixture truststore so default JVM truststore rejects ephemeral CA).
            worker_proxy.reset_counters()
            with h.coordinator(work, jar, launch_file, endpoint, prefix_unavail, secret, "unavail-ca",
                               extra_config=observation_config(native_dispatch_log), trust_ca=False) as (port, _proc):
                h.http(port, admission, 503, _intent_for(launch, release, "readiness-untrusted-ca"))
            require(worker_proxy.execute_posts == 0 and observations(calls) == [],
                    "Untrusted CA readiness check dispatched")

            # 2d. Wrong hostname (https://127.0.0.1:<port> against DNS:localhost SAN).
            prefix_host = f"native_unavail_host_{language}"
            h.tables(endpoint, prefix_host)
            worker_proxy.reset_counters()
            with h.coordinator(work, jar, wrong_host_file, endpoint, prefix_host, secret, "unavail-hostname",
                               extra_config=observation_config(native_dispatch_log)) as (port, _proc):
                h.http(port, admin + "/register", 200, {"releaseDescriptorPath": wrong_host["descriptor"]})
                h.http(port, admission, 503, _intent_for(wrong_host, release, "readiness-wrong-hostname"))
            require(worker_proxy.execute_posts == 0 and observations(calls) == [],
                    "Wrong hostname readiness check dispatched")
            _assert_zero_durable_rows(h, endpoint, prefix_host, "wrong hostname readiness rejection")
            _assert_zero_durable_rows(h, endpoint, prefix_unavail, "worker readiness rejection matrix")
            print(json.dumps({
                "case": "unavailable-or-incompatible-worker-before-admission",
                "language": language,
                "workerPid": worker_pid,
                "checks": ["offline", "wrong-artifact", "wrong-projection", "untrusted-ca", "wrong-hostname"],
                "executePostsTotal": 0,
                "businessCalls": 0,
                "genericDispatches": generic_dispatch_count(native_dispatch_log),
            }), flush=True)
            print(f"PACKAGED_WORKER_READINESS_REJECTIONS_PASS:{language}", flush=True)

            # ---- 3. Auth/scope + invalid/oversized input rejections (isolated empty tables) ----
            prefix_reject = f"native_reject_{language}"
            h.tables(endpoint, prefix_reject)
            worker_proxy.reset_counters()
            before_calls = len(observations(calls))
            base_intent = _intent_for(launch, release, "base")

            with h.coordinator(work, jar, launch_file, endpoint, prefix_reject, secret, "reject-live",
                               extra_config=observation_config(native_dispatch_log)) as (port, _proc):
                h.http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})

                # Auth & scope negatives
                h.http(port, admission, 401, base_intent | {"clientKey": "auth-missing"}, token=None)
                h.http(port, admission, 401, base_intent | {"clientKey": "auth-wrong"}, token="wrong-token")
                h.http(port, f"/tpf/control-plane/tenants/other-tenant/pipelines/{pipeline}/execution-admissions",
                       404, base_intent | {"clientKey": "scope-tenant"})
                h.http(port, f"{control}/pipelines/other-pipeline/execution-admissions",
                       404, base_intent | {"clientKey": "scope-pipeline"})

                # Invalid & oversized input negatives
                require(_raw_http_post(port, admission, b"{malformed-json") == 400,
                        "Malformed JSON did not return 400")
                h.http(port, admission, 400, {"clientKey": "inv-missing-fields"})
                h.http(port, admission, 400, base_intent | {"clientKey": "inv-shape", "inputShape": "MULTI"})
                h.http(port, admission, 400, base_intent | {"clientKey": "inv-stream", "outputStreaming": True})
                h.http(port, admission, 400, base_intent | {
                    "clientKey": "inv-type",
                    "inputPayload": base_intent["inputPayload"] | {"payloadTypeId": launch["outputType"]},
                })
                h.http(port, admission, 400, base_intent | {
                    "clientKey": "inv-encoding",
                    "inputPayload": base_intent["inputPayload"] | {"payloadEncoding": "application/json"},
                })
                h.http(port, admission, 400, base_intent | {
                    "clientKey": "inv-int32-str",
                    "inputPayload": base_intent["inputPayload"] | {"payload": '{"value":"not-int"}'},
                })
                h.http(port, admission, 400, base_intent | {
                    "clientKey": "inv-int32-overflow",
                    "inputPayload": base_intent["inputPayload"] | {"payload": '{"value":2147483648}'},
                })
                oversized_payload = '{"value":' + (" " * 16500) + '3}'
                h.http(port, admission, 413, base_intent | {
                    "clientKey": "inv-oversized-16k",
                    "inputPayload": base_intent["inputPayload"] | {"payload": oversized_payload},
                })

                # Keep coordinator alive across an ordinary sweep window and confirm zero orphan rows.
                sleep(1.5)
                _assert_zero_durable_rows(h, endpoint, prefix_reject, "auth/scope/invalid input matrix")

            require(worker_proxy.execute_posts == 0 and observations(calls)[before_calls:] == [],
                    "Rejected ingress requests reached worker")
            require(generic_dispatch_count(native_dispatch_log) == 0, "Java fallback fired on rejected ingress")
            print(json.dumps({
                "case": "ingress-auth-scope-and-invalid-input",
                "language": language,
                "workerPid": worker_pid,
                "rejectedChecks": 13,
                "oversizedStatus": 413,
                "executePostsTotal": 0,
                "businessCalls": 0,
                "genericDispatches": generic_dispatch_count(native_dispatch_log),
            }), flush=True)
            print(f"PACKAGED_INGRESS_REJECTIONS_PASS:{language}", flush=True)

            # ---- 4. Portable failures (TRANSIENT_EXHAUSTED, BUSINESS_REJECTION, UNCLASSIFIED) & distinct keys ----
            prefix_outcomes = f"native_outcomes_{language}"
            h.tables(endpoint, prefix_outcomes)
            worker_proxy.reset_counters()
            observed_by_exec: dict[str, dict] = {}
            worker_proxy.set_before_execute_forward(
                lambda raw, p=prefix_outcomes: (
                    lambda info: observed_by_exec.__setitem__(info["executionId"], info)
                )(h.verify_durable_in_flight_intent(endpoint, p, raw)))

            with h.coordinator(work, jar, launch_file, endpoint, prefix_outcomes, secret, "outcomes-live",
                               extra_config=observation_config(native_dispatch_log)) as (port, _proc):
                h.http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})

                # 4a. Portable failures
                for slug, value, code, diagnostic, expected_seq in (
                    ("transient-failure", 10, "TRANSIENT_EXHAUSTED", "Local retry budget exhausted",
                     (("normalize", 10), ("apply", 11), ("apply", 11))),
                    ("business-rejection", 11, "BUSINESS_REJECTION", "Business function rejected input",
                     (("normalize", 11), ("apply", 12))),
                    ("unclassified-failure", 12, "UNCLASSIFIED", "Unclassified business function failure",
                     (("normalize", 12), ("apply", 13))),
                ):
                    before_calls = len(observations(calls))
                    before_posts = worker_proxy.execute_posts
                    case_intent = _intent_for(launch, release, slug)
                    case_intent["inputPayload"] = case_intent["inputPayload"] | {"payload": f'{{"value":{value}}}'}
                    receipt = h.http(port, admission, 200, case_intent)
                    exec_id = receipt["executionId"]
                    exec_path = f"{control}/executions/{exec_id}"
                    status = _wait_for_terminal(h, port, exec_path)
                    require(status["status"] == "FAILED" and status["executionId"] == exec_id
                            and status.get("errorCode") == code,
                            f"Expected portable failure {code}, got {status}")
                    result = h.http(port, exec_path + "/result", 200)
                    require(result["status"] == status and result.get("resultPayload") is None,
                            f"Expected no typed payload for {code}: {result}")
                    row = _assert_portable_failure_durable(
                        h, endpoint, prefix_outcomes, tenant, exec_id, code, diagnostic, 1)
                    context = {"executionId": exec_id, "traceId": exec_id}
                    expected_calls = [{"handler": n, "value": v, "context": context} for n, v in expected_seq]
                    require(worker_proxy.execute_posts - before_posts == 1,
                            f"{slug} sent {worker_proxy.execute_posts - before_posts} execute POSTs, expected 1")
                    require(observations(calls)[before_calls:] == expected_calls,
                            f"{slug} unexpected worker calls: {observations(calls)[before_calls:]}")
                    print(json.dumps({
                        "case": slug,
                        "language": language,
                        "executionId": exec_id,
                        "workerPid": worker_pid,
                        "errorCode": code,
                        "failedStepIndex": int(row["failed_step_index"]["N"]),
                        "executePosts": 1,
                        "terminalVersion": int(row["version"]["N"]),
                        "terminalAttempt": int(row["attempt"]["N"]),
                        "businessCalls": len(expected_calls),
                        "genericDispatches": generic_dispatch_count(native_dispatch_log),
                    }), flush=True)
                    print(f"PACKAGED_PORTABLE_FAILURE_PASS:{language}:{slug}", flush=True)

                # 4b. Repeated and distinct keys under SERVER_KEY_ONLY (two distinct keys with identical input 3 + conflicts)
                before_calls = len(observations(calls))
                before_posts = worker_proxy.execute_posts
                intent_a = _intent_for(launch, release, "distinct-key-a")
                intent_b = _intent_for(launch, release, "distinct-key-b")
                receipt_a = h.http(port, admission, 200, intent_a)
                receipt_b = h.http(port, admission, 200, intent_b)
                require(receipt_a["executionId"] != receipt_b["executionId"],
                        f"Distinct keys under SERVER_KEY_ONLY collided on executionId: {receipt_a} vs {receipt_b}")
                for key_label, rec in (("a", receipt_a), ("b", receipt_b)):
                    eid = rec["executionId"]
                    st = _wait_for_terminal(h, port, f"{control}/executions/{eid}")
                    require(st["status"] == "SUCCEEDED" and st["executionId"] == eid,
                            f"Distinct key {key_label} did not settle SUCCEEDED: {st}")
                    res = h.http(port, f"{control}/executions/{eid}/result", 200)
                    require(res["resultPayload"] == {
                        "payloadTypeId": launch["outputType"],
                        "payloadEncoding": "tpf-polyglot-int32-json/1",
                        "payload": '{"value":5}',
                    }, f"Distinct key {key_label} unexpected result: {res}")

                # Exact replay, changed input conflict (409), and changed Release conflict (409).
                require(h.http(port, admission, 200, intent_a) == receipt_a,
                        "Exact replay did not return original receipt_a")
                h.http(port, admission, 409, intent_a | {"inputPayload": intent_a["inputPayload"] | {"payload": '{"value":4}'}})
                h.http(port, admission, 409, intent_a | {"releaseVersion": f"{language}-v2"})

                require(worker_proxy.callback_errors == [],
                        f"Pre-forward intent check failed in outcomes matrix: {worker_proxy.callback_errors}")
                require(worker_proxy.execute_posts - before_posts == 2,
                        f"Distinct keys expected exactly 2 sends total, got {worker_proxy.execute_posts - before_posts}")
                new_calls = observations(calls)[before_calls:]
                require(len(new_calls) == 6, f"Expected 6 calls across two distinct keys, got {len(new_calls)}")
                for rec in (receipt_a, receipt_b):
                    eid = rec["executionId"]
                    ctx = {"executionId": eid, "traceId": eid}
                    per_exec = [c for c in new_calls if c["context"] == ctx]
                    require(per_exec == [{"handler": n, "value": v, "context": ctx}
                                         for n, v in (("normalize", 3), ("apply", 4), ("finish", 8))],
                            f"Distinct key {eid} had unexpected call sequence: {per_exec}")

            require(generic_dispatch_count(native_dispatch_log) == 0, "Java fallback fired in outcomes matrix")
            print(json.dumps({
                "case": "repeated-and-distinct-keys",
                "language": language,
                "executionIds": [receipt_a["executionId"], receipt_b["executionId"]],
                "workerPid": worker_pid,
                "executePosts": 2,
                "businessCalls": 6,
                "genericDispatches": generic_dispatch_count(native_dispatch_log),
            }), flush=True)
            print(f"PACKAGED_DISTINCT_KEYS_PASS:{language}", flush=True)
