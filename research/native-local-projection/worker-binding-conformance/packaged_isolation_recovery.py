"""Packaged Java-fallback control, client-disconnect, post-settlement death, and restart-isolation proofs."""

from __future__ import annotations

from http.client import RemoteDisconnected
import json
import os
from pathlib import Path
import secrets
import sys
import threading
from time import monotonic, sleep

from packaged_worker_faults import (
    PackagedHarness,
    _assert_known_terminal_durable,
    _direct_https_request,
    _intent_for,
    _prepare_launch,
    _wait_for_terminal,
)
from recovery_proxies import (
    EXECUTE_PATH,
    DynamoBarrierProxy,
    IngressHttpBarrierProxy,
    WorkerHttpsObserverProxy,
)
from run_hosted_worker import HERE, command, host, observations, require, signed, write_launch


def generic_dispatch_count(log_path: Path) -> int:
    """Read externally persisted generic Java transition dispatch increments."""
    if not log_path.exists():
        return 0
    return sum(int(line) for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip())


def observation_config(log_path: Path, extra: dict | None = None) -> dict:
    return {
        "pipeline.telemetry.enabled": "true",
        "pipeline.telemetry.metrics.enabled": "true",
        "proof.generic-dispatch-log": str(log_path),
        **(extra or {}),
    }


def _run_standalone_client(language: str, node: str, work: Path, *args: str) -> dict:
    if language == "node":
        cmd = [node, str(HERE / "client.mjs"), *args]
    else:
        cmd = [sys.executable, "-B", str(HERE / "client.py"), *args]
    return json.loads(command(cmd, work))


def verify_java_fallback_positive_control(h: PackagedHarness, work: Path, jar: Path, classes: Path, java: str,
                                          generated: Path, bundle: Path, endpoint: str) -> None:
    """Prove the external generic Java dispatch counter increments on an ordinary Java execution."""
    prefix = "java_fallback_control"
    h.tables(endpoint, prefix)
    dispatch_log = work / "java-fallback-positive-control.log"
    secret = secrets.token_urlsafe(32)
    launch = json.loads(command([
        "java", "-cp", os.pathsep.join((str(classes), java)), "NativeWorkerReleaseConformance",
        "node", str(generated), str(bundle), "-", str(work / "java-control-release"),
        "https://localhost:65535",
    ], work))
    target = launch["target"]
    target["sharedSecretRef"] = "env:TPF_NATIVE_WORKER_SECRET"
    launch_file = write_launch(work, "java-control", {"target": target, "descriptor": launch["descriptor"]})
    tenant = target["tenantId"]
    pipeline = target["coordinatorContract"]["pipelineId"]
    release = target["releaseVersion"]
    admin = f"/tpf/admin/tenants/{tenant}/pipelines/{pipeline}/releases"
    admission = f"/tpf/control-plane/tenants/{tenant}/pipelines/{pipeline}/execution-admissions"

    extra = observation_config(dispatch_log, {
        "proof.binding.enabled": "false",
        "pipeline.orchestrator.experimental.native-worker.enabled": "false",
        "pipeline.orchestrator.idempotency-policy": "CLIENT_KEY_REQUIRED",
        "pipeline.orchestrator.control-plane.payload-materialization": "SERIALIZED",
        "pipeline.orchestrator.release-version": release,
        "pipeline.orchestrator.worker.artifact-id": launch["receipt"]["compiledTruthArtifactId"],
        "pipeline.orchestrator.worker.artifact-digest": launch["receipt"]["compiledTruthDigest"],
    })
    with h.coordinator(work, jar, launch_file, endpoint, prefix, secret, "java-control", extra_config=extra) as (port, _proc):
        registered = h.http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
        h.http(port, f"{admin}/{release}/activate", 200, {})
        h.http(port, f"/tpf/admin/tenants/{tenant}/pipelines/{pipeline}/workers/register", 200, {
            "contractVersion": launch["command"]["contractVersion"],
            "releaseVersion": release,
            "workerId": "local-java-control",
            "protocol": "local",
            "endpoint": "local://control",
            "artifactId": registered["primaryArtifactId"],
            "artifactDigest": registered["primaryArtifactDigest"],
        })
        accepted = h.http(port, f"/tpf/control-plane/tenants/{tenant}/executions", 200, {
            "pipelineId": pipeline,
            "inputShape": "UNI",
            "idempotencyKey": "java-positive-control",
            "outputStreaming": False,
            "inputPayload": {
                "payloadTypeId": "org.pipelineframework.research.ControlInput",
                "payloadEncoding": "application/tpf-transition+json",
                "payload": '{"value":3}',
            },
        })
        deadline = monotonic() + 20
        while generic_dispatch_count(dispatch_log) == 0 and monotonic() < deadline:
            sleep(0.1)
        observed = generic_dispatch_count(dispatch_log)
        require(observed >= 1,
                f"Ordinary Java positive control did not increment generic transition dispatch counter (observed={observed})")
        print(json.dumps({
            "case": "java-fallback-positive-control",
            "executionId": accepted["executionId"],
            "genericDispatches": observed,
        }), flush=True)
        print("PACKAGED_JAVA_FALLBACK_CONTROL_PASS", flush=True)


def _assert_pre_intent_failure_oracle(h: PackagedHarness, port: int, endpoint: str, prefix: str, tenant: str,
                                      admission: str, control: str, client_key: str, durable_receipt: dict,
                                      native_dispatch_log: Path) -> dict:
    """Observe WAIT_RETRY v2/a1 and terminal FAILED v4/a1 with zero dispatch intent and zero Java fallback."""
    execution_id = durable_receipt["executionId"]
    execution_path = f"{control}/executions/{execution_id}"
    require(h.http(port, f"{admission}/{client_key}", 200) == durable_receipt,
            "Original receipt changed during pre-intent failure recovery")

    observed_statuses: list[tuple[str, int, int]] = []
    seen_wait_retry = False
    deadline = monotonic() + 30
    status = h.http(port, execution_path, 200)
    while monotonic() < deadline:
        row = h.consistent_execution(endpoint, prefix + "_execution", tenant, execution_id)
        if row is not None:
            snap = (row["status"]["S"], int(row["version"]["N"]), int(row["attempt"]["N"]))
            if not observed_statuses or observed_statuses[-1] != snap:
                observed_statuses.append(snap)
            if snap == ("WAIT_RETRY", 2, 1):
                seen_wait_retry = True
        status = h.http(port, execution_path, 200)
        if status["status"] == "FAILED":
            break
        sleep(0.05)

    require(seen_wait_retry,
            f"Did not observe pre-intent WAIT_RETRY v2/a1 before FAILED; trajectory={observed_statuses}")
    require(status["status"] == "FAILED" and status["executionId"] == execution_id,
            f"Expected terminal FAILED pre-intent oracle, got {status}")
    result = h.http(port, execution_path + "/result", 200)
    require(result["status"] == status and result.get("resultPayload") is None,
            f"Expected no typed payload on pre-intent FAILED: {result}")
    require(h.http(port, f"{admission}/{client_key}", 200) == durable_receipt,
            "Original receipt changed after pre-intent failure")

    terminal_row = h.consistent_execution(endpoint, prefix + "_execution", tenant, execution_id)
    require(terminal_row is not None and terminal_row["status"]["S"] == "FAILED",
            f"Unexpected terminal execution row: {terminal_row}")
    require(int(terminal_row["version"]["N"]) == 4,
            f"Expected pre-intent FAILED version 4, got {terminal_row.get('version')} trajectory={observed_statuses}")
    require(int(terminal_row["attempt"]["N"]) == 1,
            f"Expected pre-intent FAILED attempt 1, got {terminal_row.get('attempt')}")
    for forbidden in ("lease_owner", "paired_dispatch_intent", "result_payload_json",
                      "result_payload_reference", "result_payload_digest"):
        require(forbidden not in terminal_row, f"Unexpected {forbidden} on pre-intent FAILED row: {terminal_row}")

    final_keys = h.consistent_scan(endpoint, prefix + "_key")
    intent_rows = [item for item in final_keys
                   if item.get("tenant_execution_key", {}).get("S", "").startswith("paired-dispatch:v1:")]
    receipt_rows = [item for item in final_keys if "admission_metadata" in item]
    index_rows = [item for item in final_keys if "execution_id" in item and "admission_metadata" not in item]
    require(intent_rows == [], f"Unexpected paired dispatch intent created on pre-intent failure: {intent_rows}")
    require(len(receipt_rows) == 1
            and json.loads(receipt_rows[0]["admission_metadata"]["S"])["receipt"] == durable_receipt,
            f"Receipt row altered during pre-intent failure: {receipt_rows}")
    require(len(index_rows) == 1 and index_rows[0]["execution_id"]["S"] == execution_id,
            f"Execution-key index altered during pre-intent failure: {index_rows}")
    require(len(final_keys) == 2, f"Expected exact 2 key rows (receipt + index), got {len(final_keys)}")
    require(generic_dispatch_count(native_dispatch_log) == 0,
            f"Java fallback fired during pre-intent failure: count={generic_dispatch_count(native_dispatch_log)}")
    return terminal_row


def _admit_and_kill_before_intent(h: PackagedHarness, work: Path, jar: Path, launch: dict, launch_file: Path,
                                  endpoint: str, prefix: str, secret: str, admin: str, admission: str,
                                  release: str, intent: dict, worker_proxy: WorkerHttpsObserverProxy,
                                  calls: Path, before_calls: int, suffix: str,
                                  native_dispatch_log: Path) -> tuple[dict, dict, int]:
    """Commit admission, fence claims, SIGKILL first coordinator, and verify QUEUED v0 with zero intent."""
    submit_outcome: list[tuple[str, object]] = []

    def submit(port: int):
        try:
            submit_outcome.append(("response", h.http(port, admission, 200, intent, timeout=60)))
        except (RemoteDisconnected, ConnectionResetError, BrokenPipeError) as error:
            submit_outcome.append(("disconnected", error))
        except BaseException as error:
            submit_outcome.append(("error", error))

    with DynamoBarrierProxy(endpoint) as first_dynamo:
        with h.coordinator(work, jar, launch_file, first_dynamo.endpoint, prefix, secret, suffix,
                           extra_config=observation_config(native_dispatch_log)) as (port, proc):
            h.http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
            registered = h.http(port, f"{admin}/{release}", 200)
            first_dynamo.arm_admission_before_intent(prefix + "_execution", prefix + "_key")

            t = threading.Thread(target=submit, args=(port,), daemon=True)
            t.start()
            require(first_dynamo.admission_committed.wait(timeout=20), "Admission did not commit")
            require(first_dynamo.claim_held.wait(timeout=15), "Sweep did not attempt claim before kill")

            key_items = h.consistent_scan(endpoint, prefix + "_key")
            receipt_rows = [item for item in key_items if "admission_metadata" in item]
            require(len(receipt_rows) == 1, f"Expected 1 receipt row before kill, got {key_items}")
            durable_receipt = json.loads(receipt_rows[0]["admission_metadata"]["S"])["receipt"]
            require(worker_proxy.execute_posts == 0 and observations(calls)[before_calls:] == [],
                    "Worker was invoked before coordinator kill")

            first_pid = proc.pid
            proc.kill()
            require(proc.wait(timeout=8) == -9, "First coordinator did not exit on SIGKILL")
            t.join(timeout=10)
            require(not t.is_alive() and submit_outcome and submit_outcome[0][0] == "disconnected",
                    f"Expected client disconnect on SIGKILL, got {submit_outcome}")
            first_dynamo.fence_and_discard()

    require(generic_dispatch_count(native_dispatch_log) == 0,
            "Java fallback fired before coordinator kill")
    return registered, durable_receipt, first_pid


def run_isolation_and_disconnect_cases(h: PackagedHarness, work: Path, jar: Path, classes: Path, java: str,
                                       node: str, language: str, generated: Path, bundle: Path, endpoint: str,
                                       native_dispatch_log: Path) -> None:
    """Exercise client disconnect, post-settlement death, wrong Release, missing/replaced binding, and stale worker."""
    calls = work / f"{language}-isolation-calls.jsonl"
    secret = secrets.token_urlsafe(32)

    with WorkerHttpsObserverProxy(
        cafile=work / "ca.pem",
        certfile=work / "worker.pem",
        keyfile=work / "worker.key",
    ) as worker_proxy:
        worker_port, launch, launch_file, tenant, release, admin, control, admission = _prepare_launch(
            h, work, classes, java, language, generated, bundle, worker_proxy, calls, secret, "iso")
        pipeline = launch["target"]["coordinatorContract"]["pipelineId"]
        contract_ver = launch["command"]["contractVersion"]

        # Build a genuine replacement schema-1 Release "v2" for replaced-binding restart isolation.
        replacement = json.loads(command([
            "java", "-cp", os.pathsep.join((str(classes), java)), "NativeWorkerReleaseConformance",
            language, str(generated), str(bundle), "-", str(work / f"{language}-iso-v2-release"),
            f"https://localhost:{worker_proxy.port}", "v2",
        ], work))
        replacement["target"]["sharedSecretRef"] = "env:TPF_NATIVE_WORKER_SECRET"
        require(replacement["target"]["releaseVersion"] != release
                and replacement["target"]["descriptorDigest"] != launch["target"]["descriptorDigest"],
                "Replacement binding must differ in releaseVersion and descriptorDigest")
        replacement_launch_file = write_launch(
            work, f"{language}-iso-v2",
            {"target": replacement["target"], "descriptor": replacement["descriptor"]},
        )

        # ---- Live-worker group: client disconnect, wrong Release, missing binding, replaced binding ----
        with host(language, work, node, launch) as worker_proc:
            worker_pid = worker_proc.pid

            # 1. Client disconnect before receipt + standalone zero-JVM Node/Python client reconnect.
            prefix = f"native_disconnect_{language}"
            h.tables(endpoint, prefix)
            observed_in_flight: dict = {}
            worker_proxy.reset_counters()
            worker_proxy.set_before_execute_forward(
                lambda raw, p=prefix, obs=observed_in_flight: obs.update(
                    h.verify_durable_in_flight_intent(endpoint, p, raw)))
            before_calls = len(observations(calls))
            client_key = "client-disconnect-before-receipt"
            intent = _intent_for(launch, release, client_key)

            with h.coordinator(work, jar, launch_file, endpoint, prefix, secret, "disconnect-live",
                               extra_config=observation_config(native_dispatch_log)) as (port, _proc):
                h.http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
                with IngressHttpBarrierProxy(port) as ingress_proxy:
                    ingress_proxy.arm_disconnect_before_receipt()
                    disconnect_out: list[tuple[str, object]] = []

                    def submit_via_ingress():
                        try:
                            disconnect_out.append(("ok", h.http(ingress_proxy.port, admission, 200, intent, timeout=30)))
                        except (RemoteDisconnected, ConnectionResetError, BrokenPipeError) as error:
                            disconnect_out.append(("disconnected", error))
                        except BaseException as error:
                            disconnect_out.append(("error", error))

                    t = threading.Thread(target=submit_via_ingress, daemon=True)
                    t.start()
                    require(ingress_proxy.admission_response_captured.wait(timeout=20),
                            "Ingress proxy did not capture committed 200 admission response")
                    captured_receipt = json.loads(ingress_proxy.captured_admission_response[2].decode("utf-8"))
                    key_items = h.consistent_scan(endpoint, prefix + "_key")
                    require(any("admission_metadata" in item for item in key_items),
                            "Receipt was not durably committed before client disconnect")

                    ingress_proxy.drop_client_response()
                    t.join(timeout=10)
                    require(not t.is_alive() and disconnect_out and disconnect_out[0][0] == "disconnected",
                            f"Expected submitting client disconnect before receipt, got {disconnect_out}")
                    require(ingress_proxy.dropped_client_responses == 1,
                            f"Expected 1 dropped client response, got {ingress_proxy.dropped_client_responses}")

                # Reconnect from a fresh standalone zero-JVM Node/Python client process.
                base_url = f"http://127.0.0.1:{port}"
                looked_up = _run_standalone_client(language, node, work, "lookup", base_url, "fixture-admin-token",
                                                   tenant, pipeline, client_key)
                replayed = _run_standalone_client(language, node, work, "submit", base_url, "fixture-admin-token",
                                                  tenant, pipeline, client_key, contract_ver, release,
                                                  launch["inputType"], "3")
                require(looked_up == captured_receipt and replayed == captured_receipt,
                        f"Standalone reconnect disagreed with committed receipt: lookup={looked_up} replay={replayed}")
                waited = _run_standalone_client(language, node, work, "wait", base_url, "fixture-admin-token",
                                                tenant, captured_receipt["executionId"], "30")
                status = waited["status"]
                result = waited["result"]
                require(status["status"] == "SUCCEEDED" and status["executionId"] == captured_receipt["executionId"],
                        f"Expected SUCCEEDED after client reconnect, got {status}")
                require(result["resultPayload"] == {
                    "payloadTypeId": launch["outputType"],
                    "payloadEncoding": "tpf-polyglot-int32-json/1",
                    "payload": '{"value":5}',
                }, f"Unexpected typed result from standalone client: {result}")

            execution_id = captured_receipt["executionId"]
            context = {"executionId": execution_id, "traceId": execution_id}
            expected_calls = [{"handler": name, "value": value, "context": context}
                              for name, value in (("normalize", 3), ("apply", 4), ("finish", 8))]
            terminal_row = _assert_known_terminal_durable(
                h, endpoint, prefix, tenant, execution_id, observed_in_flight, captured_receipt)
            h.assert_zero_dlq_publications(work, prefix, "disconnect-live")
            require(worker_proxy.callback_errors == [] and worker_proxy.execute_posts == 1,
                    f"Client disconnect caused unexpected dispatch count: posts={worker_proxy.execute_posts} errs={worker_proxy.callback_errors}")
            require(observations(calls)[before_calls:] == expected_calls,
                    f"Client disconnect ran unexpected calls: {observations(calls)[before_calls:]}")
            require(generic_dispatch_count(native_dispatch_log) == 0, "Java fallback fired on client disconnect")
            print(json.dumps({
                "case": "client-disconnect-before-receipt",
                "language": language,
                "executionId": execution_id,
                "workerPid": worker_pid,
                "standaloneClient": "client.mjs" if language == "node" else "client.py",
                "executePostsTotal": worker_proxy.execute_posts,
                "terminalStatus": terminal_row["status"]["S"],
                "terminalVersion": int(terminal_row["version"]["N"]),
                "terminalAttempt": int(terminal_row["attempt"]["N"]),
                "businessCalls": len(expected_calls),
                "genericDispatches": generic_dispatch_count(native_dispatch_log),
            }), flush=True)
            print(f"PACKAGED_CLIENT_DISCONNECT_PASS:{language}", flush=True)

            # 2. Wrong Release request sent directly to the prepared worker (tracked separately from coordinator).
            prefix = f"native_wrong_rel_{language}"
            h.tables(endpoint, prefix)
            worker_proxy.reset_counters()
            before_calls = len(observations(calls))
            wrong_release_cmd = json.dumps(
                launch["command"] | {"releaseVersion": f"{language}-wrong-release"}
            ).encode("utf-8")
            wrong_headers = list(signed("POST", EXECUTE_PATH, wrong_release_cmd, secret).items())
            wrong_status, _ = _direct_https_request(
                work / "ca.pem", worker_port, "POST", EXECUTE_PATH, wrong_headers, wrong_release_cmd)
            require(wrong_status == 400, f"Expected worker to reject wrong Release with 400, got {wrong_status}")
            require(worker_proxy.execute_posts == 0,
                    f"Fixture wrong-Release send must not increment coordinator count: {worker_proxy.execute_posts}")
            require(observations(calls)[before_calls:] == [],
                    f"Wrong Release request reached business handlers: {observations(calls)[before_calls:]}")
            require(h.consistent_scan(endpoint, prefix + "_execution") == []
                    and h.consistent_scan(endpoint, prefix + "_key") == [],
                    "Wrong Release fixture request created coordinator durable state")
            print(json.dumps({
                "case": "wrong-release-request",
                "language": language,
                "workerPid": worker_pid,
                "coordinatorExecutePosts": worker_proxy.execute_posts,
                "fixtureSends": 1,
                "workerHttpStatus": wrong_status,
                "businessCalls": 0,
                "genericDispatches": generic_dispatch_count(native_dispatch_log),
            }), flush=True)
            print(f"PACKAGED_WRONG_RELEASE_PASS:{language}", flush=True)

            # 3. Missing binding after restart -> pre-intent failure oracle (WAIT_RETRY v2/a1 -> FAILED v4/a1).
            prefix = f"native_missing_bind_{language}"
            h.tables(endpoint, prefix)
            worker_proxy.reset_counters()
            worker_proxy.set_before_execute_forward(None)
            before_calls = len(observations(calls))
            client_key = "missing-binding-after-restart"
            registered, durable_receipt, first_pid = _admit_and_kill_before_intent(
                h, work, jar, launch, launch_file, endpoint, prefix, secret, admin, admission, release,
                _intent_for(launch, release, client_key), worker_proxy, calls, before_calls,
                "missing-bind-first", native_dispatch_log)

            missing_cfg = observation_config(native_dispatch_log, {"proof.binding.enabled": "false"})
            with h.coordinator(work, jar, launch_file, endpoint, prefix, secret, "missing-bind-restart",
                               extra_config=missing_cfg) as (port, proc):
                second_pid = proc.pid
                require(second_pid != first_pid, "Expected fresh coordinator PID after restart")
                require(h.http(port, f"{admin}/{release}", 200) == registered,
                        "Retained Release changed with missing binding")
                terminal_row = _assert_pre_intent_failure_oracle(
                    h, port, endpoint, prefix, tenant, admission, control, client_key, durable_receipt,
                    native_dispatch_log)
            require(worker_proxy.execute_posts == 0 and observations(calls)[before_calls:] == [],
                    "Missing binding restart dispatched to worker")
            print(json.dumps({
                "case": "missing-binding-after-restart",
                "language": language,
                "executionId": durable_receipt["executionId"],
                "firstPid": first_pid,
                "secondPid": second_pid,
                "executePostsTotal": worker_proxy.execute_posts,
                "terminalStatus": terminal_row["status"]["S"],
                "terminalVersion": int(terminal_row["version"]["N"]),
                "terminalAttempt": int(terminal_row["attempt"]["N"]),
                "businessCalls": 0,
                "genericDispatches": generic_dispatch_count(native_dispatch_log),
            }), flush=True)
            print(f"PACKAGED_MISSING_BINDING_PASS:{language}", flush=True)

            # 3b. Killswitch disabled after restart (valid CDI binding still present, experimental flag off).
            prefix = f"native_killswitch_{language}"
            h.tables(endpoint, prefix)
            worker_proxy.reset_counters()
            worker_proxy.set_before_execute_forward(None)
            before_calls = len(observations(calls))
            client_key = "killswitch-disabled-after-restart"
            registered, durable_receipt, first_pid = _admit_and_kill_before_intent(
                h, work, jar, launch, launch_file, endpoint, prefix, secret, admin, admission, release,
                _intent_for(launch, release, client_key), worker_proxy, calls, before_calls,
                "killswitch-first", native_dispatch_log)

            killswitch_cfg = observation_config(native_dispatch_log, {
                "proof.binding.enabled": "true",
                "pipeline.orchestrator.experimental.native-worker.enabled": "false",
            })
            with h.coordinator(work, jar, launch_file, endpoint, prefix, secret, "killswitch-restart",
                               extra_config=killswitch_cfg) as (port, proc):
                second_pid = proc.pid
                require(second_pid != first_pid, "Expected fresh coordinator PID after killswitch restart")
                require(h.http(port, f"{admin}/{release}", 200) == registered,
                        "Retained Release changed with killswitch disabled")
                terminal_row = _assert_pre_intent_failure_oracle(
                    h, port, endpoint, prefix, tenant, admission, control, client_key, durable_receipt,
                    native_dispatch_log)
            require(worker_proxy.execute_posts == 0 and observations(calls)[before_calls:] == [],
                    "Killswitch-disabled restart dispatched to worker")
            print(json.dumps({
                "case": "killswitch-disabled-after-restart",
                "language": language,
                "executionId": durable_receipt["executionId"],
                "firstPid": first_pid,
                "secondPid": second_pid,
                "executePostsTotal": worker_proxy.execute_posts,
                "terminalStatus": terminal_row["status"]["S"],
                "terminalVersion": int(terminal_row["version"]["N"]),
                "terminalAttempt": int(terminal_row["attempt"]["N"]),
                "businessCalls": 0,
                "genericDispatches": generic_dispatch_count(native_dispatch_log),
            }), flush=True)
            print(f"PACKAGED_KILLSWITCH_DISABLED_PASS:{language}", flush=True)

            # 4. Replaced binding after restart (valid v2 Release/descriptor) -> pre-intent failure oracle.
            prefix = f"native_replaced_bind_{language}"
            h.tables(endpoint, prefix)
            worker_proxy.reset_counters()
            before_calls = len(observations(calls))
            client_key = "replaced-binding-after-restart"
            registered, durable_receipt, first_pid = _admit_and_kill_before_intent(
                h, work, jar, launch, launch_file, endpoint, prefix, secret, admin, admission, release,
                _intent_for(launch, release, client_key), worker_proxy, calls, before_calls,
                "replaced-bind-first", native_dispatch_log)

            with h.coordinator(work, jar, replacement_launch_file, endpoint, prefix, secret, "replaced-bind-restart",
                               extra_config=observation_config(native_dispatch_log)) as (port, proc):
                second_pid = proc.pid
                require(second_pid != first_pid, "Expected fresh coordinator PID after restart")
                require(h.http(port, f"{admin}/{release}", 200) == registered,
                        "Retained Release changed with replaced binding")
                terminal_row = _assert_pre_intent_failure_oracle(
                    h, port, endpoint, prefix, tenant, admission, control, client_key, durable_receipt,
                    native_dispatch_log)
            require(worker_proxy.execute_posts == 0 and observations(calls)[before_calls:] == [],
                    "Replaced binding restart dispatched to worker")
            print(json.dumps({
                "case": "replaced-binding-after-restart",
                "language": language,
                "executionId": durable_receipt["executionId"],
                "firstPid": first_pid,
                "secondPid": second_pid,
                "replacementRelease": replacement["target"]["releaseVersion"],
                "executePostsTotal": worker_proxy.execute_posts,
                "terminalStatus": terminal_row["status"]["S"],
                "terminalVersion": int(terminal_row["version"]["N"]),
                "terminalAttempt": int(terminal_row["attempt"]["N"]),
                "businessCalls": 0,
                "genericDispatches": generic_dispatch_count(native_dispatch_log),
            }), flush=True)
            print(f"PACKAGED_REPLACED_BINDING_PASS:{language}", flush=True)

            # 5. Coordinator death after settlement (hold DynamoDB SUCCEEDED ack, SIGKILL, restart with worker offline).
            prefix = f"native_post_settle_{language}"
            h.tables(endpoint, prefix)
            observed_in_flight = {}
            worker_proxy.reset_counters()
            worker_proxy.set_before_execute_forward(
                lambda raw, p=prefix, obs=observed_in_flight: obs.update(
                    h.verify_durable_in_flight_intent(endpoint, p, raw)))
            before_calls = len(observations(calls))
            settle_client_key = "coordinator-death-after-settlement"
            settle_intent = _intent_for(launch, release, settle_client_key)

            with DynamoBarrierProxy(endpoint) as settle_dynamo:
                with h.coordinator(work, jar, launch_file, settle_dynamo.endpoint, prefix, secret,
                                   "post-settle-first", extra_config=observation_config(native_dispatch_log)) as (port, proc):
                    h.http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
                    settle_registered = h.http(port, f"{admin}/{release}", 200)
                    settle_dynamo.arm_death_after_settlement(prefix + "_execution")
                    settle_receipt = h.http(port, admission, 200, settle_intent)
                    settle_exec_id = settle_receipt["executionId"]

                    require(settle_dynamo.settlement_committed.wait(timeout=20),
                            "Terminal SUCCEEDED settlement did not commit while ack was held")
                    pre_kill_row = _assert_known_terminal_durable(
                        h, endpoint, prefix, tenant, settle_exec_id, observed_in_flight, settle_receipt)
                    require(worker_proxy.execute_posts == 1,
                            f"Expected 1 execute POST before post-settlement kill, got {worker_proxy.execute_posts}")

                    settle_first_pid = proc.pid
                    proc.kill()
                    require(proc.wait(timeout=8) == -9, "First coordinator did not exit on SIGKILL")
                    settle_dynamo.fence_and_discard()

            # Also admit & kill for Case 6 (stale worker after restart) while this valid worker is still running!
            stale_prefix = f"native_stale_worker_{language}"
            h.tables(endpoint, stale_prefix)
            worker_proxy.reset_counters()
            worker_proxy.set_before_execute_forward(None)
            stale_before_calls = len(observations(calls))
            stale_key = "stale-worker-after-restart"
            stale_registered, stale_receipt, stale_first_pid = _admit_and_kill_before_intent(
                h, work, jar, launch, launch_file, endpoint, stale_prefix, secret, admin, admission, release,
                _intent_for(launch, release, stale_key), worker_proxy, calls, stale_before_calls,
                "stale-worker-first", native_dispatch_log)

        # ---- Worker is now OFFLINE: finish Case 5 (coordinator death after settlement) restart check ----
        with h.coordinator(work, jar, launch_file, endpoint, prefix, secret, "post-settle-restart",
                           extra_config=observation_config(native_dispatch_log)) as (port, proc):
            settle_second_pid = proc.pid
            require(settle_second_pid != settle_first_pid, "Expected fresh coordinator PID after post-settlement restart")
            require(h.http(port, f"{admin}/{release}", 200) == settle_registered,
                    "Retained Release changed across post-settlement restart")
            require(h.http(port, f"{admission}/{settle_client_key}", 200) == settle_receipt,
                    "Retained receipt changed across post-settlement restart")
            require(h.http(port, admission, 200, settle_intent) == settle_receipt,
                    "Replay changed receipt after post-settlement restart")
            h.http(port, admission, 409, settle_intent | {"inputPayload": settle_intent["inputPayload"] | {"payload": '{"value":4}'}})
            settle_exec_path = f"{control}/executions/{settle_exec_id}"
            settle_result = h.http(port, settle_exec_path + "/result", 200)
            require(settle_result["status"]["status"] == "SUCCEEDED"
                    and settle_result["status"]["executionId"] == settle_exec_id
                    and settle_result["resultPayload"] == {
                        "payloadTypeId": launch["outputType"],
                        "payloadEncoding": "tpf-polyglot-int32-json/1",
                        "payload": '{"value":5}',
                    }, f"Unexpected post-settlement result after restart: {settle_result}")
        post_restart_row = _assert_known_terminal_durable(
            h, endpoint, prefix, tenant, settle_exec_id, observed_in_flight, settle_receipt)
        require(post_restart_row["version"]["N"] == pre_kill_row["version"]["N"]
                and post_restart_row["attempt"]["N"] == pre_kill_row["attempt"]["N"]
                and post_restart_row["last_transition_key"]["S"] == pre_kill_row["last_transition_key"]["S"],
                "Post-settlement restart changed durable terminal version/attempt/transitionKey")
        h.assert_zero_dlq_publications(work, prefix, "post-settle-first", "post-settle-restart")
        require(generic_dispatch_count(native_dispatch_log) == 0, "Java fallback fired after post-settlement restart")
        settle_context = {"executionId": settle_exec_id, "traceId": settle_exec_id}
        settle_expected_calls = [{"handler": name, "value": value, "context": settle_context}
                                 for name, value in (("normalize", 3), ("apply", 4), ("finish", 8))]
        require(observations(calls)[before_calls:] == settle_expected_calls,
                f"Post-settlement case ran unexpected worker calls: {observations(calls)[before_calls:]}")
        print(json.dumps({
            "case": "coordinator-death-after-settlement",
            "language": language,
            "executionId": settle_exec_id,
            "firstPid": settle_first_pid,
            "secondPid": settle_second_pid,
            "executePostsTotal": 1,
            "terminalStatus": post_restart_row["status"]["S"],
            "terminalVersion": int(post_restart_row["version"]["N"]),
            "terminalAttempt": int(post_restart_row["attempt"]["N"]),
            "businessCalls": 3,
            "genericDispatches": generic_dispatch_count(native_dispatch_log),
        }), flush=True)
        print(f"PACKAGED_POST_SETTLEMENT_DEATH_PASS:{language}", flush=True)

        # ---- Case 6: Stale worker after restart (swap worker behind same origin to mismatched artifact digest) ----
        stale_calls = work / f"{language}-stale-worker-calls.jsonl"
        stale_worker_launch = launch | {
            "calls": str(stale_calls),
            "target": launch["target"] | {"runnableArtifactDigest": "sha256:" + "1" * 64},
        }
        worker_proxy.reset_counters()
        with host(language, work, node, stale_worker_launch) as stale_worker_proc:
            stale_worker_pid = stale_worker_proc.pid
            with h.coordinator(work, jar, launch_file, endpoint, stale_prefix, secret, "stale-worker-restart",
                               extra_config=observation_config(native_dispatch_log)) as (port, proc):
                stale_second_pid = proc.pid
                require(stale_second_pid != stale_first_pid, "Expected fresh coordinator PID on stale-worker restart")
                require(h.http(port, f"{admin}/{release}", 200) == stale_registered,
                        "Retained Release changed on stale-worker restart")
                terminal_row = _assert_pre_intent_failure_oracle(
                    h, port, endpoint, stale_prefix, tenant, admission, control, stale_key, stale_receipt,
                    native_dispatch_log)
        require(worker_proxy.capability_gets >= 1,
                f"Expected ordinary capability check against stale worker, got {worker_proxy.capability_gets}")
        require(worker_proxy.execute_posts == 0 and observations(stale_calls) == [],
                "Stale worker received execute POST or ran business handlers")
        print(json.dumps({
            "case": "stale-worker-after-restart",
            "language": language,
            "executionId": stale_receipt["executionId"],
            "firstPid": stale_first_pid,
            "secondPid": stale_second_pid,
            "staleWorkerPid": stale_worker_pid,
            "capabilityGetsAfterRestart": worker_proxy.capability_gets,
            "executePostsTotal": worker_proxy.execute_posts,
            "terminalStatus": terminal_row["status"]["S"],
            "terminalVersion": int(terminal_row["version"]["N"]),
            "terminalAttempt": int(terminal_row["attempt"]["N"]),
            "businessCalls": 0,
            "genericDispatches": generic_dispatch_count(native_dispatch_log),
        }), flush=True)
        print(f"PACKAGED_STALE_WORKER_PASS:{language}", flush=True)
