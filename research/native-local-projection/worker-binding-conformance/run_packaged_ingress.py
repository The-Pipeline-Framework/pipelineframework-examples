"""Exercise hosted HTTP, terminal restart retention, and admission-before-intent sweep recovery."""

import argparse
from contextlib import contextmanager
from hashlib import file_digest, sha256
from http.client import HTTPConnection, RemoteDisconnected
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import tempfile
import threading
from time import monotonic, sleep, time
from urllib.parse import urlsplit

from packaged_ingress_matrix import run_ingress_matrix_and_portable_outcomes
from packaged_isolation_recovery import (
    generic_dispatch_count,
    run_isolation_and_disconnect_cases,
    verify_java_fallback_positive_control,
)
from packaged_worker_faults import PackagedHarness, run_worker_faults_and_duplicates
from recovery_proxies import DynamoBarrierProxy, WorkerHttpsObserverProxy
from run_hosted_worker import (
    HERE, LANGUAGES, bundle_fixture, certificates, command, compile_alternate,
    compile_workers, host, local_port, observations, pinned_classpath, pinned_file,
    require, write_launch,
)

TOKEN = "fixture-admin-token"


def http(port, path, expected, body=None, token=TOKEN, timeout=5):
    connection = HTTPConnection("127.0.0.1", port, timeout=timeout)
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = "Bearer " + token
    try:
        connection.request("GET" if body is None else "POST", path,
                           None if body is None else json.dumps(body), headers)
        response = connection.getresponse()
        payload = response.read()
        require(response.status == expected, f"{path}: HTTP {response.status}, expected {expected}: {payload!r}")
        return json.loads(payload) if payload else None
    finally:
        connection.close()


def dynamo(endpoint, operation, request):
    env = os.environ | {"AWS_ACCESS_KEY_ID": "local", "AWS_SECRET_ACCESS_KEY": "local",
                        "AWS_SESSION_TOKEN": "", "AWS_PAGER": ""}
    output = subprocess.check_output(["aws", "--endpoint-url", endpoint, "--region", "us-east-1",
                                      "--output", "json", "dynamodb", operation,
                                      "--cli-input-json", json.dumps(request)], env=env, timeout=30)
    return json.loads(output)


def tables(endpoint, prefix):
    def create(suffix, keys, indexes=()):
        def schema(fields):
            return [{"AttributeName": name, "KeyType": kind}
                    for (name, _), kind in zip(fields, ("HASH", "RANGE"), strict=False)]

        fields = dict(keys)
        for _, index_keys in indexes:
            fields.update(index_keys)
        request = {
            "TableName": prefix + suffix,
            "AttributeDefinitions": [{"AttributeName": name, "AttributeType": kind} for name, kind in fields.items()],
            "KeySchema": schema(keys),
            "BillingMode": "PAY_PER_REQUEST",
        }
        if indexes:
            request["GlobalSecondaryIndexes"] = [
                {"IndexName": name, "KeySchema": schema(index_keys), "Projection": {"ProjectionType": "ALL"}}
                for name, index_keys in indexes
            ]
        dynamo(endpoint, "create-table", request)

    create("_execution", (("tenant_id", "S"), ("execution_id", "S")))
    create("_key", (("tenant_execution_key", "S"),))
    create("_payload", (("payload_id", "S"), ("payload_part", "S")))
    create("_interaction", (("tenant_id", "S"), ("interaction_id", "S")), (
        ("await-interaction-pending-by-deadline", (("query_deadline_key", "S"), ("query_deadline_sort", "S"))),
        ("await-interaction-continuation-work", (("query_continuation_key", "S"),
                                                 ("query_continuation_due_epoch_ms", "N"))),
    ))


@contextmanager
def coordinator(work, jar, launch, endpoint, prefix, secret, suffix, extra_config=None, trust_ca=True):
    port = local_port()
    config = {
        "proof.launch": launch,
        "proof.generic-dispatch-log": work / "native-generic-dispatches.log",
        "pipeline.telemetry.enabled": "true",
        "pipeline.telemetry.metrics.enabled": "true",
        "quarkus.http.host": "127.0.0.1",
        "quarkus.http.port": port,
        "quarkus.devservices.enabled": "false",
        "quarkus.grpc.server.use-separate-server": "false",
        **({"javax.net.ssl.trustStore": work / "trust.p12",
            "javax.net.ssl.trustStorePassword": "changeit"} if trust_ca else {}),
        **{"pipeline.orchestrator." + name: value for name, value in {
            "mode": "QUEUE_ASYNC", "coordination-host": "NATIVE", "state-provider": "dynamo",
            "dispatcher-provider": "event", "process-loops-disabled": "false",
            "sweep-interval": "PT1S", "max-retries": "1", "retry-delay": "PT2S",
            "idempotency-policy": "SERVER_KEY_ONLY",
            "admin.enabled": "true", "admin.admin-token": TOKEN,
            "control-plane.enabled": "true", "control-plane.admin-token": TOKEN,
            "experimental.native-worker.enabled": "true",
            "releases.registry.provider": "file", "releases.storage.provider": "local",
            "releases.storage.root": work / prefix,
            "dynamo.endpoint-override": endpoint, "dynamo.region": "us-east-1",
            "dynamo.execution-table": prefix + "_execution", "dynamo.execution-key-table": prefix + "_key",
            "dynamo.execution-payload-table": prefix + "_payload", "dynamo.await-interaction-table": prefix + "_interaction",
        }.items()},
        **(extra_config or {}),
    }
    args = ["java", *(f"-D{k}={v}" for k, v in config.items()), "-jar", str(jar)]
    log = work / f"{prefix}-{suffix}.log"
    env = os.environ | {"AWS_ACCESS_KEY_ID": "local", "AWS_SECRET_ACCESS_KEY": "local",
                        "AWS_SESSION_TOKEN": "", "TPF_NATIVE_WORKER_SECRET": secret}
    with log.open("w") as output, subprocess.Popen(args, cwd=work, env=env, stdout=output,
                                                    stderr=subprocess.STDOUT) as process:
        try:
            deadline = monotonic() + 45
            while process.poll() is None and monotonic() < deadline:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                        break
                except OSError:
                    sleep(0.1)
            else:
                raise AssertionError(f"Packaged coordinator failed to start:\n{log.read_text()}")
            try:
                yield port, process
            except Exception as error:
                error.add_note(log.read_text())
                raise
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def _consistent_scan(endpoint, table_name):
    return dynamo(endpoint, "scan", {"TableName": table_name, "ConsistentRead": True})["Items"]


def _consistent_execution(endpoint, table_name, tenant, execution_id):
    return dynamo(endpoint, "get-item", {
        "TableName": table_name,
        "Key": {"tenant_id": {"S": tenant}, "execution_id": {"S": execution_id}},
        "ConsistentRead": True,
    }).get("Item")


def _consistent_key_item(endpoint, table_name, tenant_execution_key):
    return dynamo(endpoint, "get-item", {
        "TableName": table_name,
        "Key": {"tenant_execution_key": {"S": tenant_execution_key}},
        "ConsistentRead": True,
    }).get("Item")


def _expected_intent_storage_key(tenant_id, execution_id, execution_version):
    scope = json.dumps([tenant_id, execution_id, execution_version], separators=(",", ":")).encode("utf-8")
    return "paired-dispatch:v1:sha256:" + sha256(scope).hexdigest()


def _verify_durable_in_flight_intent(endpoint, prefix, raw_body):
    cmd = json.loads(raw_body.decode("utf-8"))
    exec_id = cmd["executionId"]
    tenant = cmd["tenantId"]
    row = _consistent_execution(endpoint, prefix + "_execution", tenant, exec_id)
    require(row is not None, f"Missing execution row {exec_id} before worker execute POST")
    require(row["status"]["S"] == "RUNNING", f"Expected RUNNING before worker POST, got {row.get('status')}")
    require(int(row["version"]["N"]) == 1, f"Expected version 1 before worker POST, got {row.get('version')}")
    require(int(row["attempt"]["N"]) == 0, f"Expected attempt 0 before worker POST, got {row.get('attempt')}")
    require(cmd["attempt"] == int(row["attempt"]["N"]),
            f"Command attempt {cmd['attempt']!r} did not match leased attempt {row['attempt']['N']}")
    now_ms = int(time() * 1000)
    require("lease_owner" in row and int(row["lease_expires_epoch_ms"]["N"]) > now_ms,
            f"Expected live lease (expires > {now_ms}) before worker POST: {row}")
    require("paired_dispatch_intent" in row and "M" in row["paired_dispatch_intent"],
            f"Missing active intent locator map before worker POST: {row}")
    marker = row["paired_dispatch_intent"]["M"]
    expected_fact_key = _expected_intent_storage_key(tenant, exec_id, cmd["executionVersion"])
    require(marker["fact_key"]["S"] == expected_fact_key,
            f"Intent locator fact_key {marker['fact_key']} did not match command key {expected_fact_key}")
    require(marker["transition_key"]["S"] == cmd["transitionKey"],
            f"Intent locator transition_key {marker['transition_key']} did not match {cmd['transitionKey']}")

    fact_item = _consistent_key_item(endpoint, prefix + "_key", expected_fact_key)
    require(fact_item is not None and "paired_dispatch_intent" in fact_item,
            f"Missing immutable intent fact for {expected_fact_key}: {fact_item}")
    evidence_raw = fact_item["paired_dispatch_intent"]["S"]
    expected_digest = "sha256:" + sha256(evidence_raw.encode("utf-8")).hexdigest()
    require(marker["digest"]["S"] == expected_digest,
            f"Intent locator digest {marker['digest']} did not match fact digest {expected_digest}")
    evidence = json.loads(evidence_raw)
    expected_command_digest = "sha256:" + sha256(raw_body).hexdigest()
    for field, expected in (
        ("tenantId", tenant),
        ("executionId", exec_id),
        ("version", cmd["executionVersion"]),
        ("leaseOwner", row["lease_owner"]["S"]),
        ("transitionKey", cmd["transitionKey"]),
        ("pipelineId", cmd["pipelineId"]),
        ("contractVersion", cmd["contractVersion"]),
        ("releaseVersion", cmd["releaseVersion"]),
        ("attempt", cmd["attempt"]),
        ("commandDigest", expected_command_digest),
    ):
        require(evidence.get(field) == expected,
                f"Intent fact field {field}={evidence.get(field)!r} did not match command/lease {expected!r}")
    return {
        "executionId": exec_id,
        "transitionKey": cmd["transitionKey"],
        "factKey": expected_fact_key,
        "evidenceRaw": evidence_raw,
        "marker": marker,
        "leaseExpiresEpochMs": int(row["lease_expires_epoch_ms"]["N"]),
    }


def _assert_admitted_and_unclaimed(endpoint, prefix, client_key, expected_receipt=None, expected_index_key=None):
    key_items = _consistent_scan(endpoint, prefix + "_key")
    intent_rows = [item for item in key_items
                   if item.get("tenant_execution_key", {}).get("S", "").startswith("paired-dispatch:v1:")]
    require(intent_rows == [], f"Found unexpected paired dispatch intent before claim: {intent_rows}")
    receipt_rows = [item for item in key_items if "admission_metadata" in item]
    index_rows = [item for item in key_items if "execution_id" in item and "admission_metadata" not in item]
    require(len(receipt_rows) == 1 and len(index_rows) == 1 and len(key_items) == 2,
            f"Expected 1 native receipt row and 1 execution-key row before claim, found {key_items}")
    receipt_doc = json.loads(receipt_rows[0]["admission_metadata"]["S"])["receipt"]
    require(receipt_doc["clientKey"] == client_key, f"Unexpected receipt clientKey: {receipt_doc}")
    execution_id = receipt_doc["executionId"]
    index_key = index_rows[0]["tenant_execution_key"]["S"]
    require(index_rows[0]["execution_id"]["S"] == execution_id,
            f"Execution-key index row disagreed with receipt: {index_rows[0]}")
    if expected_receipt is not None:
        require(receipt_doc == expected_receipt,
                f"Pre-kill receipt changed from {expected_receipt} to {receipt_doc}")
    if expected_index_key is not None:
        require(index_key == expected_index_key,
                f"Execution-key index key changed from {expected_index_key} to {index_key}")

    exec_items = _consistent_scan(endpoint, prefix + "_execution")
    require(len(exec_items) == 1, f"Expected exactly 1 execution row, found {len(exec_items)}: {exec_items}")
    row = exec_items[0]
    require(row["execution_id"]["S"] == execution_id, "Execution row ID disagrees with receipt")
    require(row["status"]["S"] == "QUEUED", f"Expected QUEUED before kill, got {row.get('status')}")
    require(int(row["version"]["N"]) == 0, f"Expected version 0 before kill, got {row.get('version')}")
    require(int(row["attempt"]["N"]) == 0, f"Expected attempt 0 before kill, got {row.get('attempt')}")
    for forbidden in ("lease_owner", "paired_dispatch_intent", "result_payload_json",
                      "result_payload_reference", "result_payload_digest"):
        require(forbidden not in row, f"Unexpected {forbidden} on unclaimed execution row: {row}")
    return receipt_doc, index_key


def _distinct_port(avoid_port):
    while True:
        candidate = local_port()
        if candidate != avoid_port:
            return candidate


def verify_admission_before_intent_recovery(work, jar, classes, java, node, language, generated, bundle, endpoint):
    """Kill coordinator after durable admission commit and fenced claim, then prove ordinary sweep sends once."""
    prefix = f"native_recovery_{language}"
    tables(endpoint, prefix)
    calls = work / f"{language}-recovery-calls.jsonl"
    secret = secrets.token_urlsafe(32)
    observed_in_flight = {}

    def verify_intent_before_worker_forward(raw_body):
        observed_in_flight.update(_verify_durable_in_flight_intent(endpoint, prefix, raw_body))

    with WorkerHttpsObserverProxy(
        cafile=work / "ca.pem",
        certfile=work / "worker.pem",
        keyfile=work / "worker.key",
        before_execute_forward=verify_intent_before_worker_forward,
    ) as worker_proxy:
        worker_port = _distinct_port(worker_proxy.port)
        worker_proxy.set_upstream_port(worker_port)
        launch = json.loads(command([
            "java", "-cp", os.pathsep.join((str(classes), java)), "NativeWorkerReleaseConformance",
            language, str(generated), str(bundle), "-", str(work / f"{language}-recovery-release"),
            f"https://localhost:{worker_proxy.port}",
        ], work))
        target = launch["target"]
        target["sharedSecretRef"] = "env:TPF_NATIVE_WORKER_SECRET"
        launch.update(port=worker_port, calls=str(calls), secret=secret,
                      key=str(work / "worker.key"), cert=str(work / "worker.pem"))
        launch_file = write_launch(work, f"{language}-recovery", {"target": target, "descriptor": launch["descriptor"]})
        tenant = target["tenantId"]
        pipeline = target["coordinatorContract"]["pipelineId"]
        release = target["releaseVersion"]
        admin = f"/tpf/admin/tenants/{tenant}/pipelines/{pipeline}/releases"
        control = f"/tpf/control-plane/tenants/{tenant}"
        admission = f"{control}/pipelines/{pipeline}/execution-admissions"
        client_key = "admission-before-intent"
        intent = {
            "clientKey": client_key, "contractVersion": launch["command"]["contractVersion"],
            "releaseVersion": release, "inputShape": "UNI", "outputStreaming": False,
            "inputPayload": {"payloadTypeId": launch["inputType"],
                             "payloadEncoding": "tpf-polyglot-int32-json/1", "payload": '{"value":3}'},
        }

        with host(language, work, node, launch):
            submit_outcome = []

            def submit_admit(port):
                try:
                    submit_outcome.append(("response", http(port, admission, 200, intent, timeout=60)))
                except (RemoteDisconnected, ConnectionResetError, BrokenPipeError) as error:
                    submit_outcome.append(("disconnected", error))
                except BaseException as error:
                    submit_outcome.append(("error", error))

            # Use a dedicated proxy for the killed first coordinator and close it permanently on kill.
            with DynamoBarrierProxy(endpoint) as first_dynamo_proxy:
                with coordinator(work, jar, launch_file, first_dynamo_proxy.endpoint, prefix, secret, "recovery-first") as (port, process):
                    http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
                    registered = http(port, f"{admin}/{release}", 200)
                    first_dynamo_proxy.arm_admission_before_intent(prefix + "_execution", prefix + "_key")

                    client_thread = threading.Thread(target=submit_admit, args=(port,), daemon=True)
                    client_thread.start()

                    require(first_dynamo_proxy.admission_committed.wait(timeout=20),
                            "Admission transaction did not commit to DynamoDB while barrier was armed")
                    require(first_dynamo_proxy.claim_held.wait(timeout=15),
                            "Ordinary sweep did not attempt a claim while barrier was armed")

                    durable_receipt, index_key = _assert_admitted_and_unclaimed(endpoint, prefix, client_key)
                    execution_id = durable_receipt["executionId"]
                    require(first_dynamo_proxy.unexpected_intent_writes == 0,
                            f"Unexpected intent writes while barrier armed: {first_dynamo_proxy.unexpected_intent_writes}")
                    require(first_dynamo_proxy.held_claims >= 1, "Expected at least one fenced claim attempt")
                    require(worker_proxy.execute_posts == 0,
                            f"Worker received {worker_proxy.execute_posts} execute POST(s) before coordinator kill")
                    require(worker_proxy.capability_gets >= 1, "Expected admission capability probe before commit")
                    require(observations(calls) == [], f"Worker ran business handlers before kill: {observations(calls)}")

                    first_pid = process.pid
                    held_claims_before_kill = first_dynamo_proxy.held_claims
                    process.kill()
                    require(process.wait(timeout=8) == -9, "First coordinator did not exit on SIGKILL")
                    client_thread.join(timeout=10)
                    require(not client_thread.is_alive(), "Submitting client thread hung after coordinator SIGKILL")
                    require(submit_outcome and submit_outcome[0][0] == "disconnected",
                            f"Expected SIGKILL connection drop while admission ack was held, got: {submit_outcome}")
                    first_dynamo_proxy.fence_and_discard()

            # Confirm the durable QUEUED row survived the kill and permanently fenced first proxy without worker traffic.
            _assert_admitted_and_unclaimed(endpoint, prefix, client_key, expected_receipt=durable_receipt,
                                           expected_index_key=index_key)
            require(worker_proxy.execute_posts == 0, "Discarded process still sent an execute POST")
            require(observations(calls) == [], "Discarded process reached business handlers")
            pre_restart_caps = worker_proxy.capability_gets

            # Restart with retained DynamoDB + retained file Release registry + fresh staging directory.
            with coordinator(work, jar, launch_file, endpoint, prefix, secret, "recovery-restart") as (port, process):
                second_pid = process.pid
                require(second_pid != first_pid, "Expected a fresh coordinator PID after restart")
                require(http(port, f"{admin}/{release}", 200) == registered,
                        "Retained Release changed across coordinator restart")
                retained_receipt = http(port, f"{admission}/{client_key}", 200)
                require(retained_receipt == durable_receipt,
                        f"Retained receipt {retained_receipt} disagreed with pre-kill durable receipt {durable_receipt}")

                # Do not re-submit or mutate state: wait for the ordinary sweep to claim and complete the original row.
                execution_path = f"{control}/executions/{execution_id}"
                deadline = monotonic() + 30
                status = http(port, execution_path, 200)
                while status["status"] in {"QUEUED", "RUNNING", "WAIT_RETRY"} and monotonic() < deadline:
                    sleep(0.1)
                    status = http(port, execution_path, 200)
                require(status["status"] == "SUCCEEDED", f"Expected sweep-driven SUCCEEDED, got {status}")
                result = http(port, execution_path + "/result", 200)
                require(result["status"] == status and status["executionId"] == execution_id,
                        "Recovered result changed execution identity or status")
                require(result["resultPayload"] == {
                    "payloadTypeId": launch["outputType"],
                    "payloadEncoding": "tpf-polyglot-int32-json/1",
                    "payload": '{"value":5}',
                }, f"Unexpected recovered typed result: {result}")
                require(http(port, admission, 200, intent) == durable_receipt,
                        "Same-key replay after recovery did not return the original receipt")
                http(port, admission, 409, intent | {"inputPayload": intent["inputPayload"] | {"payload": '{"value":4}'}})

            require(worker_proxy.callback_errors == [], f"Pre-forward intent check failed: {worker_proxy.callback_errors}")
            require(worker_proxy.execute_posts == 1,
                    f"Expected exactly 1 execute POST after restart, got {worker_proxy.execute_posts}")
            require(worker_proxy.pre_forward_checks == 1,
                    f"Expected 1 pre-forward durable intent check, got {worker_proxy.pre_forward_checks}")
            require(observed_in_flight.get("executionId") == execution_id
                    and observed_in_flight.get("transitionKey") == f"{execution_id}:0:0",
                    f"Unexpected in-flight observation: {observed_in_flight}")

            terminal_row = _consistent_execution(endpoint, prefix + "_execution", tenant, execution_id)
            require(terminal_row is not None and terminal_row["status"]["S"] == "SUCCEEDED",
                    f"Unexpected final DynamoDB execution row: {terminal_row}")
            require(int(terminal_row["version"]["N"]) == 2, f"Expected terminal version 2, got {terminal_row.get('version')}")
            require(int(terminal_row["attempt"]["N"]) == 0, f"Expected terminal attempt 0, got {terminal_row.get('attempt')}")
            require("lease_owner" not in terminal_row, f"Lease owner was not cleared on settlement: {terminal_row}")
            require("paired_dispatch_intent" not in terminal_row,
                    f"Active intent locator was not cleared on known settlement: {terminal_row}")
            require(terminal_row.get("last_transition_key", {}).get("S") == f"{execution_id}:0:0",
                    f"Unexpected terminal transition key: {terminal_row}")

            final_keys = _consistent_scan(endpoint, prefix + "_key")
            final_intents = [item for item in final_keys
                             if item.get("tenant_execution_key", {}).get("S", "").startswith("paired-dispatch:v1:")]
            final_receipts = [item for item in final_keys if "admission_metadata" in item]
            final_indexes = [item for item in final_keys if "execution_id" in item and "admission_metadata" not in item]
            require(len(final_intents) == 1, f"Expected 1 retained immutable intent fact, got {len(final_intents)}")
            require(final_intents[0]["tenant_execution_key"]["S"] == observed_in_flight["factKey"]
                    and final_intents[0].get("paired_dispatch_intent", {}).get("S") == observed_in_flight["evidenceRaw"],
                    f"Terminal immutable intent fact changed: {final_intents[0]}")
            require(len(final_receipts) == 1, f"Expected 1 retained native receipt row, got {len(final_receipts)}")
            require(len(final_indexes) == 1
                    and final_indexes[0]["tenant_execution_key"]["S"] == index_key
                    and final_indexes[0]["execution_id"]["S"] == execution_id,
                    f"Terminal execution-key index row changed: {final_indexes}")
            require(len(final_keys) == 3,
                    f"Expected 1 receipt + 1 execution-key + 1 immutable intent fact row, got {len(final_keys)}")

            context = {"executionId": execution_id, "traceId": execution_id}
            expected_calls = [{"handler": name, "value": value, "context": context}
                              for name, value in (("normalize", 3), ("apply", 4), ("finish", 8))]
            require(observations(calls) == expected_calls,
                    f"Unexpected worker business calls: {observations(calls)}")

            print(json.dumps({
                "case": "admission-before-intent-recovery",
                "language": language,
                "executionId": execution_id,
                "firstPid": first_pid,
                "secondPid": second_pid,
                "heldClaimsBeforeKill": held_claims_before_kill,
                "capabilityGetsBeforeKill": pre_restart_caps,
                "capabilityGetsTotal": worker_proxy.capability_gets,
                "executePostsTotal": worker_proxy.execute_posts,
                "terminalVersion": int(terminal_row["version"]["N"]),
                "terminalAttempt": int(terminal_row["attempt"]["N"]),
                "businessCalls": len(expected_calls),
            }), flush=True)
            print(f"PACKAGED_ADMISSION_BEFORE_INTENT_RECOVERY_PASS:{language}", flush=True)


def _expected_completed_wire(output_type):
    return {
        "outcome": "COMPLETED",
        "outputPayloads": [{
            "payloadTypeId": output_type,
            "payloadEncoding": "tpf-polyglot-int32-json/1",
            "payload": '{"value":5}',
        }],
        "terminalOutputPublished": False,
        "terminalInputPassthrough": False,
    }


def _assert_unknown_terminal_durable(endpoint, prefix, tenant, execution_id, observed_in_flight):
    row = _consistent_execution(endpoint, prefix + "_execution", tenant, execution_id)
    require(row is not None and row["status"]["S"] == "REMOTE_OUTCOME_UNKNOWN",
            f"Expected REMOTE_OUTCOME_UNKNOWN execution row, got {row}")
    require(row.get("error_code", {}).get("S") == "REMOTE_OUTCOME_UNKNOWN",
            f"Expected REMOTE_OUTCOME_UNKNOWN error_code, got {row}")
    require(int(row["version"]["N"]) == 2, f"Expected unknown terminal version 2, got {row.get('version')}")
    require(int(row["attempt"]["N"]) == 0, f"Expected unknown terminal attempt 0, got {row.get('attempt')}")
    require("lease_owner" not in row, f"Lease owner was not cleared on unknown settlement: {row}")
    require(int(row.get("lease_expires_epoch_ms", {}).get("N", "-1")) == 0,
            f"Lease expiry was not cleared on unknown settlement: {row}")
    for forbidden in ("result_payload_json", "result_payload_reference", "result_payload_digest"):
        require(forbidden not in row, f"Unexpected {forbidden} on unknown terminal row: {row}")
    require(row.get("last_transition_key", {}).get("S") == f"{execution_id}:0:0",
            f"Unexpected unknown terminal transition key: {row}")
    require(row.get("paired_dispatch_intent", {}).get("M") == observed_in_flight["marker"],
            f"Active paired_dispatch_intent locator was not preserved on unknown terminal: {row}")

    final_keys = _consistent_scan(endpoint, prefix + "_key")
    final_intents = [item for item in final_keys
                     if item.get("tenant_execution_key", {}).get("S", "").startswith("paired-dispatch:v1:")]
    final_receipts = [item for item in final_keys if "admission_metadata" in item]
    require(len(final_intents) == 1, f"Expected 1 retained immutable intent fact, got {len(final_intents)}")
    require(final_intents[0]["tenant_execution_key"]["S"] == observed_in_flight["factKey"]
            and final_intents[0].get("paired_dispatch_intent", {}).get("S") == observed_in_flight["evidenceRaw"],
            f"Unknown terminal immutable intent fact changed: {final_intents[0]}")
    require(len(final_receipts) == 1 and len(final_keys) == 3,
            f"Expected 1 receipt + 1 execution-key + 1 immutable intent fact row, got {len(final_keys)}")
    return row


def _assert_zero_dlq_publications(work, prefix, *suffixes):
    for suffix in suffixes:
        log_path = work / f"{prefix}-{suffix}.log"
        if log_path.exists():
            text = log_path.read_text()
            require("Execution moved to DLQ" not in text and "LoggingDeadLetterPublisher" not in text,
                    f"Unexpected DLQ publication in {log_path.name}")


def verify_post_intent_coordinator_death(work, jar, classes, java, node, language, generated, bundle, endpoint):
    """Kill coordinator after durable intent and captured worker completion, then prove lease-expiry unknown fence."""
    prefix = f"native_post_intent_{language}"
    tables(endpoint, prefix)
    calls = work / f"{language}-post-intent-calls.jsonl"
    secret = secrets.token_urlsafe(32)
    observed_in_flight = {}
    lease_config = {
        "pipeline.orchestrator.lease-ms": "4000",
        "pipeline.orchestrator.worker.rest.request-timeout": "PT30S",
    }

    def verify_intent_before_worker_forward(raw_body):
        observed_in_flight.update(_verify_durable_in_flight_intent(endpoint, prefix, raw_body))

    with WorkerHttpsObserverProxy(
        cafile=work / "ca.pem",
        certfile=work / "worker.pem",
        keyfile=work / "worker.key",
        before_execute_forward=verify_intent_before_worker_forward,
    ) as worker_proxy:
        worker_port = _distinct_port(worker_proxy.port)
        worker_proxy.set_upstream_port(worker_port)
        launch = json.loads(command([
            "java", "-cp", os.pathsep.join((str(classes), java)), "NativeWorkerReleaseConformance",
            language, str(generated), str(bundle), "-", str(work / f"{language}-post-intent-release"),
            f"https://localhost:{worker_proxy.port}",
        ], work))
        target = launch["target"]
        target["sharedSecretRef"] = "env:TPF_NATIVE_WORKER_SECRET"
        launch.update(port=worker_port, calls=str(calls), secret=secret,
                      key=str(work / "worker.key"), cert=str(work / "worker.pem"))
        launch_file = write_launch(work, f"{language}-post-intent",
                                   {"target": target, "descriptor": launch["descriptor"]})
        tenant = target["tenantId"]
        pipeline = target["coordinatorContract"]["pipelineId"]
        release = target["releaseVersion"]
        admin = f"/tpf/admin/tenants/{tenant}/pipelines/{pipeline}/releases"
        control = f"/tpf/control-plane/tenants/{tenant}"
        admission = f"{control}/pipelines/{pipeline}/execution-admissions"
        client_key = "post-intent-death"
        intent = {
            "clientKey": client_key, "contractVersion": launch["command"]["contractVersion"],
            "releaseVersion": release, "inputShape": "UNI", "outputStreaming": False,
            "inputPayload": {"payloadTypeId": launch["inputType"],
                             "payloadEncoding": "tpf-polyglot-int32-json/1", "payload": '{"value":3}'},
        }

        with host(language, work, node, launch):
            worker_proxy.arm_hold_execute_response()
            with DynamoBarrierProxy(endpoint) as first_dynamo_proxy:
                with coordinator(work, jar, launch_file, first_dynamo_proxy.endpoint, prefix, secret,
                                 "post-intent-first", extra_config=lease_config) as (port, process):
                    http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
                    registered = http(port, f"{admin}/{release}", 200)
                    receipt = http(port, admission, 200, intent)
                    execution_id = receipt["executionId"]

                    require(worker_proxy.execute_response_captured.wait(timeout=20),
                            "Worker completion was not captured at the observer proxy")
                    require(worker_proxy.callback_errors == [],
                            f"Pre-forward intent check failed: {worker_proxy.callback_errors}")
                    captured_status, _, captured_body = worker_proxy.captured_execute_response
                    require(captured_status == 200
                            and json.loads(captured_body.decode("utf-8")) == _expected_completed_wire(launch["outputType"]),
                            f"Unexpected captured worker completion: status={captured_status} body={captured_body!r}")

                    context = {"executionId": execution_id, "traceId": execution_id}
                    expected_calls = [{"handler": name, "value": value, "context": context}
                                      for name, value in (("normalize", 3), ("apply", 4), ("finish", 8))]
                    require(observations(calls) == expected_calls,
                            f"Expected 3 completed worker calls before kill, got {observations(calls)}")
                    require(worker_proxy.execute_posts == 1,
                            f"Expected 1 execute POST before kill, got {worker_proxy.execute_posts}")

                    running_row = _consistent_execution(endpoint, prefix + "_execution", tenant, execution_id)
                    require(running_row is not None and running_row["status"]["S"] == "RUNNING"
                            and int(running_row["version"]["N"]) == 1 and int(running_row["attempt"]["N"]) == 0
                            and "lease_owner" in running_row
                            and int(running_row["lease_expires_epoch_ms"]["N"]) > int(time() * 1000)
                            and running_row.get("paired_dispatch_intent", {}).get("M") == observed_in_flight["marker"],
                            f"Expected live RUNNING row with intent before SIGKILL, got {running_row}")

                    first_pid = process.pid
                    process.kill()
                    require(process.wait(timeout=8) == -9, "First coordinator did not exit on SIGKILL")
                    first_dynamo_proxy.fence_and_discard()
                    worker_proxy.discard_execute_response()

            # Confirm execution remained RUNNING v1 after the first coordinator exited and its proxy drained,
            # before starting the replacement coordinator (lease may already have expired).
            post_kill_row = _consistent_execution(endpoint, prefix + "_execution", tenant, execution_id)
            require(post_kill_row is not None and post_kill_row["status"]["S"] == "RUNNING"
                    and int(post_kill_row["version"]["N"]) == 1 and int(post_kill_row["attempt"]["N"]) == 0
                    and post_kill_row.get("paired_dispatch_intent", {}).get("M") == observed_in_flight["marker"],
                    f"Expected RUNNING v1 row with intent after first coordinator exit, got {post_kill_row}")

            # Restart with retained DynamoDB + retained file Release registry + fresh staging directory.
            with coordinator(work, jar, launch_file, endpoint, prefix, secret,
                             "post-intent-restart", extra_config=lease_config) as (port, process):
                second_pid = process.pid
                require(second_pid != first_pid, "Expected a fresh coordinator PID after restart")
                require(http(port, f"{admin}/{release}", 200) == registered,
                        "Retained Release changed across coordinator restart")
                require(http(port, f"{admission}/{client_key}", 200) == receipt,
                        "Retained receipt changed across post-intent restart")

                execution_path = f"{control}/executions/{execution_id}"
                deadline = monotonic() + 25
                status = http(port, execution_path, 200)
                while status["status"] == "RUNNING" and monotonic() < deadline:
                    sleep(0.1)
                    status = http(port, execution_path, 200)
                require(status["status"] == "REMOTE_OUTCOME_UNKNOWN"
                        and status["executionId"] == execution_id
                        and status.get("errorCode") == "REMOTE_OUTCOME_UNKNOWN",
                        f"Expected lease-expiry REMOTE_OUTCOME_UNKNOWN, got {status}")
                result = http(port, execution_path + "/result", 200)
                require(result["status"] == status and result.get("resultPayload") is None,
                        f"Expected no typed payload for REMOTE_OUTCOME_UNKNOWN: {result}")

                # Exercise same-key replay and conflict, then keep the coordinator alive across an ordinary sweep.
                require(http(port, admission, 200, intent) == receipt,
                        "Same-key replay changed original receipt after unknown settlement")
                http(port, admission, 409, intent | {"inputPayload": intent["inputPayload"] | {"payload": '{"value":4}'}})
                sleep(2.2)
                require(http(port, execution_path, 200) == status,
                        "Subsequent ordinary sweep after replay/conflict changed REMOTE_OUTCOME_UNKNOWN status")
                require(http(port, execution_path + "/result", 200) == result,
                        "Subsequent ordinary sweep after replay/conflict changed REMOTE_OUTCOME_UNKNOWN result")

            terminal_row = _assert_unknown_terminal_durable(endpoint, prefix, tenant, execution_id, observed_in_flight)
            _assert_zero_dlq_publications(work, prefix, "post-intent-first", "post-intent-restart")
            require(worker_proxy.execute_posts == 1,
                    f"Post-intent recovery must not resend; saw {worker_proxy.execute_posts} execute POSTs")
            require(observations(calls) == expected_calls,
                    f"Post-intent recovery ran additional worker calls: {observations(calls)}")

            print(json.dumps({
                "case": "post-intent-coordinator-death",
                "language": language,
                "executionId": execution_id,
                "firstPid": first_pid,
                "secondPid": second_pid,
                "capabilityGetsTotal": worker_proxy.capability_gets,
                "executePostsTotal": worker_proxy.execute_posts,
                "terminalStatus": terminal_row["status"]["S"],
                "terminalVersion": int(terminal_row["version"]["N"]),
                "terminalAttempt": int(terminal_row["attempt"]["N"]),
                "businessCalls": len(expected_calls),
            }), flush=True)
            print(f"PACKAGED_POST_INTENT_DEATH_RECOVERY_PASS:{language}", flush=True)


def verify_stale_settlement_authority(work, jar, classes, java, node, language, generated, bundle, endpoint):
    """Keep coordinator alive, fence lease renewals until sweep marks unknown, then observe late settlement fail CAS."""
    prefix = f"native_stale_settlement_{language}"
    tables(endpoint, prefix)
    calls = work / f"{language}-stale-settlement-calls.jsonl"
    secret = secrets.token_urlsafe(32)
    observed_in_flight = {}
    lease_config = {
        "pipeline.orchestrator.lease-ms": "4000",
        "pipeline.orchestrator.worker.rest.request-timeout": "PT30S",
    }

    def verify_intent_before_worker_forward(raw_body):
        observed_in_flight.update(_verify_durable_in_flight_intent(endpoint, prefix, raw_body))

    with WorkerHttpsObserverProxy(
        cafile=work / "ca.pem",
        certfile=work / "worker.pem",
        keyfile=work / "worker.key",
        before_execute_forward=verify_intent_before_worker_forward,
    ) as worker_proxy:
        worker_port = _distinct_port(worker_proxy.port)
        worker_proxy.set_upstream_port(worker_port)
        launch = json.loads(command([
            "java", "-cp", os.pathsep.join((str(classes), java)), "NativeWorkerReleaseConformance",
            language, str(generated), str(bundle), "-", str(work / f"{language}-stale-settlement-release"),
            f"https://localhost:{worker_proxy.port}",
        ], work))
        target = launch["target"]
        target["sharedSecretRef"] = "env:TPF_NATIVE_WORKER_SECRET"
        launch.update(port=worker_port, calls=str(calls), secret=secret,
                      key=str(work / "worker.key"), cert=str(work / "worker.pem"))
        launch_file = write_launch(work, f"{language}-stale-settlement",
                                   {"target": target, "descriptor": launch["descriptor"]})
        tenant = target["tenantId"]
        pipeline = target["coordinatorContract"]["pipelineId"]
        release = target["releaseVersion"]
        admin = f"/tpf/admin/tenants/{tenant}/pipelines/{pipeline}/releases"
        control = f"/tpf/control-plane/tenants/{tenant}"
        admission = f"{control}/pipelines/{pipeline}/execution-admissions"
        client_key = "stale-settlement"
        intent = {
            "clientKey": client_key, "contractVersion": launch["command"]["contractVersion"],
            "releaseVersion": release, "inputShape": "UNI", "outputStreaming": False,
            "inputPayload": {"payloadTypeId": launch["inputType"],
                             "payloadEncoding": "tpf-polyglot-int32-json/1", "payload": '{"value":3}'},
        }

        with host(language, work, node, launch), DynamoBarrierProxy(endpoint) as dynamo_proxy:
            with coordinator(work, jar, launch_file, dynamo_proxy.endpoint, prefix, secret,
                             "stale-live", extra_config=lease_config) as (port, process):
                coordinator_pid = process.pid
                http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
                dynamo_proxy.arm_stale_settlement(prefix + "_execution")
                worker_proxy.arm_hold_execute_response()

                receipt = http(port, admission, 200, intent)
                execution_id = receipt["executionId"]
                context = {"executionId": execution_id, "traceId": execution_id}
                expected_calls = [{"handler": name, "value": value, "context": context}
                                  for name, value in (("normalize", 3), ("apply", 4), ("finish", 8))]

                require(worker_proxy.execute_response_captured.wait(timeout=20),
                        "Worker completion was not captured at the observer proxy")
                require(worker_proxy.callback_errors == [],
                        f"Pre-forward intent check failed: {worker_proxy.callback_errors}")
                captured_status, _, captured_body = worker_proxy.captured_execute_response
                require(captured_status == 200
                        and json.loads(captured_body.decode("utf-8")) == _expected_completed_wire(launch["outputType"]),
                        f"Unexpected captured worker completion: status={captured_status} body={captured_body!r}")
                require(observations(calls) == expected_calls,
                        f"Expected 3 completed worker calls, got {observations(calls)}")

                require(dynamo_proxy.renewal_held.wait(timeout=10),
                        "Lease renewal did not fire while worker completion was held")

                # Wait for the ordinary sweep on the live coordinator to mark the expired lease unknown.
                deadline = monotonic() + 20
                row = _consistent_execution(endpoint, prefix + "_execution", tenant, execution_id)
                while (row is None or row["status"]["S"] == "RUNNING") and monotonic() < deadline:
                    sleep(0.1)
                    row = _consistent_execution(endpoint, prefix + "_execution", tenant, execution_id)
                require(row is not None and row["status"]["S"] == "REMOTE_OUTCOME_UNKNOWN"
                        and int(row["version"]["N"]) == 2,
                        f"Ordinary sweep did not commit REMOTE_OUTCOME_UNKNOWN on expired lease: {row}")
                require(dynamo_proxy.late_settlement_observations == [],
                        "Observed late settlement before worker completion was released")

                # Release worker completion FIRST so late settlement hits DynamoDB and fails its condition
                # before releasing the renewal gate (a renewal failure could otherwise cancel settlement).
                worker_proxy.release_execute_response()
                require(dynamo_proxy.late_settlement_rejected.wait(timeout=15),
                        "Late succeeded settlement did not reach DynamoDB and fail ConditionalCheckFailedException")
                held_renewals = dynamo_proxy.held_renewals
                confirmed_rejections = dynamo_proxy.confirmed_late_settlement_rejections
                require(confirmed_rejections >= 1, "Expected at least one confirmed ConditionalCheckFailedException")
                dynamo_proxy.release_held_renewals()

                execution_path = f"{control}/executions/{execution_id}"
                status = http(port, execution_path, 200)
                require(status["status"] == "REMOTE_OUTCOME_UNKNOWN"
                        and status["executionId"] == execution_id
                        and status.get("errorCode") == "REMOTE_OUTCOME_UNKNOWN",
                        f"Unexpected HTTP status after rejected late settlement: {status}")
                result = http(port, execution_path + "/result", 200)
                require(result["status"] == status and result.get("resultPayload") is None,
                        f"Expected no typed result after rejected late settlement: {result}")

                # Exercise same-key replay and conflict, then keep the coordinator alive across an ordinary sweep.
                require(http(port, admission, 200, intent) == receipt,
                        "Same-key replay changed original receipt after stale settlement")
                http(port, admission, 409, intent | {"inputPayload": intent["inputPayload"] | {"payload": '{"value":4}'}})
                sleep(2.2)
                require(http(port, execution_path, 200) == status,
                        "Subsequent ordinary sweep after replay/conflict changed stale-settlement terminal status")
                require(http(port, execution_path + "/result", 200) == result,
                        "Subsequent ordinary sweep after replay/conflict changed stale-settlement terminal result")

            terminal_row = _assert_unknown_terminal_durable(endpoint, prefix, tenant, execution_id, observed_in_flight)
            _assert_zero_dlq_publications(work, prefix, "stale-live")
            require(worker_proxy.execute_posts == 1,
                    f"Stale settlement must not resend; saw {worker_proxy.execute_posts} execute POSTs")
            require(observations(calls) == expected_calls,
                    f"Stale settlement ran additional worker calls: {observations(calls)}")

            print(json.dumps({
                "case": "stale-settlement-authority",
                "language": language,
                "executionId": execution_id,
                "coordinatorPid": coordinator_pid,
                "heldRenewals": held_renewals,
                "rejectedLateSettlements": confirmed_rejections,
                "capabilityGetsTotal": worker_proxy.capability_gets,
                "executePostsTotal": worker_proxy.execute_posts,
                "terminalStatus": terminal_row["status"]["S"],
                "terminalVersion": int(terminal_row["version"]["N"]),
                "terminalAttempt": int(terminal_row["attempt"]["N"]),
                "businessCalls": len(expected_calls),
            }), flush=True)
            print(f"PACKAGED_STALE_SETTLEMENT_PASS:{language}", flush=True)


def run(work, manifest, endpoint, runtime_version, maven, maven_args):
    address = urlsplit(endpoint)
    require(address.scheme == "http" and address.hostname in {"127.0.0.1", "localhost"}, "Expected local DynamoDB")
    pins = json.loads(manifest.read_bytes())
    compiler = pinned_classpath(manifest.parent, pins["compilerClasspath"])
    java = pinned_classpath(manifest.parent, pins["javaClasspath"])
    packages = {language: pinned_file(manifest.parent, pins[field], suffix)
                for language, field, suffix in (("node", "nodePackage", ".tgz"), ("python", "pythonWheel", ".whl"))}
    node = shutil.which("node")
    require(node is not None, "Node.js must be on PATH")
    compile_workers(work, compiler, json.loads((HERE / "vectors.json").read_bytes())["boundaryTypeIds"])
    certificates(work)
    classes = work / "classes"
    resources = work / "coordinator-resources"
    metadata = resources / "META-INF/pipeline"
    metadata.mkdir(parents=True)
    contract = work / "node/pipeline-contract.json"
    require(contract.read_bytes() == (work / "python/pipeline-contract.json").read_bytes(), "Contract differs by language")
    shutil.copyfile(contract, metadata / "pipeline-contract.json")
    command(["javac", "--release", "21", "-proc:none", "-cp", java, "-d", str(classes),
             str(HERE / "NativeWorkerReleaseConformance.java")], work)
    fixture = work / "coordinator"
    shutil.copytree(HERE / "coordinator", fixture, ignore=shutil.ignore_patterns("target"))
    command([maven, "-B", *maven_args, "-f", str(fixture / "pom.xml"), "package",
             f"-Dpipelineframework.runtime.version={runtime_version}", f"-Dproof.contract.resources={resources}"],
            work, timeout=180)
    jar = fixture / "target/quarkus-app/quarkus-run.jar"

    runtime_name = f"pipelineframework-{runtime_version}.jar"
    expected = next(pin["sha256"] for pin in pins["javaClasspath"] if Path(pin["path"]).name == runtime_name)
    embedded = list((jar.parent / "lib/main").glob(f"*.{runtime_name}"))
    require(len(embedded) == 1, "Expected one selected runtime JAR in the packaged coordinator")
    with embedded[0].open("rb") as artifact:
        require(file_digest(artifact, "sha256").hexdigest() == expected, "Packaged runtime differs from candidate")

    harness = PackagedHarness(
        http=http,
        tables=tables,
        coordinator=coordinator,
        consistent_execution=_consistent_execution,
        consistent_scan=_consistent_scan,
        verify_durable_in_flight_intent=_verify_durable_in_flight_intent,
        expected_completed_wire=_expected_completed_wire,
        assert_unknown_terminal_durable=_assert_unknown_terminal_durable,
        assert_zero_dlq_publications=_assert_zero_dlq_publications,
        distinct_port=_distinct_port,
    )
    native_dispatch_log = work / "native-generic-dispatches.log"
    java_control_ran = False

    for language in LANGUAGES:
        generated = work / language
        alternate = compile_alternate(work, compiler, language)
        bundle, _ = bundle_fixture(language, generated, alternate, packages[language], work, node)
        if not java_control_ran:
            verify_java_fallback_positive_control(harness, work, jar, classes, java, generated, bundle, endpoint)
            java_control_ran = True
        worker_port = local_port()
        launch = json.loads(command(["java", "-cp", os.pathsep.join((str(classes), java)), "NativeWorkerReleaseConformance",
                                     language, str(generated), str(bundle), "-", str(work / f"{language}-release"),
                                     f"https://localhost:{worker_port}"], work))
        target = launch["target"]
        target["sharedSecretRef"] = "env:TPF_NATIVE_WORKER_SECRET"
        secret = secrets.token_urlsafe(32)
        calls = work / f"{language}-calls.jsonl"
        launch.update(port=worker_port, calls=str(calls), secret=secret,
                      key=str(work / "worker.key"), cert=str(work / "worker.pem"))
        launch_file = write_launch(work, language, {"target": target, "descriptor": launch["descriptor"]})
        prefix = f"native_ingress_{language}"
        tables(endpoint, prefix)
        tenant = target["tenantId"]
        pipeline = target["coordinatorContract"]["pipelineId"]
        release = target["releaseVersion"]
        admin = f"/tpf/admin/tenants/{tenant}/pipelines/{pipeline}/releases"
        control = f"/tpf/control-plane/tenants/{tenant}"
        admission = f"{control}/pipelines/{pipeline}/execution-admissions"
        intent = {"clientKey": "packaged-ingress", "contractVersion": launch["command"]["contractVersion"],
                  "releaseVersion": release, "inputShape": "UNI", "outputStreaming": False,
                  "inputPayload": {"payloadTypeId": launch["inputType"],
                                   "payloadEncoding": "tpf-polyglot-int32-json/1", "payload": '{"value":3}'}}
        with host(language, work, node, launch):
            with coordinator(work, jar, launch_file, endpoint, prefix, secret, "first") as (port, process):
                http(port, admin + "/register", 200, {"releaseDescriptorPath": launch["descriptor"]})
                registered = http(port, f"{admin}/{release}", 200)
                http(port, admission, 401, intent | {"clientKey": "denied"}, token=None)
                http(port, admission + "/denied", 404)
                require(observations(calls) == [], "Unauthorized request reached business code")
                for suffix in ("_execution", "_key"):
                    rows = dynamo(endpoint, "scan", {"TableName": prefix + suffix, "ConsistentRead": True})
                    require(rows["Count"] == 0, "Unauthorized request wrote durable execution state")
                receipt = http(port, admission, 200, intent)
                execution = control + "/executions/" + receipt["executionId"]
                deadline = monotonic() + 30
                status = http(port, execution, 200)
                while status["status"] in {"QUEUED", "RUNNING", "WAIT_RETRY"} and monotonic() < deadline:
                    sleep(0.1)
                    status = http(port, execution, 200)
                require(status["status"] == "SUCCEEDED", f"Expected successful settlement: {status}")
                result = http(port, execution + "/result", 200)
                require(result["status"] == status and status["executionId"] == receipt["executionId"],
                        "Result changed the execution identity or status")
                require(result["resultPayload"] == {"payloadTypeId": launch["outputType"],
                        "payloadEncoding": "tpf-polyglot-int32-json/1", "payload": '{"value":5}'},
                        f"Unexpected typed result: {result}")
                first_pid = process.pid
                process.kill()
                require(process.wait(timeout=8) == -9, "Coordinator did not exit on SIGKILL")
        # Worker is offline during lookup and replay. No registration is repeated.
        with coordinator(work, jar, launch_file, endpoint, prefix, secret, "restart") as (port, process):
            require(http(port, f"{admin}/{release}", 200) == registered, "Release changed across restart")
            require(http(port, admission + "/packaged-ingress", 200) == receipt, "Retained receipt changed")
            require(http(port, admission, 200, intent) == receipt, "Replay did not return the original receipt")
            require(http(port, execution + "/result", 200) == result, "Retained result changed")
            http(port, admission, 409, intent | {"inputPayload": intent["inputPayload"] | {"payload": '{"value":4}'}})
            require(process.pid != first_pid, "Expected a fresh coordinator process")
        context = {"executionId": receipt["executionId"], "traceId": receipt["executionId"]}
        require(observations(calls) == [{"handler": name, "value": value, "context": context}
                for name, value in (("normalize", 3), ("apply", 4), ("finish", 8))], "Unexpected business calls or context")
        print(f"PACKAGED_HOSTED_INGRESS_PASS:{language}", flush=True)
        verify_admission_before_intent_recovery(work, jar, classes, java, node, language, generated, bundle, endpoint)
        verify_post_intent_coordinator_death(work, jar, classes, java, node, language, generated, bundle, endpoint)
        verify_stale_settlement_authority(work, jar, classes, java, node, language, generated, bundle, endpoint)
        run_worker_faults_and_duplicates(harness, work, jar, classes, java, node, language, generated, bundle, endpoint)
        run_isolation_and_disconnect_cases(
            harness, work, jar, classes, java, node, language, generated, bundle, endpoint, native_dispatch_log)
        run_ingress_matrix_and_portable_outcomes(
            harness, work, jar, classes, java, node, language, generated, alternate, bundle, endpoint, native_dispatch_log)
        require(generic_dispatch_count(native_dispatch_log) == 0,
                f"Unexpected Java fallback dispatch(es) during {language} native cases: {generic_dispatch_count(native_dispatch_log)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("endpoint")
    parser.add_argument("runtime_version")
    parser.add_argument("maven")
    parser.add_argument("maven_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="tpf-packaged-ingress-") as directory:
        run(Path(directory).resolve(), args.manifest.resolve(strict=True), args.endpoint,
            args.runtime_version, args.maven, args.maven_args)
