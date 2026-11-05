"""External resource faults over production hosts; importing this module performs no I/O.

Only an explicitly disposable, tagged, exact release stack can be mutated. Native
semantic records are read for assertions and are never edited. Boundary unit tests
exercise procedure logic; they do not establish live-provider conformance.
"""
from collections import deque
from contextlib import contextmanager
import base64
import hashlib
import importlib.util
from pathlib import Path
import json
import time
import uuid

SUPPORTED_SCENARIOS = {"6", "7", "14", "15", "16", "18", "19", "22", "missing-registration", "suspended-timeout",
                       "nonterminal-provider-loss", "suspended-continuation"}
CALLBACK_FAILURE_VARIANT = "INJECTED_CALLBACK_FAILURE"
SOURCES = {
    "stop": "https://docs.aws.amazon.com/cli/latest/reference/lambda/stop-durable-execution.html",
    "history": "https://docs.aws.amazon.com/cli/latest/reference/lambda/get-durable-execution-history.html",
    "mapping": "https://docs.aws.amazon.com/cli/latest/reference/lambda/update-event-source-mapping.html",
    "historical": "fault-tests/src/test/java/org/pipelineframework/awsproof/ProofFaultScenarioCatalog.java",
    "callbackFailure": "https://docs.aws.amazon.com/cli/latest/reference/lambda/send-durable-execution-callback-failure.html",
    "streamFailure": "https://docs.aws.amazon.com/lambda/latest/dg/services-dynamodb-errors.html",
}


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def value(row, key, kind="S"):
    return row.get(key, {}).get(kind)


class OwnedResources:
    """Validate stack identity, disposal authority, and exact resource membership."""
    def __init__(self, producer):
        self.p = producer
        control = producer.config.get("resourceFaults", {})
        require(control.get("disposable") is True and bool(control.get("owner")),
                "resource faults require explicit disposable stack ownership")
        self.manifest = json.loads(producer.outputs["DeploymentReleaseManifest"])
        stack = producer.aws.call("cloudformation", "describe-stacks", stack_name=producer.config["stack"])["Stacks"][0]
        require(stack["StackId"] == control.get("stackId") == self.manifest["stackId"],
                "resource fault stack identity differs from pinned release")
        tags = {entry["Key"]: entry["Value"] for entry in stack.get("Tags", [])}
        require(tags.get("tpf-conformance-disposable") == "true"
                and tags.get("tpf-conformance-owner") == control["owner"],
                "resource fault stack must carry matching disposable owner tags")
        resources = producer.aws.call("cloudformation", "list-stack-resources", stack_name=stack["StackId"])
        require(not resources.get("NextToken"), "resource ownership enumeration was truncated")
        self.resources = resources["StackResourceSummaries"]
        self.p.aws.capture("resource-fault-authority", {"stackId": stack["StackId"], "owner": control["owner"],
                           "sources": SOURCES, "resourceCount": len(self.resources)})

    def owns(self, physical_id, resource_type):
        require(any(row["PhysicalResourceId"] == physical_id and row["ResourceType"] == resource_type
                    for row in self.resources), "fault target is outside the owned disposable stack: " + physical_id)
        return physical_id

    def logical(self, logical_id, resource_type):
        matches = [row["PhysicalResourceId"] for row in self.resources
                   if row["LogicalResourceId"] == logical_id and row["ResourceType"] == resource_type]
        require(len(matches) == 1, "owned resource is missing or ambiguous: " + logical_id)
        return matches[0]

    def version(self, role, handler):
        target = self.owns(self.manifest["functionVersionArns"][role], "AWS::Lambda::Version")
        config = self.p.aws.call("lambda", "get-function-configuration", function_name=target)
        require(config.get("Environment", {}).get("Variables", {}).get("QUARKUS_LAMBDA_HANDLER") == handler,
                "resource procedure must invoke the production " + handler + " handler")
        return target

    def provider(self, arn):
        version = self.owns(self.manifest["functionVersionArns"]["durable"], "AWS::Lambda::Version")
        require(arn.startswith(version + "/durable-execution/"), "provider execution belongs to another release version")
        return arn


def item(producer, table, pk, sk):
    return producer.aws.call("dynamodb", "get-item", table_name=table, consistent_read=True,
                             key={"pk": {"S": pk}, "sk": {"S": sk}}).get("Item", {})


def registration(producer, session, generation=1):
    table = producer.outputs["CallbackBindingTableName"]
    return item(producer, table, session["tenant"] + "#" + session["executionId"],
                "REGISTRATION#" + f"{generation:020d}")


def bounded_history(producer, arn, max_pages=100):
    """Read complete native history within a finite bound; never return a partial oracle."""
    require(isinstance(max_pages, int) and not isinstance(max_pages, bool) and 1 <= max_pages <= 100,
            "history page bound must be between 1 and 100")
    events = []
    seen = set()
    marker = None
    for _ in range(max_pages):
        parameters = {"DurableExecutionArn": arn, "IncludeExecutionData": True, "MaxItems": 100}
        if marker is not None:
            parameters["Marker"] = marker
        page = producer.aws.call("lambda", "get-durable-execution-history",
                                 cli_input_json=parameters, no_paginate=True)
        require(isinstance(page, dict) and isinstance(page.get("Events"), list), "invalid native history page")
        events.extend(page["Events"])
        next_marker = page.get("NextMarker")
        if next_marker is None or next_marker == "":
            return {"Events": events}
        require(isinstance(next_marker, str) and next_marker.strip() and next_marker not in seen,
                "invalid or repeated native history marker")
        seen.add(next_marker)
        marker = next_marker
    raise NotImplementedError("native history page bound exhausted; complete callback state is unproved")


def parked(producer, session, owned):
    row = producer.poll(lambda: registration(producer, session), bool)
    require(value(row, "tenant_id") == session["tenant"] and value(row, "execution_id") == session["executionId"],
            "parked callback registration has a different semantic owner")
    require(value(row, "interaction_id") == session["first"]["interactionId"], "parked callback joined the wrong Await")
    arn = owned.provider(value(row, "provider_execution_arn"))
    require(arn == session["provider"]["DurableExecutionArn"], "registration belongs to another provider execution")
    history = producer.poll(lambda: bounded_history(producer, arn),
        lambda page: any(event.get("EventType") == "CallbackStarted" and event.get("Name") == "await-completion-0"
                         for event in page.get("Events", [])))
    require(not any(event.get("Name") == "await-completion-0" and event.get("EventType")
                    in {"CallbackSucceeded", "CallbackFailed", "CallbackTimedOut"} for event in history["Events"]),
            "fault did not reach an unresolved provider callback")
    producer.poll(lambda: producer.binding(session["first"]), bool)
    return row, arn


@contextmanager
def paused_mappings(producer, owned, roles):
    original = []
    attempted = []
    try:
        # Validate every target before changing any mapping.
        target = owned.version("awaitWakeup", "aws-durable-await-wakeup")
        sources = {"awaitState": producer.parameters["AwaitInteractionStreamArn"]}
        if "callbackBinding" in roles:
            table = owned.owns(producer.outputs["CallbackBindingTableName"], "AWS::DynamoDB::Table")
            sources["callbackBinding"] = producer.aws.call("dynamodb", "describe-table", table_name=table)["Table"]["LatestStreamArn"]
        for role in roles:
            require(role in sources, "resource fault mapping role is not an Await Stream boundary")
            uuid = owned.owns(owned.manifest["mappingIds"][role], "AWS::Lambda::EventSourceMapping")
            mapping = producer.aws.call("lambda", "get-event-source-mapping", uuid=uuid)
            require(mapping["State"] == "Enabled", "fault mapping must initially be enabled")
            require(mapping["FunctionArn"] == target and mapping["EventSourceArn"] == sources[role],
                    "fault mapping does not target its pinned production Await role and source")
            original.append((uuid, mapping))
        for uuid, mapping in original:
            attempted.append((uuid, mapping))
            producer.aws.call("lambda", "update-event-source-mapping", uuid=uuid, no_enabled=True)
            producer.poll(lambda: producer.aws.call("lambda", "get-event-source-mapping", uuid=uuid),
                          lambda current: current["State"] == "Disabled")
        producer.aws.capture("resource-fault-injected", {"disabledMappings": [uuid for uuid, _ in original]})
        yield
    finally:
        # Restore even after an uncertain update response or failure inside the procedure.
        failures = []
        for uuid, mapping in reversed(attempted):
            try:
                producer.aws.call("lambda", "update-event-source-mapping", uuid=uuid, enabled=True)
                restored = producer.poll(lambda: producer.aws.call("lambda", "get-event-source-mapping", uuid=uuid),
                                         lambda current: current["State"] == "Enabled")
                require(restored["FunctionArn"] == mapping["FunctionArn"]
                        and restored["EventSourceArn"] == mapping["EventSourceArn"], "restored mapping changed its target")
                producer.aws.capture("resource-fault-restored", {"uuid": uuid, "state": restored})
            except Exception as failure:
                failures.append({"uuid": uuid, "failure": str(failure)})
        if failures:
            producer.aws.capture("resource-fault-restore-failed", failures)
            raise RuntimeError("failed to restore owned stream mappings; see raw evidence")


def actual_registration_record(producer, owned, row, include_location=False):
    table = owned.owns(producer.outputs["CallbackBindingTableName"], "AWS::DynamoDB::Table")
    stream = producer.aws.call("dynamodb", "describe-table", table_name=table)["Table"]["LatestStreamArn"]
    description = producer.aws.call("dynamodbstreams", "describe-stream", stream_arn=stream)["StreamDescription"]
    if description.get("LastEvaluatedShardId"):
        raise NotImplementedError("registration stream shard pagination is not implemented")
    # Give each live shard one turn before revisiting an empty, still-open shard.
    pending = deque((shard["ShardId"], None) for shard in description["Shards"])
    for _ in range(100):
        if not pending:
            break
        shard_id, cursor = pending.popleft()
        if cursor is None:
            cursor = producer.aws.call("dynamodbstreams", "get-shard-iterator", stream_arn=stream,
                                      shard_id=shard_id, shard_iterator_type="TRIM_HORIZON")["ShardIterator"]
        page = producer.aws.call("dynamodbstreams", "get-records", shard_iterator=cursor, limit=100)
        for record in page["Records"]:
            data = record.get("dynamodb", {})
            if data.get("Keys") == {"pk": row["pk"], "sk": row["sk"]} and data.get("NewImage") == row:
                require(record.get("eventName") in {"INSERT", "MODIFY"}, "registration record is not an admission event")
                # GetRecords omits the ARN Lambda adds; preserve all provider event fields.
                record = dict(record, eventSourceARN=stream)
                producer.aws.capture("actual-registration-stream-record", record)
                return {"streamArn": stream, "shardId": shard_id, "record": record} if include_location else record
        cursor = page.get("NextShardIterator")
        # Empty pages may precede records; only a closed iterator ends this shard.
        if cursor:
            pending.append((shard_id, cursor))
    if pending:
        raise NotImplementedError("mechanical Stream read budget exhausted; actual fault record is unproved")
    raise NotImplementedError("actual retained registration Stream record is unavailable; no synthetic replay is allowed")


def assert_single_execution(producer, session, status):
    rows = producer.rows("ExecutionTable", session["tenant"])
    require(len(rows) == 1 and value(rows[0], "execution_id") == session["executionId"],
            "resource recovery duplicated or replaced the semantic execution")
    require(value(rows[0], "status") == status, "authoritative native execution did not reach " + status)
    require(value(rows[0], "pipeline_id") == producer.contract["pipelineId"]
            and value(rows[0], "contract_version") == producer.contract["contractVersion"]
            and value(rows[0], "release_version") == producer.parameters["ReleaseVersion"],
            "native recovery changed pipeline, contract, or release pins")
    return rows[0]


def assert_success(producer, session, arn, generation):
    producer.poll(lambda: producer.action("STATUS", session["tenant"], session["executionId"]),
                  lambda response: response.get("executionStatus") == "SUCCEEDED")
    result = json.loads(producer.action("RESULT", session["tenant"], session["executionId"])["resultJson"])
    require(result in ({"request": "second-approved"}, [{"request": "second-approved"}]), "wrong typed native final result")
    terminal = producer.poll(lambda: producer.aws.call("lambda", "get-durable-execution", durable_execution_arn=arn,
                                                     include_execution_data=True), lambda current: current.get("Status") == "SUCCEEDED")
    output = json.loads(terminal["Result"])
    pins = {"tenantId": session["tenant"], "executionId": session["executionId"], "generation": generation,
            "pipelineId": producer.contract["pipelineId"], "contractVersion": producer.contract["contractVersion"],
            "releaseVersion": producer.parameters["ReleaseVersion"]}
    require(all(output.get(key) == expected for key, expected in pins.items()), "provider output changed semantic identity or release pins")
    require(json.loads(output["resultJson"]) == result, "provider/native result divergence")
    interactions = producer.rows("AwaitInteractionTable", session["tenant"])
    require(len(interactions) == 2 and all(value(row, "status") == "COMPLETED" for row in interactions)
            and len({value(row, "interaction_id") for row in interactions}) == 2,
            "recovery duplicated, skipped, or failed to complete an Await")
    native = assert_single_execution(producer, session, "SUCCEEDED")
    producer.aws.capture("resource-fault-final-state", {"native": native, "provider": terminal, "result": result,
                                                       "interactions": interactions, "pins": pins})


def complete_remaining(producer, session, first_completed=False):
    if not first_completed:
        producer.complete(session["first"], "first-approved")
    second = producer.poll(lambda: producer.action("STATUS", session["tenant"], session["executionId"]),
        lambda response: bool(response.get("awaitCheckpoint")) and response["awaitCheckpoint"]["interactionId"]
                         != session["first"]["interactionId"])["awaitCheckpoint"]
    require("SecondApproval" in second["stepId"], "recovery selected the wrong second Await")
    producer.complete(second, "second-approved")


def delete_callback_rows(producer, owned, session, row):
    table = owned.owns(producer.outputs["CallbackBindingTableName"], "AWS::DynamoDB::Table")
    binding = producer.binding(session["first"])
    require(bool(binding), "missing-registration challenge requires a real admitted binding")
    for candidate, kind, expected_pk, expected_sk in [
        (row, "REGISTRATION", session["tenant"] + "#" + session["executionId"], "REGISTRATION#00000000000000000001"),
        (binding, "BINDING", session["tenant"] + "#" + session["first"]["interactionId"], "GEN#00000000000000000001")]:
        require(value(candidate, "record_type") == kind and value(candidate, "pk") == expected_pk
                and value(candidate, "sk") == expected_sk and value(candidate, "tenant_id") == session["tenant"]
                and value(candidate, "execution_id") == session["executionId"]
                and value(candidate, "provider_execution_arn") == value(row, "provider_execution_arn"),
                "deletion target is not the exact disposable candidate callback row")
    for candidate in (row, binding):
        producer.aws.capture("deleted-disposable-callback-row", candidate)
        producer.aws.call("dynamodb", "delete-item", table_name=table,
            key={"pk": candidate["pk"], "sk": candidate["sk"]},
            condition_expression="record_type = :kind AND provider_execution_arn = :arn AND provider_callback_id = :callback",
            expression_attribute_values={":kind": candidate["record_type"], ":arn": candidate["provider_execution_arn"],
                                         ":callback": candidate["provider_callback_id"]})
        require(not item(producer, table, value(candidate, "pk"), value(candidate, "sk")), "callback row deletion was not observed")


def stopped(producer, scenario, owned):
    session = producer.open_execution(scenario)
    row, arn = parked(producer, session, owned)
    if scenario == "missing-registration":
        delete_callback_rows(producer, owned, session, row)
    producer.aws.call("lambda", "stop-durable-execution", durable_execution_arn=arn)
    stopped_state = producer.poll(lambda: producer.aws.call("lambda", "get-durable-execution", durable_execution_arn=arn),
                                  lambda state: state.get("Status") == "STOPPED")
    require(producer.action("STATUS", session["tenant"], session["executionId"])["executionStatus"] == "WAITING_EXTERNAL",
            "provider stop changed native semantics")
    target = owned.version("reconciliation", "aws-durable-reconcile")
    # This is the actual terminal provider ARN, consumed by the normal production event adapter.
    response = producer.aws.invoke(target, {"detail": {"durableExecutionArn": arn}})
    producer.aws.capture("stopped-provider-reconciliation", {"stopped": stopped_state, "handlerResponse": response})
    replacement = producer.poll(lambda: registration(producer, session, 2), bool)
    require(value(replacement, "generation", "N") == "2" and value(replacement, "interaction_id") == session["first"]["interactionId"],
            "production reconciliation failed to reconstruct the same parked Await")
    replacement_arn = owned.provider(value(replacement, "provider_execution_arn"))
    require(replacement_arn != arn and value(replacement, "execution_id") == session["executionId"],
            "replacement did not preserve the semantic execution")
    complete_remaining(producer, session)
    assert_success(producer, session, replacement_arn, 2)
    require(producer.aws.call("lambda", "get-durable-execution", durable_execution_arn=arn)["Status"] == "STOPPED",
            "old stopped provider unexpectedly resumed")


def pending_before_deadline(producer, session, minimum_seconds=0):
    native = assert_single_execution(producer, session, "WAITING_EXTERNAL")
    interactions = producer.rows("AwaitInteractionTable", session["tenant"])
    matches = [entry for entry in interactions if value(entry, "interaction_id") == session["first"]["interactionId"]]
    require(len(matches) == 1 and value(matches[0], "status") in {"WAITING", "DISPATCHING", "DISPATCHED", "COMPLETION_OBSERVED"},
            "provider fault requires the same pending native Await, never a completed or timed-out interaction")
    deadline = int(value(matches[0], "deadline_epoch_ms", "N"))
    require(deadline / 1000 - time.time() > minimum_seconds, "provider fault must precede the actual native Await deadline")
    return {"native": native, "interaction": matches[0], "deadlineEpochMs": deadline}


def closed_callback(producer, owned):
    """Recovery diagnostic only; original scenario 14 CallbackTimedOut remains unimplemented."""
    session = producer.open_execution("14")
    row, arn = parked(producer, session, owned)
    callback_id = value(row, "provider_callback_id")
    require(isinstance(callback_id, str) and callback_id.strip()
            and value(row, "record_type") == "REGISTRATION" and value(row, "generation", "N") == "1"
            and all(value(row, key) == expected for key, expected in {
                "tenant_id": session["tenant"], "execution_id": session["executionId"],
                "interaction_id": session["first"]["interactionId"], "pipeline_id": producer.contract["pipelineId"],
                "contract_version": producer.contract["contractVersion"], "release_version": producer.parameters["ReleaseVersion"]}.items()),
            "provider fault requires an authentic first-generation callback registration")
    binding = producer.binding(session["first"])
    require(value(binding, "provider_callback_id") == callback_id and value(binding, "provider_execution_arn") == arn
            and value(binding, "generation", "N") == "1" and value(binding, "interaction_id") == session["first"]["interactionId"],
            "provider fault cannot target a callback outside the admitted first binding")
    require(not registration(producer, session, 2), "replacement existed before the requested callback failure")
    history = bounded_history(producer, arn)
    starts = [event for event in history["Events"] if event.get("EventType") == "CallbackStarted"
              and event.get("Name") == "await-completion-0"
              and event.get("CallbackStartedDetails", {}).get("CallbackId") == callback_id]
    require(len(starts) == 1 and bool(starts[0].get("Id")), "registered callback is not the actual parked provider operation")
    operation_id = starts[0]["Id"]
    require(not any(event.get("Id") == operation_id and event.get("EventType")
                    in {"CallbackSucceeded", "CallbackFailed", "CallbackTimedOut"} for event in history["Events"]),
            "provider callback closed before the requested fault")
    require(producer.aws.call("lambda", "get-durable-execution", durable_execution_arn=arn).get("Status") == "RUNNING",
            "callback fault requires a currently running parked provider execution")
    before = pending_before_deadline(producer, session, minimum_seconds=60)
    error = {"ErrorType": "ConformanceProviderCallbackClosed", "ErrorMessage": "fixture external callback failure: " + session["tenant"]}
    producer.aws.capture("closed-callback-fault-before", {"id": "14", "registration": row,
                         "providerOperationId": operation_id, "native": before,
                         "faultVariant": CALLBACK_FAILURE_VARIANT, "elapsedExpiry": False})
    producer.aws.call("lambda", "send-durable-execution-callback-failure", callback_id=callback_id, error=error)
    failed = producer.poll(lambda: producer.aws.call("lambda", "get-durable-execution", durable_execution_arn=arn),
                           lambda state: state.get("Status") == "FAILED", timeout=60)
    pending_before_deadline(producer, session)
    closed_history = bounded_history(producer, arn)
    failures = [event for event in closed_history["Events"] if event.get("Id") == operation_id
                and event.get("EventType") == "CallbackFailed"]
    retained_error = failures[0].get("CallbackFailedDetails", {}).get("Error", {}) if len(failures) == 1 else {}
    require(len(failures) == 1 and retained_error.get("Truncated") is not True
            and isinstance(retained_error.get("Payload"), dict)
            and all(retained_error["Payload"].get(key) == value for key, value in error.items()),
            "provider failure did not retain the exact injected callback cause")
    target = owned.version("reconciliation", "aws-durable-reconcile")
    response = producer.aws.invoke(target, {"detail": {"durableExecutionArn": arn, "status": "FAILED"}})
    replacement = producer.poll(lambda: registration(producer, session, 2), bool, timeout=120)
    require(value(replacement, "generation", "N") == "2"
            and value(replacement, "interaction_id") == session["first"]["interactionId"]
            and value(replacement, "execution_id") == session["executionId"]
            and value(replacement, "provider_callback_id") != callback_id,
            "closed callback recovery did not reconstruct the same native Await in generation 2")
    replacement_arn = owned.provider(value(replacement, "provider_execution_arn"))
    require(replacement_arn != arn, "closed callback reused its failed provider execution")
    pending_before_deadline(producer, session)
    producer.aws.capture("closed-callback-reconciliation", {"oldProvider": failed, "failureEvent": failures[0],
                         "handlerResponse": response, "replacement": replacement,
                         "faultVariant": CALLBACK_FAILURE_VARIANT, "elapsedExpiry": False})
    complete_remaining(producer, session)
    assert_success(producer, session, replacement_arn, 2)
    require(producer.aws.call("lambda", "get-durable-execution", durable_execution_arn=arn).get("Status") == "FAILED",
            "old failed callback execution unexpectedly resumed")
    producer.aws.capture("closed-callback-diagnostic-outcome", {"id": "14", "diagnosticStatus": "PASSED",
                         "faultVariant": CALLBACK_FAILURE_VARIANT, "elapsedExpiry": False,
                         "originalScenarioStatus": "NOT_IMPLEMENTED"})


def retained_stream_failure(document, location, target):
    """SQS failure destinations retain metadata, not the DynamoDB payload itself."""
    context = document.get("requestContext", {})
    response = document.get("responseContext", {})
    batch = document.get("DDBStreamBatchInfo", {})
    if not all(isinstance(part, dict) for part in (context, response, batch)):
        return False
    sequence = location["record"].get("dynamodb", {}).get("SequenceNumber", "")
    start, end = batch.get("startSequenceNumber", ""), batch.get("endSequenceNumber", "")
    return (context.get("condition") == "RetryAttemptsExhausted"
        and context.get("functionArn") in {target, target.rsplit(":", 1)[0]}
        and str(response.get("executedVersion", "")) == target.rsplit(":", 1)[1]
        and batch.get("streamArn") == location["streamArn"] and batch.get("shardId") == location["shardId"]
        and all(isinstance(value, str) and value.isdecimal() for value in (start, sequence, end))
        and int(start) <= int(sequence) <= int(end))


def retained_decoder_failure(producer, owned):
    """Scenario 18: current decoder rejects REGISTRATION; BINDING is acknowledged."""
    table = owned.owns(producer.outputs["CallbackBindingTableName"], "AWS::DynamoDB::Table")
    require(owned.logical("CallbackBindingTable", "AWS::DynamoDB::Table") == table,
            "malformed-record fault must target the mechanically owned callback table")
    queue = owned.owns(owned.manifest["queueUrls"]["streamFailure"], "AWS::SQS::Queue")
    target = owned.version("awaitWakeup", "aws-durable-await-wakeup")
    stream = producer.aws.call("dynamodb", "describe-table", table_name=table)["Table"]["LatestStreamArn"]
    queue_arn = producer.aws.call("sqs", "get-queue-attributes", queue_url=queue, attribute_names=["QueueArn"])["Attributes"]["QueueArn"]
    mapping_id = owned.owns(owned.manifest["mappingIds"]["callbackBinding"], "AWS::Lambda::EventSourceMapping")
    mapping = producer.aws.call("lambda", "get-event-source-mapping", uuid=mapping_id)
    require(mapping.get("State") == "Enabled" and mapping.get("FunctionArn") == target
            and mapping.get("EventSourceArn") == stream
            and mapping.get("DestinationConfig", {}).get("OnFailure", {}).get("Destination") == queue_arn
            and "ReportBatchItemFailures" in mapping.get("FunctionResponseTypes", [])
            and isinstance(mapping.get("MaximumRetryAttempts"), int) and not isinstance(mapping["MaximumRetryAttempts"], bool)
            and 0 <= mapping["MaximumRetryAttempts"] <= 10,
            "real owned Stream decoder and bounded retained failure destination are required")
    owner = producer.config["resourceFaults"]["owner"]
    fault_id = uuid.uuid4().hex
    fault_row = {"pk": {"S": "CONFORMANCE_FAULT#18#" + fault_id}, "sk": {"S": "REGISTRATION#FAULT#" + fault_id},
                 "record_type": {"S": "REGISTRATION"}, "conformance_fault_owner": {"S": owner}, "conformance_fault_id": {"S": fault_id}}
    attempted = False
    try:
        attempted = True  # Cleanup also covers a committed PutItem whose response is lost.
        producer.aws.call("dynamodb", "put-item", table_name=table, item=fault_row,
                          condition_expression="attribute_not_exists(pk) AND attribute_not_exists(sk)")
        require(item(producer, table, value(fault_row, "pk"), value(fault_row, "sk")) == fault_row,
                "mechanical fault row was not retained exactly")
        location = actual_registration_record(producer, owned, fault_row, include_location=True)
        sequence = location["record"].get("dynamodb", {}).get("SequenceNumber")
        require(isinstance(sequence, str) and sequence.isdecimal(), "actual Stream fault sequence is unavailable")
        decoded = producer.aws.invoke(target, {"Records": [location["record"]]})
        require(decoded.get("batchItemFailures") == [{"itemIdentifier": sequence}],
                "actual production decoder did not reject the isolated mechanical fault record")
        producer.aws.capture("retained-stream-fault-adaptation", {"id": "18", "historicalRecordType": "BINDING",
                             "currentFaultRecordType": "REGISTRATION", "reason": "production acknowledges BINDING; malformed REGISTRATION exercises its decoder",
                             "row": fault_row, "location": location, "handlerResponse": decoded, "mapping": mapping})
        observed = None
        for _ in range(120):
            page = producer.aws.call("sqs", "receive-message", queue_url=queue, max_number_of_messages=10,
                                     wait_time_seconds=1, visibility_timeout=0)
            for message in page.get("Messages", []):
                try:
                    document = json.loads(message["Body"])
                except (KeyError, TypeError, ValueError):
                    continue
                if isinstance(document, dict) and retained_stream_failure(document, location, target):
                    observed = message
                    break
            if observed is not None:
                break
        require(observed is not None, "no retained failure record correlates to the actual fault Stream sequence and pinned handler")
        producer.aws.capture("retained-stream-failure-record", {"message": observed, "faultLocation": location,
                             "queue": queue, "payloadRetainedSeparately": True, "queueRecordDeleted": False})
    finally:
        if attempted:
            remaining = item(producer, table, value(fault_row, "pk"), value(fault_row, "sk"))
            require(not remaining or remaining == fault_row, "mechanical fault cleanup refuses a changed or foreign row")
            if remaining:
                producer.aws.call("dynamodb", "delete-item", table_name=table, key={"pk": fault_row["pk"], "sk": fault_row["sk"]},
                    condition_expression="record_type = :kind AND conformance_fault_owner = :owner AND conformance_fault_id = :fault",
                    expression_attribute_values={":kind": {"S": "REGISTRATION"}, ":owner": {"S": owner}, ":fault": {"S": fault_id}})
            require(not item(producer, table, value(fault_row, "pk"), value(fault_row, "sk")), "isolated mechanical fault row cleanup failed")
            producer.aws.capture("retained-stream-fault-cleanup", {"faultId": fault_id, "mechanicalRowRemoved": True,
                                 "nativeSemanticRecordsEdited": False})


def missed_stream(producer, owned):
    session = producer.open_execution("19")
    row, arn = parked(producer, session, owned)
    record = actual_registration_record(producer, owned, row)
    wakeup = owned.version("awaitWakeup", "aws-durable-await-wakeup")
    table = owned.owns(producer.outputs["CallbackBindingTableName"], "AWS::DynamoDB::Table")
    delivery_pk = session["tenant"] + "#" + session["first"]["interactionId"]
    with paused_mappings(producer, owned, ["awaitState", "callbackBinding"]):
        producer.complete(session["first"], "first-approved")
        producer.poll(lambda: producer.rows("AwaitInteractionTable", session["tenant"]),
                      lambda rows: any(value(candidate, "interaction_id") == session["first"]["interactionId"]
                                       and value(candidate, "status") == "COMPLETED" for candidate in rows))
        require(not item(producer, table, delivery_pk, "DELIVERED#1"),
                "missed Stream fault did not suppress callback delivery")
        # Registration Stream replay re-reads the authoritative semantic checkpoint.
        # It repairs the missing consequence without replaying the completion record.
        response = producer.aws.invoke(wakeup, {"Records": [record]})
        require(not response.get("batchItemFailures"), "production registration reconciliation rejected the real record")
        delivery = producer.poll(lambda: item(producer, table, delivery_pk, "DELIVERED#1"), bool)
        require(value(delivery, "provider_callback_id") == value(row, "provider_callback_id")
                and value(delivery, "provider_outcome") in {"CALLBACK_ACCEPTED", "PROVIDER_STATE_SUCCEEDED"},
                "reconciliation did not retain successful delivery of the actual parked callback")
        producer.aws.capture("missed-stream-reconciliation", {"registrationEventId": record.get("eventID"),
                             "missedInteractionId": session["first"]["interactionId"], "handlerResponse": response, "delivery": delivery})
    complete_remaining(producer, session, first_completed=True)
    assert_success(producer, session, arn, 1)


def timeout(producer, scenario, owned):
    rule = owned.logical("DueWorkRule", "AWS::Events::Rule")
    schedule = producer.aws.call("events", "describe-rule", name=rule)
    require(schedule["State"] == "ENABLED" and schedule["ScheduleExpression"] == "rate(1 minute)",
            "real production due-work schedule is absent")
    due_work = owned.version("dueWork", "aws-durable-due-work")
    owned.version("executionWakeup", "aws-durable-execution-wakeup")
    queue = owned.owns(producer.outputs["DueWorkQueueUrl"], "AWS::SQS::Queue")
    queue_arn = producer.aws.call("sqs", "get-queue-attributes", queue_url=queue,
                                  attribute_names=["QueueArn"])["Attributes"]["QueueArn"]
    targets = producer.aws.call("events", "list-targets-by-rule", rule=rule)
    require(not targets.get("NextToken") and len(targets["Targets"]) == 1 and targets["Targets"][0]["Arn"] == queue_arn,
            "real due-work schedule does not target the owned production queue")
    mapping_id = owned.owns(owned.manifest["mappingIds"]["dueWork"], "AWS::Lambda::EventSourceMapping")
    mapping = producer.aws.call("lambda", "get-event-source-mapping", uuid=mapping_id)
    require(mapping["State"] == "Enabled" and mapping["FunctionArn"] == due_work and mapping["EventSourceArn"] == queue_arn,
            "production due-work queue is not wired to its pinned host")
    session = producer.open_execution(scenario)
    _, arn = parked(producer, session, owned)
    interactions = producer.rows("AwaitInteractionTable", session["tenant"])
    first = [row for row in interactions if value(row, "interaction_id") == session["first"]["interactionId"]]
    require(len(first) == 1 and value(first[0], "status") in {"WAITING", "DISPATCHING", "DISPATCHED", "COMPLETION_OBSERVED"}, "deadline challenge did not start with a pending native Await")
    deadline = int(value(first[0], "deadline_epoch_ms", "N"))
    remaining = deadline / 1000 - time.time()
    require(0 < remaining <= 600, "compiler-generated Await deadline must be future and bounded for this procedure")
    producer.aws.capture("real-await-deadline", {"interaction": first[0], "schedule": schedule, "targets": targets, "mapping": mapping,
                                               "providerArn": arn, "callerSweep": False})
    # No completion, synthetic clock, row edits, direct sweep, or provider timeout is injected.
    # AWS scheduled due-work drives TPF's actual deadline while its driver callback is parked.
    producer.poll(lambda: producer.rows("AwaitInteractionTable", session["tenant"]),
                  lambda rows: any(value(row, "interaction_id") == session["first"]["interactionId"]
                                   and value(row, "status") == "TIMED_OUT" for row in rows), timeout=remaining + 180)
    require(time.time() * 1000 >= deadline, "native timeout was admitted before its actual deadline")
    producer.poll(lambda: producer.action("STATUS", session["tenant"], session["executionId"]),
                  lambda response: response.get("executionStatus") == "FAILED")
    terminal = producer.poll(lambda: producer.aws.call("lambda", "get-durable-execution", durable_execution_arn=arn,
                                                     include_execution_data=True), lambda state: state.get("Status") == "FAILED")
    require(bool(terminal.get("Error")), "provider deadline failure has no authoritative error")
    native = assert_single_execution(producer, session, "FAILED")
    require(value(native, "error_code") == "AWAIT_TIMEOUT"
            and value(native, "error_message") == "Await interaction timed out: " + session["first"]["interactionId"],
            "native parent failure is not the admitted Await timeout consequence")
    final_interactions = producer.rows("AwaitInteractionTable", session["tenant"])
    require(len(final_interactions) == 1 and value(final_interactions[0], "status") == "TIMED_OUT",
            "timeout admitted a completion or advanced the second Await")
    producer.aws.capture("resource-fault-final-state", {"native": native, "provider": terminal,
                                                       "interactions": final_interactions, "deadlineEpochMs": deadline})



def native_rows(producer, parameter, tenant):
    """One strong native page; incomplete observations cannot establish the oracle."""
    page = producer.aws.call("dynamodb", "query", table_name=producer.parameters[parameter],
        consistent_read=True, no_paginate=True, limit=100,
        key_condition_expression="tenant_id = :tenant", expression_attribute_values={":tenant": {"S": tenant}})
    if page.get("LastEvaluatedKey"):
        raise NotImplementedError("native procedure observation exceeds its one-page bound")
    return page["Items"]


@contextmanager
def paused_worker(producer, owned, role, transition_handler="aws-durable-transition-worker"):
    """Pause only an exact manifested Lambda worker; COMPUTE has no such control."""
    require(role in {"work", "transition"}, "unsupported worker fault boundary")
    if role == "transition" and producer.config["placement"] == "COMPUTE":
        raise NotImplementedError("COMPUTE transition consumer has no owned pause/resume control")
    target = owned.version(role, "aws-durable-work-item" if role == "work" else transition_handler)
    queue = owned.owns(owned.manifest["queueUrls"]["work" if role == "work" else "transitionRequest"], "AWS::SQS::Queue")
    arn = producer.aws.call("sqs", "get-queue-attributes", queue_url=queue,
                            attribute_names=["QueueArn"])["Attributes"]["QueueArn"]
    mapping_id = owned.owns(owned.manifest["mappingIds"][role], "AWS::Lambda::EventSourceMapping")
    original = producer.aws.call("lambda", "get-event-source-mapping", uuid=mapping_id)
    require(original["State"] == "Enabled" and original["FunctionArn"] == target
            and original["EventSourceArn"] == arn, "worker mapping differs from exact production release")
    try:
        producer.aws.call("lambda", "update-event-source-mapping", uuid=mapping_id, no_enabled=True)
        producer.poll(lambda: producer.aws.call("lambda", "get-event-source-mapping", uuid=mapping_id),
                      lambda state: state["State"] == "Disabled")
        producer.aws.capture("worker-delivery-paused", {"role": role, "mapping": original})
        yield
    finally:
        producer.aws.call("lambda", "update-event-source-mapping", uuid=mapping_id, enabled=True)
        restored = producer.poll(lambda: producer.aws.call("lambda", "get-event-source-mapping", uuid=mapping_id),
                                 lambda state: state["State"] == "Enabled")
        require(restored["FunctionArn"] == target and restored["EventSourceArn"] == arn,
                "restored worker mapping changed its production target")
        producer.aws.capture("worker-delivery-restored", {"role": role, "mapping": restored})


def start_native(producer, scenario, request):
    tenant = "conformance-" + scenario + "-" + str(producer.random.getrandbits(64))
    producer.release(tenant)
    start = producer.aws.invoke(producer.outputs["IngressFunctionArn"], {"tenantId": tenant,
        "idempotencyKey": tenant + ":start", "pipelineId": producer.contract["pipelineId"],
        "contractVersion": producer.contract["contractVersion"], "releaseVersion": producer.parameters["ReleaseVersion"],
        "inputJson": json.dumps({"request": request}), "generation": 1})
    rows = producer.poll(lambda: native_rows(producer, "ExecutionTable", tenant), lambda rows: len(rows) == 1)
    session = {"tenant": tenant, "executionId": value(rows[0], "execution_id"), "start": start}
    require(bool(session["executionId"]), "native admission has no execution identity")
    return session


def nonterminal_loss(producer, owned):
    with paused_worker(producer, owned, "work"):
        session = start_native(producer, "nonterminal-provider-loss", "initial-approval")
        native = assert_single_execution(producer, session, "QUEUED")
        require(not native_rows(producer, "AwaitInteractionTable", session["tenant"]),
                "provider-loss cut must precede the first native Await")
        provider = producer.poll(lambda: producer.provider(session["start"]["durableExecutionName"]), bool)
        arn = owned.provider(provider["DurableExecutionArn"])
        recovery = producer.poll(lambda: item(producer, producer.outputs["CallbackBindingTableName"],
            session["tenant"] + "#" + session["executionId"], "RECOVERY#00000000000000000001"), bool)
        require(value(recovery, "record_type") == "RECOVERY" and value(recovery, "provider_execution_arn") == arn
                and all(value(recovery, name) == expected for name, expected in {
                    "tenant_id": session["tenant"], "execution_id": session["executionId"],
                    "pipeline_id": producer.contract["pipelineId"], "contract_version": producer.contract["contractVersion"],
                    "release_version": producer.parameters["ReleaseVersion"]}.items()), "nonterminal recovery locator differs")
        producer.aws.call("lambda", "stop-durable-execution", durable_execution_arn=arn)
        producer.poll(lambda: producer.aws.call("lambda", "get-durable-execution", durable_execution_arn=arn),
                      lambda current: current.get("Status") == "STOPPED")
        assert_single_execution(producer, session, "QUEUED")
        target = owned.version("reconciliation", "aws-durable-reconcile")
        producer.aws.invoke(target, {"detail": {"durableExecutionArn": arn}})
        successor = producer.poll(lambda: item(producer, producer.outputs["CallbackBindingTableName"],
            session["tenant"] + "#" + session["executionId"], "RECOVERY#00000000000000000002"), bool)
        replacement = owned.provider(value(successor, "provider_execution_arn"))
        require(value(successor, "record_type") == "RECOVERY" and value(successor, "generation", "N") == "2"
                and replacement != arn and all(value(successor, key) == value(recovery, key) for key in
                    ("tenant_id", "execution_id", "pipeline_id", "contract_version", "release_version")),
                "nonterminal successor changed the native identity or release")
        require(not native_rows(producer, "AwaitInteractionTable", session["tenant"]), "paused work advanced native Await")
        producer.aws.capture("nonterminal-provider-loss-cut", {"native": native, "recovery": recovery,
                             "successor": successor, "oldProviderArn": arn})
    session["first"] = producer.poll(lambda: producer.action("STATUS", session["tenant"], session["executionId"]),
        lambda response: response.get("executionStatus") == "WAITING_EXTERNAL" and bool(response.get("awaitCheckpoint")))["awaitCheckpoint"]
    require("FirstApproval" in session["first"]["stepId"], "replacement skipped first generated Await")
    complete_remaining(producer, session)
    assert_success(producer, session, replacement, 2)
    require(producer.aws.call("lambda", "get-durable-execution", durable_execution_arn=arn)["Status"] == "STOPPED",
            "old stopped provider resumed")


def suspended_continuation(producer, owned):
    if producer.config["placement"] == "COMPUTE":
        raise NotImplementedError("COMPUTE transition consumer has no owned pause/resume control")
    if not producer.config.get("itemDeployment"):
        raise NotImplementedError("suspended item continuation requires the compiler-generated item deployment")
    p = type(producer)(producer.config | producer.config["itemDeployment"] | {"fixtureRole": "item"},
                       producer.bundle, producer.seed, producer.aws)
    p.preflight()
    item_owned = OwnedResources(p)
    schedule = scheduled_due_work(p, item_owned)
    session = start_native(p, "suspended-continuation", "3")
    interactions = p.poll(lambda: native_rows(p, "AwaitInteractionTable", session["tenant"]), lambda rows: len(rows) == 3)
    interaction_ids = {value(record, "interaction_id") for record in interactions}
    unit_ids = {value(record, "unit_id") for record in interactions}
    require(len(interaction_ids) == 3 and None not in interaction_ids and len(unit_ids) == 1 and None not in unit_ids
            and {value(record, "item_index", "N") for record in interactions} == {"0", "1", "2"}
            and all(value(record, "execution_id") == session["executionId"] for record in interactions),
            "generated item Await identities differ")
    session["first"] = p.poll(lambda: p.action("STATUS", session["tenant"], session["executionId"]),
        lambda current: current.get("executionStatus") == "WAITING_EXTERNAL" and bool(current.get("awaitCheckpoint")))["awaitCheckpoint"]
    require(session["first"]["interactionId"] in {value(record, "interaction_id") for record in interactions},
            "provider parked at a different native item Await")
    session["provider"] = p.poll(lambda: p.provider(session["start"]["durableExecutionName"]), bool)
    _, arn = parked(p, session, item_owned)
    unit_id = next(iter(unit_ids))
    component = lambda value: base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")
    ack_pk = "RELEASE#" + component(session["tenant"]) + "#" + component(session["executionId"])
    ack_sk = "UNIT#" + component(unit_id)
    with paused_worker(p, item_owned, "transition"), paused_mappings(p, item_owned, ["awaitState", "callbackBinding"]):
        for record in interactions:
            p.complete({"tenantId": session["tenant"], "interactionId": value(record, "interaction_id"),
                        "correlationId": value(record, "correlation_id")}, "approved-" + value(record, "item_index", "N"))
        pending = p.poll(lambda: native_rows(p, "AwaitInteractionTable", session["tenant"]),
            lambda rows: len(rows) == 3 and all(value(row, "status") == "COMPLETED"
                and row.get("continuation_completed", {}).get("BOOL") is not True
                and int(value(row, "continuation_attempt", "N") or "0") >= 2 for row in rows))
        require({value(record, "interaction_id") for record in pending} == interaction_ids
                and all(value(record, "execution_id") == session["executionId"] and value(record, "unit_id") == unit_id
                        for record in pending), "pending continuation work changed its native item identities")
        require(p.action("STATUS", session["tenant"], session["executionId"])["executionStatus"] == "WAITING_EXTERNAL",
                "parent released before its item continuations")
        require(not item(p, p.parameters["AwaitCompletionLifecycleTable"], ack_pk, ack_sk),
                "parent release acknowledgement exists before item continuation convergence")
        history = bounded_history(p, arn)
        require(not any(event.get("EventType") in {"CallbackSucceeded", "CallbackFailed", "CallbackTimedOut"}
                        for event in history["Events"]), "Durable was not parked at pending item continuation")
        p.aws.capture("suspended-item-continuation-cut", {"interactions": pending, "providerArn": arn,
                      "schedule": schedule, "callerSweep": False, "downstreamBusinessSuffix": False})
    # Only production workers, due-work and Streams progress the retained item work.
    final = p.poll(lambda: native_rows(p, "AwaitInteractionTable", session["tenant"]),
        lambda rows: len(rows) == 3 and all(row.get("continuation_completed", {}).get("BOOL") is True for row in rows))
    require({value(record, "interaction_id") for record in final} == interaction_ids
            and all(value(record, "execution_id") == session["executionId"] and value(record, "unit_id") == unit_id
                    for record in final), "settled continuation work changed its native item identities")
    units = native_rows(p, "AwaitUnitTable", session["tenant"])
    require(len(units) == 1 and value(units[0], "unit_id") == unit_id
            and value(units[0], "completed_item_count", "N") == "3"
            and value(units[0], "expected_item_count", "N") == "3"
            and units[0].get("dispatch_complete", {}).get("BOOL") is True
            and {"continuation:0", "continuation:1", "continuation:2"}.issubset(set(units[0].get("completed_item_keys", {}).get("SS", []))),
            "native item continuation counts/facts did not converge")
    executions = p.poll(lambda: native_rows(p, "ExecutionTable", session["tenant"]),
        lambda rows: len(rows) == 4 and all(value(row, "status") == "SUCCEEDED" for row in rows))
    require(all(all(value(record, key) == expected for key, expected in {"pipeline_id": p.contract["pipelineId"],
        "contract_version": p.contract["contractVersion"], "release_version": p.parameters["ReleaseVersion"]}.items())
        for record in executions), "native item consequences changed release pins")
    parents = [row for row in executions if value(row, "execution_id") == session["executionId"]]
    require(len(parents) == 1, "item continuation lost its native parent")
    expected_keys = {value(parents[0], "execution_key") + ":await-item:" + unit_id + ":" + str(index) for index in range(3)}
    require({value(row, "execution_key") for row in executions if row not in parents} == expected_keys,
            "item child consequences differ from exact native lineage")
    ack = item(p, p.parameters["AwaitCompletionLifecycleTable"], ack_pk, ack_sk)
    require(value(ack, "record_type") == "AWAIT_UNIT_RELEASE_ACKNOWLEDGEMENT"
            and value(ack, "unit_id") == unit_id and value(ack, "execution_id") == session["executionId"]
            and value(ack, "tenant_id") == session["tenant"]
            and value(ack, "disposition") == "CONTINUATION_QUEUED"
            and all(value(ack, key) == expected for key, expected in {"pipeline_id": p.contract["pipelineId"],
                "contract_version": p.contract["contractVersion"], "release_version": p.parameters["ReleaseVersion"]}.items()),
            "item parent lacks exact pinned native release acknowledgement")
    result = json.loads(p.action("RESULT", session["tenant"], session["executionId"])["resultJson"])
    require(isinstance(result, list) and [entry["request"] for entry in result] == ["approved-0", "approved-1", "approved-2"],
            "typed terminal-item results differ")
    terminal = p.poll(lambda: p.aws.call("lambda", "get-durable-execution", durable_execution_arn=arn,
                                      include_execution_data=True), lambda current: current.get("Status") == "SUCCEEDED")
    output = json.loads(terminal["Result"])
    require(all(output.get(key) == expected for key, expected in {"tenantId": session["tenant"],
        "executionId": session["executionId"], "pipelineId": p.contract["pipelineId"], "contractVersion": p.contract["contractVersion"],
        "releaseVersion": p.parameters["ReleaseVersion"], "generation": 1}.items()) and json.loads(output["resultJson"]) == result,
        "provider output changed native item result or release pins")
    p.aws.capture("suspended-item-continuation-outcome", {"interactions": final, "unit": units[0], "executions": executions,
                   "releaseAcknowledgement": ack, "provider": terminal, "callerSweep": False, "downstreamBusinessSuffix": False})


def scheduled_due_work(producer, owned):
    rule = owned.logical("DueWorkRule", "AWS::Events::Rule")
    schedule = producer.aws.call("events", "describe-rule", name=rule)
    require(schedule["State"] == "ENABLED" and schedule["ScheduleExpression"] == "rate(1 minute)", "production due-work schedule absent")
    target = owned.version("dueWork", "aws-durable-due-work")
    queue = owned.owns(producer.outputs["DueWorkQueueUrl"], "AWS::SQS::Queue")
    arn = producer.aws.call("sqs", "get-queue-attributes", queue_url=queue, attribute_names=["QueueArn"])["Attributes"]["QueueArn"]
    targets = producer.aws.call("events", "list-targets-by-rule", rule=rule)
    require(not targets.get("NextToken") and len(targets["Targets"]) == 1 and targets["Targets"][0]["Arn"] == arn,
            "due-work rule targets another resource")
    mapping_id = owned.owns(owned.manifest["mappingIds"]["dueWork"], "AWS::Lambda::EventSourceMapping")
    mapping = producer.aws.call("lambda", "get-event-source-mapping", uuid=mapping_id)
    require(mapping["State"] == "Enabled" and mapping["FunctionArn"] == target and mapping["EventSourceArn"] == arn,
            "due-work mapping differs from pinned production host")
    return {"rule": schedule, "targets": targets, "mapping": mapping}



def generation_fence(producer, owned):
    """Historical15: a live parked predecessor cannot win successor completion."""
    session = producer.open_execution("15")
    predecessor, old_arn = parked(producer, session, owned)
    pins = {"tenant_id": session["tenant"], "execution_id": session["executionId"],
            "pipeline_id": producer.contract["pipelineId"], "contract_version": producer.contract["contractVersion"],
            "release_version": producer.parameters["ReleaseVersion"]}
    require(value(predecessor, "generation", "N") == "1" and value(predecessor, "record_type") == "REGISTRATION"
            and all(value(predecessor, key) == expected for key, expected in pins.items()),
            "predecessor callback does not match the admitted native execution/release")
    record = actual_registration_record(producer, owned, predecessor)
    require(record.get("eventName") in {"INSERT", "MODIFY"} and bool(record.get("eventSourceARN"))
            and record.get("dynamodb", {}).get("Keys") == {"pk": predecessor["pk"], "sk": predecessor["sk"]}
            and record.get("dynamodb", {}).get("NewImage") == predecessor,
            "generation race requires the authentic retained predecessor Stream image")
    table = owned.owns(producer.outputs["CallbackBindingTableName"], "AWS::DynamoDB::Table")
    delivery_pk = session["tenant"] + "#" + session["first"]["interactionId"]
    require(not item(producer, table, delivery_pk, "DELIVERED#1"), "predecessor callback was already delivered")
    require(producer.aws.call("lambda", "get-durable-execution", durable_execution_arn=old_arn)["Status"] == "RUNNING",
            "generation race requires the old provider to remain running")
    ingress = owned.owns(owned.manifest["functionVersionArns"]["ingress"], "AWS::Lambda::Version")
    require(ingress == producer.outputs["IngressFunctionArn"], "generation resume ingress differs from pinned release")
    ingress_config = producer.aws.call("lambda", "get-function-configuration", function_name=ingress)
    require(ingress_config["Handler"] == "org.pipelineframework.aws.durable.AwsDurableIngressHandler::handleRequest",
            "generation resume must cross production ingress")
    resume = {"tenantId": session["tenant"], "idempotencyKey": session["executionId"],
              "pipelineId": producer.contract["pipelineId"], "contractVersion": producer.contract["contractVersion"],
              "releaseVersion": producer.parameters["ReleaseVersion"], "inputJson": "{}",
              "resumeExecutionId": session["executionId"], "generation": 2}
    started = producer.aws.invoke(ingress, resume)
    require(bool(started.get("durableExecutionName")), "successor ingress did not return actual provider start identity")
    successor = producer.poll(lambda: registration(producer, session, 2), bool)
    successor_arn = owned.provider(value(successor, "provider_execution_arn"))
    require(successor_arn != old_arn and value(successor, "generation", "N") == "2"
            and value(successor, "record_type") == "REGISTRATION"
            and value(successor, "interaction_id") == session["first"]["interactionId"]
            and value(successor, "provider_callback_id") != value(predecessor, "provider_callback_id")
            and value(successor, "provider_execution_name") == started["durableExecutionName"]
            and all(value(successor, key) == expected for key, expected in pins.items()),
            "successor changed native identity/release or reused old callback authority")
    binding = producer.poll(lambda: item(producer, table, delivery_pk, "GEN#00000000000000000002"), bool)
    require(value(binding, "generation", "N") == "2" and value(binding, "interaction_id") == session["first"]["interactionId"]
            and value(binding, "provider_execution_arn") == successor_arn
            and value(binding, "provider_callback_id") == value(successor, "provider_callback_id"),
            "successor callback binding is not the actual generation-two authority")
    require(producer.aws.call("lambda", "get-durable-execution", durable_execution_arn=old_arn)["Status"] == "RUNNING",
            "old provider stopped before successor fencing was challenged")
    producer.complete(session["first"], "first-approved")
    producer.poll(lambda: native_rows(producer, "AwaitInteractionTable", session["tenant"]),
        lambda rows: any(value(row, "interaction_id") == session["first"]["interactionId"]
                         and value(row, "status") == "COMPLETED" for row in rows))
    wakeup = owned.version("awaitWakeup", "aws-durable-await-wakeup")
    response = producer.aws.invoke(wakeup, {"Records": [record]})
    require(not response.get("batchItemFailures"), "stale authentic registration was not acknowledged")
    require(item(producer, table, delivery_pk, "GEN#00000000000000000002") == binding
            and registration(producer, session, 2) == successor, "old replay changed retained successor authority")
    require(not item(producer, table, delivery_pk, "DELIVERED#1"), "old generation admitted successful callback delivery")
    complete_remaining(producer, session, first_completed=True)
    assert_success(producer, session, successor_arn, 2)
    history = bounded_history(producer, old_arn)
    starts = [event for event in history["Events"] if event.get("EventType") == "CallbackStarted"
              and event.get("Name") == "await-completion-0"
              and event.get("CallbackStartedDetails", {}).get("CallbackId") == value(predecessor, "provider_callback_id")]
    require(len(starts) == 1 and bool(starts[0].get("Id")), "old callback history does not retain its authentic operation")
    require(not any(event.get("EventType") in {"CallbackSucceeded", "CallbackFailed", "CallbackTimedOut"}
                    and (event.get("Id") == starts[0]["Id"] or event.get("Name") == "await-completion-0")
                    for event in history["Events"]), "superseded provider callback acquired a terminal outcome")
    require(not item(producer, table, delivery_pk, "DELIVERED#1")
            and producer.aws.call("lambda", "get-durable-execution", durable_execution_arn=old_arn)["Status"] == "RUNNING",
            "old provider did not remain parked through replacement success")
    producer.aws.capture("generation-fence-outcome", {"predecessor": predecessor, "successor": successor,
                         "binding": binding, "staleRecord": record, "oldHistory": history,
                         "oldProviderStillRunning": True, "seededRandomizedCoverage": False})
    # Cleanup happens only after the original live-predecessor assertions have succeeded.
    producer.aws.call("lambda", "stop-durable-execution", durable_execution_arn=old_arn)
    stopped = producer.poll(lambda: producer.aws.call("lambda", "get-durable-execution", durable_execution_arn=old_arn),
                            lambda current: current.get("Status") == "STOPPED")
    producer.aws.capture("generation-fence-cleanup", {"oldProviderArn": old_arn, "status": stopped["Status"]})



def transition_fixture_binding(producer, owned):
    if producer.config["placement"] != "FUNCTION":
        raise NotImplementedError("COMPUTE transition consumer has no fixture delivery fault wrapper")
    spec = importlib.util.spec_from_file_location("transition_fixture", Path(__file__).with_name("transition-fault-fixture.py"))
    fixture = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fixture)
    root = Path(producer.bundle)
    if not (root / "conformance-transition-fault.yaml").is_file() or "ConformanceTransitionFaultManifest" not in producer.outputs:
        raise NotImplementedError("FUNCTION transition delivery fixture wiring is not deployed/assembled")
    source = (root / "aws-durable-queue-async.yaml").read_bytes()
    derived = (root / "conformance-transition-fault.yaml").read_bytes()
    binding = fixture.verify(root / "function.zip", source, derived)
    require(json.loads((root / "build-manifest.json").read_text()).get("transitionFaultFixture") == binding,
            "candidate transition fixture manifest differs from inspected archive/template")
    live = json.loads(producer.outputs["ConformanceTransitionFaultManifest"])
    for key in ("schemaVersion", "fixtureOnly", "handler", "delegate", "productionTemplateSha256"):
        require(live.get(key) == binding[key], "deployed fixture manifest differs: " + key)
    target = owned.version("transition", fixture.HANDLER)
    require(live.get("transitionVersionArn") == target, "fixture version differs from release manifest")
    table = owned.owns(live["faultTable"], "AWS::DynamoDB::Table")
    config = producer.aws.call("lambda", "get-function-configuration", function_name=target)
    require(config["Environment"]["Variables"].get("TPF_CONFORMANCE_TRANSITION_FAULT_TABLE") == table,
            "fixture handler fault table differs from owned manifest")
    require(config["CodeSha256"] == base64.b64encode(hashlib.sha256((root / "function.zip").read_bytes()).digest()).decode(),
            "fixture transition code differs from inspected candidate archive")
    producer.aws.capture("transition-fixture-binding", {"archive": binding, "deployed": live, "function": config})
    return table


def transition_request(producer, session, queue):
    page = producer.poll(lambda: producer.aws.call("sqs", "receive-message", queue_url=queue,
        max_number_of_messages=1, wait_time_seconds=1, visibility_timeout=60), lambda page: bool(page.get("Messages")), timeout=30)
    message = page["Messages"][0]
    body = message["Body"]
    request = json.loads(body)
    command = json.loads(request["commandEnvelope"])
    require(all(command.get(key) == expected for key, expected in {
        "tenantId": session["tenant"], "executionId": session["executionId"],
        "pipelineId": producer.contract["pipelineId"], "contractVersion": producer.contract["contractVersion"],
        "releaseVersion": producer.parameters["ReleaseVersion"], "currentStepIndex": 0, "attempt": 0}.items())
        and all(isinstance(request.get(key), str) and bool(request[key]) for key in ("requestId", "timestamp", "nonce", "signature"))
        and bool(command.get("transitionKey")), "captured transition does not belong to the exact fresh native caller")
    # This is the actual queue packet, not a fabricated signature. The native caller later
    # verifies the real response signature and requestId before it can advance.
    producer.aws.capture("authentic-transition-request", {"message": message, "command": command})
    return message, request, command


def transition_redelivery(producer, owned, scenario="7"):
    require(scenario in {"6", "7"}, "unsupported transition delivery cut")
    table = transition_fixture_binding(producer, owned)
    queue = owned.owns(owned.manifest["queueUrls"]["transitionRequest"], "AWS::SQS::Queue")
    with paused_worker(producer, owned, "transition", transition_handler="conformance-transition-delivery"):
        session = start_native(producer, scenario, "initial-approval")
        require(not native_rows(producer, "AwaitInteractionTable", session["tenant"]),
                "transition request was already consumed before the requested fault")
        message, request, command = transition_request(producer, session, queue)
        arms = []
        for point in (("AFTER_ACK",) if scenario == "6" else ("BEFORE_DELEGATE", "AFTER_ACK")):
            fault_id = session["tenant"] + "#TRANSITION#" + point
            producer.aws.call("dynamodb", "put-item", table_name=table, item={"fault_id": {"S": fault_id}},
                              condition_expression="attribute_not_exists(fault_id)")
            arms.append((fault_id, point))
        for _ in range(0 if scenario == "6" else 2):
            producer.aws.call("sqs", "send-message", queue_url=queue, message_body=message["Body"])
        producer.aws.call("sqs", "change-message-visibility", queue_url=queue,
                          receipt_handle=message["ReceiptHandle"], visibility_timeout=0)
    claims = []
    digest = hashlib.sha256(message["Body"].encode()).hexdigest()
    for fault_id, point in arms:
        claim = producer.poll(lambda: producer.aws.call("dynamodb", "get-item", table_name=table, consistent_read=True,
            key={"fault_id": {"S": fault_id + "#CLAIM"}}).get("Item", {}), bool)
        require(value(claim, "point") == point and value(claim, "request_id") == request["requestId"]
                and value(claim, "body_sha256") == digest and bool(value(claim, "message_id"))
                and value(claim, "delegate") == "org.pipelineframework.aws.durable.AwsDurableTransitionWorkerHandler",
                "delivery cut did not correlate to the captured real production request")
        claims.append(claim)
    status = producer.poll(lambda: producer.action("STATUS", session["tenant"], session["executionId"]),
        lambda status: status.get("executionStatus") == "WAITING_EXTERNAL" and bool(status.get("awaitCheckpoint")))
    session["first"] = status["awaitCheckpoint"]
    require("FirstApproval" in session["first"]["stepId"], "redelivery did not park at the first generated Await")
    interactions = native_rows(producer, "AwaitInteractionTable", session["tenant"])
    require(len(interactions) == 1 and value(interactions[0], "interaction_id") == session["first"]["interactionId"]
            and value(interactions[0], "execution_id") == session["executionId"], "redelivery repeated first Await admission")
    native = assert_single_execution(producer, session, "WAITING_EXTERNAL")
    require(value(native, "attempt", "N") == str(command["attempt"]),
            "native caller retried with another request before the correlated publication oracle")
    provider = producer.poll(lambda: producer.provider(session["start"]["durableExecutionName"]), bool)
    arn = owned.provider(provider["DurableExecutionArn"])
    session["provider"] = provider
    # executeBlocking returns only after a requestId-matched, authenticated response.
    # Merely receiving an ACK for an auth rejection/replayed nonce cannot satisfy this.
    producer.aws.capture("transition-redelivery-native-consequence", {"id": scenario, "requestId": request["requestId"],
        "transitionKey": command["transitionKey"], "claims": claims, "native": native, "interactions": interactions,
        "responseObservation": "NATIVE_CALLER_AUTHENTICATED_RESPONSE_ACCEPTANCE", "rawResponseCaptured": False,
        "nonceReplayMayDiscardDuplicate": True})
    complete_remaining(producer, session)
    assert_success(producer, session, arn, 1)

def run(producer, scenario):
    if scenario not in SUPPORTED_SCENARIOS:
        raise NotImplementedError("resource fault procedure is not implemented: " + scenario)
    owned = OwnedResources(producer)
    if scenario in {"16", "missing-registration"}:
        stopped(producer, scenario, owned)
    elif scenario == "nonterminal-provider-loss":
        nonterminal_loss(producer, owned)
    elif scenario == "suspended-continuation":
        suspended_continuation(producer, owned)
    elif scenario in {"6", "7"}:
        transition_redelivery(producer, owned, scenario)
    elif scenario == "15":
        generation_fence(producer, owned)
    elif scenario == "14":
        closed_callback(producer, owned)
    elif scenario == "18":
        retained_decoder_failure(producer, owned)
    elif scenario == "19":
        missed_stream(producer, owned)
    else:
        timeout(producer, scenario, owned)
