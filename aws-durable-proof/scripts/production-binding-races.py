"""Production binding-order procedure; import performs no AWS or filesystem writes.

Offline boundary tests are not live conformance. The caller owns deployment
preflight, execution authority, evidence persistence, and adequacy decisions.
"""
import importlib.util
import json
import hashlib
import re
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "binding_race_resource_faults", Path(__file__).with_name("production-resource-faults.py"))
RESOURCE_FAULTS = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(RESOURCE_FAULTS)

SUPPORTED_SCENARIOS = frozenset({"10", "pre-registration-provider-loss"})
GAPS = {
    "seeded-generation-races":
        "The historical randomized oracle includes provider callback throttling and "
        "wakeup-before/after faults absent from the current action proxy. Repeating "
        "the deterministic generation-fence procedure does not satisfy that oracle.",
}


def unresolved_callback(producer, arn, callback_id):
    """Require authentic, unresolved first callback authority, never a model event."""
    history = RESOURCE_FAULTS.bounded_history(producer, arn)
    starts = [event for event in history["Events"]
              if event.get("EventType") == "CallbackStarted"
              and event.get("Name") == "await-completion-0"
              and event.get("CallbackStartedDetails", {}).get("CallbackId") == callback_id]
    RESOURCE_FAULTS.require(len(starts) == 1 and bool(starts[0].get("Id")),
                            "binding order requires the authentic first provider callback")
    RESOURCE_FAULTS.require(not any(event.get("EventType") in {
        "CallbackSucceeded", "CallbackFailed", "CallbackTimedOut"}
        and (event.get("Id") == starts[0]["Id"] or event.get("Name") == "await-completion-0")
        for event in history["Events"]), "first provider callback already resolved before binding")
    RESOURCE_FAULTS.require(producer.aws.call("lambda", "get-durable-execution",
        durable_execution_arn=arn).get("Status") == "RUNNING", "provider is not running at the binding cut")
    return history


def completion_before_binding(producer, owned):
    r = RESOURCE_FAULTS
    table = owned.owns(producer.outputs["CallbackBindingTableName"], "AWS::DynamoDB::Table")
    # Only the production Stream adapters bind this callback. Pause both inputs
    # before admission, then restore them even if an assertion or AWS call fails.
    with r.paused_mappings(producer, owned, ("awaitState", "callbackBinding")):
        session = producer.open_execution("10")
        row = producer.poll(lambda: r.registration(producer, session), bool)
        arn = owned.provider(r.value(row, "provider_execution_arn"))
        pins = {"tenant_id": session["tenant"], "execution_id": session["executionId"],
                "pipeline_id": producer.contract["pipelineId"],
                "contract_version": producer.contract["contractVersion"],
                "release_version": producer.parameters["ReleaseVersion"],
                "interaction_id": session["first"]["interactionId"],
                "correlation_id": session["first"]["correlationId"]}
        r.require(r.value(row, "record_type") == "REGISTRATION"
                  and r.value(row, "generation", "N") == "1"
                  and r.value(row, "occurrence", "N") == "1"
                  and all(r.value(row, key) == expected for key, expected in pins.items())
                  and arn == session["provider"]["DurableExecutionArn"]
                  and bool(r.value(row, "provider_callback_id")),
                  "first registration differs from the admitted native identity/release")
        pk = session["tenant"] + "#" + session["first"]["interactionId"]
        r.require(not producer.binding(session["first"])
                  and not r.item(producer, table, pk, "DELIVERED#1"),
                  "callback was already bound or delivered before first completion")
        before = unresolved_callback(producer, arn, r.value(row, "provider_callback_id"))
        producer.complete(session["first"], "first-approved")
        interactions = producer.poll(lambda: producer.rows("AwaitInteractionTable", session["tenant"]),
            lambda rows: any(r.value(current, "interaction_id") == session["first"]["interactionId"]
                             and r.value(current, "status") == "COMPLETED" for current in rows))
        completed = [current for current in interactions
                     if r.value(current, "interaction_id") == session["first"]["interactionId"]]
        r.require(len(completed) == 1 and all(r.value(completed[0], key) == expected
                  for key, expected in pins.items() if key not in {"pipeline_id", "contract_version", "release_version"}), "completion changed native Await identity or release")
        checkpoints = producer.action("READ_EXECUTION_AWAITS", session["tenant"], session["executionId"]).get("awaitCheckpoints", [])
        observed = [checkpoint for checkpoint in checkpoints
                    if checkpoint.get("interactionId") == session["first"]["interactionId"]]
        semantic_pins = {"tenantId": session["tenant"], "executionId": session["executionId"],
                         "interactionId": session["first"]["interactionId"],
                         "correlationId": session["first"]["correlationId"], "status": "COMPLETED",
                         "pipelineId": producer.contract["pipelineId"], "contractVersion": producer.contract["contractVersion"],
                         "releaseVersion": producer.parameters["ReleaseVersion"]}
        r.require(len(observed) == 1 and all(observed[0].get(key) == expected
                  for key, expected in semantic_pins.items()), "semantic completion changed identity or release")
        # This after-admission absence check is the order oracle. A queue send or
        # a BEFORE_FORWARD claim alone cannot prove completion preceded binding.
        r.require(not producer.binding(session["first"])
                  and not r.item(producer, table, pk, "DELIVERED#1"),
                  "completion-before-binding cut was lost before native completion")
        after = unresolved_callback(producer, arn, r.value(row, "provider_callback_id"))
        r.require(r.registration(producer, session) == row, "first registration changed at the order cut")
        producer.aws.capture("completion-before-binding-cut", {
            "scenario": "10", "registration": row, "nativeCompletion": completed[0], "semanticCheckpoint": observed[0],
            "beforeHistory": before, "afterHistory": after,
            "bindingAbsentAfterNativeCompletion": True, "deliveryAbsentAfterNativeCompletion": True})
    binding = producer.poll(lambda: producer.binding(session["first"]), bool)
    r.require(r.value(binding, "record_type") == "BINDING"
              and r.value(binding, "generation", "N") == "1"
              and r.value(binding, "occurrence", "N") == "1"
              and all(r.value(binding, key) == expected for key, expected in pins.items()
                      if key not in {"pipeline_id", "contract_version", "release_version"})
              and r.value(binding, "provider_execution_arn") == arn
              and r.value(binding, "provider_callback_id") == r.value(row, "provider_callback_id"),
              "restored production streams bound another callback or semantic identity")
    r.complete_remaining(producer, session, first_completed=True)
    r.assert_success(producer, session, arn, 1)
    receipt = producer.poll(lambda: r.item(producer, table, pk, "DELIVERED#1"), bool)
    r.require(r.value(receipt, "record_type") == "DELIVERY"
              and r.value(receipt, "pk") == pk and r.value(receipt, "sk") == "DELIVERED#1"
              and r.value(receipt, "provider_outcome") in {"CALLBACK_ACCEPTED", "PROVIDER_STATE_SUCCEEDED"}
              and r.value(receipt, "provider_callback_id") == r.value(row, "provider_callback_id"),
              "completed first callback has no matching production delivery evidence")
    producer.aws.capture("completion-before-binding-outcome", {
        "scenario": "10", "binding": binding, "delivery": receipt,
        "seed": producer.seed, "callerSweep": False, "historicalRepairSubstitute": False})


def fault_item(producer, fault_id):
    return producer.aws.call("dynamodb", "get-item", table_name=producer.config["faultTable"],
        consistent_read=True, key={"fault_id": {"S": fault_id}}).get("Item", {})


def arm_registration_hold(producer, tenant, action, owned):
    r = RESOURCE_FAULTS
    # Auxiliary fixture stacks need a separately adjudicated authority contract.
    # This prepared procedure admits only fixtures in the owned disposable stack.
    owned.owns(producer.config["faultTable"], "AWS::DynamoDB::Table")
    proxy = owned.owns(producer.config["faultProxyVersionArn"], "AWS::Lambda::Version")
    r.require(bool(re.fullmatch(r"arn:[^:]+:lambda:[^:]+:[0-9]{12}:function:[^:]+:[0-9]+", proxy)),
              "registration hold requires a pinned numbered fixture proxy")
    configuration = producer.aws.call("lambda", "get-function-configuration", function_name=proxy)
    variables = configuration.get("Environment", {}).get("Variables", {})
    r.require(configuration.get("Handler") == "org.pipelineframework.awsconformance.faults.ActionTransportFaultProxy::handleRequest"
              and variables.get("TPF_CONFORMANCE_TARGET_ACTION_VERSION") == action
              and variables.get("TPF_CONFORMANCE_FAULT_TABLE") == producer.config["faultTable"],
              "registration hold fixture target/table/handler differs from declared boundary")
    coordinator = producer.aws.call("lambda", "get-function-configuration",
                                    function_name=producer.outputs["DurableAliasArn"])
    r.require(coordinator.get("Environment", {}).get("Variables", {}).get("TPF_AWS_DURABLE_ACTION_FUNCTION") == proxy,
              "production driver does not cross the held fixture boundary")
    arm = tenant + "#REGISTER_CALLBACK#BEFORE_FORWARD"
    producer.aws.call("dynamodb", "put-item", table_name=producer.config["faultTable"],
        item={"fault_id": {"S": arm}, "mode": {"S": "HOLD"}}, condition_expression="attribute_not_exists(fault_id)")
    return arm


def release_registration_hold(producer, arm):
    release_id = arm + "#RELEASE"
    if not fault_item(producer, release_id):
        producer.aws.call("dynamodb", "put-item", table_name=producer.config["faultTable"],
            item={"fault_id": {"S": release_id}}, condition_expression="attribute_not_exists(fault_id)")
    release = producer.poll(lambda: fault_item(producer, release_id), bool)
    RESOURCE_FAULTS.require(RESOURCE_FAULTS.value(release, "fault_id") == release_id,
                           "registration fixture release marker differs")
    producer.aws.capture("registration-hold-released", release)


def challenge_stale_registration(producer, action, request):
    """Only the authentic native FunctionError with the exact typed failure counts."""
    r = RESOURCE_FAULTS
    before = len(producer.aws.references)
    try:
        producer.aws.invoke(action, request)
    except RuntimeError:
        r.require(len(producer.aws.references) == before + 1, "stale challenge lacks its native Lambda receipt")
        reference = producer.aws.references[-1]
        receipt = json.loads((producer.aws.root / reference["path"]).read_text())
        metadata, response = receipt.get("metadata", {}), receipt.get("response", {})
        r.require(receipt.get("function") == action and receipt.get("request") == request
                  and metadata.get("StatusCode") == 200 and metadata.get("FunctionError") in {"Handled", "Unhandled"}
                  and response.get("errorType") == "org.pipelineframework.aws.durable.AwsDurableStaleGenerationException"
                  and response.get("errorMessage") == "provider generation is no longer current",
                  "stale registration was not rejected by the exact native generation-fence exception")
        producer.aws.capture("stale-registration-challenge", {"nativeReceipt": reference, "typedRejection": response})
        return
    raise AssertionError("old held callback registration was admitted after the successor fence")


def pre_registration_provider_loss(producer, owned):
    r = RESOURCE_FAULTS
    table = owned.owns(producer.outputs["CallbackBindingTableName"], "AWS::DynamoDB::Table")
    action = owned.version("action", "aws-durable-action")
    r.require(action == producer.config["actionVersionArn"], "held target differs from release-pinned production action")
    # Reject foreign fixture resources before registering even a release fixture.
    owned.owns(producer.config["faultTable"], "AWS::DynamoDB::Table")
    owned.owns(producer.config["faultProxyVersionArn"], "AWS::Lambda::Version")
    tenant = "conformance-pre-registration-provider-loss-" + str(producer.random.getrandbits(64))
    producer.release(tenant)
    arm = arm_registration_hold(producer, tenant, action, owned)
    released = False
    try:
        start = producer.aws.invoke(producer.outputs["IngressFunctionArn"], {
            "tenantId": tenant, "idempotencyKey": tenant + ":start", "pipelineId": producer.contract["pipelineId"],
            "contractVersion": producer.contract["contractVersion"], "releaseVersion": producer.parameters["ReleaseVersion"],
            "inputJson": json.dumps({"request": "initial-approval"}), "generation": 1})
        rows = producer.poll(lambda: producer.rows("ExecutionTable", tenant), lambda rows: len(rows) == 1)
        execution_id = r.value(rows[0], "execution_id")
        status = producer.poll(lambda: producer.action("STATUS", tenant, execution_id),
            lambda current: current.get("executionStatus") == "WAITING_EXTERNAL" and bool(current.get("awaitCheckpoint")))
        first = status["awaitCheckpoint"]
        r.require("FirstApproval" in first["stepId"], "held admission did not reach the first generated Await")
        provider = producer.poll(lambda: producer.provider(start["durableExecutionName"]), bool)
        arn = owned.provider(provider["DurableExecutionArn"])
        session = {"tenant": tenant, "executionId": execution_id, "first": first, "provider": provider, "start": start}
        pins = {"tenant_id": tenant, "execution_id": execution_id, "pipeline_id": producer.contract["pipelineId"],
                "contract_version": producer.contract["contractVersion"], "release_version": producer.parameters["ReleaseVersion"]}
        partition = tenant + "#" + execution_id
        recovery = producer.poll(lambda: r.item(producer, table, partition, "RECOVERY#00000000000000000001"), bool)
        r.require(r.value(recovery, "record_type") == "RECOVERY" and r.value(recovery, "generation", "N") == "1"
                  and r.value(recovery, "provider_execution_arn") == arn
                  and r.value(recovery, "provider_execution_name") == start["durableExecutionName"]
                  and all(r.value(recovery, key) == expected for key, expected in pins.items()),
                  "pre-registration recovery locator differs from admitted identity/release")
        claim = producer.poll(lambda: fault_item(producer, arm + "#CLAIM"), bool)
        raw = r.value(claim, "request_json")
        r.require(isinstance(raw, str) and r.value(claim, "request_sha256") == hashlib.sha256(raw.encode()).hexdigest(),
                  "held request has no authentic byte digest")
        request = json.loads(raw)
        expected = {"operation": "REGISTER_CALLBACK", "tenantId": tenant, "executionId": execution_id,
                    "generation": 1, "pipelineId": producer.contract["pipelineId"],
                    "contractVersion": producer.contract["contractVersion"], "releaseVersion": producer.parameters["ReleaseVersion"],
                    "providerExecutionArn": arn, "providerExecutionName": start["durableExecutionName"]}
        expected_identity = {"tenantId": tenant, "executionId": execution_id, "interactionId": first["interactionId"],
                             "correlationId": first["correlationId"], "generation": 1, "occurrence": 1}
        r.require(r.value(claim, "fault_id") == arm + "#CLAIM" and r.value(claim, "mode") == "HOLD"
                  and r.value(claim, "point") == "BEFORE_FORWARD" and r.value(claim, "operation") == "REGISTER_CALLBACK"
                  and r.value(claim, "target") == action and r.value(claim, "tenant_id") == tenant
                  and r.value(claim, "execution_id") == execution_id and r.value(claim, "generation", "N") == "1"
                  and all(request.get(key) == value for key, value in expected.items())
                  and request.get("awaitIdentity") == expected_identity and bool(request.get("providerCallbackId")),
                  "hold claim does not correlate the actual first registration request")
        history = unresolved_callback(producer, arn, request["providerCallbackId"])
        arm_record = fault_item(producer, arm)
        r.require(r.value(arm_record, "mode") == "HOLD" and not fault_item(producer, arm + "#RELEASE")
                  and not r.registration(producer, session) and not producer.binding(first),
                  "pre-registration cut was not held before actual registration/binding")
        native = r.assert_single_execution(producer, session, "WAITING_EXTERNAL")
        interactions = producer.rows("AwaitInteractionTable", tenant)
        r.require(len(interactions) == 1 and r.value(interactions[0], "interaction_id") == first["interactionId"]
                  and r.value(interactions[0], "status") == "PENDING", "held admission duplicated or completed its first Await")
        producer.aws.call("lambda", "stop-durable-execution", durable_execution_arn=arn)
        stopped = producer.poll(lambda: producer.aws.call("lambda", "get-durable-execution", durable_execution_arn=arn),
                                lambda current: current.get("Status") == "STOPPED")
        r.require(not r.registration(producer, session), "old callback registration reached production while HOLD was active")
        r.assert_single_execution(producer, session, "WAITING_EXTERNAL")
        producer.aws.capture("pre-registration-provider-loss-cut", {"claim": claim, "recovery": recovery,
            "native": native, "firstAwait": interactions[0], "history": history, "stoppedProvider": stopped})
        release_registration_hold(producer, arm)
        released = True
        target = owned.version("reconciliation", "aws-durable-reconcile")
        response = producer.aws.invoke(target, {"detail": {"durableExecutionArn": arn}})
        successor = producer.poll(lambda: r.registration(producer, session, 2), bool)
        successor_arn = owned.provider(r.value(successor, "provider_execution_arn"))
        r.require(successor_arn != arn and r.value(successor, "record_type") == "REGISTRATION"
                  and r.value(successor, "generation", "N") == "2" and r.value(successor, "occurrence", "N") == "1"
                  and all(r.value(successor, key) == value for key, value in pins.items())
                  and r.value(successor, "interaction_id") == first["interactionId"]
                  and r.value(successor, "correlation_id") == first["correlationId"]
                  and r.value(successor, "provider_callback_id") != request["providerCallbackId"],
                  "production repair did not retain the same pinned native Await with new callback authority")
        fence = r.item(producer, table, partition, "SUCCESSOR#1")
        claim_two = r.item(producer, table, partition, "CLAIM#2")
        r.require(r.value(fence, "record_type") == "GENERATION_SUCCESSOR" and r.value(fence, "generation", "N") == "2"
                  and r.value(claim_two, "record_type") == "GENERATION_CLAIM"
                  and all(r.value(claim_two, key) == value for key, value in pins.items()
                          if key in {"pipeline_id", "contract_version", "release_version"}),
                  "replacement lacks the retained generation-two claim and predecessor fence")
        binding = producer.poll(lambda: r.item(producer, table, tenant + "#" + first["interactionId"],
                                               "GEN#00000000000000000002"), bool)
        r.require(r.value(binding, "provider_execution_arn") == successor_arn
                  and r.value(binding, "provider_callback_id") == r.value(successor, "provider_callback_id"),
                  "replacement did not bind its own callback authority")
        # Release can expose an in-flight predecessor retry before repair fences it.
        # Its retained row is legitimate history; the explicit post-fence challenge
        # below must reject new registration authority and preserve the successor.
        late_predecessor = r.registration(producer, session)
        challenge_stale_registration(producer, action, request)
        r.require(r.registration(producer, session, 2) == successor
                  and r.item(producer, table, tenant + "#" + first["interactionId"], "GEN#00000000000000000002") == binding,
                  "stale held request changed successor authority")
        r.complete_remaining(producer, session)
        r.assert_success(producer, session, successor_arn, 2)
        r.require(producer.aws.call("lambda", "get-durable-execution", durable_execution_arn=arn)["Status"] == "STOPPED"
                  and not r.item(producer, table, tenant + "#" + first["interactionId"], "DELIVERED#1"),
                  "old stopped provider admitted a completion after successor repair")
        producer.aws.capture("pre-registration-provider-loss-outcome", {"successor": successor,
            "generationClaim": claim_two, "fence": fence, "binding": binding, "repairResponse": response,
            "latePredecessorRegistration": late_predecessor, "oldProviderStopped": True,
            "seed": producer.seed, "callerSweep": False, "historicalRepairSubstitute": False})
    finally:
        if not released:
            release_registration_hold(producer, arm)


def run(producer, scenario):
    if scenario not in SUPPORTED_SCENARIOS:
        raise NotImplementedError(GAPS.get(scenario, "binding race procedure is not implemented: " + scenario))
    owned = RESOURCE_FAULTS.OwnedResources(producer)
    if scenario == "10":
        completion_before_binding(producer, owned)
    else:
        pre_registration_provider_loss(producer, owned)
