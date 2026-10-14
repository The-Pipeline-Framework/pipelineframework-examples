"""Procedure boundary tests with substituted AWS; never live conformance evidence."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location("resource_faults", Path(__file__).parents[1] / "scripts/production-resource-faults.py")
FAULTS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FAULTS)

STACK = "arn:aws:cloudformation:us-east-1:123456789012:stack/disposable/run"
VERSION = "arn:aws:lambda:us-east-1:123456789012:function:driver:7"
PROVIDER = VERSION + "/durable-execution/run/start"
SESSION = {"tenant": "conformance-owner", "executionId": "semantic-original",
           "first": {"tenantId": "conformance-owner", "interactionId": "first", "stepId": "FirstApproval"},
           "provider": {"DurableExecutionArn": PROVIDER}}


def row(**values):
    return {key: {"N" if isinstance(value, int) else "S": str(value)} for key, value in values.items()}


REGISTRATION = row(pk="conformance-owner#semantic-original", sk="REGISTRATION#00000000000000000001",
                   record_type="REGISTRATION", tenant_id="conformance-owner", execution_id="semantic-original",
                   interaction_id="first", provider_execution_arn=PROVIDER, provider_callback_id="actual-callback", generation=1,
                   pipeline_id="generated-pipeline", contract_version="compiler-contract", release_version="release-pinned")
BINDING = row(pk="conformance-owner#first", sk="GEN#00000000000000000001", record_type="BINDING",
              tenant_id="conformance-owner", execution_id="semantic-original", interaction_id="first",
              provider_execution_arn=PROVIDER, provider_callback_id="actual-callback", generation=1)


def producer():
    boundary = SimpleNamespace(call=Mock(), invoke=Mock(), capture=Mock())
    result = SimpleNamespace(aws=boundary, config={"stack": "disposable", "resourceFaults": {
        "disposable": True, "stackId": STACK, "owner": "run-owner"}},
        outputs={"DeploymentReleaseManifest": json.dumps({"stackId": STACK,
                 "functionVersionArns": {"durable": VERSION}, "mappingIds": {"awaitState": "mapping-a", "callbackBinding": "mapping-b"}}),
                 "CallbackBindingTableName": "mechanical-table", "DueWorkQueueUrl": "owned-due-work-queue"},
        contract={"pipelineId": "generated-pipeline", "contractVersion": "compiler-contract"},
        parameters={"ReleaseVersion": "release-pinned", "AwaitInteractionStreamArn": "actual-stream"},
        open_execution=Mock(return_value=SESSION), binding=Mock(return_value=BINDING),
        rows=Mock(), action=Mock(), complete=Mock())

    def poll(observe, accept, timeout=180):
        observed = observe()
        if not accept(observed):
            raise AssertionError("boundary fixture rejected expected observation")
        return observed

    result.poll = poll
    return result


class ProductionResourceFaultTest(unittest.TestCase):

    def test_transition_redelivery_requires_real_correlated_cut_and_native_consequence(self):
        from contextlib import contextmanager
        import hashlib
        for wrong in (None, "request-pin", "claim-request", "claim-hash", "auth-reject-ack", "repeated-await", "caller-retry"):
            with self.subTest(wrong=wrong):
                p = producer()
                p.config["placement"] = "FUNCTION"
                p.random = SimpleNamespace(getrandbits=lambda _: 7)
                p.bundle = "not-used-by-substituted-archive-boundary"
                session = {"tenant": "tenant-7", "executionId": "execution-7", "start": {"durableExecutionName": "provider-name"}}
                command = {"tenantId": "tenant-7", "executionId": "foreign" if wrong == "request-pin" else "execution-7",
                    "pipelineId": p.contract["pipelineId"], "contractVersion": p.contract["contractVersion"],
                    "releaseVersion": p.parameters["ReleaseVersion"], "currentStepIndex": 0, "attempt": 0, "transitionKey": "transition-7"}
                request = {"requestId": "request-7", "timestamp": "actual-timestamp", "nonce": "actual-nonce", "signature": "actual-signature", "commandEnvelope": json.dumps(command)}
                body = json.dumps(request)
                message = {"Body": body, "ReceiptHandle": "actual-receipt", "MessageId": "captured-message"}
                first = {"tenantId": "tenant-7", "interactionId": "first-7", "stepId": "FirstApproval"}
                p.action.return_value = {} if wrong == "auth-reject-ack" else {"executionStatus": "WAITING_EXTERNAL", "awaitCheckpoint": first}
                p.provider = Mock(return_value={"DurableExecutionArn": PROVIDER})
                owned = SimpleNamespace(owns=lambda target, kind: target, provider=lambda arn: arn,
                    manifest={"queueUrls": {"transitionRequest": "request-queue"}})
                @contextmanager
                def pause(*args, **kwargs): yield
                def aws(service, operation, **args):
                    if operation == "receive-message": return {"Messages": [message]}
                    if operation == "get-item":
                        point = "BEFORE_DELEGATE" if "BEFORE_DELEGATE" in args["key"]["fault_id"]["S"] else "AFTER_ACK"
                        return {"Item": row(point=point, request_id="foreign" if wrong == "claim-request" else "request-7",
                            body_sha256="wrong" if wrong == "claim-hash" else hashlib.sha256(body.encode()).hexdigest(),
                            message_id="actual-delivery", delegate="org.pipelineframework.aws.durable.AwsDurableTransitionWorkerHandler")}
                    return {}
                p.aws.call.side_effect = aws
                interaction = row(tenant_id="tenant-7", execution_id="execution-7", interaction_id="first-7")
                with patch.object(FAULTS, "transition_fixture_binding", return_value="fault-table"), \
                     patch.object(FAULTS, "paused_worker", side_effect=pause), \
                     patch.object(FAULTS, "start_native", return_value=session), \
                     patch.object(FAULTS, "native_rows", side_effect=[[], [interaction, interaction] if wrong == "repeated-await" else [interaction]]), \
                     patch.object(FAULTS, "assert_single_execution", return_value=row(status="WAITING_EXTERNAL", attempt=1 if wrong == "caller-retry" else 0)), \
                     patch.object(FAULTS, "complete_remaining") as complete, patch.object(FAULTS, "assert_success") as success:
                    if wrong:
                        with self.assertRaises(AssertionError): FAULTS.transition_redelivery(p, owned)
                        complete.assert_not_called(); success.assert_not_called()
                    else:
                        FAULTS.transition_redelivery(p, owned)
                        complete.assert_called_once(); success.assert_called_once_with(p, session, PROVIDER, 1)
                    copies = [call.kwargs["message_body"] for call in p.aws.call.call_args_list if call.args[1] == "send-message"]
                    self.assertEqual([] if wrong == "request-pin" else [body, body], copies)
                    self.assertFalse(any(call.args[1] in {"delete-item", "update-item"} for call in p.aws.call.call_args_list))

    def test_transition_fixture_compute_is_explicitly_unavailable_before_any_io(self):
        p = producer(); p.config["placement"] = "COMPUTE"
        with self.assertRaisesRegex(NotImplementedError, "COMPUTE"):
            FAULTS.transition_fixture_binding(p, SimpleNamespace())
        p.aws.call.assert_not_called(); p.aws.invoke.assert_not_called()

    def test_generation_fence_uses_live_predecessor_actual_resume_and_retained_stream(self):
        for wrong in (None, "predecessor-pin", "fabricated-record", "same-provider", "successor-pin", "old-not-running",
                      "replay-retry", "old-delivery", "successor-binding", "old-history"):
            with self.subTest(wrong=wrong):
                p = producer()
                ingress = "arn:aws:lambda:us-east-1:123456789012:function:ingress:8"
                p.outputs["IngressFunctionArn"] = ingress
                replacement_arn = VERSION + "/durable-execution/replacement/start"
                predecessor = REGISTRATION | row(release_version="foreign") if wrong == "predecessor-pin" else REGISTRATION
                successor = REGISTRATION | row(generation=2, provider_execution_arn=PROVIDER if wrong == "same-provider" else replacement_arn,
                    provider_callback_id="successor-callback", provider_execution_name="successor-start",
                    release_version="foreign" if wrong == "successor-pin" else "release-pinned")
                binding = BINDING | row(generation=2, provider_execution_arn=replacement_arn, provider_callback_id="successor-callback")
                record = {"eventSourceARN": "mechanical-stream", "eventName": "INSERT", "eventID": "retained-real-event",
                    "dynamodb": {"Keys": {"pk": REGISTRATION["pk"], "sk": REGISTRATION["sk"]}, "NewImage": predecessor}}
                if wrong == "fabricated-record": record["dynamodb"]["NewImage"] = row(record_type="REGISTRATION")
                owned = SimpleNamespace(provider=lambda value: value, owns=lambda target, kind: target,
                    version=lambda *args: VERSION, manifest={"functionVersionArns": {"ingress": ingress}})
                state = {"stopped": False, "bindingReads": 0, "deliveryReads": 0}
                def aws(service, operation, **args):
                    if operation == "get-function-configuration":
                        return {"Handler": "org.pipelineframework.aws.durable.AwsDurableIngressHandler::handleRequest"}
                    if operation == "stop-durable-execution": state["stopped"] = True; return {}
                    if operation == "get-durable-execution":
                        return {"Status": "STOPPED" if state["stopped"] or wrong == "old-not-running" else "RUNNING"}
                    raise AssertionError(operation)
                def invoke(target, payload):
                    return {"durableExecutionName": "successor-start"} if target == ingress else {"batchItemFailures": [{"itemIdentifier": "old"}] if wrong == "replay-retry" else []}
                def observed_item(producer, table, pk, sk):
                    if sk == "GEN#00000000000000000002":
                        state["bindingReads"] += 1
                        return binding | row(provider_callback_id="foreign") if wrong == "successor-binding" and state["bindingReads"] > 1 else binding
                    self.assertEqual("DELIVERED#1", sk)
                    state["deliveryReads"] += 1
                    return row(record_type="DELIVERY") if wrong == "old-delivery" and state["deliveryReads"] > 1 else {}
                p.aws.call.side_effect, p.aws.invoke.side_effect = aws, invoke
                history = {"Events": [{"EventType": "CallbackStarted", "Name": "await-completion-0", "Id": "old-operation",
                    "CallbackStartedDetails": {"CallbackId": "actual-callback"}}]}
                if wrong == "old-history": history["Events"].append({"EventType": "CallbackSucceeded", "Id": "old-operation"})
                with patch.object(FAULTS, "parked", return_value=(predecessor, PROVIDER)), \
                     patch.object(FAULTS, "actual_registration_record", return_value=record), \
                     patch.object(FAULTS, "registration", return_value=successor), \
                     patch.object(FAULTS, "native_rows", return_value=[row(interaction_id="first", status="COMPLETED")]), \
                     patch.object(FAULTS, "item", side_effect=observed_item), \
                     patch.object(FAULTS, "bounded_history", return_value=history), \
                     patch.object(FAULTS, "complete_remaining") as complete, \
                     patch.object(FAULTS, "assert_success") as success:
                    if wrong:
                        with self.assertRaises(AssertionError): FAULTS.generation_fence(p, owned)
                        self.assertFalse(state["stopped"], "failed fencing assertions cannot stop the old provider early")
                    else:
                        FAULTS.generation_fence(p, owned)
                        self.assertTrue(state["stopped"])
                        complete.assert_called_once_with(p, SESSION, first_completed=True)
                        success.assert_called_once_with(p, SESSION, replacement_arn, 2)
                        resume = p.aws.invoke.call_args_list[0].args[1]
                        self.assertEqual({"tenantId": SESSION["tenant"], "idempotencyKey": SESSION["executionId"],
                            "pipelineId": "generated-pipeline", "contractVersion": "compiler-contract", "releaseVersion": "release-pinned",
                            "inputJson": "{}", "resumeExecutionId": SESSION["executionId"], "generation": 2}, resume)
                        self.assertEqual({"Records": [record]}, p.aws.invoke.call_args_list[1].args[1])
                        outcome = next(call.args[1] for call in p.aws.capture.call_args_list if call.args[0] == "generation-fence-outcome")
                        self.assertTrue(outcome["oldProviderStillRunning"])
                        self.assertFalse(outcome["seededRandomizedCoverage"])
                self.assertNotIn("REDRIVE", [call.args[0] for call in p.action.call_args_list])
                self.assertNotIn("SUBMIT", [call.args[0] for call in p.action.call_args_list])
                if wrong in {"predecessor-pin", "fabricated-record", "old-not-running"}: p.aws.invoke.assert_not_called()

    def test_nonterminal_successor_recovery_preserves_native_parent_before_first_await(self):
        from contextlib import nullcontext
        for placement in ("FUNCTION", "COMPUTE"):
            with self.subTest(placement=placement):
                p = producer()
                p.config["placement"] = placement
                p.provider = Mock(return_value={"DurableExecutionArn": PROVIDER})
                recovery = REGISTRATION | row(record_type="RECOVERY")
                replacement = VERSION + "/durable-execution/replacement/start"
                successor = recovery | row(generation=2, provider_execution_arn=replacement)
                p.aws.call.return_value = {"Status": "STOPPED"}
                p.action.return_value = {"executionStatus": "WAITING_EXTERNAL", "awaitCheckpoint": SESSION["first"]}
                owned = SimpleNamespace(provider=lambda value: value, version=lambda *args: VERSION)
                with patch.object(FAULTS, "paused_worker", return_value=nullcontext()) as pause, \
                     patch.object(FAULTS, "start_native", return_value=SESSION | {"start": {"durableExecutionName": "start"}}), \
                     patch.object(FAULTS, "assert_single_execution", return_value={}) as parent, \
                     patch.object(FAULTS, "native_rows", return_value=[]), \
                     patch.object(FAULTS, "item", side_effect=[recovery, successor]), \
                     patch.object(FAULTS, "complete_remaining") as complete, \
                     patch.object(FAULTS, "assert_success") as success:
                    FAULTS.nonterminal_loss(p, owned)
                pause.assert_called_once_with(p, owned, "work")
                self.assertEqual(["QUEUED", "QUEUED"], [call.args[2] for call in parent.call_args_list])
                self.assertEqual(PROVIDER, p.aws.invoke.call_args.args[1]["detail"]["durableExecutionArn"])
                success.assert_called_once_with(p, SESSION | {"start": {"durableExecutionName": "start"}}, replacement, 2)
                complete.assert_called_once()
                self.assertNotIn("SWEEP", [call.args[0] for call in p.action.call_args_list])

    def test_due_work_schedule_rejects_foreign_target_truncation_or_disabled_mapping(self):
        for wrong in ("target", "truncated", "mapping", "schedule"):
            with self.subTest(wrong=wrong):
                p = producer()
                owned = SimpleNamespace(logical=lambda *args: "rule", version=lambda *args: VERSION,
                    owns=lambda target, kind: target, manifest={"mappingIds": {"dueWork": "due-map"}})
                def aws(service, operation, **args):
                    if operation == "describe-rule": return {"State": "DISABLED" if wrong == "schedule" else "ENABLED", "ScheduleExpression": "rate(1 minute)"}
                    if operation == "get-queue-attributes": return {"Attributes": {"QueueArn": "due-arn"}}
                    if operation == "list-targets-by-rule": return {"Targets": [{"Arn": "foreign" if wrong == "target" else "due-arn"}], **({"NextToken": "opaque"} if wrong == "truncated" else {})}
                    return {"State": "Disabled" if wrong == "mapping" else "Enabled", "FunctionArn": VERSION, "EventSourceArn": "due-arn"}
                p.aws.call.side_effect = aws
                with self.assertRaises(AssertionError): FAULTS.scheduled_due_work(p, owned)
                p.aws.invoke.assert_not_called()


    def test_new_native_observation_rejects_incomplete_page(self):
        p = producer()
        p.parameters["ExecutionTable"] = "native"
        p.aws.call.return_value = {"Items": [], "LastEvaluatedKey": {"tenant_id": {"S": "owner"}}}
        with self.assertRaisesRegex(NotImplementedError, "one-page"):
            FAULTS.native_rows(p, "ExecutionTable", "owner")
        self.assertTrue(p.aws.call.call_args.kwargs["consistent_read"])
        self.assertTrue(p.aws.call.call_args.kwargs["no_paginate"])
        self.assertEqual(100, p.aws.call.call_args.kwargs["limit"])

    def test_worker_pause_rejects_foreign_mapping_and_restores_after_uncertain_disable(self):
        for wrong in ("version", "source", "state", "disable-response"):
            with self.subTest(wrong=wrong):
                p = producer()
                p.config["placement"] = "FUNCTION"
                owned = SimpleNamespace(version=lambda *args: VERSION, owns=lambda value, kind: value,
                    manifest={"mappingIds": {"work": "worker-map"}, "queueUrls": {"work": "work-queue"}})
                original = {"State": "Enabled", "FunctionArn": VERSION, "EventSourceArn": "queue-arn"}
                mapping = original | ({"FunctionArn": "foreign"} if wrong == "version" else
                                      {"EventSourceArn": "foreign"} if wrong == "source" else
                                      {"State": "Disabled"} if wrong == "state" else {})
                def aws(service, operation, **args):
                    if operation == "get-queue-attributes": return {"Attributes": {"QueueArn": "queue-arn"}}
                    if operation == "get-event-source-mapping": return mapping if p.aws.call.call_count == 2 else original
                    if operation == "update-event-source-mapping" and args.get("no_enabled"):
                        raise RuntimeError("lost disable response")
                    return {}
                p.aws.call.side_effect = aws
                with self.assertRaises(RuntimeError if wrong == "disable-response" else AssertionError):
                    with FAULTS.paused_worker(p, owned, "work"):
                        self.fail("invalid setup must not enter fault cut")
                mutations = [call.kwargs for call in p.aws.call.call_args_list if call.args[1] == "update-event-source-mapping"]
                self.assertEqual([{"uuid": "worker-map", "no_enabled": True}, {"uuid": "worker-map", "enabled": True}]
                                 if wrong == "disable-response" else [], mutations)

    def test_compute_continuation_has_no_pause_control_and_never_substitutes(self):
        p = producer()
        p.config["placement"] = "COMPUTE"
        with self.assertRaisesRegex(NotImplementedError, "COMPUTE.*pause"):
            FAULTS.suspended_continuation(p, SimpleNamespace())
        with self.assertRaisesRegex(NotImplementedError, "COMPUTE.*pause"):
            with FAULTS.paused_worker(p, SimpleNamespace(), "transition"):
                self.fail("COMPUTE cannot use Lambda mapping")
        p.aws.call.assert_not_called()
        p.aws.invoke.assert_not_called()
        p.open_execution.assert_not_called()

    def test_nonterminal_loss_refuses_await_or_foreign_recovery_before_stop(self):
        from contextlib import nullcontext
        for wrong in ("await", "locator"):
            with self.subTest(wrong=wrong):
                p = producer()
                p.provider = Mock(return_value={"DurableExecutionArn": PROVIDER})
                recovery = REGISTRATION | row(record_type="RECOVERY", provider_execution_arn="foreign" if wrong == "locator" else PROVIDER)
                owned = SimpleNamespace(provider=lambda value: value, version=Mock())
                with patch.object(FAULTS, "paused_worker", return_value=nullcontext()), \
                     patch.object(FAULTS, "start_native", return_value=SESSION | {"start": {"durableExecutionName": "actual-start"}}), \
                     patch.object(FAULTS, "assert_single_execution", return_value={}), \
                     patch.object(FAULTS, "native_rows", return_value=[REGISTRATION] if wrong == "await" else []), \
                     patch.object(FAULTS, "item", return_value=recovery):
                    with self.assertRaises(AssertionError): FAULTS.nonterminal_loss(p, owned)
                self.assertNotIn("stop-durable-execution", [call.args[1] for call in p.aws.call.call_args_list])
                p.aws.invoke.assert_not_called()

    def test_item_continuation_requires_retry_child_lineage_and_native_ack(self):
        from contextlib import nullcontext
        for wrong in (None, "no-retry", "child", "ack", "provider-pin", "order"):
            with self.subTest(wrong=wrong):
                p = producer()
                p.config.update(placement="FUNCTION", itemDeployment={"stack": "item-stack"})
                p.bundle, p.seed, p.preflight = Path("unused"), 1, Mock()
                p.parameters["AwaitCompletionLifecycleTable"] = "lifecycle"
                p.provider = Mock(return_value={"DurableExecutionArn": PROVIDER})
                class Fixture(SimpleNamespace):
                    def __init__(self, *args): self.__dict__.update(p.__dict__)
                source = Fixture()
                initial = [row(interaction_id="item-" + str(index), unit_id="actual-qualified-unit", item_index=index,
                    execution_id="semantic-original", correlation_id="correlation-" + str(index)) for index in range(3)]
                pending = [entry | row(status="COMPLETED", continuation_attempt=1 if wrong == "no-retry" else 2) for entry in initial]
                final = [entry | {"continuation_completed": {"BOOL": True}} for entry in pending]
                calls = iter([initial, pending, final])
                unit = row(unit_id="actual-qualified-unit", completed_item_count=3, expected_item_count=3)
                unit["dispatch_complete"] = {"BOOL": True}
                unit["completed_item_keys"] = {"SS": ["continuation:" + str(index) for index in range(3)]}
                parent = row(execution_id="semantic-original", execution_key="parent-key", status="SUCCEEDED")
                children = [row(execution_id="child-" + str(index), execution_key="parent-key:await-item:actual-qualified-unit:" + str(index),
                                status="SUCCEEDED") for index in range(3)]
                for record in [parent, *children]:
                    record.update(row(pipeline_id="generated-pipeline", contract_version="compiler-contract", release_version="release-pinned"))
                if wrong == "child": children[0] = children[0] | row(execution_key="foreign")
                def native(producer, table, tenant):
                    if table == "AwaitInteractionTable": return next(calls)
                    if table == "AwaitUnitTable": return [unit]
                    return [parent, *children]
                ack = row(record_type="AWAIT_UNIT_RELEASE_ACKNOWLEDGEMENT", unit_id="actual-qualified-unit",
                    execution_id="semantic-original", tenant_id=SESSION["tenant"], disposition="CONTINUATION_QUEUED", pipeline_id="generated-pipeline",
                    contract_version="compiler-contract", release_version="foreign" if wrong == "ack" else "release-pinned")
                result = [{"request": "approved-" + str(index)} for index in range(3)]
                if wrong == "order": result = [result[1], result[0], result[2]]
                p.action.side_effect = lambda operation, *args: {"executionStatus": "WAITING_EXTERNAL", "awaitCheckpoint": {"interactionId": "item-0"}} if operation == "STATUS" else {"resultJson": json.dumps(result)}
                output = {"tenantId": SESSION["tenant"], "executionId": SESSION["executionId"], "pipelineId": "generated-pipeline",
                    "contractVersion": "compiler-contract", "releaseVersion": "foreign" if wrong == "provider-pin" else "release-pinned",
                    "generation": 1, "resultJson": json.dumps(result)}
                p.aws.call.return_value = {"Status": "SUCCEEDED", "Result": json.dumps(output)}
                with patch.object(FAULTS, "OwnedResources", return_value=SimpleNamespace()), \
                     patch.object(FAULTS, "scheduled_due_work", return_value={"actualSchedule": True}), \
                     patch.object(FAULTS, "start_native", return_value=SESSION | {"start": {"durableExecutionName": "actual-start"}}), \
                     patch.object(FAULTS, "parked", return_value=(REGISTRATION, PROVIDER)), \
                     patch.object(FAULTS, "paused_worker", return_value=nullcontext()), \
                     patch.object(FAULTS, "paused_mappings", return_value=nullcontext()), \
                     patch.object(FAULTS, "native_rows", side_effect=native), \
                     patch.object(FAULTS, "bounded_history", return_value={"Events": [{"EventType": "CallbackStarted"}]}), \
                     patch.object(FAULTS, "item", side_effect=[{}, ack]):
                    if wrong:
                        with self.assertRaises(AssertionError): FAULTS.suspended_continuation(source, SimpleNamespace())
                        self.assertFalse(any(call.args[0] == "suspended-item-continuation-outcome" for call in p.aws.capture.call_args_list))
                    else:
                        FAULTS.suspended_continuation(source, SimpleNamespace())
                        outcome = next(call.args[1] for call in p.aws.capture.call_args_list if call.args[0] == "suspended-item-continuation-outcome")
                        self.assertFalse(outcome["callerSweep"])
                        self.assertFalse(outcome["downstreamBusinessSuffix"])
                self.assertNotIn("SWEEP", [call.args[0] for call in p.action.call_args_list])

    def test_actual_stream_record_does_not_starve_second_shard_behind_open_empty_first(self):
        for include_location in (False, True):
            with self.subTest(include_location=include_location):
                p = producer()
                owned = SimpleNamespace(owns=lambda target, kind: target)
                authentic = {"eventID": "actual-second-shard-event", "eventName": "INSERT",
                             "dynamodb": {"Keys": {"pk": REGISTRATION["pk"], "sk": REGISTRATION["sk"]},
                                          "NewImage": REGISTRATION, "SequenceNumber": "500"}}
                def aws(service, operation, **arguments):
                    if operation == "describe-table": return {"Table": {"LatestStreamArn": "actual-stream"}}
                    if operation == "describe-stream":
                        return {"StreamDescription": {"Shards": [{"ShardId": "first"}, {"ShardId": "second"}]}}
                    if operation == "get-shard-iterator": return {"ShardIterator": arguments["shard_id"] + "-iterator"}
                    if operation == "get-records":
                        if arguments["shard_iterator"] == "second-iterator":
                            return {"Records": [authentic], "NextShardIterator": None}
                        return {"Records": [], "NextShardIterator": "first-iterator-next"}
                    raise AssertionError(operation)
                p.aws.call.side_effect = aws
                actual = FAULTS.actual_registration_record(p, owned, REGISTRATION, include_location)
                expected = authentic | {"eventSourceARN": "actual-stream"}
                self.assertEqual({"streamArn": "actual-stream", "shardId": "second", "record": expected}
                                 if include_location else expected, actual)
                reads = [call for call in p.aws.call.call_args_list if call.args[1] == "get-records"]
                self.assertEqual(["first-iterator", "second-iterator"],
                                 [call.kwargs["shard_iterator"] for call in reads])

    def test_actual_stream_record_follows_empty_pages_with_live_iterators(self):
        for empty_pages in (1, 3):
            for include_location in (False, True):
                with self.subTest(empty_pages=empty_pages, include_location=include_location):
                    p = producer()
                    owned = SimpleNamespace(owns=lambda target, kind: target)
                    authentic = {"eventID": "actual-provider-event", "eventName": "INSERT",
                                 "dynamodb": {"Keys": {"pk": REGISTRATION["pk"], "sk": REGISTRATION["sk"]},
                                              "NewImage": REGISTRATION, "SequenceNumber": "500"}}
                    p.aws.call.side_effect = [
                        {"Table": {"LatestStreamArn": "actual-stream"}},
                        {"StreamDescription": {"Shards": [{"ShardId": "actual-shard"}]}},
                        {"ShardIterator": "iterator-0"},
                        *[{"Records": [], "NextShardIterator": "iterator-" + str(index + 1)}
                          for index in range(empty_pages)],
                        {"Records": [authentic], "NextShardIterator": "after-record"},
                    ]
                    actual = FAULTS.actual_registration_record(p, owned, REGISTRATION, include_location)
                    expected = authentic | {"eventSourceARN": "actual-stream"}
                    self.assertEqual({"streamArn": "actual-stream", "shardId": "actual-shard", "record": expected}
                                     if include_location else expected, actual)
                    reads = [call for call in p.aws.call.call_args_list if call.args[1] == "get-records"]
                    self.assertEqual(["iterator-" + str(index) for index in range(empty_pages + 1)],
                                     [call.kwargs["shard_iterator"] for call in reads])
                    p.aws.capture.assert_called_once_with("actual-registration-stream-record", expected)

    def test_actual_stream_record_stops_at_closed_iterator_and_global_read_budget(self):
        for closed in (True, False):
            with self.subTest(closed=closed):
                p = producer()
                owned = SimpleNamespace(owns=lambda target, kind: target)
                def aws(service, operation, **arguments):
                    if operation == "describe-table": return {"Table": {"LatestStreamArn": "actual-stream"}}
                    if operation == "describe-stream":
                        return {"StreamDescription": {"Shards": [{"ShardId": "actual-shard"}, {"ShardId": "other-shard"}]}}
                    if operation == "get-shard-iterator":
                        return {"ShardIterator": "iterator-0" if arguments["shard_id"] == "actual-shard" else "other-iterator"}
                    if operation == "get-records":
                        return {"Records": [], "NextShardIterator": None if closed else arguments["shard_iterator"] + "-next"}
                    raise AssertionError(operation)
                p.aws.call.side_effect = aws
                with self.assertRaisesRegex(NotImplementedError, "unavailable" if closed else "read budget exhausted"):
                    FAULTS.actual_registration_record(p, owned, REGISTRATION)
                reads = [call for call in p.aws.call.call_args_list if call.args[1] == "get-records"]
                self.assertEqual(2 if closed else 100, len(reads))
                if closed:
                    self.assertEqual(["iterator-0", "other-iterator"],
                                     [call.kwargs["shard_iterator"] for call in reads])
                else:
                    self.assertEqual(50, sum(call.kwargs["shard_iterator"].startswith("iterator-0") for call in reads))
                    self.assertEqual(50, sum(call.kwargs["shard_iterator"].startswith("other-iterator") for call in reads))
                p.aws.capture.assert_not_called()

    def callback_fixture(self):
        p = producer()
        native = row(execution_id="semantic-original", status="WAITING_EXTERNAL", pipeline_id="generated-pipeline",
                     contract_version="compiler-contract", release_version="release-pinned")
        interaction = row(interaction_id="first", status="DISPATCHED", deadline_epoch_ms=300000)
        p.rows.side_effect = lambda table, tenant: [native] if table == "ExecutionTable" else [interaction]
        replacement_arn = VERSION + "/durable-execution/replacement/start"
        replacement = REGISTRATION | row(generation=2, provider_execution_arn=replacement_arn, provider_callback_id="new-callback")
        opened = {"Id": "actual-operation", "EventType": "CallbackStarted", "Name": "await-completion-0",
                  "CallbackStartedDetails": {"CallbackId": "actual-callback"}}
        error = {"ErrorType": "ConformanceProviderCallbackClosed", "ErrorMessage": "fixture external callback failure: conformance-owner"}
        closed = {"Id": "actual-operation", "EventType": "CallbackFailed", "CallbackFailedDetails": {"Error": {"Payload": error}}}
        owned = SimpleNamespace(version=lambda *args: VERSION, provider=lambda arn: arn)
        def aws(service, operation, **arguments):
            return {"Status": "RUNNING" if p.aws.call.call_count == 1 else "FAILED"} if operation == "get-durable-execution" else {}
        p.aws.call.side_effect = aws
        return p, owned, opened, closed, replacement, replacement_arn

    def test_closed_callback_requires_authentic_provider_history_and_native_deadline_before_mutation(self):
        for wrong in ("callback", "binding", "history", "closed", "timed-out", "deadline"):
            with self.subTest(wrong=wrong):
                p, owned, opened, closed, replacement, _ = self.callback_fixture()
                registration_row = REGISTRATION | row(provider_callback_id="foreign") if wrong == "callback" else REGISTRATION
                if wrong == "binding": p.binding.return_value = BINDING | row(provider_callback_id="foreign")
                if wrong == "history": opened = opened | {"CallbackStartedDetails": {"CallbackId": "foreign"}}
                if wrong == "timed-out":
                    p.rows.side_effect = lambda table, tenant: [row(execution_id="semantic-original", status="WAITING_EXTERNAL",
                        pipeline_id="generated-pipeline", contract_version="compiler-contract", release_version="release-pinned")]
                with patch.object(FAULTS, "parked", return_value=(registration_row, PROVIDER)), \
                     patch.object(FAULTS, "registration", return_value={}), \
                     patch.object(FAULTS, "bounded_history", return_value={"Events": [opened, closed] if wrong == "closed" else [opened]}), \
                     patch.object(FAULTS.time, "time", return_value=400 if wrong == "deadline" else 100):
                    with self.assertRaises(AssertionError): FAULTS.closed_callback(p, owned)
                self.assertNotIn("send-durable-execution-callback-failure", [call.args[1] for call in p.aws.call.call_args_list])
                p.aws.invoke.assert_not_called()

    def test_closed_callback_recovery_requires_exact_failure_cause_and_same_generation_two_await(self):
        for wrong in (None, "cause", "replacement", "deadline-after-fault"):
            with self.subTest(wrong=wrong):
                p, owned, opened, closed, replacement, replacement_arn = self.callback_fixture()
                if wrong == "cause": closed = closed | {"CallbackFailedDetails": {"Error": {"Payload": {"ErrorType": "unrelated"}}}}
                if wrong == "replacement": replacement = replacement | row(interaction_id="foreign")
                with patch.object(FAULTS, "parked", return_value=(REGISTRATION, PROVIDER)), \
                     patch.object(FAULTS, "registration", side_effect=[{}, replacement]), \
                     patch.object(FAULTS, "bounded_history", side_effect=[{"Events": [opened]}, {"Events": [opened, closed]}]), \
                     patch.object(FAULTS.time, "time", side_effect=[100, 400] if wrong == "deadline-after-fault" else [100, 100, 100]), \
                     patch.object(FAULTS, "complete_remaining") as complete, patch.object(FAULTS, "assert_success") as success:
                    if wrong:
                        with self.assertRaises(AssertionError): FAULTS.closed_callback(p, owned)
                        success.assert_not_called()
                    else:
                        FAULTS.closed_callback(p, owned)
                        complete.assert_called_once_with(p, SESSION)
                        success.assert_called_once_with(p, SESSION, replacement_arn, 2)
                        self.assertEqual(PROVIDER, p.aws.invoke.call_args.args[1]["detail"]["durableExecutionArn"])
                        diagnostic = next(call.args[1] for call in p.aws.capture.call_args_list
                                          if call.args[0] == "closed-callback-diagnostic-outcome")
                        self.assertEqual("INJECTED_CALLBACK_FAILURE", diagnostic["faultVariant"])
                        self.assertIs(False, diagnostic["elapsedExpiry"])
                        self.assertEqual("NOT_IMPLEMENTED", diagnostic["originalScenarioStatus"])
                callback_calls = [call for call in p.aws.call.call_args_list if call.args[1] == "send-durable-execution-callback-failure"]
                self.assertEqual(1, len(callback_calls))
                self.assertEqual("actual-callback", callback_calls[0].kwargs["callback_id"])

    def stream_fixture(self, decoder_fails=True, unrelated=False, uncertain=False):
        p = producer()
        state = {}
        location = {"streamArn": "binding-stream", "shardId": "actual-shard", "record": {
            "eventSourceARN": "binding-stream", "eventName": "INSERT", "dynamodb": {"SequenceNumber": "500"}}}
        document = {"requestContext": {"functionArn": VERSION, "condition": "RetryAttemptsExhausted"},
                    "responseContext": {"executedVersion": "7"}, "DDBStreamBatchInfo": {"streamArn": "binding-stream",
                    "shardId": "actual-shard", "startSequenceNumber": "499", "endSequenceNumber": "501"}}
        mapping = {"State": "Enabled", "FunctionArn": VERSION, "EventSourceArn": "binding-stream",
                   "DestinationConfig": {"OnFailure": {"Destination": "failure-queue-arn"}},
                   "FunctionResponseTypes": ["ReportBatchItemFailures"], "MaximumRetryAttempts": 5}
        owned = SimpleNamespace(owns=lambda target, kind: target, logical=lambda *args: "mechanical-table",
            version=lambda *args: VERSION, manifest={"queueUrls": {"streamFailure": "owned-failure-queue"},
                                                     "mappingIds": {"callbackBinding": "mapping-b"}})
        def aws(service, operation, **arguments):
            if operation == "describe-table": return {"Table": {"LatestStreamArn": "binding-stream"}}
            if operation == "get-queue-attributes": return {"Attributes": {"QueueArn": "failure-queue-arn"}}
            if operation == "get-event-source-mapping": return mapping
            if operation == "put-item":
                state.update(arguments["item"])
                if uncertain: raise RuntimeError("put response lost after commit")
                return {}
            if operation == "get-item": return {"Item": dict(state)} if state else {}
            if operation == "delete-item": state.clear(); return {}
            if operation == "receive-message":
                event = document | {"DDBStreamBatchInfo": document["DDBStreamBatchInfo"] | {"shardId": "unrelated"}} if unrelated else document
                return {"Messages": [{"MessageId": "retained-real-failure", "Body": json.dumps(event)}]}
            raise AssertionError(operation)
        p.aws.call.side_effect = aws
        p.aws.invoke.return_value = {"batchItemFailures": [{"itemIdentifier": "500"}] if decoder_fails else []}
        return p, owned, location, document, state, mapping

    def test_retained_stream_failure_correlates_exact_source_shard_sequence_and_numbered_handler(self):
        _, _, location, document, _, _ = self.stream_fixture()
        self.assertTrue(FAULTS.retained_stream_failure(document, location, VERSION))
        mutations = [document | {"requestContext": document["requestContext"] | {"functionArn": VERSION.replace(":7", ":8")}},
                     document | {"responseContext": {"executedVersion": "8"}},
                     document | {"requestContext": document["requestContext"] | {"condition": "RecordAgeExceeded"}},
                     document | {"DDBStreamBatchInfo": document["DDBStreamBatchInfo"] | {"streamArn": "foreign"}},
                     document | {"DDBStreamBatchInfo": document["DDBStreamBatchInfo"] | {"shardId": "foreign"}},
                     document | {"DDBStreamBatchInfo": document["DDBStreamBatchInfo"] | {"endSequenceNumber": "498"}},
                     document | {"DDBStreamBatchInfo": None}]
        for mutation in mutations:
            with self.subTest(mutation=mutation): self.assertFalse(FAULTS.retained_stream_failure(mutation, location, VERSION))

    def test_real_decoder_failure_retains_correlated_evidence_and_cleans_only_its_mechanical_row(self):
        p, owned, location, _, state, _ = self.stream_fixture()
        with patch.object(FAULTS, "actual_registration_record", return_value=location):
            FAULTS.retained_decoder_failure(p, owned)
        self.assertEqual({}, state)
        calls = p.aws.call.call_args_list
        self.assertEqual(["put-item", "delete-item"], [call.args[1] for call in calls if call.args[1] in {"put-item", "delete-item"}])
        inserted = next(call.kwargs for call in calls if call.args[1] == "put-item")
        self.assertEqual("mechanical-table", inserted["table_name"])
        self.assertEqual("REGISTRATION", inserted["item"]["record_type"]["S"])
        self.assertNotIn("tenant_id", inserted["item"])
        removed = next(call.kwargs for call in calls if call.args[1] == "delete-item")
        self.assertIn("conformance_fault_owner", removed["condition_expression"])
        self.assertNotIn("delete-message", [call.args[1] for call in calls])
        p.complete.assert_not_called()
        self.assertTrue(any(call.args[0] == "retained-stream-fault-adaptation" for call in p.aws.capture.call_args_list))

    def test_decoder_success_unrelated_dlq_or_uncertain_put_never_pass_and_restore_own_fault_row(self):
        for wrong in ("decoder-success", "unrelated-dlq", "uncertain-put"):
            with self.subTest(wrong=wrong):
                p, owned, location, _, state, _ = self.stream_fixture(decoder_fails=wrong != "decoder-success",
                    unrelated=wrong == "unrelated-dlq", uncertain=wrong == "uncertain-put")
                with patch.object(FAULTS, "actual_registration_record", return_value=location):
                    with self.assertRaises((AssertionError, RuntimeError)): FAULTS.retained_decoder_failure(p, owned)
                self.assertEqual({}, state)
                self.assertFalse(any(call.args[0] == "retained-stream-failure-record" for call in p.aws.capture.call_args_list))
                reads = [call for call in p.aws.call.call_args_list if call.args[1] == "receive-message"]
                self.assertEqual(120 if wrong == "unrelated-dlq" else 0, len(reads))

    def test_foreign_stream_mapping_is_rejected_before_any_fault_row_write(self):
        p, owned, location, _, _, mapping = self.stream_fixture()
        mapping["FunctionArn"] = VERSION.replace(":7", ":8")
        with self.assertRaisesRegex(AssertionError, "real owned Stream decoder"):
            FAULTS.retained_decoder_failure(p, owned)
        self.assertNotIn("put-item", [call.args[1] for call in p.aws.call.call_args_list])

    def test_actual_parked_callback_on_second_native_history_page_is_observed_completely(self):
        p = producer()
        callback = {"EventType": "CallbackStarted", "Name": "await-completion-0",
                    "CallbackStartedDetails": {"CallbackId": "actual-callback"}}
        p.aws.call.side_effect = [{"Item": REGISTRATION},
                                 {"Events": [{"EventType": "ExecutionStarted"}], "NextMarker": "opaque/+=="},
                                 {"Events": [callback]}]
        owned = SimpleNamespace(owns=lambda target, kind: target, provider=lambda arn: arn)
        self.assertEqual((REGISTRATION, PROVIDER), FAULTS.parked(p, SESSION, owned))
        calls = [call for call in p.aws.call.call_args_list if call.args[1] == "get-durable-execution-history"]
        self.assertEqual(2, len(calls))
        expected = {"DurableExecutionArn": PROVIDER, "IncludeExecutionData": True, "MaxItems": 100}
        self.assertEqual(expected, calls[0].kwargs["cli_input_json"])
        self.assertEqual(expected | {"Marker": "opaque/+=="}, calls[1].kwargs["cli_input_json"])
        self.assertTrue(all(call.kwargs["no_paginate"] is True for call in calls))
        p.binding.assert_called_once_with(SESSION["first"])

    def test_later_history_page_terminal_callback_prevents_false_parked_result(self):
        for terminal in ("CallbackSucceeded", "CallbackFailed", "CallbackTimedOut"):
            with self.subTest(terminal=terminal):
                p = producer()
                p.aws.call.side_effect = [{"Item": REGISTRATION},
                    {"Events": [{"EventType": "CallbackStarted", "Name": "await-completion-0"}], "NextMarker": "later"},
                    {"Events": [{"EventType": terminal, "Name": "await-completion-0"}]}]
                owned = SimpleNamespace(owns=lambda target, kind: target, provider=lambda arn: arn)
                with self.assertRaisesRegex(AssertionError, "unresolved provider callback"):
                    FAULTS.parked(p, SESSION, owned)
                p.binding.assert_not_called()

    def test_history_repeated_or_invalid_marker_never_returns_partial_callback_evidence(self):
        callback = {"EventType": "CallbackStarted", "Name": "await-completion-0"}
        p = producer()
        p.aws.call.side_effect = [{"Events": [callback], "NextMarker": "same"},
                                 {"Events": [], "NextMarker": "same"}]
        with self.assertRaisesRegex(AssertionError, "invalid or repeated native history marker"):
            FAULTS.bounded_history(p, PROVIDER)
        self.assertEqual(2, p.aws.call.call_count)
        for marker in (False, 0, [], {}, "   "):
            with self.subTest(marker=marker):
                p = producer()
                p.aws.call.return_value = {"Events": [callback], "NextMarker": marker}
                with self.assertRaisesRegex(AssertionError, "invalid or repeated native history marker"):
                    FAULTS.bounded_history(p, PROVIDER)
                self.assertEqual(1, p.aws.call.call_count)

    def test_history_page_budget_never_promotes_incomplete_callback_history(self):
        p = producer()
        p.aws.call.side_effect = [{"Events": [{"EventType": "CallbackStarted", "Name": "await-completion-0"}], "NextMarker": "page-two"},
                                 {"Events": [], "NextMarker": "unread-third-page"}]
        with self.assertRaisesRegex(NotImplementedError, "complete callback state is unproved"):
            FAULTS.bounded_history(p, PROVIDER, max_pages=2)
        self.assertEqual(2, p.aws.call.call_count)

    def test_resource_guard_accepts_exact_owned_stack_and_rejects_foreign_or_non_disposable(self):
        p = producer()
        stack = {"StackId": STACK, "Tags": [{"Key": "tpf-conformance-disposable", "Value": "true"},
                   {"Key": "tpf-conformance-owner", "Value": "run-owner"}]}
        resources = [{"PhysicalResourceId": VERSION, "ResourceType": "AWS::Lambda::Version", "LogicalResourceId": "DurableVersion"}]
        p.aws.call.side_effect = [{"Stacks": [stack]}, {"StackResourceSummaries": resources}]
        owned = FAULTS.OwnedResources(p)
        self.assertEqual(PROVIDER, owned.provider(PROVIDER))
        with self.assertRaisesRegex(AssertionError, "another release"):
            owned.provider(PROVIDER.replace(":7/", ":8/"))
        with self.assertRaisesRegex(AssertionError, "outside"):
            owned.owns("shared-table", "AWS::DynamoDB::Table")
        p.config["resourceFaults"]["disposable"] = False
        p.aws.call.reset_mock()
        with self.assertRaisesRegex(AssertionError, "explicit disposable"):
            FAULTS.run(p, "16")
        p.aws.call.assert_not_called()
        p.open_execution.assert_not_called()

    def test_mapping_fault_restores_after_body_failure_and_uncertain_disable_response(self):
        for uncertain in (False, True):
            with self.subTest(uncertain=uncertain):
                p = producer()
                state = {"mapping-a": "Enabled"}
                snapshot = {"FunctionArn": VERSION, "EventSourceArn": "actual-stream"}

                def aws(service, operation, **arguments):
                    uuid = arguments["uuid"]
                    if operation == "update-event-source-mapping":
                        state[uuid] = "Enabled" if arguments.get("enabled") else "Disabled"
                        if uncertain and arguments.get("no_enabled"):
                            raise RuntimeError("transport response lost after update")
                        return {}
                    return snapshot | {"State": state[uuid]}

                p.aws.call.side_effect = aws
                owned = SimpleNamespace(manifest={"mappingIds": {"awaitState": "mapping-a"}},
                                        owns=lambda target, kind: target, version=lambda *args: VERSION)
                with self.assertRaises(RuntimeError):
                    with FAULTS.paused_mappings(p, owned, ["awaitState"]):
                        self.assertEqual("Disabled", state["mapping-a"])
                        raise RuntimeError("production observation failed")
                self.assertEqual("Enabled", state["mapping-a"])
                self.assertTrue(any(call.args[0] == "resource-fault-restored" for call in p.aws.capture.call_args_list))

    def test_ownership_failure_before_disable_performs_no_resource_mutation(self):
        p = producer()
        p.aws.call.side_effect = [{"Table": {"LatestStreamArn": "binding-stream"}},
                                 {"State": "Enabled", "FunctionArn": VERSION, "EventSourceArn": "actual-stream"}]
        def owns(target, kind):
            if target == "foreign":
                raise AssertionError("outside owned resources")
            return target
        owned = SimpleNamespace(manifest={"mappingIds": {"awaitState": "mapping-a", "callbackBinding": "foreign"}},
                                owns=owns, version=lambda *args: VERSION)
        with self.assertRaisesRegex(AssertionError, "outside"):
            with FAULTS.paused_mappings(p, owned, ["awaitState", "callbackBinding"]):
                self.fail("unsafe procedure body was entered")
        self.assertEqual(["describe-table", "get-event-source-mapping"], [call.args[1] for call in p.aws.call.call_args_list])

    def test_owned_mapping_with_wrong_role_target_or_source_is_rejected_before_any_disable(self):
        for wrong in ("function", "source"):
            with self.subTest(wrong=wrong):
                p = producer()
                p.aws.call.side_effect = [{"Table": {"LatestStreamArn": "binding-stream"}},
                    {"State": "Enabled", "FunctionArn": VERSION, "EventSourceArn": "actual-stream"},
                    {"State": "Enabled", "FunctionArn": "other-version" if wrong == "function" else VERSION,
                     "EventSourceArn": "wrong-stream" if wrong == "source" else "binding-stream"}]
                owned = SimpleNamespace(manifest={"mappingIds": {"awaitState": "mapping-a", "callbackBinding": "mapping-b"}},
                                        owns=lambda target, kind: target, version=lambda *args: VERSION)
                with self.assertRaisesRegex(AssertionError, "pinned production Await role and source"):
                    with FAULTS.paused_mappings(p, owned, ["awaitState", "callbackBinding"]):
                        self.fail("unsafe procedure body was entered")
                self.assertNotIn("update-event-source-mapping", [call.args[1] for call in p.aws.call.call_args_list])

    def test_missing_registration_deletes_only_exact_mechanical_rows_and_rejects_semantic_row(self):
        p = producer()
        owned = SimpleNamespace(owns=lambda target, kind: target)
        p.aws.call.return_value = {}
        FAULTS.delete_callback_rows(p, owned, SESSION, REGISTRATION)
        deletions = [call for call in p.aws.call.call_args_list if call.args[1] == "delete-item"]
        self.assertEqual(2, len(deletions))
        self.assertEqual({"REGISTRATION#00000000000000000001", "GEN#00000000000000000001"},
                         {call.kwargs["key"]["sk"]["S"] for call in deletions})
        for call in deletions:
            self.assertEqual("mechanical-table", call.kwargs["table_name"])
            self.assertIn("provider_callback_id", call.kwargs["condition_expression"])
        p.aws.call.reset_mock()
        with self.assertRaisesRegex(AssertionError, "exact disposable"):
            FAULTS.delete_callback_rows(p, owned, SESSION, REGISTRATION | {"record_type": {"S": "EXECUTION"}})
        p.aws.call.assert_not_called()

    def test_success_oracle_accepts_valid_control_and_rejects_duplicate_admission_wrong_result_or_release(self):
        expected_result = {"request": "second-approved"}
        native = row(execution_id="semantic-original", status="SUCCEEDED", pipeline_id="generated-pipeline",
                     contract_version="compiler-contract", release_version="release-pinned")
        for wrong in (None, "duplicate", "result", "release"):
            with self.subTest(wrong=wrong):
                p = producer()
                result = {"request": "wrong"} if wrong == "result" else expected_result
                p.action.side_effect = [{"executionStatus": "SUCCEEDED"}, {"resultJson": json.dumps(result)}]
                output = {"tenantId": "conformance-owner", "executionId": "semantic-original", "generation": 2,
                          "pipelineId": "generated-pipeline", "contractVersion": "compiler-contract",
                          "releaseVersion": "foreign" if wrong == "release" else "release-pinned", "resultJson": json.dumps(result)}
                p.aws.call.return_value = {"Status": "SUCCEEDED", "Result": json.dumps(output)}
                p.rows.side_effect = [[row(interaction_id="first", status="COMPLETED"), row(interaction_id="second", status="COMPLETED")],
                                     [native, native] if wrong == "duplicate" else [native]]
                if wrong:
                    with self.assertRaises(AssertionError):
                        FAULTS.assert_success(p, SESSION, PROVIDER, 2)
                else:
                    FAULTS.assert_success(p, SESSION, PROVIDER, 2)
                    self.assertEqual("resource-fault-final-state", p.aws.capture.call_args.args[0])

    def test_missed_stream_cannot_pass_if_callback_delivered_without_reconciliation(self):
        p = producer()
        p.rows.return_value = [row(interaction_id="first", status="COMPLETED")]
        p.aws.call.return_value = {"Item": row(provider_outcome="CALLBACK_ACCEPTED")}
        owned = SimpleNamespace(version=lambda *args: "real-wakeup", owns=lambda target, kind: target)
        from contextlib import nullcontext
        with patch.object(FAULTS, "parked", return_value=(REGISTRATION, PROVIDER)), \
             patch.object(FAULTS, "actual_registration_record", return_value={"eventID": "provider-event"}), \
             patch.object(FAULTS, "paused_mappings", return_value=nullcontext()):
            with self.assertRaisesRegex(AssertionError, "did not suppress"):
                FAULTS.missed_stream(p, owned)
        p.aws.invoke.assert_not_called()

    def test_real_timeout_control_requires_native_timeout_and_rejects_provider_only_failure(self):
        for wrong in (None, "provider_only", "unrelated_native_failure", "wrong_timeout_message"):
            with self.subTest(wrong=wrong):
                p = producer()
                first = row(interaction_id="first", status="DISPATCHED", deadline_epoch_ms=110000)
                terminal = row(interaction_id="first", status="DISPATCHED" if wrong == "provider_only" else "TIMED_OUT", deadline_epoch_ms=110000)
                p.rows.side_effect = [[first], [terminal], [row(execution_id="semantic-original", status="FAILED",
                    error_code="UNRELATED_WORKER_FAILURE" if wrong == "unrelated_native_failure" else "AWAIT_TIMEOUT",
                    error_message="Await interaction timed out: another-interaction" if wrong == "wrong_timeout_message"
                                  else "Await interaction timed out: first",
                    pipeline_id="generated-pipeline", contract_version="compiler-contract", release_version="release-pinned")], [terminal]]
                p.action.return_value = {"executionStatus": "FAILED"}
                p.aws.call.side_effect = [{"State": "ENABLED", "ScheduleExpression": "rate(1 minute)"},
                                          {"Attributes": {"QueueArn": "production-due-work-arn"}},
                                          {"Targets": [{"Arn": "production-due-work-arn"}]},
                                          {"State": "Enabled", "FunctionArn": "pinned-production", "EventSourceArn": "production-due-work-arn"},
                                          {"Status": "FAILED", "Error": {"ErrorType": "TPF_FAILED"}}]
                owned = SimpleNamespace(logical=lambda *args: "production-tick", version=lambda *args: "pinned-production",
                    owns=lambda target, kind: target, manifest={"mappingIds": {"dueWork": "production-mapping"}})
                with patch.object(FAULTS, "parked", return_value=(REGISTRATION, PROVIDER)), \
                     patch.object(FAULTS.time, "time", side_effect=[100, 111]):
                    if wrong:
                        message = "rejected expected" if wrong == "provider_only" else "not the admitted Await timeout consequence"
                        with self.assertRaisesRegex(AssertionError, message):
                            FAULTS.timeout(p, "22", owned)
                    else:
                        FAULTS.timeout(p, "22", owned)
                        self.assertEqual("resource-fault-final-state", p.aws.capture.call_args.args[0])
                p.complete.assert_not_called()
                self.assertTrue(all(call.args[0] == "STATUS" for call in p.action.call_args_list))


if __name__ == "__main__":
    unittest.main()
