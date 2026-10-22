"""Offline procedure boundary tests, with all AWS responses substituted.

These reject invalid order/evidence; they cannot certify a deployed application.
"""
import importlib.util
import json
import hashlib
import tempfile
from types import SimpleNamespace
from pathlib import Path
import unittest
from unittest.mock import Mock

SPEC = importlib.util.spec_from_file_location(
    "binding_races", Path(__file__).parents[1] / "scripts/production-binding-races.py")
RACES = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RACES)

STACK = "arn:aws:cloudformation:region:123456789012:stack/disposable/run"
VERSION = "arn:aws:lambda:region:123456789012:function:driver:7"
WAKEUP = "arn:aws:lambda:region:123456789012:function:wakeup:8"
PROVIDER = VERSION + "/durable-execution/run/start"
PINS = {"tenantId": "fresh-tenant", "executionId": "original", "pipelineId": "generated",
        "contractVersion": "contract", "releaseVersion": "pinned"}
FIRST = PINS | {"interactionId": "first", "correlationId": "first-correlation", "stepId": "FirstApproval"}
SECOND = PINS | {"interactionId": "second", "correlationId": "second-correlation", "stepId": "SecondApproval"}


def row(**values):
    return {key: {"N" if isinstance(value, int) else "S": str(value)} for key, value in values.items()}


IDENTITY = {"tenant_id": "fresh-tenant", "execution_id": "original", "interaction_id": "first",
            "correlation_id": "first-correlation"}
RELEASE = {"pipeline_id": "generated", "contract_version": "contract", "release_version": "pinned"}
REGISTRATION = row(**IDENTITY, **RELEASE, record_type="REGISTRATION", generation=1, occurrence=1,
    provider_execution_arn=PROVIDER, provider_callback_id="authentic-callback")
BINDING = row(**IDENTITY, record_type="BINDING", generation=1, occurrence=1,
    provider_execution_arn=PROVIDER, provider_callback_id="authentic-callback")
RECEIPT = row(pk="fresh-tenant#first", sk="DELIVERED#1", record_type="DELIVERY",
    provider_callback_id="authentic-callback", provider_outcome="CALLBACK_ACCEPTED")


class OfflineProducer:
    """Permitted protocol replies only; unknown calls fail instead of reaching AWS."""
    def __init__(self, wrong=None, placement="FUNCTION"):
        self.wrong, self.seed = wrong, 1337
        self.completed, self.terminal = False, False
        self.mapping_states = {"mapping-a": "Enabled", "mapping-b": "Enabled"}
        self.events = []
        self.config = {"stack": "disposable", "placement": placement,
                       "resourceFaults": {"disposable": True, "owner": "owner", "stackId": STACK}}
        self.parameters = {"ReleaseVersion": "pinned", "AwaitInteractionStreamArn": "semantic-stream"}
        self.contract = {"pipelineId": "generated", "contractVersion": "contract"}
        self.manifest = {"stackId": STACK, "functionVersionArns": {"durable": VERSION, "awaitWakeup": WAKEUP},
                         "mappingIds": {"awaitState": "mapping-a", "callbackBinding": "mapping-b"}}
        self.outputs = {"CallbackBindingTableName": "mechanical-table",
                        "DeploymentReleaseManifest": json.dumps(self.manifest)}
        self.aws = Mock(call=Mock(side_effect=self.call), invoke=Mock(side_effect=AssertionError("no direct invoke expected")))
        self.session = {"tenant": "fresh-tenant", "executionId": "original", "first": FIRST,
                        "provider": {"DurableExecutionArn": PROVIDER}}

    def poll(self, observe, accept, timeout=180):
        result = observe()
        if not accept(result):
            raise AssertionError("offline observation rejected by the production procedure")
        return result

    def open_execution(self, scenario):
        assert scenario == "10"
        assert all(state == "Disabled" for state in self.mapping_states.values())
        self.events.append("admission")
        return self.session

    def binding(self, checkpoint):
        if self.wrong == "bound-before-completion" or self.completed and self.wrong == "bound-at-cut":
            return BINDING
        if any(state == "Disabled" for state in self.mapping_states.values()):
            return {}
        return BINDING | row(provider_callback_id="foreign") if self.wrong == "wrong-restored-binding" else BINDING

    def complete(self, checkpoint, request):
        if checkpoint == FIRST:
            assert request == "first-approved"
            assert all(state == "Disabled" for state in self.mapping_states.values())
            self.completed = True
            self.events.append("native-completion")
        else:
            assert checkpoint == SECOND and request == "second-approved"
            assert all(state == "Enabled" for state in self.mapping_states.values())
            self.terminal = True
            self.events.append("second-completion")

    def rows(self, table, tenant):
        assert tenant == "fresh-tenant"
        if table == "AwaitInteractionTable":
            first = row(**IDENTITY, status="COMPLETED" if self.completed else "PENDING")
            if self.wrong == "native-wrong-owner": first |= row(execution_id="foreign")
            if self.wrong == "not-admitted": first |= row(status="PENDING")
            if self.wrong == "duplicate-first": return [first, first]
            return [first, row(tenant_id=tenant, execution_id="original", interaction_id="second", status="COMPLETED")] if self.terminal else [first]
        assert table == "ExecutionTable"
        result = row(tenant_id=tenant, execution_id="original", **RELEASE, status="SUCCEEDED")
        return [result, result] if self.wrong == "duplicate-execution" else [result]

    def action(self, operation, tenant, execution_id):
        assert (tenant, execution_id) == ("fresh-tenant", "original")
        if operation == "READ_EXECUTION_AWAITS":
            checkpoint = FIRST | {"status": "COMPLETED"}
            if self.wrong == "semantic-release": checkpoint["releaseVersion"] = "foreign"
            return {"awaitCheckpoints": [checkpoint]}
        if operation == "STATUS":
            return {"executionStatus": "SUCCEEDED"} if self.terminal else {"awaitCheckpoint": SECOND}
        assert operation == "RESULT"
        return {"resultJson": json.dumps({"request": "second-approved"})}

    def call(self, service, operation, **args):
        self.events.append(operation)
        if operation == "describe-stacks":
            return {"Stacks": [{"StackId": STACK, "Tags": [
                {"Key": "tpf-conformance-disposable", "Value": "true"},
                {"Key": "tpf-conformance-owner", "Value": "owner"}]}]}
        if operation == "list-stack-resources":
            entries = [(VERSION, "AWS::Lambda::Version"), (WAKEUP, "AWS::Lambda::Version"),
                       ("mechanical-table", "AWS::DynamoDB::Table"),
                       ("mapping-a", "AWS::Lambda::EventSourceMapping"), ("mapping-b", "AWS::Lambda::EventSourceMapping")]
            return {"StackResourceSummaries": [{"PhysicalResourceId": identity, "ResourceType": kind} for identity, kind in entries]}
        if operation == "get-function-configuration":
            return {"Environment": {"Variables": {"QUARKUS_LAMBDA_HANDLER": "aws-durable-await-wakeup"}}}
        if operation == "describe-table": return {"Table": {"LatestStreamArn": "mechanical-stream"}}
        if operation == "update-event-source-mapping":
            self.mapping_states[args["uuid"]] = "Disabled" if args.get("no_enabled") else "Enabled"
            return {}
        if operation == "get-event-source-mapping":
            return {"State": self.mapping_states[args["uuid"]], "FunctionArn": WAKEUP,
                    "EventSourceArn": "semantic-stream" if args["uuid"] == "mapping-a" else "mechanical-stream"}
        if operation == "get-item":
            key = args["key"]["sk"]["S"]
            if key.startswith("REGISTRATION"):
                result = REGISTRATION
                if self.wrong == "registration-pin": result |= row(release_version="foreign")
                if self.wrong == "registration-changed" and self.completed: result |= row(provider_callback_id="changed")
                return {"Item": result}
            assert key == "DELIVERED#1"
            if self.wrong == "delivered-at-cut" and self.completed: return {"Item": RECEIPT}
            if not self.terminal or self.wrong == "missing-receipt": return {}
            receipt = RECEIPT | row(provider_callback_id="foreign") if self.wrong == "foreign-receipt" else RECEIPT
            if self.wrong == "closed-receipt": receipt |= row(provider_outcome="CLOSED")
            return {"Item": receipt}
        if operation == "get-durable-execution-history":
            events = [{"EventType": "CallbackStarted", "Name": "await-completion-0", "Id": "real-operation",
                       "CallbackStartedDetails": {"CallbackId": "authentic-callback"}}]
            if self.wrong == "foreign-callback": events[0]["CallbackStartedDetails"]["CallbackId"] = "foreign"
            if self.wrong == "callback-resolved": events.append({"EventType": "CallbackSucceeded", "Id": "real-operation"})
            return {"Events": events}
        if operation == "get-durable-execution":
            if not self.terminal: return {"Status": "STOPPED" if self.wrong == "provider-stopped" else "RUNNING"}
            output = PINS | {"generation": 1, "resultJson": json.dumps({"request": "second-approved"})}
            if self.wrong == "final-provider-pin": output["releaseVersion"] = "foreign"
            return {"Status": "SUCCEEDED", "Result": json.dumps(output)}
        raise AssertionError("unexpected substituted boundary: " + service + " " + operation)


class PreRegistrationProducer:
    def __init__(self, root, wrong=None, late_registration=False):
        self.wrong, self.seed, self.late_registration = wrong, 1337, late_registration
        self.tenant = "conformance-pre-registration-provider-loss-7"
        self.first = FIRST | {"tenantId": self.tenant}
        self.second = SECOND | {"tenantId": self.tenant}
        self.pins = RELEASE | {"tenant_id": self.tenant, "execution_id": "original"}
        self.identity = {"tenant_id": self.tenant, "execution_id": "original", "interaction_id": "first", "correlation_id": "first-correlation"}
        self.action_version = VERSION.replace("driver:7", "action:9")
        self.proxy = VERSION.replace("driver:7", "proxy:10")
        self.repair = VERSION.replace("driver:7", "repair:11")
        self.ingress = VERSION.replace("driver:7", "ingress:12")
        self.successor_arn = VERSION + "/durable-execution/successor/start"
        self.faults, self.events = {}, []
        self.started, self.stopped, self.repaired, self.first_completed, self.terminal = False, False, False, False, False
        self.arm = self.tenant + "#REGISTER_CALLBACK#BEFORE_FORWARD"
        self.config = {"stack": "disposable", "actionVersionArn": self.action_version, "faultProxyVersionArn": self.proxy,
                       "faultTable": "fixture-faults", "resourceFaults": {"disposable": True, "owner": "owner", "stackId": STACK}}
        self.parameters = {"ReleaseVersion": "pinned"}
        self.contract = {"pipelineId": "generated", "contractVersion": "contract"}
        self.outputs = {"CallbackBindingTableName": "mechanical-table", "DurableAliasArn": "driver-alias", "IngressFunctionArn": self.ingress,
                       "DeploymentReleaseManifest": json.dumps({"stackId": STACK, "functionVersionArns": {
                           "durable": VERSION, "action": self.action_version, "reconciliation": self.repair}})}
        self.random = SimpleNamespace(getrandbits=lambda _: 7)
        self.aws = SimpleNamespace(root=Path(root), references=[], capture=Mock(), call=Mock(side_effect=self.call), invoke=Mock(side_effect=self.invoke))
        self.old_request = {"operation": "REGISTER_CALLBACK", "tenantId": self.tenant, "executionId": "original",
            "generation": 1, "pipelineId": "generated", "contractVersion": "contract", "releaseVersion": "pinned",
            "providerExecutionArn": PROVIDER, "providerExecutionName": "original-start", "providerCallbackId": "authentic-callback",
            "awaitIdentity": {"tenantId": self.tenant, "executionId": "original", "interactionId": "first",
                              "correlationId": "first-correlation", "generation": 1, "occurrence": 1}}
        self.raw = json.dumps(self.old_request)
        self.successor = row(**self.pins, interaction_id="first", correlation_id="first-correlation", record_type="REGISTRATION",
                             generation=2, occurrence=1, provider_execution_arn=self.successor_arn, provider_callback_id="successor-callback")
        self.binding_two = row(**self.identity, record_type="BINDING", generation=2, occurrence=1,
                               provider_execution_arn=self.successor_arn, provider_callback_id="successor-callback")

    def poll(self, observe, accept, timeout=180):
        value = observe()
        if not accept(value): raise AssertionError("offline observation rejected by production procedure")
        return value

    def release(self, tenant):
        assert tenant == self.tenant
        self.events.append("release-fixture")

    def provider(self, name):
        assert name == "original-start"
        return {"DurableExecutionArn": PROVIDER}

    def rows(self, table, tenant):
        assert tenant == self.tenant
        if table == "ExecutionTable":
            native = row(**self.pins, status="SUCCEEDED" if self.terminal else "WAITING_EXTERNAL")
            return [native, native] if self.wrong == "duplicate-native" else [native]
        assert table == "AwaitInteractionTable"
        first = row(**self.identity, status="COMPLETED" if self.first_completed else "PENDING")
        return [first, row(tenant_id=tenant, execution_id="original", interaction_id="second", status="COMPLETED")] if self.terminal else [first]

    def binding(self, checkpoint):
        return BINDING if self.wrong == "bound-before-cut" else {}

    def action(self, operation, tenant, execution):
        assert (tenant, execution) == (self.tenant, "original")
        if operation == "RESULT": return {"resultJson": json.dumps({"request": "second-approved"})}
        assert operation == "STATUS"
        return {"executionStatus": "SUCCEEDED"} if self.terminal else {
            "executionStatus": "WAITING_EXTERNAL", "awaitCheckpoint": self.second if self.first_completed else self.first}

    def complete(self, checkpoint, request):
        assert self.stopped and self.repaired and self.arm + "#RELEASE" in self.faults
        if checkpoint == self.first:
            assert request == "first-approved"
            self.first_completed = True
        else:
            assert checkpoint == self.second and request == "second-approved"
            self.terminal = True

    def invoke(self, function, payload):
        self.events.append("invoke:" + function)
        if function == self.ingress:
            assert self.faults[self.arm]["mode"] == {"S": "HOLD"}
            self.started = True
            return {"durableExecutionName": "original-start"}
        if function == self.repair:
            assert self.stopped and self.arm + "#RELEASE" in self.faults
            assert payload == {"detail": {"durableExecutionArn": PROVIDER}}
            self.repaired = True
            return {"repaired": True}
        assert function == self.action_version and payload == self.old_request and self.repaired
        if self.wrong == "stale-accepted": return {"bindingCreated": True}
        error_type = "java.io.IOException" if self.wrong == "arbitrary-error" else "org.pipelineframework.aws.durable.AwsDurableStaleGenerationException"
        receipt = {"function": function, "request": payload, "metadata": {"StatusCode": 200, "FunctionError": "Unhandled"},
                   "response": {"errorType": error_type, "errorMessage": "provider generation is no longer current"}}
        if self.wrong == "transport-error": receipt["metadata"] = {"cliError": "transport failed"}
        path = "stale-native-receipt.json"
        (self.aws.root / path).write_text(json.dumps(receipt))
        self.aws.references.append({"path": path, "sha256": "offline-fixture"})
        raise RuntimeError("native FunctionError receipt recorded")

    def call(self, service, operation, **args):
        self.events.append(operation)
        if operation == "describe-stacks":
            return {"Stacks": [{"StackId": STACK, "Tags": [{"Key": "tpf-conformance-disposable", "Value": "true"},
                {"Key": "tpf-conformance-owner", "Value": "owner"}]}]}
        if operation == "list-stack-resources":
            resources = [(VERSION, "AWS::Lambda::Version"), (self.action_version, "AWS::Lambda::Version"),
                         (self.repair, "AWS::Lambda::Version"), ("mechanical-table", "AWS::DynamoDB::Table"),
                         (self.proxy, "AWS::Lambda::Version"), ("fixture-faults", "AWS::DynamoDB::Table")]
            if self.wrong == "foreign-proxy": resources.remove((self.proxy, "AWS::Lambda::Version"))
            if self.wrong == "foreign-fault-table": resources.remove(("fixture-faults", "AWS::DynamoDB::Table"))
            return {"StackResourceSummaries": [{"PhysicalResourceId": identity, "ResourceType": kind}
                                              for identity, kind in resources]}
        if operation == "get-function-configuration":
            function = args["function_name"]
            if function == self.proxy:
                return {"Handler": "org.pipelineframework.awsconformance.faults.ActionTransportFaultProxy::handleRequest",
                        "Environment": {"Variables": {"TPF_CONFORMANCE_TARGET_ACTION_VERSION": self.action_version,
                                                      "TPF_CONFORMANCE_FAULT_TABLE": "fixture-faults"}}}
            if function == "driver-alias":
                return {"Environment": {"Variables": {"TPF_AWS_DURABLE_ACTION_FUNCTION": self.proxy}}}
            return {"Environment": {"Variables": {"QUARKUS_LAMBDA_HANDLER": "aws-durable-action" if function == self.action_version else "aws-durable-reconcile"}}}
        if operation == "put-item":
            fault_id = args["item"]["fault_id"]["S"]
            self.faults[fault_id] = args["item"]
            return {}
        if operation == "get-item":
            if args["table_name"] == "fixture-faults":
                fault_id = args["key"]["fault_id"]["S"]
                if fault_id == self.arm + "#CLAIM":
                    claim = row(fault_id=fault_id, mode="HOLD", point="BEFORE_FORWARD", operation="REGISTER_CALLBACK",
                        target=self.action_version, tenant_id=self.tenant, execution_id="original", generation=1,
                        request_json=self.raw, request_sha256=hashlib.sha256(self.raw.encode()).hexdigest())
                    if self.wrong == "claim-digest": claim |= row(request_sha256="wrong")
                    if self.wrong == "claim-stage": claim |= row(point="AFTER_RESPONSE")
                    if self.wrong == "claim-owner": claim |= row(execution_id="foreign")
                    return {"Item": claim}
                return {"Item": self.faults[fault_id]} if fault_id in self.faults else {}
            key = args["key"]["sk"]["S"]
            if key == "REGISTRATION#00000000000000000001":
                if self.wrong == "registration-present" or self.repaired and self.late_registration:
                    return {"Item": row(**self.pins, record_type="REGISTRATION", generation=1, occurrence=1,
                                        provider_execution_arn=PROVIDER, provider_callback_id="authentic-callback")}
                return {}
            if key.startswith("RECOVERY#"):
                recovery = row(**self.pins, record_type="RECOVERY", generation=1,
                               provider_execution_arn=PROVIDER, provider_execution_name="original-start")
                if self.wrong == "recovery-pin": recovery |= row(release_version="foreign")
                return {"Item": recovery}
            if key == "REGISTRATION#00000000000000000002":
                result = self.successor
                if self.wrong == "successor-pin": result |= row(release_version="foreign")
                return {"Item": result}
            if key == "SUCCESSOR#1": return {"Item": row(record_type="GENERATION_SUCCESSOR", generation=3 if self.wrong == "wrong-fence" else 2)}
            if key == "CLAIM#2": return {"Item": row(record_type="GENERATION_CLAIM", **RELEASE)}
            if key == "GEN#00000000000000000002": return {"Item": self.binding_two}
            assert key == "DELIVERED#1"
            return {"Item": RECEIPT} if self.wrong == "old-delivery" else {}
        if operation == "stop-durable-execution":
            assert self.arm + "#RELEASE" not in self.faults
            self.stopped = True
            return {}
        if operation == "get-durable-execution-history":
            return {"Events": [{"EventType": "CallbackStarted", "Name": "await-completion-0", "Id": "authentic-operation",
                                "CallbackStartedDetails": {"CallbackId": "authentic-callback"}}]}
        if operation == "get-durable-execution":
            if args["durable_execution_arn"] == PROVIDER: return {"Status": "STOPPED" if self.stopped else "RUNNING"}
            assert args["durable_execution_arn"] == self.successor_arn
            output = PINS | {"tenantId": self.tenant, "generation": 2, "resultJson": json.dumps({"request": "second-approved"})}
            return {"Status": "SUCCEEDED", "Result": json.dumps(output)}
        raise AssertionError("unexpected substituted boundary: " + operation)


class ProductionBindingRaceTest(unittest.TestCase):
    def test_foreign_proxy_or_fault_table_rejects_before_any_fixture_or_aws_mutation(self):
        for wrong in ("foreign-proxy", "foreign-fault-table"):
            with self.subTest(wrong=wrong), tempfile.TemporaryDirectory() as directory:
                producer = PreRegistrationProducer(directory, wrong)
                with self.assertRaisesRegex(AssertionError, "outside the owned disposable stack"):
                    RACES.run(producer, "pre-registration-provider-loss")
                self.assertEqual({}, producer.faults)
                self.assertNotIn("release-fixture", producer.events)
                self.assertFalse(any(call.args[1] in {"put-item", "delete-item", "update-item", "stop-durable-execution"}
                                     for call in producer.aws.call.call_args_list))
                producer.aws.invoke.assert_not_called()

    def test_pre_registration_loss_is_cut_before_forward_and_repaired_with_typed_fencing(self):
        for late_registration in (False, True):
            with tempfile.TemporaryDirectory() as directory:
                producer = PreRegistrationProducer(directory, late_registration=late_registration)
                RACES.run(producer, "pre-registration-provider-loss")
                self.assertTrue(producer.terminal)
                self.assertIn(producer.arm + "#RELEASE", producer.faults)
                labels = [call.args[0] for call in producer.aws.capture.call_args_list]
                self.assertIn("pre-registration-provider-loss-cut", labels)
                self.assertIn("stale-registration-challenge", labels)
                self.assertIn("pre-registration-provider-loss-outcome", labels)
                self.assertFalse(any(call.args[1] in {"delete-item", "update-item"} for call in producer.aws.call.call_args_list))

    def test_pre_registration_invalid_cut_or_arbitrary_error_cannot_pass_and_releases_fixture(self):
        for wrong in ("claim-digest", "claim-stage", "claim-owner", "registration-present", "bound-before-cut", "recovery-pin",
                      "duplicate-native", "successor-pin", "wrong-fence", "stale-accepted", "arbitrary-error", "transport-error", "old-delivery"):
            with self.subTest(wrong=wrong), tempfile.TemporaryDirectory() as directory:
                producer = PreRegistrationProducer(directory, wrong)
                with self.assertRaises(AssertionError): RACES.run(producer, "pre-registration-provider-loss")
                self.assertIn(producer.arm + "#RELEASE", producer.faults)
                self.assertNotIn("pre-registration-provider-loss-outcome", [call.args[0] for call in producer.aws.capture.call_args_list])

    def test_completion_order_and_pinned_convergence_for_both_placements(self):
        for placement in ("FUNCTION", "COMPUTE"):
            with self.subTest(placement=placement):
                producer = OfflineProducer(placement=placement)
                RACES.run(producer, "10")
                self.assertEqual({"mapping-a": "Enabled", "mapping-b": "Enabled"}, producer.mapping_states)
                self.assertLess(producer.events.index("admission"), producer.events.index("native-completion"))
                labels = [call.args[0] for call in producer.aws.capture.call_args_list]
                self.assertIn("completion-before-binding-cut", labels)
                self.assertIn("completion-before-binding-outcome", labels)
                producer.aws.invoke.assert_not_called()
                self.assertFalse(any(call.args[1] in {"put-item", "delete-item", "update-item", "send-durable-execution-callback-success"}
                                     for call in producer.aws.call.call_args_list))

    def test_invalid_order_identity_or_final_state_rejects_and_restores_mappings(self):
        for wrong in ("registration-pin", "bound-before-completion", "bound-at-cut", "delivered-at-cut",
                      "foreign-callback", "callback-resolved", "provider-stopped", "native-wrong-owner",
                      "not-admitted", "duplicate-first", "semantic-release", "registration-changed",
                      "wrong-restored-binding", "missing-receipt", "foreign-receipt", "closed-receipt", "final-provider-pin", "duplicate-execution"):
            with self.subTest(wrong=wrong):
                producer = OfflineProducer(wrong)
                with self.assertRaises(AssertionError): RACES.run(producer, "10")
                self.assertEqual({"mapping-a": "Enabled", "mapping-b": "Enabled"}, producer.mapping_states)
                self.assertNotIn("completion-before-binding-outcome",
                                 [call.args[0] for call in producer.aws.capture.call_args_list])

    def test_unimplemented_additional_oracles_do_not_touch_aws(self):
        self.assertEqual(frozenset({"10", "pre-registration-provider-loss"}), RACES.SUPPORTED_SCENARIOS)
        for scenario in ("seeded-generation-races", "unknown"):
            producer = OfflineProducer()
            with self.assertRaises(NotImplementedError): RACES.run(producer, scenario)
            producer.aws.call.assert_not_called()
            producer.aws.invoke.assert_not_called()

    def test_missing_disposal_authority_rejects_before_aws(self):
        producer = OfflineProducer()
        producer.config["resourceFaults"]["disposable"] = False
        with self.assertRaisesRegex(AssertionError, "ownership"): RACES.run(producer, "10")
        producer.aws.call.assert_not_called()


if __name__ == "__main__":
    unittest.main()
