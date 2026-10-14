"""Work fault boundary and oracle tests; substituted AWS is not live conformance."""
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import zipfile

ROOT = Path(__file__).parents[1]
SPEC = importlib.util.spec_from_file_location("production_work_faults", ROOT / "scripts/production-work-faults.py")
WORK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WORK)


class WorkFaultTest(unittest.TestCase):
    def fixture(self):
        producer = MagicMock()
        producer.contract = {"pipelineId": "pipeline", "contractVersion": "contract"}
        producer.parameters = {"ReleaseVersion": "release", "ReleaseArtifactDigest": "digest"}
        producer.config = {"placement": "FUNCTION"}
        producer.poll.side_effect = lambda observe, predicate, **kwargs: self.observation(observe(), predicate)
        session = {"tenant": "tenant", "executionId": "execution", "start": {"durableExecutionName": "provider"}}
        body = json.dumps({"tenantId": "tenant", "executionId": "execution"})
        message = {"Body": body, "MessageId": "actual-id", "ReceiptHandle": "actual-receipt"}
        native = {key: {"S": val} for key, val in {"tenant_id": "tenant", "execution_id": "execution",
            "pipeline_id": "pipeline", "contract_version": "contract", "release_version": "release", "status": "WAITING_EXTERNAL"}.items()}
        native["attempt"] = {"N": "0"}
        interaction = {"interaction_id": {"S": "first"}, "execution_id": {"S": "execution"}}
        producer.rows.return_value = [native]
        producer.action.return_value = {"executionStatus": "WAITING_EXTERNAL", "awaitCheckpoint":
            {"tenantId": "tenant", "interactionId": "first", "stepId": "FirstApproval"}}
        producer.provider.return_value = {"DurableExecutionArn": "provider-arn"}
        owned = MagicMock()
        owned.manifest = {"queueUrls": {"work": "queue"}, "mappingIds": {"work": "mapping"}}
        owned.owns.side_effect = lambda resource, kind: resource
        owned.provider.side_effect = lambda arn: arn
        state = {"UUID": "mapping", "State": "Enabled", "FunctionArn": "version", "EventSourceArn": "queue-arn", "BatchSize": 1}
        def call(service, operation, **arguments):
            if operation == "get-queue-attributes": return {"Attributes": {"QueueArn": "queue-arn"}}
            if operation == "get-event-source-mapping": return dict(state)
            if operation == "list-event-source-mappings": return {"EventSourceMappings": [dict(state)]}
            if operation == "update-event-source-mapping":
                state["State"] = "Disabled" if arguments.get("no_enabled") else "Enabled"
                return dict(state)
            if operation == "receive-message": return {"Messages": [message]}
            if operation == "get-item":
                point = "BEFORE_DELEGATE" if "BEFORE_DELEGATE" in arguments["key"]["fault_id"]["S"] else "AFTER_ACK"
                return {"Item": {key: {"S": val} for key, val in {"fault_id": arguments["key"]["fault_id"]["S"],
                    "point": point, "tenant_id": "tenant", "execution_id": "execution", "body_sha256": hashlib.sha256(body.encode()).hexdigest(),
                    "work_item_type": WORK.WORK_TYPE, "delegate": WORK.DELEGATE, "message_id": "actual-delivery",
                    "pipeline_id": "pipeline", "contract_version": "contract", "release_version": "release",
                    "interaction_id": "first", "await_step_id": "FirstApproval", "execution_status": "WAITING_EXTERNAL",
                    "commit_observation": "PRODUCTION_CONTROL_PLANE_POINT_READS"}.items()}}
            return {}
        producer.aws.call.side_effect = call
        return producer, owned, session, message, native, interaction, state

    def observation(self, value, predicate):
        if not predicate(value): raise AssertionError("expected authoritative progress never observed")
        return value

    def run_procedure(self, fixture, scenario):
        p, owned, session, message, native, interaction, state = fixture
        initial = dict(native, status={"S": "QUEUED"})
        p.rows.side_effect = [[initial], [native]]
        with patch.object(WORK.RESOURCES, "OwnedResources", return_value=owned), \
             patch.object(WORK, "fixture_binding", return_value=("version", "fault-table")), \
             patch.object(WORK.RESOURCES, "start_native", return_value=session), \
             patch.object(WORK.RESOURCES, "native_rows", side_effect=[[], [interaction]]), \
             patch.object(WORK.RESOURCES, "complete_remaining") as complete, \
             patch.object(WORK.RESOURCES, "assert_success") as final:
            WORK.run(p, scenario)
            complete.assert_called_once()
            final.assert_called_once_with(p, session, "provider-arn", 1)

    def test_both_historical_cuts_correlate_exact_work_and_native_consequence(self):
        for scenario, point in (("4", "BEFORE_DELEGATE"), ("5", "AFTER_ACK")):
            with self.subTest(scenario=scenario):
                fixture = self.fixture()
                self.run_procedure(fixture, scenario)
                p, _, _, message, _, _, state = fixture
                armed = [call.kwargs for call in p.aws.call.call_args_list if call.args[1] == "put-item"]
                self.assertEqual("tenant#WORK#" + point, armed[0]["item"]["fault_id"]["S"])
                duplicates = [call.kwargs["message_body"] for call in p.aws.call.call_args_list if call.args[1] == "send-message"]
                self.assertEqual([message["Body"], message["Body"]], duplicates)
                self.assertEqual("Enabled", state["State"])

    def test_ack_without_semantic_progress_fails(self):
        fixture = self.fixture()
        fixture[0].action.return_value = {"executionStatus": "QUEUED"}
        with self.assertRaisesRegex(AssertionError, "progress"):
            self.run_procedure(fixture, "5")

    def test_changed_release_retry_or_duplicate_await_fails(self):
        for cut in ("release", "attempt", "interaction"):
            with self.subTest(cut=cut):
                fixture = self.fixture()
                if cut == "release": fixture[4]["release_version"] = {"S": "wrong"}
                elif cut == "attempt": fixture[4]["attempt"] = {"N": "1"}
                else: fixture[5]["interaction_id"] = {"S": "other"}
                with self.assertRaises(AssertionError): self.run_procedure(fixture, "4")

    def test_foreign_work_body_fails_and_mapping_restores(self):
        fixture = self.fixture()
        fixture[3]["Body"] = '{"tenantId":"foreign","executionId":"execution"}'
        with self.assertRaisesRegex(AssertionError, "work request"):
            self.run_procedure(fixture, "4")
        self.assertEqual("Enabled", fixture[6]["State"])

    def test_changed_claim_fields_fail(self):
        for key in ("execution_id", "tenant_id", "work_item_type", "body_sha256", "point", "delegate", "message_id",
                    "pipeline_id", "contract_version", "release_version", "interaction_id", "await_step_id", "execution_status", "commit_observation"):
            with self.subTest(key=key):
                fixture = self.fixture()
                original = fixture[0].aws.call.side_effect
                def changed(service, operation, **args):
                    result = original(service, operation, **args)
                    if operation == "get-item": result["Item"][key] = {"S": "" if key == "message_id" else "wrong"}
                    return result
                fixture[0].aws.call.side_effect = changed
                with self.assertRaisesRegex(AssertionError, "claim"):
                    self.run_procedure(fixture, "5")

    def test_second_consumer_or_wrong_mapping_rejected_before_mutation(self):
        for cut in ("consumer", "batch", "target"):
            with self.subTest(cut=cut):
                fixture = self.fixture()
                p, owned, _, _, _, _, state = fixture
                if cut == "batch": state["BatchSize"] = 2
                elif cut == "target": state["FunctionArn"] = "other"
                else:
                    original = p.aws.call.side_effect
                    p.aws.call.side_effect = lambda service, operation, **args: {"EventSourceMappings": [state, state]} if operation == "list-event-source-mappings" else original(service, operation, **args)
                with self.assertRaises(AssertionError):
                    with WORK.paused_work(p, owned, "version"): self.fail("yielded unsafe mapping")
                self.assertFalse(any(call.args[1] == "update-event-source-mapping" for call in p.aws.call.call_args_list))

    def test_uncertain_pause_response_still_restores(self):
        p, owned, _, _, _, _, state = self.fixture()
        original = p.aws.call.side_effect
        def uncertain(service, operation, **args):
            result = original(service, operation, **args)
            if operation == "update-event-source-mapping" and args.get("no_enabled"): raise RuntimeError("lost response")
            return result
        p.aws.call.side_effect = uncertain
        with self.assertRaisesRegex(RuntimeError, "lost response"):
            with WORK.paused_work(p, owned, "version"): pass
        self.assertEqual("Enabled", state["State"])

    def test_exact_archive_and_template_require_discovered_delegate_and_wrapper(self):
        source = b"Parameters:\n  WorkHandler: {Type: String, Default: aws-durable-work-item}\nResources:\n  Work:\n    Variables: {<<: *ActionEnvironment, QUARKUS_LAMBDA_HANDLER: !Ref WorkHandler}\nOutputs:\n"
        derived = WORK.derived_template(source)
        names = [WORK.WRAPPER + ".class", WORK.WRAPPER + "_Bean.class", WORK.DELEGATE.replace(".", "/") + ".class", WORK.DELEGATE.replace(".", "/") + "_Bean.class"]
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "function.zip"
            for missing in [None] + names:
                with zipfile.ZipFile(archive, "w") as output:
                    for name in names:
                        if name != missing: output.writestr(name, b"fixture-class")
                if missing:
                    with self.assertRaises(AssertionError): WORK.verify_fixture(archive, source, derived)
                else:
                    self.assertEqual(WORK.DELEGATE, WORK.verify_fixture(archive, source, derived)["delegate"])
                    with self.assertRaises(AssertionError): WORK.verify_fixture(archive, source, derived + b"uncontrolled change")

    def test_no_fixture_wiring_remains_unimplemented(self):
        p, owned, *_ = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            p.bundle = Path(directory)
            p.outputs = {}
            with self.assertRaises(NotImplementedError): WORK.fixture_binding(p, owned)

    def test_live_fixture_requires_exact_version_table_archive_and_manifest(self):
        source = b"Parameters:\n  WorkHandler: {Type: String, Default: aws-durable-work-item}\nResources:\n  Work:\n    Variables: {<<: *ActionEnvironment, QUARKUS_LAMBDA_HANDLER: !Ref WorkHandler}\nOutputs:\n"
        derived = WORK.derived_template(source)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "function.zip"
            with zipfile.ZipFile(archive, "w") as output:
                for name in (WORK.WRAPPER + ".class", WORK.WRAPPER + "_Bean.class",
                             WORK.DELEGATE.replace(".", "/") + ".class", WORK.DELEGATE.replace(".", "/") + "_Bean.class"):
                    output.writestr(name, b"fixture-class")
            binding = WORK.verify_fixture(archive, source, derived)
            (root / "aws-durable-queue-async.yaml").write_bytes(source)
            (root / "conformance-work-fault.yaml").write_bytes(derived)
            digest = hashlib.sha256(archive.read_bytes()).digest()
            for cut in (None, "version", "table", "code", "manifest", "artifact", "build"):
                with self.subTest(cut=cut):
                    p, owned, *_ = self.fixture()
                    p.bundle, p.archive_name = root, "function.zip"
                    p.parameters["ReleaseArtifactDigest"] = digest.hex()
                    owned.manifest["artifactSha256"] = "wrong" if cut == "artifact" else digest.hex()
                    owned.version.return_value = "version"
                    live = dict(binding, workVersionArn="other" if cut == "version" else "version", faultTable="fault-table")
                    if cut == "manifest": live["delegate"] = "proof.delegate"
                    p.outputs = {"ConformanceWorkFaultManifest": json.dumps(live)}
                    (root / "build-manifest.json").write_text(json.dumps({"workFaultFixture": {} if cut == "build" else binding}))
                    p.aws.call.side_effect = None
                    p.aws.call.return_value = {"CodeSha256": "wrong" if cut == "code" else base64.b64encode(digest).decode(),
                        "Environment": {"Variables": {"TPF_CONFORMANCE_WORK_FAULT_TABLE": "other" if cut == "table" else "fault-table"}}}
                    if cut:
                        with self.assertRaises(AssertionError): WORK.fixture_binding(p, owned)
                    else: self.assertEqual(("version", "fault-table"), WORK.fixture_binding(p, owned))


if __name__ == "__main__": unittest.main()
