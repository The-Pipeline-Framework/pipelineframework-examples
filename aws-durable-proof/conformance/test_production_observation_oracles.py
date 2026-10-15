"""Adversarial runner oracles; AWS is substituted, not production certification."""
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location("observation_runner", Path(__file__).parents[1] / "scripts/run-production-conformance.py")
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class ProductionObservationTest(unittest.TestCase):
    def stream_producer(self, root, pages, shards=("one",), truncated=False):
        (root / "pipeline-contract.json").write_text('{"pipelineId":"generated"}')
        boundary = SimpleNamespace(call=Mock(), invoke=Mock(return_value={"batchItemFailures": []}), capture=Mock())
        producer = RUNNER.ProductionScenarios({"placement": "FUNCTION", "awaitWakeupVersionArn": "pinned-wakeup"}, root, 7, boundary)
        producer.parameters = {"AwaitInteractionStreamArn": "actual-stream"}
        producer.outputs = {"DeploymentReleaseManifest": json.dumps({"functionVersionArns": {"awaitWakeup": "pinned-wakeup"}})}
        def call(service, operation, **arguments):
            if operation == "describe-stream":
                return {"StreamDescription": {"Shards": [{"ShardId": shard} for shard in shards],
                                              **({"LastEvaluatedShardId": "more"} if truncated else {})}}
            if operation == "get-shard-iterator":
                return {"ShardIterator": arguments["shard_id"]}
            if operation == "get-records":
                return pages(arguments["shard_iterator"])
            if operation == "get-function-configuration":
                return {"Environment": {"Variables": {"QUARKUS_LAMBDA_HANDLER": "aws-durable-await-wakeup"}}}
            raise AssertionError(operation)
        boundary.call.side_effect = call
        return producer

    @staticmethod
    def record(**overrides):
        image = {"tenant_id": {"S": "tenant"}, "interaction_id": {"S": "interaction"},
                 "correlation_id": {"S": "correlation"}, "status": {"S": "COMPLETED"}}
        image.update({key: {"S": value} for key, value in overrides.items()})
        return {"eventID": "actual-event", "eventName": "MODIFY", "dynamodb": {"NewImage": image}}

    def checkpoint(self):
        return {"tenantId": "tenant", "interactionId": "interaction", "correlationId": "correlation"}

    def test_empty_live_page_does_not_hide_later_completed_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            producer = self.stream_producer(root, lambda cursor: {"Records": [], "NextShardIterator": "next"}
                if cursor == "one" else {"Records": [self.record()]})
            producer.replay_completed_stream_record(self.checkpoint())
            self.assertEqual(2, producer.aws.invoke.call_count)
            payload = producer.aws.invoke.call_args.args[1]
            self.assertEqual("actual-event", payload["Records"][0]["eventID"])
            self.assertEqual("actual-stream", payload["Records"][0]["eventSourceARN"])

    def test_foreign_same_handler_cannot_acknowledge_the_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            producer = self.stream_producer(Path(directory), lambda _: {"Records": [self.record()]})
            producer.config["awaitWakeupVersionArn"] = "foreign-version"
            with self.assertRaisesRegex(ValueError, "manifest"):
                producer.replay_completed_stream_record(self.checkpoint())
            producer.aws.invoke.assert_not_called()

    def test_later_shard_is_read_before_empty_shard_uses_the_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            producer = self.stream_producer(Path(directory), lambda cursor: {"Records": [], "NextShardIterator": cursor}
                if cursor == "one" else {"Records": [self.record()]}, shards=("one", "two"))
            producer.replay_completed_stream_record(self.checkpoint())
            reads = [call.kwargs["shard_iterator"] for call in producer.aws.call.call_args_list if call.args[1] == "get-records"]
            self.assertEqual(["one", "two"], reads)

    def test_wrong_tenant_or_correlation_cannot_supply_replayed_record(self):
        for field in ("tenant_id", "correlation_id"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                producer = self.stream_producer(Path(directory), lambda _: {"Records": [self.record(**{field: "foreign"})]})
                with self.assertRaises(NotImplementedError):
                    producer.replay_completed_stream_record(self.checkpoint())
                producer.aws.invoke.assert_not_called()

    def test_exhaustion_and_truncated_shards_remain_inconclusive(self):
        for truncated in (False, True):
            with self.subTest(truncated=truncated), tempfile.TemporaryDirectory() as directory:
                producer = self.stream_producer(Path(directory), lambda cursor: {"Records": [], "NextShardIterator": cursor}, truncated=truncated)
                with self.assertRaises(NotImplementedError):
                    producer.replay_completed_stream_record(self.checkpoint())
                reads = [call for call in producer.aws.call.call_args_list if call.args[1] == "get-records"]
                self.assertEqual(0 if truncated else 100, len(reads))
                producer.aws.invoke.assert_not_called()

    def test_item_result_must_preserve_numeric_lineage_order(self):
        for count in (3, 101):
            for permuted in (False, True):
                with self.subTest(count=count, permuted=permuted), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    (root / "pipeline-contract.json").write_text('{"pipelineId":"generated"}')
                    producer = RUNNER.ProductionScenarios({"placement": "FUNCTION", "itemDeployment": {}}, root, 7, SimpleNamespace())
                    producer.config["itemDeployment"] = {"stack": "item"}
                    result = [{"request": "approved-" + str(index)} for index in range(count)]
                    if permuted:
                        result[0], result[-1] = result[-1], result[0]
                    interactions = [{"unit_id": {"S": "unit"}, "interaction_id": {"S": str(index)},
                                     "correlation_id": {"S": "correlation-" + str(index)}, "item_index": {"N": str(index)},
                                     "status": {"S": "COMPLETED"}} for index in range(count)]
                    boundary = SimpleNamespace(call=Mock(), invoke=Mock(return_value={"durableExecutionName": "provider"}), capture=Mock())
                    def call(service, operation, **arguments):
                        if operation == "get-durable-execution":
                            return {"Status": "SUCCEEDED", "Result": json.dumps({"executionId": "execution", "generation": 1,
                                                                                "resultJson": json.dumps(result)})}
                        return {}
                    boundary.call.side_effect = call
                    producer.aws = boundary
                    item = SimpleNamespace(preflight=Mock(), release=Mock(), complete=Mock(), aws=boundary,
                        outputs={"IngressFunctionArn": "ingress", "AwaitCompletionQueueUrl": "completion"},
                        parameters={"ReleaseVersion": "release"}, contract={"pipelineId": "item-pipeline", "contractVersion": "contract"},
                        action=Mock(side_effect=lambda operation, *args: {"resultJson": json.dumps(result)} if operation == "RESULT"
                                    else {"executionStatus": "SUCCEEDED"}),
                        provider=Mock(return_value={"DurableExecutionArn": "provider"}),
                        rows=Mock(side_effect=lambda table, tenant: [{"execution_id": {"S": "execution"}}] if table == "ExecutionTable" else interactions))
                    def poll(observe, predicate):
                        observed = observe()
                        if not predicate(observed): raise AssertionError("fixture observation rejected")
                        return observed
                    item.poll = poll
                    with patch.object(RUNNER, "ProductionScenarios", return_value=item):
                        if permuted:
                            with self.assertRaisesRegex(ValueError, "order"):
                                producer.item_awaits("more-than-100-await-records" if count == 101 else "concurrent-item-await")
                        else:
                            producer.item_awaits("more-than-100-await-records" if count == 101 else "concurrent-item-await")


if __name__ == "__main__":
    unittest.main()
