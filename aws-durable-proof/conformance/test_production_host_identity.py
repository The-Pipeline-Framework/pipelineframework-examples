"""Exact-artifact preflight controls; all provider replies are substituted."""
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location("identity_runner", Path(__file__).parents[1] / "scripts/run-production-conformance.py")
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)
ROLES = ("durable", "ingress", "action", "work", "transition", "awaitCompletion", "awaitWakeup",
         "executionWakeup", "dueWork", "reconciliation")


class ProductionHostIdentityTest(unittest.TestCase):
    def preflight(self, wrong=None, placement="FUNCTION"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pipeline-contract.json").write_text('{"pipelineId":"generated","contractVersion":"contract"}')
            (root / "function.zip").write_bytes(b"exact action package")
            (root / "coordinator.jar").write_bytes(b"exact coordinator package")
            action_digest = hashlib.sha256((root / "function.zip").read_bytes()).digest()
            durable_digest = hashlib.sha256((root / "coordinator.jar").read_bytes()).digest()
            encoded_action, encoded_durable = (base64.b64encode(digest).decode() for digest in (action_digest, durable_digest))
            versions = {role: "arn:aws:lambda:us-east-1:123456789012:function:" + role + ":7" for role in ROLES}
            if placement == "COMPUTE": versions["transition"] = ""
            if wrong == "missing-role": versions.pop("dueWork")
            manifest = {"schemaVersion": 1, "pipelineId": "generated", "contractVersion": "contract", "releaseId": "release",
                        "artifactId": "conformance-action", "artifactSha256": action_digest.hex(), "placement": placement,
                        "isolationMode": "FRESH_RELEASE_ISOLATED_STACK", "functionVersionArns": versions,
                        "actionCodeSha256": "wrong" if wrong == "manifest-action" else encoded_action,
                        "durableCodeSha256": "wrong" if wrong == "manifest-durable" else encoded_durable}
            parameters = {"PipelineId": "generated", "ContractVersion": "contract", "ReleaseArtifactId": "conformance-action",
                          "TransitionWorkerPlacement": placement, "ReleaseArtifactDigest": action_digest.hex(), "ReleaseVersion": "release"}
            boundary = SimpleNamespace(region="us-east-1", capture=Mock(), call=Mock())
            def call(service, operation, **arguments):
                if operation == "describe-stacks":
                    return {"Stacks": [{"Parameters": [{"ParameterKey": key, "ParameterValue": value} for key, value in parameters.items()],
                        "Outputs": [{"OutputKey": "DeploymentReleaseManifest", "OutputValue": json.dumps(manifest)},
                                    {"OutputKey": "DurableAliasArn", "OutputValue": "alias"}]}]}
                if operation == "get-function-configuration":
                    name = arguments["function_name"]
                    role = name.split(":")[-2] if name != "alias" else "durable"
                    code = encoded_durable if role in {"durable", "ingress"} else encoded_action
                    return {"Version": "7", "CodeSha256": "wrong" if wrong == role else code,
                            "FunctionArn": name, "Handler": "org.pipelineframework.aws.durable.AwsDurableCoordinatorHandler::handleRequest",
                            "DurableConfig": {"ExecutionTimeout": 300},
                            "Environment": {"Variables": {"QUARKUS_LAMBDA_HANDLER": "aws-durable-action"}}}
                if operation == "get-caller-identity": return {"Arn": "assumed-role/test", "Account": "123456789012"}
                if operation == "get-account-settings": return {}
                raise AssertionError(operation)
            boundary.call.side_effect = call
            producer = RUNNER.ProductionScenarios({"stack": "owned", "placement": placement, "actionVersionArn": versions["action"],
                "operationHttpUrl": "https://operations.example.invalid/"}, root, 7, boundary)
            with patch.object(RUNNER.GATE, "verify_operation_http_binding", return_value={}), \
                 patch.object(RUNNER.GATE, "verify_runtime_operation_http_binding"):
                producer.preflight()

    def test_all_manifested_host_packages_must_match_the_inspected_candidate(self):
        self.preflight()
        self.preflight(placement="COMPUTE")
        for wrong in ROLES + ("missing-role", "manifest-action", "manifest-durable"):
            with self.subTest(wrong=wrong), self.assertRaises(ValueError):
                self.preflight(wrong)


if __name__ == "__main__": unittest.main()
