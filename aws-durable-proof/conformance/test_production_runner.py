import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("runner", Path(__file__).parents[1] / "scripts/run-production-conformance.py")
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class ProductionBoundaryTest(unittest.TestCase):
    def test_injected_callback_diagnostic_normal_return_cannot_pass_original_expiry_scenario(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pipeline-contract.json").write_text('{"pipelineId":"generated"}')
            boundary = RUNNER.AwsBoundary("us-east-1", root)
            producer = RUNNER.ProductionScenarios({"placement":"FUNCTION"}, root, 7, boundary)
            with patch.object(RUNNER.RESOURCE_FAULTS, "run", return_value=None) as diagnostic:
                outcome = producer.run(["14"])[0]
            diagnostic.assert_called_once_with(producer, "14")
            self.assertEqual("14", outcome["id"])
            self.assertEqual("NOT_IMPLEMENTED", outcome["status"])
            self.assertEqual("INJECTED_CALLBACK_FAILURE", outcome["faultVariant"])
            self.assertIs(False, outcome["elapsedExpiry"])
            self.assertEqual("PASSED", outcome["diagnosticStatus"])
            self.assertIn("CallbackTimedOut", outcome["reason"])

    def test_failed_invoke_cannot_reuse_previous_success_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "invoke-output.json").write_text('{"oldSuccess":true}')
            boundary = RUNNER.AwsBoundary("us-east-1", root)
            with patch.object(RUNNER.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, "", "denied")):
                with self.assertRaisesRegex(RuntimeError, "Lambda invocation failed"):
                    boundary.invoke("pinned-version", {"operation": "STATUS"})
            raw = json.loads((root / boundary.references[0]["path"]).read_text())
            self.assertEqual({}, raw["response"])
            self.assertEqual("denied", raw["metadata"]["cliError"])

    def test_unsupported_obligation_remains_explicitly_unimplemented(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pipeline-contract.json").write_text('{"pipelineId":"generated"}')
            boundary = RUNNER.AwsBoundary("us-east-1", root)
            producer = RUNNER.ProductionScenarios({"placement":"FUNCTION"}, root, 7, boundary)
            with patch.object(RUNNER.subprocess, "run") as aws:
                outcome = producer.run(["concurrent-item-await"])
                aws.assert_not_called()
            self.assertEqual("NOT_IMPLEMENTED", outcome[0]["status"])
            self.assertEqual([], outcome[0]["rawEvidence"])
