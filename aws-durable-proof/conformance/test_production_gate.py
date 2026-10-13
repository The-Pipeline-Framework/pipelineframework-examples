"""Negative acceptance tests for the production promotion evidence boundary."""
import copy
import importlib.util
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

SPEC = importlib.util.spec_from_file_location("production_gate", Path(__file__).parents[1] / "scripts" / "verify-production-conformance.py")
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


class ProductionEvidenceGateTest(unittest.TestCase):
    def setUp(self):
        self.sha = "a" * 40
        self.digests = {"action": "sha256:" + "b" * 64}
        self.revisions = {"runtime": self.sha, "compiler": "c" * 40, "bom": "d" * 40}
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "raw.json").write_text('{"observed":"test-fixture-only"}')
        self.reference = {"path": "raw.json", "sha256": hashlib.sha256((self.root / "raw.json").read_bytes()).hexdigest()}
        obligations = json.loads(GATE.OBLIGATIONS.read_text())
        scenarios = [str(value) for value in obligations["historicalScenarioIds"]] + obligations["additionalScenarios"]
        self.report = {
            "schemaVersion": 2, "executionPath": "PRODUCTION",
            "revisions": self.revisions, "sourceTreeClean": True,
            "artifactDigests": self.digests, "region": "us-east-2", "seed": 12345,
            "operationHttpUrl": "https://operation-host.example.invalid/",
            "capabilityChecks": {"status": "PASSED", "rawEvidence": self.reference},
            "cleanup": {"status": "PASSED", "retainedResourcesRemaining": [], "rawEvidence": self.reference},
            "scenarios": [{"id": scenario, "placement": placement, "status": "PASSED",
                           "rawEvidence": [self.reference]}
                          for placement in obligations["placements"] for scenario in scenarios],
        }
        for row in self.report["scenarios"]:
            if row["id"] == "14":
                # Synthetic schema control only; this fixture is not live timeout evidence.
                row.update(faultVariant="CALLBACK_TIMEOUT", elapsedExpiry=True)

    def check(self, report):
        GATE.verify_evidence(report, self.digests, self.revisions, self.root)

    def test_complete_report_has_every_historical_id_and_new_obligation_on_both_placements(self):
        self.check(self.report)

    def test_original_callback_expiry_gate_rejects_injected_missing_or_nonexpiry_variants(self):
        for mutation in ({}, {"faultVariant": "INJECTED_CALLBACK_FAILURE", "elapsedExpiry": False},
                         {"faultVariant": "CALLBACK_TIMEOUT", "elapsedExpiry": False},
                         {"faultVariant": "CALLBACK_TIMEOUT", "elapsedExpiry": "true"}):
            report = copy.deepcopy(self.report)
            row = next(row for row in report["scenarios"] if row["id"] == "14")
            row.pop("faultVariant")
            row.pop("elapsedExpiry")
            row.update(mutation)
            with self.subTest(mutation=mutation), self.assertRaisesRegex(GATE.GateFailure, "callback expiry"):
                self.check(report)

    def test_historical_report_cannot_promote(self):
        for mutation in ({"schemaVersion": 1}, {"executionPath": "HISTORICAL_PROOF"}):
            with self.subTest(mutation=mutation), self.assertRaises(GATE.GateFailure):
                self.check(self.report | mutation)

    def test_missing_new_coverage_or_compute_or_one_historical_id_fails(self):
        for excluded in ("sequential-await", "22", "COMPUTE"):
            report = copy.deepcopy(self.report)
            report["scenarios"] = [row for row in report["scenarios"] if row["id"] != excluded and row["placement"] != excluded]
            with self.subTest(excluded=excluded), self.assertRaises(GATE.GateFailure):
                self.check(report)

    def test_failed_skipped_duplicate_or_evidence_free_scenario_fails(self):
        for mutation in ({"status": "FAILED"}, {"status": "SKIPPED"}, {"rawEvidence": []}):
            report = copy.deepcopy(self.report)
            report["scenarios"][0].update(mutation)
            with self.subTest(mutation=mutation), self.assertRaises(GATE.GateFailure):
                self.check(report)
        report = copy.deepcopy(self.report)
        report["scenarios"].append(report["scenarios"][0])
        with self.assertRaises(GATE.GateFailure):
            self.check(report)

    def test_wrong_candidate_or_artifact_failed_cleanup_or_unchecked_region_fails(self):
        mutations = ({"artifactDigests": {}}, {"cleanup": {"status": "FAILED"}},
                     {"operationHttpUrl": "UNCONFIGURED"}, {"operationHttpUrl": ""},
                     {"capabilityChecks": {}}, {"region": ""}, {"seed": "12345"},
                     {"revisions": {"runtime": "e" * 40, "compiler": "c" * 40, "bom": "d" * 40}},
                     {"revisions": {"runtime": self.sha, "compiler": "main", "bom": "d" * 40}})
        for mutation in mutations:
            with self.subTest(mutation=mutation), self.assertRaises(GATE.GateFailure):
                self.check(self.report | mutation)

    def test_raw_evidence_must_exist_match_digest_and_stay_inside_bundle(self):
        for reference in ({"path": "absent.json", "sha256": "0" * 64},
                          {"path": "raw.json", "sha256": "0" * 64},
                          {"path": "/etc/passwd", "sha256": "0" * 64},
                          {"path": "../raw.json", "sha256": "0" * 64},
                          "private-artifact:claimed.json"):
            report = copy.deepcopy(self.report)
            report["scenarios"][0]["rawEvidence"] = [reference]
            with self.subTest(reference=reference), self.assertRaises(GATE.GateFailure):
                self.check(report)

    def test_dirty_candidate_retained_resources_and_wrong_compiler_or_bom_fail(self):
        for mutation in ({"sourceTreeClean": False},
                         {"cleanup": self.report["cleanup"] | {"retainedResourcesRemaining": ["version:1"]}},
                         {"revisions": self.revisions | {"compiler": "e" * 40}},
                         {"revisions": self.revisions | {"bom": "e" * 40}}):
            with self.subTest(mutation=mutation), self.assertRaises(GATE.GateFailure):
                self.check(self.report | mutation)

    def test_proof_classes_or_references_are_rejected_through_nested_jars(self):
        def archive(name, content):
            output = io.BytesIO()
            with zipfile.ZipFile(output, "w") as jar:
                jar.writestr(name, content)
            return output.getvalue()
        for nested in (archive("org/pipelineframework/awsproof/Fake.class", b"class"),
                       archive("generated/Adapter.class", b"org/pipelineframework/awsproof/Fake"),
                       archive("generated/Adapter.class", b"org.pipelineframework.awsproof.Fake")):
            with self.assertRaises(GATE.GateFailure):
                GATE.inspect_archive(archive("lib/host.jar", nested), "function.zip")


if __name__ == "__main__":
    unittest.main()
