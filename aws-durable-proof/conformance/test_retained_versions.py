"""Inventory boundary tests with substituted AWS; no live or semantic cleanup evidence."""
import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location("retained_versions", Path(__file__).parents[1] / "scripts/inspect-retained-versions.py")
INVENTORY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = INVENTORY
SPEC.loader.exec_module(INVENTORY)

PREFIX = "arn:aws:lambda:us-east-1:123456789012:function:release-"
KEY = "arn:aws:kms:us-east-1:123456789012:key/key-current"
SECRET = "arn:aws:secretsmanager:us-east-1:123456789012:secret:signing-abcdef"
SHA = base64.b64encode(b"a" * 32).decode()


def value():
    return {"schemaVersion": 1, "pipelineId": "generated-pipeline", "contractVersion": "sha256:generated-contract",
            "releaseId": "release", "artifactId": "action", "publicationId": "publication", "stackId": "owned-stack",
            "artifactSha256": "a" * 64, "actionCodeSha256": SHA, "durableCodeSha256": SHA,
            "placement": "FUNCTION", "isolationMode": "FRESH_RELEASE_ISOLATED_STACK", "runtimeUpdateMode": "FunctionUpdate",
            "functionVersionArns": {role: PREFIX + role + ":7" for role in INVENTORY.ROLES}, "hostKmsKeyArn": KEY,
            "queueUrls": {name: "https://sqs.us-east-1.amazonaws.com/123456789012/release-" + name for name in ("work", "transitionRequest", "transitionResponse")},
            "signing": {"resumeToken": {"arn": SECRET, "versionId": "r" * 32}, "worker": {"arn": SECRET, "versionId": "w" * 32},
                        "currentKmsKeyArn": KEY, "retainedSecretAccessArns": [SECRET], "retainedKmsAccessArns": [KEY]}}


def manifest(payload=None):
    payload = payload or value()
    source = json.dumps(payload).encode()
    return INVENTORY.ReleaseManifest.read(source, hashlib.sha256(source).hexdigest(), "a" * 64)


def configuration(role, payload):
    variables = {"TPF_AWS_DURABLE_PUBLICATION_ID": payload["publicationId"]}
    if role == "durable":
        variables["TPF_AWS_DURABLE_ACTION_FUNCTION"] = payload["functionVersionArns"]["action"]
    elif role == "ingress":
        variables["TPF_AWS_DURABLE_QUALIFIER"] = "7"
    else:
        variables["TPF_AWS_DURABLE_CONFIGURATION"] = json.dumps({"pipeline-id": payload["pipelineId"],
            "worker.sqs.pipeline-id": payload["pipelineId"], "worker.sqs.contract-version": payload["contractVersion"],
            "release-version": payload["releaseId"], "worker.sqs.release-version": payload["releaseId"],
            "worker.sqs.artifact-id": payload["artifactId"], "worker.sqs.artifact-digest": payload["artifactSha256"]})
        settings = json.loads(variables["TPF_AWS_DURABLE_CONFIGURATION"])
        settings.update({"queue-url": payload["queueUrls"]["work"],
            "worker.sqs.request-queue-url": payload["queueUrls"]["transitionRequest"],
            "worker.sqs.response-queue-url": payload["queueUrls"]["transitionResponse"]})
        variables["TPF_AWS_DURABLE_CONFIGURATION"] = json.dumps(settings)
        for name, prefix in (("resumeToken", "RESUME_TOKEN_SECRET"), ("worker", "WORKER_SECRET")):
            variables["TPF_AWS_DURABLE_" + prefix + "_ARN"] = payload["signing"][name]["arn"]
            variables["TPF_AWS_DURABLE_" + prefix + "_VERSION"] = payload["signing"][name]["versionId"]
    return {"FunctionArn": payload["functionVersionArns"][role], "Version": "7", "CodeSha256": SHA,
            "KMSKeyArn": KEY, "Environment": {"Variables": variables}, "RuntimeVersionConfig": {"RuntimeVersionArn": "provider-runtime"}}


def client(payload):
    configurations = {arn: configuration(role, payload) for role, arn in payload["functionVersionArns"].items() if arn}
    return SimpleNamespace(
        list_versions_by_function=Mock(side_effect=lambda **parameters: {"Versions": [{"FunctionArn": parameters["FunctionName"] + ":7", "Version": "7"}]}),
        get_function_configuration=Mock(side_effect=lambda **parameters: copy.deepcopy(configurations[parameters["FunctionName"]])))


class RetainedVersionInventoryTest(unittest.TestCase):
    def test_malformed_falsy_marker_is_unknown_rather_than_advisory_all_clear(self):
        for marker in ([], {}, False, 0, "   "):
            with self.subTest(marker=marker):
                pinned = manifest(); aws = client(pinned.value)
                aws.list_versions_by_function.side_effect = None
                aws.list_versions_by_function.return_value = {"Versions": [], "NextMarker": marker}
                report = INVENTORY.AdvisoryInventory(aws, max_pages=1).inspect(pinned)
                self.assertFalse(report["completeWithinManifestedFunctionScope"])
                self.assertEqual(10, report["classifications"]["unknown"])

    def test_extra_signing_manifest_fields_cannot_leak_into_advisory_output(self):
        payload = value()
        payload["signing"]["privatePlaintext"] = "outer-sensitive-canary"
        payload["signing"]["resumeToken"]["value"] = "resume-sensitive-canary"
        payload["signing"]["worker"]["value"] = "worker-sensitive-canary"
        pinned = manifest(payload)
        report = INVENTORY.AdvisoryInventory(client(payload)).inspect(pinned)
        serialized = json.dumps(report)
        for secret in ("outer-sensitive-canary", "resume-sensitive-canary", "worker-sensitive-canary"):
            self.assertNotIn(secret, serialized)
        signing = next(entry for entry in report["entries"] if entry.get("role") == "work")["signing"]
        self.assertEqual({"arn", "versionId"}, set(signing["resumeToken"]))
        self.assertEqual({"arn", "versionId"}, set(signing["worker"]))
        self.assertEqual({"resumeToken", "worker", "currentKmsKeyArn", "retainedSecretAccessArns", "retainedKmsAccessArns"}, set(signing))

    def test_exact_manifest_correlates_numeric_versions_code_release_and_signing_without_cleanup_authority(self):
        pinned = manifest()
        report = INVENTORY.AdvisoryInventory(client(pinned.value)).inspect(pinned)
        self.assertEqual({"matched": 10}, report["classifications"])
        self.assertTrue(report["completeWithinManifestedFunctionScope"])
        self.assertFalse(report["cleanupAuthorized"])
        self.assertFalse(report["semanticExecutionInventoryObserved"])
        self.assertEqual("advisory", report["mode"])
        self.assertEqual(20, report["apiCalls"])
        self.assertIn("manifest-reference-only", next(row for row in report["entries"] if row.get("role") == "work")["secretCmkVerification"])

    def test_manifest_rejects_mutable_targets_missing_hosts_unpinned_keys_and_different_source_or_artifact(self):
        for mutation in (
            lambda item: item["functionVersionArns"].update(work=PREFIX + "work:live"),
            lambda item: item["functionVersionArns"].pop("awaitCompletion"),
            lambda item: item["signing"]["worker"].update(versionId="AWSCURRENT"),
            lambda item: item["signing"].update(retainedKmsAccessArns=[KEY + "-other"]),
            lambda item: item.update(artifactSha256="b" * 64),
            lambda item: item.update(isolationMode="COMPATIBLE_IN_PLACE"),
        ):
            payload = value(); mutation(payload)
            with self.assertRaises(ValueError):
                manifest(payload)
        source = json.dumps(value()).encode()
        with self.assertRaises(ValueError):
            INVENTORY.ReleaseManifest.read(source, "b" * 64, "a" * 64)
        with self.assertRaises(ValueError):
            INVENTORY.ReleaseManifest.read(b'{"schemaVersion":1,"schemaVersion":1}', hashlib.sha256(b'{"schemaVersion":1,"schemaVersion":1}').hexdigest(), "a" * 64)

    def test_pages_are_exhausted_and_unmanifested_versions_remain_visible(self):
        pinned = manifest(); aws = client(pinned.value)
        def pages(**parameters):
            arn = parameters["FunctionName"]
            if "Marker" not in parameters:
                return {"Versions": [{"FunctionArn": arn + ":7", "Version": "7"}], "NextMarker": "next-page"}
            self.assertEqual("next-page", parameters["Marker"])
            return {"Versions": [{"FunctionArn": arn + ":8", "Version": "8"}]}
        aws.list_versions_by_function.side_effect = pages
        report = INVENTORY.AdvisoryInventory(aws).inspect(pinned)
        self.assertTrue(report["completeWithinManifestedFunctionScope"])
        self.assertEqual(10, report["classifications"]["unmanifested"])
        self.assertEqual(30, report["apiCalls"])

    def test_page_bounds_repeated_markers_and_read_failures_never_report_complete(self):
        pinned = manifest(); aws = client(pinned.value)
        aws.list_versions_by_function.side_effect = None
        aws.list_versions_by_function.return_value = {"Versions": [], "NextMarker": "unread-next-page"}
        bounded = INVENTORY.AdvisoryInventory(aws, max_pages=1).inspect(pinned)
        self.assertFalse(bounded["completeWithinManifestedFunctionScope"])
        self.assertEqual(10, sum(row.get("reason") == "page-bound-exhausted" for row in bounded["entries"]))
        for page in ({"Versions": [], "NextMarker": "same"}, {}):
            pinned = manifest(); aws = client(pinned.value)
            aws.list_versions_by_function.side_effect = None
            aws.list_versions_by_function.return_value = page
            report = INVENTORY.AdvisoryInventory(aws, max_pages=2).inspect(pinned)
            self.assertFalse(report["completeWithinManifestedFunctionScope"])
            self.assertEqual(10, report["classifications"]["unknown"])
        pinned = manifest(); aws = client(pinned.value)
        aws.list_versions_by_function.side_effect = RuntimeError("private-secret")
        report = INVENTORY.AdvisoryInventory(aws).inspect(pinned)
        self.assertFalse(report["completeWithinManifestedFunctionScope"])
        self.assertNotIn("private-secret", json.dumps(report))

    def test_missing_denied_and_mismatched_configuration_are_distinct_and_do_not_expose_secret_values(self):
        pinned = manifest(); aws = client(pinned.value)
        def observe(**parameters):
            role = next(role for role, arn in pinned.value["functionVersionArns"].items() if arn == parameters["FunctionName"])
            if role in ("work", "awaitCompletion"):
                failure = RuntimeError("private-secret-value")
                failure.response = {"Error": {"Code": "ResourceNotFoundException" if role == "work" else "AccessDeniedException"}}
                raise failure
            result = configuration(role, pinned.value)
            if role == "action":
                result["Environment"]["Variables"]["TPF_AWS_DURABLE_WORKER_SECRET_VERSION"] = "z" * 32
                result["Environment"]["Variables"]["PIPELINE_ORCHESTRATOR_WORKER_SQS_SHARED_SECRET"] = "private-secret-value"
            return result
        aws.get_function_configuration.side_effect = observe
        report = INVENTORY.AdvisoryInventory(aws).inspect(pinned)
        self.assertEqual({"matched": 7, "missing": 1, "unknown": 1, "mismatch": 1}, report["classifications"])
        self.assertFalse(report["completeWithinManifestedFunctionScope"])
        self.assertNotIn("private-secret-value", json.dumps(report))

    def test_compute_has_no_lambda_transition_inventory_and_cli_uses_native_marker_contract(self):
        payload = value(); payload["placement"] = "COMPUTE"; payload["functionVersionArns"]["transition"] = ""
        pinned = manifest(payload)
        self.assertEqual({"matched": 9}, INVENTORY.AdvisoryInventory(client(payload)).inspect(pinned)["classifications"])
        with patch.object(INVENTORY.subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout='{"Versions":[]}', stderr="")) as run:
            INVENTORY.AwsCliReadClient("us-east-1").list_versions_by_function(FunctionName=PREFIX + "work", MaxItems=50, Marker="native-marker")
        command = run.call_args.args[0]
        self.assertIn("--no-paginate", command)
        self.assertNotIn("--max-items", command)
        self.assertEqual({"FunctionName": PREFIX + "work", "MaxItems": 50, "Marker": "native-marker"}, json.loads(command[command.index("--cli-input-json") + 1]))


if __name__ == "__main__":
    unittest.main()
