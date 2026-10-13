#!/usr/bin/env python3
"""Advisory inventory of explicitly manifested Lambda versions; never cleanup authority.

Only ListVersionsByFunction and GetFunctionConfiguration are used. Version absence,
provider history expiry and incomplete reads cannot establish semantic execution completion.
No secret values, semantic tables, execution history or account-wide resources are read.
"""
import argparse
import base64
import hashlib
import json
import re
import subprocess
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

VERSION_ARN = re.compile(r"arn:aws[a-z-]*:lambda:[a-z0-9-]+:[0-9]{12}:function:[A-Za-z0-9_-]+:([1-9][0-9]*)$")
SECRET_ARN = re.compile(r"arn:aws[a-z-]*:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@-]+-[A-Za-z0-9]{6}$")
KEY_ARN = re.compile(r"arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:key/[A-Za-z0-9-]+$")
ROLES = {"durable", "action", "ingress", "work", "transition", "awaitCompletion", "awaitWakeup", "executionWakeup", "dueWork", "reconciliation"}
SOURCES = {
    "versions": "https://docs.aws.amazon.com/lambda/latest/api/API_ListVersionsByFunction.html",
    "configuration": "https://docs.aws.amazon.com/lambda/latest/api/API_GetFunctionConfiguration.html",
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON property")
        result[key] = value
    return result


@dataclass(frozen=True)
class ReleaseManifest:
    source_sha256: str
    value: dict[str, Any]

    @classmethod
    def read(cls, source: bytes, expected_source_sha256: str, expected_artifact_sha256: str):
        digest = hashlib.sha256(source).hexdigest()
        require(digest == expected_source_sha256, "manifest source digest differs from the immutable release bundle")
        value = json.loads(source, object_pairs_hook=unique_object)
        require(isinstance(value, dict) and value.get("schemaVersion") == 1, "unsupported manifest schema")
        for name in ("pipelineId", "contractVersion", "releaseId", "artifactId", "publicationId", "stackId"):
            require(isinstance(value.get(name), str) and bool(value[name]), "missing manifest identity: " + name)
        require(re.fullmatch(r"[a-f0-9]{64}", expected_artifact_sha256) is not None
                and value.get("artifactSha256") == expected_artifact_sha256, "release artifact digest differs")
        for name in ("actionCodeSha256", "durableCodeSha256"):
            require(isinstance(value.get(name), str), "missing package digest")
            try:
                require(len(base64.b64decode(value[name], validate=True)) == 32, "invalid package SHA256")
            except (ValueError, base64.binascii.Error) as failure:
                raise ValueError("invalid package SHA256") from failure
        require(value.get("placement") in ("FUNCTION", "COMPUTE"), "unsupported placement")
        require(value.get("isolationMode") in ("FRESH_RELEASE_ISOLATED_STACK", "DRAINED_RELEASE_MIGRATION"), "unsupported release isolation")
        require(value.get("runtimeUpdateMode") == "FunctionUpdate", "unsupported retained runtime policy")
        versions = value.get("functionVersionArns")
        require(isinstance(versions, dict) and set(versions) == ROLES, "incomplete manifested host set")
        for role, arn in versions.items():
            require(isinstance(arn, str) and (bool(VERSION_ARN.fullmatch(arn))
                    or (role == "transition" and value["placement"] == "COMPUTE" and arn == "")), "host must have a numeric qualified version")
        require(value["placement"] != "FUNCTION" or bool(versions["transition"]), "FUNCTION transition is missing")
        require(value["placement"] != "COMPUTE" or versions["transition"] == "", "COMPUTE must not have a Lambda transition consumer")
        queues = value.get("queueUrls", {})
        require(isinstance(queues, dict) and all(isinstance(queues.get(name), str) and queues[name]
                for name in ("work", "transitionRequest", "transitionResponse")), "missing signed queue identity")
        signing = value.get("signing", {})
        for name in ("resumeToken", "worker"):
            reference = signing.get(name, {})
            require(isinstance(reference.get("arn"), str) and bool(SECRET_ARN.fullmatch(reference["arn"])), "invalid scoped signing ARN")
            require(isinstance(reference.get("versionId"), str) and bool(re.fullmatch(r"[A-Za-z0-9-]{32,64}", reference["versionId"])), "signing VersionId is not pinned")
        for name, pattern in (("retainedSecretAccessArns", SECRET_ARN), ("retainedKmsAccessArns", KEY_ARN)):
            require(isinstance(signing.get(name), list) and bool(signing[name])
                    and all(isinstance(item, str) and pattern.fullmatch(item) for item in signing[name]), "invalid retained signing access set")
        require(signing.get("currentKmsKeyArn") in signing["retainedKmsAccessArns"], "current signing CMK lacks retained access")
        require(all(signing[name]["arn"] in signing["retainedSecretAccessArns"] for name in ("resumeToken", "worker")), "current signing ARN lacks retained access")
        require(isinstance(value.get("hostKmsKeyArn"), str) and bool(KEY_ARN.fullmatch(value["hostKmsKeyArn"])), "invalid host CMK")
        return cls(digest, value)


class AdvisoryInventory:
    def __init__(self, client, max_pages: int = 10):
        require(1 <= max_pages <= 100, "page bound must be 1..100")
        self.client = client
        self.max_pages = max_pages
        self.api_calls = 0

    def call(self, operation, **parameters):
        self.api_calls += 1
        return getattr(self.client, operation)(**parameters)

    def inspect(self, manifest: ReleaseManifest) -> dict[str, Any]:
        value = manifest.value
        entries = []
        complete = True
        functions = {arn.rsplit(":", 1)[0] for arn in value["functionVersionArns"].values() if arn}
        manifested = set(value["functionVersionArns"].values())
        for function in sorted(functions):
            marker = None
            seen_markers = set()
            seen_versions = set()
            exhausted = False
            try:
                for _ in range(self.max_pages):
                    parameters = {"FunctionName": function, "MaxItems": 50}
                    if marker:
                        parameters["Marker"] = marker
                    response = self.call("list_versions_by_function", **parameters)
                    require(isinstance(response.get("Versions"), list), "invalid version page")
                    for version in response["Versions"]:
                        arn = version.get("FunctionArn", "")
                        if version.get("Version") == "$LATEST":
                            continue
                        require(bool(VERSION_ARN.fullmatch(arn)) and arn.rsplit(":", 1)[0] == function, "invalid scoped version response")
                        require(version.get("Version") == arn.rsplit(":", 1)[1], "version number differs from qualified ARN")
                        if arn not in manifested and arn not in seen_versions:
                            entries.append({"functionVersionArn": arn, "classification": "unmanifested"})
                        seen_versions.add(arn)
                    marker = response.get("NextMarker")
                    if marker is None or marker == "":
                        exhausted = True
                        break
                    require(isinstance(marker, str) and marker not in seen_markers, "invalid or repeated pagination marker")
                    seen_markers.add(marker)
                if not exhausted:
                    complete = False
                    entries.append({"functionArn": function, "classification": "unknown", "reason": "page-bound-exhausted"})
            except Exception:
                complete = False
                entries.append({"functionArn": function, "classification": "unknown", "reason": "version-enumeration-failed"})
        for role, arn in sorted(value["functionVersionArns"].items()):
            if not arn:
                continue
            entry = {"role": role, "functionVersionArn": arn}
            try:
                configuration = self.call("get_function_configuration", FunctionName=arn)
                differences = self.differences(value, role, arn, configuration)
                entry.update(classification="mismatch" if differences else "matched", differences=differences)
                entry["runtimeVersionArn"] = configuration.get("RuntimeVersionConfig", {}).get("RuntimeVersionArn", "")
                if role not in ("durable", "ingress"):
                    signing = value["signing"]
                    entry["signing"] = {
                        name: {key: signing[name][key] for key in ("arn", "versionId")}
                        for name in ("resumeToken", "worker")
                    } | {name: signing[name] for name in (
                        "retainedSecretAccessArns", "retainedKmsAccessArns", "currentKmsKeyArn")}
                    entry["secretCmkVerification"] = "manifest-reference-only; secret CMKs were not queried"
            except Exception as failure:
                code = getattr(failure, "response", {}).get("Error", {}).get("Code")
                entry.update(classification="missing" if code == "ResourceNotFoundException" else "unknown",
                             reason="version-not-found" if code == "ResourceNotFoundException" else "configuration-read-failed")
                if entry["classification"] == "unknown":
                    complete = False
            entries.append(entry)
        return {"mode": "advisory", "cleanupAuthorized": False, "semanticExecutionInventoryObserved": False,
                "manifestSourceSha256": manifest.source_sha256,
                "releaseIdentity": {name: value[name] for name in ("pipelineId", "contractVersion", "releaseId", "artifactId", "artifactSha256")},
                "completeWithinManifestedFunctionScope": complete, "maxPagesPerFunction": self.max_pages,
                "apiCalls": self.api_calls, "classifications": dict(Counter(entry["classification"] for entry in entries)),
                "entries": entries, "sources": SOURCES}

    def differences(self, manifest, role, arn, configuration):
        differences = []
        def compare(name, observed, expected):
            if observed != expected:
                differences.append(name)
        compare("numeric-version-arn", configuration.get("FunctionArn"), arn)
        compare("numeric-version", configuration.get("Version"), arn.rsplit(":", 1)[1])
        compare("package-sha256", configuration.get("CodeSha256"), manifest["durableCodeSha256" if role in ("durable", "ingress") else "actionCodeSha256"])
        compare("host-cmk", configuration.get("KMSKeyArn"), manifest["hostKmsKeyArn"])
        variables = configuration.get("Environment", {}).get("Variables", {})
        compare("publication-id", variables.get("TPF_AWS_DURABLE_PUBLICATION_ID"), manifest["publicationId"])
        if role == "durable":
            compare("action-version-target", variables.get("TPF_AWS_DURABLE_ACTION_FUNCTION"), manifest["functionVersionArns"]["action"])
        elif role == "ingress":
            compare("durable-version-qualifier", variables.get("TPF_AWS_DURABLE_QUALIFIER"), manifest["functionVersionArns"]["durable"].rsplit(":", 1)[1])
        else:
            configuration = json.loads(variables.get("TPF_AWS_DURABLE_CONFIGURATION", "{}"), object_pairs_hook=unique_object)
            for key, name in (("pipeline-id", "pipelineId"), ("worker.sqs.pipeline-id", "pipelineId"),
                              ("worker.sqs.contract-version", "contractVersion"), ("release-version", "releaseId"),
                              ("worker.sqs.release-version", "releaseId"), ("worker.sqs.artifact-id", "artifactId"),
                              ("worker.sqs.artifact-digest", "artifactSha256")):
                compare(key, configuration.get(key), manifest[name])
            for key, name in (("queue-url", "work"), ("worker.sqs.request-queue-url", "transitionRequest"),
                              ("worker.sqs.response-queue-url", "transitionResponse")):
                compare(key, configuration.get(key), manifest["queueUrls"][name])
            for name, prefix in (("resumeToken", "RESUME_TOKEN_SECRET"), ("worker", "WORKER_SECRET")):
                compare(name + "-arn", variables.get("TPF_AWS_DURABLE_" + prefix + "_ARN"), manifest["signing"][name]["arn"])
                compare(name + "-version-id", variables.get("TPF_AWS_DURABLE_" + prefix + "_VERSION"), manifest["signing"][name]["versionId"])
            if any(name in variables for name in ("PIPELINE_ORCHESTRATOR_RESUME_TOKEN_SECRET", "PIPELINE_ORCHESTRATOR_WORKER_SQS_SHARED_SECRET")):
                differences.append("plaintext-signing-configuration")
        return differences


class AwsCliReadClient:
    def __init__(self, region):
        self.region = region

    def read(self, operation, parameters):
        command = ["aws", "lambda", operation, "--region", self.region, "--output", "json", "--no-cli-pager", "--no-paginate"]
        # Pass native MaxItems/Marker through the API input; CLI --max-items is a
        # paginator option that can otherwise substitute a different NextToken contract.
        command.extend(["--cli-input-json", json.dumps(parameters)])
        result = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
        if result.returncode:
            failure = RuntimeError("AWS read failed")
            failure.response = {"Error": {"Code": "ResourceNotFoundException" if "ResourceNotFoundException" in result.stderr else "Unknown"}}
            raise failure
        return json.loads(result.stdout, object_pairs_hook=unique_object)

    def list_versions_by_function(self, **parameters):
        return self.read("list-versions-by-function", parameters)

    def get_function_configuration(self, **parameters):
        return self.read("get-function-configuration", parameters)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=["advisory"])
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--artifact-sha256", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--max-pages", type=int, default=10)
    arguments = parser.parse_args()
    manifest = ReleaseManifest.read(arguments.manifest.read_bytes(), arguments.manifest_sha256, arguments.artifact_sha256)
    report = AdvisoryInventory(AwsCliReadClient(arguments.region), arguments.max_pages).inspect(manifest)
    print(json.dumps(report, sort_keys=True))
    return 0 if report["completeWithinManifestedFunctionScope"] and set(report["classifications"]) <= {"matched"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
