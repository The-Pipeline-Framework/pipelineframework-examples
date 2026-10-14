#!/usr/bin/env python3
"""Assemble and inspect the actual production packages without contacting AWS."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shutil
import subprocess
import zipfile

SPEC = importlib.util.spec_from_file_location("production_gate", Path(__file__).with_name("verify-production-conformance.py"))
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)
FAULT_SPEC = importlib.util.spec_from_file_location("transition_fixture", Path(__file__).with_name("transition-fault-fixture.py"))
TRANSITION_FIXTURE = importlib.util.module_from_spec(FAULT_SPEC)
FAULT_SPEC.loader.exec_module(TRANSITION_FIXTURE)
WORK_SPEC = importlib.util.spec_from_file_location("work_fixture", Path(__file__).with_name("production-work-faults.py"))
WORK_FIXTURE = importlib.util.module_from_spec(WORK_SPEC)
WORK_SPEC.loader.exec_module(WORK_FIXTURE)


def combined_delivery_template(source):
    """Compose the two exact fixture transformations; retain original production provenance."""
    transition = TRANSITION_FIXTURE.derived_template(source)
    combined = WORK_FIXTURE.derived_template(transition)
    old = b'"productionTemplateSha256":"' + hashlib.sha256(transition).hexdigest().encode() + b'"'
    new = b'"productionTemplateSha256":"' + hashlib.sha256(source).hexdigest().encode() + b'"'
    GATE.require(combined.count(old) == 1, "combined work fixture provenance is missing or ambiguous")
    return combined.replace(old, new, 1)


def prepare(action, coordinator, deployment, operation_http, item, output, revisions, source_tree_clean, operation_http_url):
    for component, revision in revisions.items():
        GATE.require(bool(re.fullmatch(r"[a-f0-9]{40}", revision)), f"exact {component} SHA required")
    GATE.verify_operation_http_binding(action, item, operation_http_url)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    for source, name in ((action, "function.zip"), (coordinator, "coordinator.jar"), (deployment, "deployment.jar"), (operation_http, "operation-http.zip"), (item, "item-function.zip")):
        destination = output / name
        if Path(source).resolve() != destination.resolve():
            shutil.copyfile(source, destination)
    with zipfile.ZipFile(output / "deployment.jar") as archive:
        resources = [name for name in archive.namelist() if name.endswith("aws-durable-queue-async.yaml")]
        GATE.require(len(resources) == 1, "production deployment SAM resource must be unique")
        (output / "aws-durable-queue-async.yaml").write_bytes(archive.read(resources[0]))
    digests = GATE.verify_artifacts(output / "function.zip", output / "coordinator.jar", output / "deployment.jar",
                                    output / "aws-durable-queue-async.yaml", output / "operation-http.zip", output / "item-function.zip")
    _, contracts = GATE.inspect_archive((output / "function.zip").read_bytes(), "function.zip")
    contract = next(contract for contract in contracts if contract.get("pipelineId") == "org.pipelineframework.awsconformance")
    (output / "pipeline-contract.json").write_text(json.dumps(contract, indent=2) + "\n")
    _, item_contracts = GATE.inspect_archive((output / "item-function.zip").read_bytes(), "item-function.zip")
    item_contract = next(value for value in item_contracts if value.get("pipelineId") == "org.pipelineframework.awsconformance.items")
    (output / "item-pipeline-contract.json").write_text(json.dumps(item_contract, indent=2) + "\n")
    manifest = {"schemaVersion": 2, "executionPath": "PRODUCTION", "revisions": revisions,
                "sourceTreeClean": source_tree_clean, "artifactDigests": digests,
                "artifactArchitecture": "PASSED", "awsExecution": "NOT_RUN",
                "operationHttpUrl": operation_http_url, "operationHttpBinding": "PASSED",
                "pipelineId": contract["pipelineId"], "contractVersion": contract["contractVersion"]}
    source_template = (output / "aws-durable-queue-async.yaml").read_bytes()
    fixture_template = TRANSITION_FIXTURE.derived_template(source_template)
    manifest["transitionFaultFixture"] = TRANSITION_FIXTURE.verify(output / "function.zip", source_template, fixture_template)
    (output / "conformance-transition-fault.yaml").write_bytes(fixture_template)
    work_template = WORK_FIXTURE.derived_template(source_template)
    manifest["workFaultFixture"] = WORK_FIXTURE.verify_fixture(output / "function.zip", source_template, work_template)
    WORK_FIXTURE.verify_fixture(output / "item-function.zip", source_template, work_template)
    (output / "conformance-work-fault.yaml").write_bytes(work_template)
    combined = combined_delivery_template(source_template)
    (output / "conformance-worker-faults.yaml").write_bytes(combined)
    manifest["workerFaultTemplateSha256"] = hashlib.sha256(combined).hexdigest()
    (output / "build-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action-archive", required=True)
    parser.add_argument("--coordinator-archive", required=True)
    parser.add_argument("--deployment-archive", required=True)
    parser.add_argument("--operation-http-archive", required=True)
    parser.add_argument("--item-archive", required=True)
    parser.add_argument("--operation-http-url", required=True, help="Exact generated operation host URL used for compiling both action archives")
    parser.add_argument("--output", required=True)
    parser.add_argument("--runtime-sha", required=True, help="Exact source SHA attested by the production runtime build")
    parser.add_argument("--runtime-source-tree-clean", action="store_true", help="The production runtime build attests a clean source tree; defaults to unproved")
    parser.add_argument("--compiler-sha", required=True)
    parser.add_argument("--bom-sha", required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    status = subprocess.check_output(["git", "status", "--porcelain"], cwd=root, text=True)
    manifest = prepare(args.action_archive, args.coordinator_archive, args.deployment_archive, args.operation_http_archive, args.item_archive, args.output,
                       {"runtime": args.runtime_sha, "compiler": args.compiler_sha, "bom": args.bom_sha},
                       args.runtime_source_tree_clean and not status.strip(), args.operation_http_url)
    manifest["examplesRevision"] = revision
    (Path(args.output) / "build-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
