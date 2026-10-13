#!/usr/bin/env python3
"""Deployment-free artifact and evidence gates. Historical proof reports cannot pass.

This checks supplied build/run evidence; it does not create AWS resources or certify
the human-owned product acceptance. Run evidence remains required after artifact QA.
"""
import argparse
import hashlib
import io
import json
from pathlib import Path
import re
import sys
import zipfile
from urllib.parse import urlsplit

PRODUCTION_HANDLERS = {
    "AwsDurableActionGatewayHandler", "AwsDurableWorkItemHandler",
    "AwsDurableAwaitCompletionHandler", "AwsDurableTransitionWorkerHandler",
    "AwsDurableAwaitWakeupHandler", "AwsDurableReconciliationHandler",
    "AwsDurableExecutionWakeupHandler", "AwsDurableDueWorkHandler",
}
PRODUCTION_SELECTORS = {
    "aws-durable-action", "aws-durable-work-item", "aws-durable-await-completion",
    "aws-durable-transition-worker", "aws-durable-await-wakeup", "aws-durable-reconcile",
    "aws-durable-execution-wakeup", "aws-durable-due-work",
}
PRODUCTION_PREFIX = "org/pipelineframework/aws/durable/"
GENERATED_DECODER = "org/pipelineframework/awsconformance/orchestrator/service/AwsDurablePipelineInputDecoder.class"
OBLIGATIONS = Path(__file__).resolve().parents[1] / "conformance" / "obligations.json"
OPERATION_CLIENTS = {
    "action": {"process-first-approval": "ProcessFirstApprovalRestClient", "process-second-approval": "ProcessSecondApprovalRestClient"},
    "itemAction": {"process-item-approvals": "ProcessItemApprovalsRestClient"},
}


class GateFailure(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise GateFailure(message)


def inspect_archive(data, label, depth=0):
    require(depth < 8, f"nested archive depth exceeded: {label}")
    classes, contracts = set(), []
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        for member in archive.infolist():
            name = member.filename
            require("org/pipelineframework/awsproof/" not in name,
                    f"historical proof package in production closure: {label}:{name}")
            if name.endswith(".class"):
                classes.add(name)
                content = archive.read(member)
                require(b"org/pipelineframework/awsproof/" not in content
                        and b"org.pipelineframework.awsproof." not in content,
                        f"historical proof reference in production class: {label}:{name}")
            elif name.endswith(".jar"):
                nested_classes, nested_contracts = inspect_archive(archive.read(member), f"{label}:{name}", depth + 1)
                classes.update(nested_classes)
                contracts.extend(nested_contracts)
            elif name.endswith("META-INF/pipeline/pipeline-contract.json"):
                contracts.append(json.loads(archive.read(member)))
    return classes, contracts


def require_operation_http_url(value):
    require(isinstance(value, str) and value and not re.search(r"\s|[${}@\\]", value),
            "operation HTTP URL must be a resolved literal URL")
    try:
        parsed = urlsplit(value)
        valid_port = parsed.port is None or 0 < parsed.port < 65536
    except ValueError as failure:
        raise GateFailure("operation HTTP URL is invalid") from failure
    require(parsed.scheme in {"http", "https"} and bool(parsed.hostname) and valid_port
            and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment,
            "operation HTTP URL must be an absolute HTTP(S) base URL without credentials, query or fragment")
    return value


def compiled_operation_bindings(path, clients):
    """Attest the literal configuration of the canonical flat Quarkus Lambda package.

    Alternate/profile/property aliases fail closed: resolving arbitrary SmallRye
    sources is not an archive gate's authority. Maven filtering writes these three
    generated clients' configKey URLs literally into root application.properties.
    The generated orchestrator-client sidecar supplies ordinal-90 defaults; root
    application.properties has Quarkus's ordinal 250 and overrides those defaults.
    """
    expected_keys = {"quarkus.rest-client." + client + ".url" for client in clients}
    records = []

    def visit(data, origin, depth):
        require(depth < 8, "operation URL configuration archive depth exceeded")
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            for member in archive.infolist():
                name = member.filename
                if name.endswith(".jar"):
                    visit(archive.read(member), origin + ":" + name, depth + 1)
                elif name.endswith(("application.yaml", "application.yml")):
                    require(name not in {"application.yaml", "application.yml"},
                            "alternate application YAML prevents exact compiled operation URL attestation")
                elif name.endswith(".properties"):
                    for line in archive.read(member).decode("utf-8").splitlines():
                        stripped = line.lstrip()
                        if not stripped.strip() or stripped.startswith(("#", "!")):
                            continue
                        match = re.match(r"([^:=\s]+)\s*(?:[=:]\s*|\s+)(.*)$", stripped)
                        if not match:
                            continue
                        key, value = match.groups()
                        require(origin or name != "application.properties" or key not in {
                            "config_ordinal", "quarkus.config.locations", "smallrye.config.locations"},
                            "external/ordinal configuration prevents exact compiled operation URL attestation")
                        decoded_key = re.sub(r"\\u([a-fA-F0-9]{4})", lambda match: chr(int(match.group(1), 16)), key)
                        decoded_key = re.sub(r"\\(.)", lambda match: match.group(1), decoded_key)
                        key_unquoted = decoded_key.replace('"', '')
                        is_binding = any(client in key_unquoted or class_name in key_unquoted for client, class_name in clients.items())
                        if is_binding and re.search(r"(?:\.|/)(?:url|uri|override-uri)$", key_unquoted):
                            if not origin and name == "META-INF/pipeline/orchestrator-clients.properties" and key in expected_keys:
                                continue  # Existing OrchestratorClientConfigSource.getOrdinal() is fixed at 90.
                            require(not origin and name == "application.properties" and key in expected_keys,
                                    "alternate/profile operation URL binding cannot be attested: " + key)
                            records.append((key, value))

    data = Path(path).read_bytes()
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        require(sum(member.filename == "application.properties" for member in archive.infolist()) == 1,
                "action archive requires exactly one root compiled application.properties")
    visit(data, "", 0)
    require(len(records) == len(expected_keys) and {key for key, _ in records} == expected_keys,
            "compiled operation URL bindings are missing or duplicated")
    return dict(records)


def verify_operation_http_binding(action_archive, item_archive, expected_url):
    expected_url = require_operation_http_url(expected_url)
    bindings = {}
    for role, archive in (("action", action_archive), ("itemAction", item_archive)):
        values = compiled_operation_bindings(archive, OPERATION_CLIENTS[role])
        for key, value in values.items():
            require_operation_http_url(value)
            require(value == expected_url, "compiled operation HTTP URL differs from the producer URL: " + key)
        bindings[role] = values
    return {"operationHttpUrl": expected_url, "compiledBindings": bindings}


def verify_runtime_operation_http_binding(environment, clients, expected_url):
    """Reject a published function environment that overrides a compiled URL."""
    expected_url = require_operation_http_url(expected_url)
    client_names = [re.sub(r"[^A-Z0-9]", "", name.upper()) for name in (*clients.keys(), *clients.values())]
    for key, value in environment.items():
        require(key not in {"QUARKUS_CONFIG_LOCATIONS", "SMALLRYE_CONFIG_LOCATIONS"} or not value,
                "external function configuration prevents exact operation URL attestation")
        require(key not in {"JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS"} or "quarkus.rest-client" not in value,
                "JVM client overrides prevent exact operation URL attestation")
        normalized = re.sub(r"[^A-Z0-9]", "", key.upper())
        if normalized.endswith(("URL", "URI")) and any(client in normalized for client in client_names):
            require(value == expected_url, "published function environment overrides the compiled operation HTTP URL: " + key)


def verify_artifacts(action_archive, coordinator_archive, deployment_archive, template_path, operation_http_archive, item_archive):
    action = Path(action_archive).read_bytes()
    coordinator = Path(coordinator_archive).read_bytes()
    action_classes, contracts = inspect_archive(action, str(action_archive))
    coordinator_classes, _ = inspect_archive(coordinator, str(coordinator_archive))
    for handler in PRODUCTION_HANDLERS:
        require(any(name.endswith(PRODUCTION_PREFIX + handler + ".class") for name in action_classes),
                f"production action closure lacks {handler}")
        require(any(name.endswith(PRODUCTION_PREFIX + handler + "_Bean.class") for name in action_classes),
                f"production handler was not discovered by Quarkus CDI: {handler}")
    for handler in ("AwsDurableCoordinatorHandler", "AwsDurableIngressHandler"):
        require(any(name.endswith(PRODUCTION_PREFIX + handler + ".class") for name in coordinator_classes),
                f"production coordinator closure lacks {handler}")
    require(any(name.endswith(GENERATED_DECODER) for name in action_classes),
            "fixture lacks compiler-generated AWS Durable input decoder")
    fixture_contracts = [contract for contract in contracts if contract.get("pipelineId") == "org.pipelineframework.awsconformance"]
    require(len(fixture_contracts) == 1, "fixture requires exactly one generated conformance pipeline contract")
    contract = fixture_contracts[0]
    require(contract.get("platform") == "FUNCTION", "fixture coordinator packaging must be FUNCTION")
    require(len(contract.get("steps", [])) >= 2, "fixture does not exercise sequential Await")
    require(sum(bool(step.get("deferredCompletion")) for step in contract["steps"]) >= 2,
            "fixture contract lacks two compiler-owned Await boundaries")
    item = Path(item_archive).read_bytes()
    item_classes, item_contracts = inspect_archive(item, str(item_archive))
    item_contracts = [value for value in item_contracts if value.get("pipelineId") == "org.pipelineframework.awsconformance.items"]
    require(len(item_contracts) == 1, "item fixture requires its independently generated pipeline contract")
    item_steps = item_contracts[0].get("steps", [])
    require(len(item_steps) == 1 and item_steps[0].get("cardinality") == "ONE_TO_MANY"
            and item_steps[0].get("deferredCompletion", {}).get("transportType") == "interaction-api",
            "item fixture must retain compiler-owned ONE_TO_MANY and per-item Await")
    require(any(name.endswith("org/pipelineframework/awsconformance/items/orchestrator/service/AwsDurablePipelineInputDecoder.class")
                for name in item_classes), "item fixture lacks its generated production input decoder")
    for handler in PRODUCTION_HANDLERS:
        require(any(name.endswith(PRODUCTION_PREFIX + handler + "_Bean.class") for name in item_classes),
                f"item production handler was not discovered by Quarkus CDI: {handler}")
    operation_http = Path(operation_http_archive).read_bytes()
    operation_classes, _ = inspect_archive(operation_http, str(operation_http_archive))
    for resource in ("ProcessFirstApprovalResource", "ProcessSecondApprovalResource"):
        require(any(name.endswith("org/pipelineframework/awsconformance/pipeline/" + resource + ".class")
                    for name in operation_classes), "operation HTTP artifact lacks generated REST resource " + resource)
    require(any(name.endswith("org/pipelineframework/awsconformance/items/pipeline/ProcessItemApprovalsResource.class")
                for name in operation_classes), "operation HTTP artifact lacks the generated item operation")
    require(any(name.endswith("io/quarkus/amazon/lambda/http/LambdaHttpHandler.class") for name in operation_classes),
            "operation HTTP artifact lacks the supported Quarkus Lambda HTTP bridge")
    require(not any(name.endswith(PRODUCTION_PREFIX + handler + ".class")
                    for name in operation_classes for handler in PRODUCTION_HANDLERS),
            "HTTP bridge must be separate from the production action handler artifact")
    template = Path(template_path).read_bytes()
    with zipfile.ZipFile(deployment_archive) as deployment:
        names = [name for name in deployment.namelist() if name.endswith("aws-durable-queue-async.yaml")]
        require(len(names) == 1, "deployment artifact lacks unique production SAM contract")
        require(template == deployment.read(names[0]), "deployed SAM is not the production deployment artifact resource")
    template_text = template.decode()
    require("awsproof" not in template_text and "proof-" not in template_text,
            "historical proof handlers are forbidden in production SAM")
    for selector in PRODUCTION_SELECTORS:
        require(selector in template_text, f"production SAM lacks handler selector {selector}")
    for handler in ("AwsDurableCoordinatorHandler", "AwsDurableIngressHandler"):
        require("org.pipelineframework.aws.durable." + handler in template_text,
                f"production SAM lacks direct handler {handler}")
    return {
        "action": "sha256:" + hashlib.sha256(action).hexdigest(),
        "coordinator": "sha256:" + hashlib.sha256(coordinator).hexdigest(),
        "deployment": "sha256:" + hashlib.sha256(Path(deployment_archive).read_bytes()).hexdigest(),
        "template": "sha256:" + hashlib.sha256(template).hexdigest(),
        "operationHttp": "sha256:" + hashlib.sha256(operation_http).hexdigest(),
        "itemAction": "sha256:" + hashlib.sha256(item).hexdigest(),
    }


def verify_raw_evidence(reference, evidence_root):
    require(isinstance(reference, dict), "raw evidence must contain a path and SHA-256 digest")
    path, digest = reference.get("path"), reference.get("sha256")
    require(isinstance(path, str) and path and not Path(path).is_absolute(), "raw evidence path must be relative")
    root = Path(evidence_root).resolve()
    resolved = (root / path).resolve()
    require(resolved.is_relative_to(root) and resolved.is_file(), f"raw evidence file absent or outside bundle: {path}")
    require(isinstance(digest, str) and re.fullmatch(r"[a-f0-9]{64}", digest), f"raw evidence digest required: {path}")
    require(hashlib.sha256(resolved.read_bytes()).hexdigest() == digest, f"raw evidence digest differs: {path}")


def verify_examples_revision(evidence, expected_revision):
    require(isinstance(expected_revision, str) and re.fullmatch(r"[a-f0-9]{40}", expected_revision),
            "exact examples producer SHA required")
    require(evidence.get("examplesRevision") == expected_revision,
            "report examples producer SHA differs from the trusted evidence run")


def verify_evidence(evidence, artifact_digests, expected_revisions, evidence_root):
    obligations = json.loads(OBLIGATIONS.read_text())
    require(evidence.get("schemaVersion") == 2 and evidence.get("executionPath") == "PRODUCTION",
            "production report v2 required; historical proof is diagnostic evidence")
    revisions = evidence.get("revisions", {})
    for component in ("runtime", "compiler", "bom"):
        require(isinstance(revisions.get(component), str) and re.fullmatch(r"[a-f0-9]{40}", revisions[component]),
                f"missing immutable {component} source SHA")
    require(revisions == expected_revisions, "report source SHAs differ from the exact runtime/compiler/BOM candidates")
    require(evidence.get("artifactDigests") == artifact_digests, "report artifact digests differ from inspected artifacts")
    require_operation_http_url(evidence.get("operationHttpUrl"))
    require(isinstance(evidence.get("region"), str) and bool(evidence["region"]), "region evidence required")
    require(evidence.get("capabilityChecks", {}).get("status") == "PASSED", "provider capability checks missing or failed")
    verify_raw_evidence(evidence["capabilityChecks"].get("rawEvidence"), evidence_root)
    require(isinstance(evidence.get("seed"), int) and not isinstance(evidence["seed"], bool), "random race seed required")
    require(evidence.get("cleanup", {}).get("status") == "PASSED", "cleanup including retained resources must pass")
    require(evidence["cleanup"].get("retainedResourcesRemaining") == [], "retained resource cleanup is incomplete")
    verify_raw_evidence(evidence["cleanup"].get("rawEvidence"), evidence_root)
    require(evidence.get("sourceTreeClean") is True, "promotion requires an immutable clean source candidate")
    required_scenarios = {str(value) for value in obligations["historicalScenarioIds"]} | set(obligations["additionalScenarios"])
    rows = evidence.get("scenarios", [])
    require(isinstance(rows, list), "scenario outcomes must be a list")
    observed = set()
    for row in rows:
        require(isinstance(row, dict), "scenario outcome must be an object")
        key = (row.get("placement"), str(row.get("id")))
        require(key not in observed, f"duplicate scenario outcome: {key}")
        observed.add(key)
        require(row.get("status") == "PASSED", f"scenario did not pass: {key}")
        if key[1] == "14":
            require(row.get("faultVariant") == "CALLBACK_TIMEOUT" and row.get("elapsedExpiry") is True,
                    f"original callback expiry variant required; injected failure is diagnostic only: {key}")
        require(isinstance(row.get("rawEvidence"), list) and bool(row["rawEvidence"]),
                f"scenario lacks raw evidence references: {key}")
        for reference in row["rawEvidence"]:
            verify_raw_evidence(reference, evidence_root)
    required = {(placement, scenario) for placement in obligations["placements"] for scenario in required_scenarios}
    require(observed == required, f"scenario coverage differs: missing={sorted(required - observed)}, extra={sorted(observed - required)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action-archive", required=True)
    parser.add_argument("--coordinator-archive", required=True)
    parser.add_argument("--deployment-archive", required=True)
    parser.add_argument("--template", required=True)
    parser.add_argument("--operation-http-archive", required=True)
    parser.add_argument("--item-archive", required=True)
    parser.add_argument("--operation-http-url", help="Exact URL compiled into both action archives; required for candidate preparation")
    parser.add_argument("--evidence")
    parser.add_argument("--runtime-sha")
    parser.add_argument("--compiler-sha")
    parser.add_argument("--bom-sha")
    parser.add_argument("--examples-sha")
    args = parser.parse_args()
    try:
        digests = verify_artifacts(args.action_archive, args.coordinator_archive, args.deployment_archive, args.template, args.operation_http_archive, args.item_archive)
        binding_status = "NOT_RUN"
        if args.operation_http_url:
            verify_operation_http_binding(args.action_archive, args.item_archive, args.operation_http_url)
            binding_status = "PASSED"
        if args.evidence:
            evidence = json.loads(Path(args.evidence).read_text())
            verify_operation_http_binding(args.action_archive, args.item_archive, evidence.get("operationHttpUrl"))
            if args.operation_http_url:
                require(args.operation_http_url == evidence["operationHttpUrl"], "report operation URL differs from candidate URL")
            binding_status = "PASSED"
            require(all((args.runtime_sha, args.compiler_sha, args.bom_sha)), "runtime/compiler/BOM source SHAs are required for promotion evidence")
            verify_examples_revision(evidence, args.examples_sha)
            verify_evidence(evidence, digests,
                            {"runtime": args.runtime_sha, "compiler": args.compiler_sha, "bom": args.bom_sha},
                            Path(args.evidence).parent)
        print(json.dumps({"artifactArchitecture": "PASSED", "artifactDigests": digests,
                          "operationHttpBinding": binding_status,
                          "promotionEvidence": "PASSED" if args.evidence else "NOT_RUN"}, indent=2))
        return 0
    except (GateFailure, OSError, ValueError, zipfile.BadZipFile) as failure:
        print(f"production conformance gate failed: {failure}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
