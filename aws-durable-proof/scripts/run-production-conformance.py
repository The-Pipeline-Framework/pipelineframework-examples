#!/usr/bin/env python3
"""Run production scenarios against an explicitly supplied disposable deployment.

No AWS operation occurs without --execute. Unsupported or unexecuted obligations
remain visible and fail promotion. This runner never calls a historical proof handler.
"""
import argparse
import base64
import re
import hashlib
import importlib.util
import json
from pathlib import Path
import random
import subprocess
import time

SPEC = importlib.util.spec_from_file_location("production_gate", Path(__file__).with_name("verify-production-conformance.py"))
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)
RESOURCE_SPEC = importlib.util.spec_from_file_location("production_resource_faults", Path(__file__).with_name("production-resource-faults.py"))
RESOURCE_FAULTS = importlib.util.module_from_spec(RESOURCE_SPEC)
RESOURCE_SPEC.loader.exec_module(RESOURCE_FAULTS)


class AwsBoundary:
    def __init__(self, region, evidence_root):
        self.region, self.root = region, Path(evidence_root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.references = []

    def capture(self, label, value):
        path = self.root / f"{len(self.references):05d}-{label}.json"
        path.write_text(json.dumps(value, indent=2) + "\n")
        reference = {"path": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        self.references.append(reference)
        return reference

    def call(self, service, operation, **arguments):
        args = ["aws", service, operation, "--region", self.region, "--output", "json", "--no-cli-pager"]
        for key, value in arguments.items():
            args.append("--" + key.replace("_", "-"))
            if value is not True:
                args.append(json.dumps(value) if isinstance(value, (dict, list)) else str(value))
        result = subprocess.run(args, capture_output=True, text=True, check=False, timeout=120)
        self.capture(service + "-" + operation, {"arguments": arguments, "exitCode": result.returncode,
                     "stdout": result.stdout, "stderr": result.stderr})
        if result.returncode:
            raise RuntimeError(f"AWS boundary failed: {service} {operation}; see raw evidence")
        return json.loads(result.stdout) if result.stdout.strip() else {}

    def invoke(self, function, payload):
        input_path = self.root / "invoke-input.json"
        output_path = self.root / "invoke-output.json"
        input_path.write_text(json.dumps(payload))
        output_path.unlink(missing_ok=True)
        args = ["aws", "lambda", "invoke", "--region", self.region, "--no-cli-pager", "--output", "json",
                "--function-name", function, "--invocation-type", "RequestResponse",
                "--payload", "fileb://" + str(input_path.resolve()), str(output_path.resolve())]
        result = subprocess.run(args, capture_output=True, text=True, check=False, timeout=200)
        metadata = json.loads(result.stdout) if result.returncode == 0 else {"cliError": result.stderr}
        output = json.loads(output_path.read_text()) if output_path.exists() else {}
        self.capture("lambda-invoke", {"function": function, "request": payload, "metadata": metadata, "response": output})
        if result.returncode or metadata.get("FunctionError"):
            raise RuntimeError("Lambda invocation failed; see raw evidence")
        return output


class ProductionScenarios:
    ACTION_FAULTS = {"2": ("SUBMIT", "BEFORE_FORWARD"), "3": ("SUBMIT", "AFTER_RESPONSE"),
                     "8": ("BIND_CALLBACK", "BEFORE_FORWARD"), "9": ("BIND_CALLBACK", "AFTER_RESPONSE")}

    def __init__(self, configuration, bundle, seed, boundary):
        self.config, self.bundle, self.seed, self.aws = configuration, Path(bundle), seed, boundary
        self.item_fixture = configuration.get("fixtureRole") == "item"
        self.archive_name = "item-function.zip" if self.item_fixture else "function.zip"
        self.contract_name = "item-pipeline-contract.json" if self.item_fixture else "pipeline-contract.json"
        self.contract = json.loads((self.bundle / self.contract_name).read_text())
        self.random = random.Random(seed)
        self.parameters = {}
        self.outputs = {}

    def preflight(self):
        compiled_binding = GATE.verify_operation_http_binding(self.bundle / "function.zip", self.bundle / "item-function.zip",
                                                             self.config.get("operationHttpUrl"))
        self.aws.capture("compiled-operation-http-binding", compiled_binding)
        stack = self.aws.call("cloudformation", "describe-stacks", stack_name=self.config["stack"])["Stacks"][0]
        self.parameters = {item["ParameterKey"]: item["ParameterValue"] for item in stack["Parameters"]}
        self.outputs = {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}
        GATE.require(self.parameters["PipelineId"] == self.contract["pipelineId"], "deployed pipeline differs from generated fixture")
        GATE.require(self.parameters["ContractVersion"] == self.contract["contractVersion"], "deployed worker differs from generated contract")
        GATE.require(self.parameters["ReleaseArtifactId"] == "conformance-action", "deployed release artifact identity differs")
        GATE.require(self.parameters["TransitionWorkerPlacement"] == self.config["placement"], "worker placement differs")
        digest = hashlib.sha256((self.bundle / self.archive_name).read_bytes()).hexdigest()
        GATE.require(self.parameters["ReleaseArtifactDigest"] == digest, "deployed release artifact differs")
        deployment = json.loads(self.outputs["DeploymentReleaseManifest"])
        for key, expected in {"pipelineId": self.contract["pipelineId"], "contractVersion": self.contract["contractVersion"],
            "releaseId": self.parameters["ReleaseVersion"], "artifactId": "conformance-action",
            "artifactSha256": digest, "placement": self.config["placement"]}.items():
            GATE.require(deployment.get(key) == expected, "deployment manifest identity differs: " + key)
        GATE.require(deployment.get("schemaVersion") == 1, "production deployment manifest schema differs")
        GATE.require(deployment["isolationMode"] in {"FRESH_RELEASE_ISOLATED_STACK", "DRAINED_RELEASE_MIGRATION"}, "release isolation evidence missing")
        for role, arn in deployment["functionVersionArns"].items():
            if role == "transition" and self.config["placement"] == "COMPUTE":
                GATE.require(not arn, "COMPUTE must not claim a Lambda transition consumer")
                continue
            GATE.require(bool(re.fullmatch(r"arn:[^:]+:lambda:[^:]+:[0-9]{12}:function:[^:]+:[0-9]+", arn)),
                         "every production Lambda host requires a retained numeric version: " + role)
            function = self.aws.call("lambda", "get-function-configuration", function_name=arn)
            GATE.require(str(function["Version"]) == arn.rsplit(":", 1)[1], "numeric host version changed: " + role)
        GATE.require(deployment["functionVersionArns"]["action"] == self.config["actionVersionArn"],
                     "producer action differs from release manifest pinned host")
        self.aws.capture("deployment-release-manifest", deployment)
        account = self.aws.call("sts", "get-caller-identity")
        GATE.require(not account["Arn"].endswith(":root"), "account-root credentials are forbidden")
        self.aws.call("lambda", "get-account-settings")
        coordinator = self.aws.call("lambda", "get-function-configuration", function_name=self.outputs["DurableAliasArn"])
        GATE.require(coordinator["Handler"] == "org.pipelineframework.aws.durable.AwsDurableCoordinatorHandler::handleRequest", "production coordinator required")
        GATE.require(bool(coordinator.get("DurableConfig")), "provider Durable capability missing")
        target = self.aws.call("lambda", "get-function-configuration", function_name=self.config["actionVersionArn"])
        GATE.verify_runtime_operation_http_binding(target["Environment"]["Variables"],
            GATE.OPERATION_CLIENTS["itemAction" if self.item_fixture else "action"], self.config["operationHttpUrl"])
        GATE.require(target["Environment"]["Variables"]["QUARKUS_LAMBDA_HANDLER"] == "aws-durable-action", "production action handler required")
        GATE.require(target["CodeSha256"] == base64.b64encode(bytes.fromhex(digest)).decode(),
                     "deployed action code differs from inspected production archive")
        return self.aws.capture("capability-checks", {"account": account["Account"], "region": self.aws.region,
                               "placement": self.config["placement"], "contract": self.contract["contractVersion"],
                               "productionCoordinator": coordinator["FunctionArn"], "productionAction": target["FunctionArn"]})

    def poll(self, observation, predicate, timeout=180):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = observation()
            if predicate(value):
                return value
            time.sleep(1)
        raise AssertionError("Production observation did not reach its expected state; see raw evidence")

    def action(self, operation, tenant, execution_id):
        return self.aws.invoke(self.config["actionVersionArn"], {"operation": operation, "tenantId": tenant,
            "executionId": execution_id, "pipelineId": self.contract["pipelineId"],
            "contractVersion": self.contract["contractVersion"], "releaseVersion": self.parameters["ReleaseVersion"], "generation": 1})

    def rows(self, table, tenant):
        arguments = {"table_name": self.parameters[table], "consistent_read": True, "no_paginate": True,
            "limit": 100, "key_condition_expression": "tenant_id = :tenant",
            "expression_attribute_values": {":tenant": {"S": tenant}}}
        rows, cursors = [], set()
        while True:
            page = self.aws.call("dynamodb", "query", **arguments)
            rows.extend(page["Items"])
            cursor = page.get("LastEvaluatedKey")
            if not cursor:
                return rows
            encoded = json.dumps(cursor, sort_keys=True)
            GATE.require(encoded not in cursors, "Dynamo observation pagination repeated its cursor")
            cursors.add(encoded)
            arguments["exclusive_start_key"] = cursor

    def release(self, tenant):
        root = Path(__file__).resolve().parents[2]
        input_path = self.aws.root / "release-input.json"
        input_path.write_text(json.dumps({"region": self.aws.region, "releaseTable": self.parameters["ReleaseTable"],
            "bucket": self.parameters["ReleaseArtifactBucket"], "kmsKeyArn": self.config["artifactKmsKeyArn"],
            "tenant": tenant, "releaseVersion": self.parameters["ReleaseVersion"],
            "functionArchive": str((self.bundle / self.archive_name).resolve()),
            "pipelineContract": str((self.bundle / self.contract_name).resolve())}))
        args = [str(root / "mvnw"), "-f", "aws-durable-proof/production-tests/pom.xml", "exec:java",
                '-Dmaven.repo.local=' + str(root / '.m2/repository'), '-Dexec.args=' + str(input_path.resolve()), '--no-transfer-progress']
        result = subprocess.run(args, cwd=root, capture_output=True, text=True, check=False, timeout=180)
        self.aws.capture("production-release-registration", {"exitCode": result.returncode, "stdout": result.stdout, "stderr": result.stderr})
        GATE.require(result.returncode == 0, "production release fixture registration failed")

    def arm(self, tenant, operation, point):
        proxy = self.aws.call("lambda", "get-function-configuration", function_name=self.config["faultProxyVersionArn"])
        GATE.require(proxy["Environment"]["Variables"]["TPF_CONFORMANCE_TARGET_ACTION_VERSION"] == self.config["actionVersionArn"],
                     "fault proxy must target the actual pinned production action")
        coordinator = self.aws.call("lambda", "get-function-configuration", function_name=self.outputs["DurableAliasArn"])
        GATE.require(coordinator["Environment"]["Variables"]["TPF_AWS_DURABLE_ACTION_FUNCTION"] == self.config["faultProxyVersionArn"],
                     "production driver must cross the declared external fault boundary")
        fault_id = tenant + "#" + operation + "#" + point
        self.aws.call("dynamodb", "put-item", table_name=self.config["faultTable"],
            item={"fault_id": {"S": fault_id}}, condition_expression="attribute_not_exists(fault_id)")
        return fault_id

    def provider(self, execution_name):
        executions = self.aws.call("lambda", "list-durable-executions-by-function",
            function_name=self.outputs["DurableAliasArn"], durable_execution_name=execution_name,
            max_items=10).get("DurableExecutions", [])
        GATE.require(len(executions) <= 1, "provider start identity admitted duplicate executions")
        return executions[0] if executions else {}

    def binding(self, checkpoint):
        return self.aws.call("dynamodb", "get-item", table_name=self.outputs["CallbackBindingTableName"],
            consistent_read=True, key={"pk": {"S": checkpoint["tenantId"] + "#" + checkpoint["interactionId"]},
                "sk": {"S": "GEN#00000000000000000001"}}).get("Item", {})

    def replay_completed_stream_record(self, checkpoint):
        stream = self.parameters["AwaitInteractionStreamArn"]
        description = self.aws.call("dynamodbstreams", "describe-stream", stream_arn=stream)["StreamDescription"]
        for shard in description["Shards"]:
            cursor = self.aws.call("dynamodbstreams", "get-shard-iterator", stream_arn=stream,
                shard_id=shard["ShardId"], shard_iterator_type="TRIM_HORIZON")["ShardIterator"]
            for _ in range(100):
                page = self.aws.call("dynamodbstreams", "get-records", shard_iterator=cursor, limit=100)
                for record in page["Records"]:
                    image = record.get("dynamodb", {}).get("NewImage", {})
                    if (image.get("interaction_id", {}).get("S") == checkpoint["interactionId"]
                        and image.get("status", {}).get("S") == "COMPLETED"):
                        # GetRecords omits the event-source context that Lambda supplies.
                        record["eventSourceARN"] = stream
                        target = self.config["awaitWakeupVersionArn"]
                        configuration = self.aws.call("lambda", "get-function-configuration", function_name=target)
                        GATE.require(configuration["Environment"]["Variables"]["QUARKUS_LAMBDA_HANDLER"]
                                     == "aws-durable-await-wakeup", "actual production stream handler required")
                        for _ in range(2):
                            response = self.aws.invoke(target, {"Records": [record]})
                            GATE.require(not response.get("batchItemFailures"), "actual stream record replay failed")
                        return
                cursor = page.get("NextShardIterator")
                if not cursor or not page["Records"]:
                    break
        raise AssertionError("No actual completed semantic Dynamo Stream record was captured")

    def complete(self, checkpoint, request):
        return self.aws.call("sqs", "send-message", queue_url=self.outputs["AwaitCompletionQueueUrl"],
            message_body=json.dumps({"tenantId": checkpoint["tenantId"], "interactionId": checkpoint["interactionId"],
                "correlationId": checkpoint["correlationId"], "idempotencyKey": checkpoint["interactionId"] + ":complete",
                "responsePayload": {"request": request}, "actor": "production-conformance"}))

    def open_execution(self, scenario):
        tenant = "conformance-" + scenario + "-" + str(self.random.getrandbits(64))
        self.release(tenant)
        fault = self.arm(tenant, *self.ACTION_FAULTS[scenario]) if scenario in self.ACTION_FAULTS else ""
        payload = {"tenantId": tenant, "idempotencyKey": tenant + ":start", "pipelineId": self.contract["pipelineId"],
                   "contractVersion": self.contract["contractVersion"], "releaseVersion": self.parameters["ReleaseVersion"],
                   "inputJson": json.dumps({"request": "initial-approval"}), "generation": 1}
        start = self.aws.invoke(self.outputs["IngressFunctionArn"], payload)
        if scenario == "1":
            # The first real provider response is intentionally discarded at the external caller boundary.
            again = self.aws.invoke(self.outputs["IngressFunctionArn"], payload)
            GATE.require(start["durableExecutionName"] == again["durableExecutionName"], "lost ingress response changed provider identity")
        rows = self.poll(lambda: self.rows("ExecutionTable", tenant), lambda rows: len(rows) == 1)
        execution_id = rows[0]["execution_id"]["S"]
        first = self.poll(lambda: self.action("STATUS", tenant, execution_id),
            lambda status: status.get("executionStatus") == "WAITING_EXTERNAL" and bool(status.get("awaitCheckpoint")))["awaitCheckpoint"]
        GATE.require("FirstApproval" in first["stepId"], "first Await is not the compiler-produced first boundary")
        provider = self.poll(lambda: self.provider(start["durableExecutionName"]), bool)
        return {"tenant": tenant, "executionId": execution_id, "first": first, "start": start,
                "fault": fault, "provider": provider}

    def finish_execution(self, session, scenario="sequential-await"):
        tenant, execution_id = session["tenant"], session["executionId"]
        first, start, fault = session["first"], session["start"], session["fault"]
        if scenario == "11":
            self.poll(lambda: self.binding(first), bool)
        self.complete(first, "first-approved")
        second = self.poll(lambda: self.action("STATUS", tenant, execution_id),
            lambda status: bool(status.get("awaitCheckpoint")) and status["awaitCheckpoint"]["interactionId"] != first["interactionId"])["awaitCheckpoint"]
        GATE.require("SecondApproval" in second["stepId"], "second Await is not the compiler-produced second boundary")
        if scenario == "12":
            self.replay_completed_stream_record(first)
        self.complete(second, "second-approved")
        self.poll(lambda: self.action("STATUS", tenant, execution_id), lambda status: status.get("executionStatus") == "SUCCEEDED")
        result = json.loads(self.action("RESULT", tenant, execution_id)["resultJson"])
        GATE.require(result == {"request": "second-approved"} or result == [{"request": "second-approved"}], "typed final output differs")
        provider = self.poll(lambda: self.provider(start["durableExecutionName"]), bool)
        terminal = self.poll(lambda: self.aws.call("lambda", "get-durable-execution",
            durable_execution_arn=provider["DurableExecutionArn"], include_execution_data=True),
            lambda execution: execution.get("Status") == "SUCCEEDED")
        output = json.loads(terminal["Result"])
        GATE.require(output["executionId"] == execution_id and output["generation"] == 1,
                     "provider result changed the admitted semantic execution or generation")
        GATE.require(json.loads(output["resultJson"]) == result, "provider output differs from production semantic result")
        interactions = self.rows("AwaitInteractionTable", tenant)
        GATE.require(len(interactions) == 2, "sequential Await did not retain exactly two semantic interactions")
        GATE.require(len(self.rows("ExecutionTable", tenant)) == 1, "transport retry duplicated semantic admission")
        if fault:
            claim = self.aws.call("dynamodb", "get-item", table_name=self.config["faultTable"], consistent_read=True,
                key={"fault_id": {"S": fault + "#CLAIM"}})
            GATE.require(bool(claim.get("Item")), "requested action transport fault was not observed")
        self.aws.capture("scenario-outcome", {"id": scenario, "tenant": tenant, "executionId": execution_id,
                         "firstInteraction": first["interactionId"], "secondInteraction": second["interactionId"], "result": result})

    def sequential(self, scenario):
        self.finish_execution(self.open_execution(scenario), scenario)

    def item_awaits(self, scenario):
        configuration = self.config | self.config["itemDeployment"] | {"fixtureRole": "item"}
        producer = ProductionScenarios(configuration, self.bundle, self.seed, self.aws)
        producer.preflight()
        count = 101 if scenario == "more-than-100-await-records" else 3
        tenant = "conformance-items-" + str(self.random.getrandbits(64))
        producer.release(tenant)
        start = self.aws.invoke(producer.outputs["IngressFunctionArn"], {"tenantId": tenant,
            "idempotencyKey": tenant + ":start", "pipelineId": producer.contract["pipelineId"],
            "contractVersion": producer.contract["contractVersion"], "releaseVersion": producer.parameters["ReleaseVersion"],
            "inputJson": json.dumps({"request": str(count)}), "generation": 1})
        execution = producer.poll(lambda: producer.rows("ExecutionTable", tenant), lambda rows: len(rows) == 1)[0]
        execution_id = execution["execution_id"]["S"]
        interactions = producer.poll(lambda: producer.rows("AwaitInteractionTable", tenant), lambda rows: len(rows) == count)
        GATE.require(len({row["unit_id"]["S"] for row in interactions}) == 1, "items did not share one real Await unit")
        GATE.require({int(row["item_index"]["N"]) for row in interactions} == set(range(count)),
                     "generated item lineage lost or duplicated an item")
        checkpoints = [{"tenantId": tenant, "interactionId": row["interaction_id"]["S"],
                        "correlationId": row["correlation_id"]["S"], "index": int(row["item_index"]["N"])}
                       for row in interactions]
        self.random.shuffle(checkpoints)
        for offset in range(0, count, 10):
            batch = checkpoints[offset:offset + 10]
            entries = [{"Id": str(item["index"]), "MessageBody": json.dumps({"tenantId": tenant,
                "interactionId": item["interactionId"], "correlationId": item["correlationId"],
                "idempotencyKey": item["interactionId"] + ":complete", "actor": "production-conformance",
                "responsePayload": {"request": "approved-" + str(item["index"])}})} for item in batch]
            sent = self.aws.call("sqs", "send-message-batch", queue_url=producer.outputs["AwaitCompletionQueueUrl"], entries=entries)
            GATE.require(not sent.get("Failed"), "real item completions were not admitted to SQS")
        # A duplicate crosses the actual worker and admission boundary after all items exist.
        producer.complete(checkpoints[0], "approved-" + str(checkpoints[0]["index"]))
        producer.poll(lambda: producer.action("STATUS", tenant, execution_id),
                      lambda status: status.get("executionStatus") == "SUCCEEDED")
        result = json.loads(producer.action("RESULT", tenant, execution_id)["resultJson"])
        GATE.require(isinstance(result, list) and len(result) == count, "item result was truncated or duplicated")
        GATE.require(sorted(item["request"] for item in result) == sorted("approved-" + str(index) for index in range(count)),
                     "typed item completion values were lost or changed")
        retained = producer.rows("AwaitInteractionTable", tenant)
        GATE.require(len(retained) == count and all(row["status"]["S"] == "COMPLETED" for row in retained),
                     "all real semantic item interactions must settle once")
        provider = producer.poll(lambda: producer.provider(start["durableExecutionName"]), bool)
        terminal = producer.poll(lambda: self.aws.call("lambda", "get-durable-execution",
            durable_execution_arn=provider["DurableExecutionArn"], include_execution_data=True),
            lambda value: value.get("Status") == "SUCCEEDED")
        output = json.loads(terminal["Result"])
        GATE.require(output["executionId"] == execution_id and output["generation"] == 1
                     and json.loads(output["resultJson"]) == result, "native item output differs from admitted semantic output")
        GATE.require(len(producer.rows("ExecutionTable", tenant)) == 1, "concurrent completion duplicated execution admission")
        self.aws.capture("item-scenario-outcome", {"id": scenario, "tenant": tenant, "executionId": execution_id,
                         "itemCount": count, "result": result, "pipelineContract": producer.contract["contractVersion"]})

    def run(self, scenarios):
        outcomes = []
        for scenario in scenarios:
            start = len(self.aws.references)
            try:
                if scenario in RESOURCE_FAULTS.SUPPORTED_SCENARIOS:
                    RESOURCE_FAULTS.run(self, scenario)
                elif scenario in {"concurrent-item-await", "more-than-100-await-records"} and self.config.get("itemDeployment"):
                    self.item_awaits(scenario)
                elif scenario in {"1", "2", "3", "8", "9", "11", "12", "sequential-await"}:
                    self.sequential(scenario)
                else:
                    outcomes.append({"id": scenario, "placement": self.config["placement"], "status": "NOT_IMPLEMENTED",
                                     "rawEvidence": [], "reason": "Production scenario procedure is not implemented"})
                    continue
                status, reason = "PASSED", ""
            except NotImplementedError as failure:
                status, reason = "NOT_IMPLEMENTED", str(failure)
                self.aws.capture("scenario-capability-unavailable", {"id": scenario, "failure": reason})
            except Exception as failure:
                status, reason = "FAILED", str(failure)
                self.aws.capture("scenario-failure", {"id": scenario, "failure": reason})
            diagnostic = {}
            if scenario == "14":
                diagnostic = {"faultVariant": RESOURCE_FAULTS.CALLBACK_FAILURE_VARIANT,
                              "elapsedExpiry": False, "diagnosticStatus": status}
                if status == "PASSED":
                    status = "NOT_IMPLEMENTED"
                    reason = "Original callback expiry scenario requires actual CallbackTimedOut; injected callback failure is diagnostic only"
                    self.aws.capture("scenario-original-coverage-pending", {"id": scenario, "reason": reason, **diagnostic})
            outcomes.append({"id": scenario, "placement": self.config["placement"], "status": status,
                             "rawEvidence": self.aws.references[start:], "reason": reason, **diagnostic})
        return outcomes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--configuration", required=True)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    config = json.loads(Path(args.configuration).read_text())
    obligations = json.loads(GATE.OBLIGATIONS.read_text())
    scenarios = [str(value) for value in obligations["historicalScenarioIds"]] + obligations["additionalScenarios"]
    if not args.execute:
        print(json.dumps({"awsExecution": "NOT_RUN", "placement": config["placement"], "seed": args.seed,
                          "requiredScenarios": scenarios}, indent=2))
        return 0
    bundle = Path(args.bundle)
    GATE.verify_operation_http_binding(bundle / "function.zip", bundle / "item-function.zip", config.get("operationHttpUrl"))
    manifest = json.loads((bundle / "build-manifest.json").read_text())
    GATE.require(manifest.get("operationHttpUrl") == config["operationHttpUrl"], "prepared candidate URL differs from producer URL")
    digests = GATE.verify_artifacts(bundle / "function.zip", bundle / "coordinator.jar", bundle / "deployment.jar",
                                   bundle / "aws-durable-queue-async.yaml", bundle / "operation-http.zip", bundle / "item-function.zip")
    GATE.require(manifest["artifactDigests"] == digests, "bundle bytes changed after preparation")
    boundary = AwsBoundary(config["region"], bundle)
    producer = ProductionScenarios(config, bundle, args.seed, boundary)
    capability = producer.preflight()
    report = manifest | {"region": config["region"], "seed": args.seed,
        "operationHttpUrl": config["operationHttpUrl"],
        "capabilityChecks": {"status": "PASSED", "rawEvidence": capability},
        "cleanup": {"status": "NOT_RUN", "retainedResourcesRemaining": [config["stack"]]},
        "scenarios": producer.run(scenarios)}
    (bundle / ("production-report-" + config["placement"] + ".json")).write_text(json.dumps(report, indent=2) + "\n")
    return 0 if all(row["status"] == "PASSED" for row in report["scenarios"]) else 2


if __name__ == "__main__":
    raise SystemExit(main())
