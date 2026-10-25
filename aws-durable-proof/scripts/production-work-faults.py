"""Historical work cuts over production delegation; import performs no AWS I/O.

Work messages contain only tenantId/executionId. Release pins come from the native
execution and exact deployment, never invented fields or an acknowledgement.
"""
import base64
from contextlib import contextmanager
import hashlib
import importlib.util
import json
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("work_resource_faults", Path(__file__).with_name("production-resource-faults.py"))
RESOURCES = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RESOURCES)
require, value = RESOURCES.require, RESOURCES.value

SUPPORTED_SCENARIOS = {"4", "5"}
HANDLER = "conformance-work-delivery"
DELEGATE = "org.pipelineframework.aws.durable.AwsDurableWorkItemHandler"
WORK_TYPE = "org.pipelineframework.orchestrator.ExecutionWorkItem"
WRAPPER = "org/pipelineframework/awsconformance/faults/WorkDeliveryFaultHandler"


def derived_template(source):
    """Add only work fixture wiring to the exact production SAM.

    This is a standalone work fixture. Combining it with transition fixture wiring
    requires a separately validated combined candidate; arbitrary changes fail closed.
    """
    text = source.decode()
    def replace_once(before, after):
        nonlocal text
        require(text.count(before) == 1, "production work fixture anchor missing or ambiguous: " + before)
        text = text.replace(before, after, 1)
    replace_once("  WorkHandler: {Type: String, Default: aws-durable-work-item}",
                 "  WorkHandler: {Type: String, Default: " + HANDLER + ", AllowedValues: [" + HANDLER + "]}")
    replace_once("Variables: {<<: *ActionEnvironment, QUARKUS_LAMBDA_HANDLER: !Ref WorkHandler}",
                 "Variables: {<<: *ActionEnvironment, QUARKUS_LAMBDA_HANDLER: !Ref WorkHandler, TPF_CONFORMANCE_WORK_FAULT_TABLE: !Ref ConformanceWorkFaultTable}")
    replace_once("Resources:\n", "Resources:\n  ConformanceWorkFaultTable:\n    Type: AWS::DynamoDB::Table\n    Properties:\n      BillingMode: PAY_PER_REQUEST\n      AttributeDefinitions: [{AttributeName: fault_id, AttributeType: S}]\n      KeySchema: [{AttributeName: fault_id, KeyType: HASH}]\n      SSESpecification: {SSEEnabled: true}\n  ConformanceWorkFaultAccess:\n    Type: AWS::IAM::Policy\n    Properties:\n      PolicyName: conformance-work-fault\n      Roles: [!Ref WorkerRole]\n      PolicyDocument:\n        Version: '2012-10-17'\n        Statement:\n          - Effect: Allow\n            Action: [dynamodb:GetItem, dynamodb:PutItem]\n            Resource: !GetAtt ConformanceWorkFaultTable.Arn\n")
    digest = hashlib.sha256(source).hexdigest()
    replace_once("Outputs:\n", "Outputs:\n  ConformanceWorkFaultManifest:\n    Value: !Sub |\n      {\"schemaVersion\":1,\"fixtureOnly\":true,\"handler\":\"" + HANDLER + "\",\"delegate\":\"" + DELEGATE + "\",\"workItemType\":\"" + WORK_TYPE + "\",\"workVersionArn\":\"${WorkVersion}\",\"faultTable\":\"${ConformanceWorkFaultTable}\",\"productionTemplateSha256\":\"" + digest + "\"}\n")
    return text.encode()


def verify_fixture(archive, source, derived):
    spec = importlib.util.spec_from_file_location("work_fixture_gate", Path(__file__).with_name("verify-production-conformance.py"))
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)
    classes, _ = gate.inspect_archive(Path(archive).read_bytes(), str(archive))
    for name in (WRAPPER + ".class", WRAPPER + "_Bean.class", DELEGATE.replace(".", "/") + ".class",
                 DELEGATE.replace(".", "/") + "_Bean.class"):
        require(any(entry.endswith(name) for entry in classes), "work fixture lacks discovered wrapper/production delegate: " + name)
    require(derived == derived_template(source), "work fixture differs from exact production-derived wiring")
    return {"schemaVersion": 1, "fixtureOnly": True, "handler": HANDLER, "delegate": DELEGATE,
            "workItemType": WORK_TYPE, "productionTemplateSha256": hashlib.sha256(source).hexdigest(),
            "fixtureTemplateSha256": hashlib.sha256(derived).hexdigest()}


def fixture_binding(producer, owned):
    root = Path(producer.bundle)
    if not (root / "conformance-work-fault.yaml").is_file() or "ConformanceWorkFaultManifest" not in producer.outputs:
        raise NotImplementedError("work delivery fixture wiring is not deployed/assembled")
    archive = root / producer.archive_name
    binding = verify_fixture(archive, (root / "aws-durable-queue-async.yaml").read_bytes(),
                             (root / "conformance-work-fault.yaml").read_bytes())
    require(json.loads((root / "build-manifest.json").read_text()).get("workFaultFixture") == binding,
            "candidate work fixture manifest differs from archive/template")
    live = json.loads(producer.outputs["ConformanceWorkFaultManifest"])
    for key in ("schemaVersion", "fixtureOnly", "handler", "delegate", "workItemType", "productionTemplateSha256"):
        require(live.get(key) == binding[key], "deployed work fixture manifest differs: " + key)
    target = owned.version("work", HANDLER)
    require(live.get("workVersionArn") == target, "work fixture version differs from release manifest")
    table = owned.owns(live["faultTable"], "AWS::DynamoDB::Table")
    configuration = producer.aws.call("lambda", "get-function-configuration", function_name=target)
    digest = hashlib.sha256(archive.read_bytes()).digest()
    require(configuration.get("CodeSha256") == base64.b64encode(digest).decode(), "work fixture code differs from inspected archive")
    require(configuration.get("Environment", {}).get("Variables", {}).get("TPF_CONFORMANCE_WORK_FAULT_TABLE") == table,
            "work fixture fault table differs from owned manifest")
    require(owned.manifest.get("artifactSha256") == digest.hex()
            and producer.parameters["ReleaseArtifactDigest"] == digest.hex(), "work release artifact digest differs")
    producer.aws.capture("work-fixture-binding", {"archive": binding, "deployed": live, "function": configuration})
    return target, table


@contextmanager
def paused_work(producer, owned, target):
    queue = owned.owns(owned.manifest["queueUrls"]["work"], "AWS::SQS::Queue")
    arn = producer.aws.call("sqs", "get-queue-attributes", queue_url=queue,
                            attribute_names=["QueueArn"])["Attributes"]["QueueArn"]
    mapping_id = owned.owns(owned.manifest["mappingIds"]["work"], "AWS::Lambda::EventSourceMapping")
    original = producer.aws.call("lambda", "get-event-source-mapping", uuid=mapping_id)
    require(original["State"] == "Enabled" and original["FunctionArn"] == target
            and original["EventSourceArn"] == arn and original.get("BatchSize") == 1,
            "work mapping differs from exact one-record fixture")
    mappings = producer.aws.call("lambda", "list-event-source-mappings", event_source_arn=arn)
    require(not mappings.get("NextMarker") and len(mappings.get("EventSourceMappings", [])) == 1
            and mappings["EventSourceMappings"][0].get("UUID") == mapping_id,
            "work queue must have exactly one manifested Lambda consumer")
    try:
        producer.aws.call("lambda", "update-event-source-mapping", uuid=mapping_id, no_enabled=True)
        producer.poll(lambda: producer.aws.call("lambda", "get-event-source-mapping", uuid=mapping_id),
                      lambda state: state["State"] == "Disabled")
        producer.aws.capture("work-delivery-paused", {"mapping": original, "target": target})
        yield queue
    finally:
        producer.aws.call("lambda", "update-event-source-mapping", uuid=mapping_id, enabled=True)
        restored = producer.poll(lambda: producer.aws.call("lambda", "get-event-source-mapping", uuid=mapping_id),
                                 lambda state: state["State"] == "Enabled")
        require(restored["FunctionArn"] == target and restored["EventSourceArn"] == arn
                and restored.get("BatchSize") == 1, "restored work mapping changed its fixture binding")
        producer.aws.capture("work-delivery-restored", restored)


def authentic_request(producer, session, queue):
    page = producer.poll(lambda: producer.aws.call("sqs", "receive-message", queue_url=queue,
        max_number_of_messages=1, wait_time_seconds=1, visibility_timeout=60), lambda page: bool(page.get("Messages")), timeout=30)
    require(len(page["Messages"]) == 1, "work request observation is not one message")
    message = page["Messages"][0]
    body = message["Body"]
    request = json.loads(body)
    require(isinstance(request, dict) and request == {"tenantId": session["tenant"], "executionId": session["executionId"]},
            "actual work request differs from admitted native execution")
    require(all(isinstance(message.get(key), str) and bool(message[key]) for key in ("MessageId", "ReceiptHandle")),
            "work request lacks authentic queue receipt identity")
    producer.aws.capture("authentic-work-request", {"message": message, "workItemType": WORK_TYPE})
    return message


def run(producer, scenario):
    require(scenario in SUPPORTED_SCENARIOS, "unsupported historical work cut")
    owned = RESOURCES.OwnedResources(producer)
    target, table = fixture_binding(producer, owned)
    point = "BEFORE_DELEGATE" if scenario == "4" else "AFTER_ACK"
    with paused_work(producer, owned, target) as queue:
        session = RESOURCES.start_native(producer, scenario, "initial-approval")
        RESOURCES.assert_single_execution(producer, session, "QUEUED")
        require(not RESOURCES.native_rows(producer, "AwaitInteractionTable", session["tenant"]), "work was consumed before fault injection")
        message = authentic_request(producer, session, queue)
        digest = hashlib.sha256(message["Body"].encode()).hexdigest()
        fault_id = session["tenant"] + "#WORK#" + point
        producer.aws.call("dynamodb", "put-item", table_name=table, condition_expression="attribute_not_exists(fault_id)",
            item={key: {"S": val} for key, val in {"fault_id": fault_id, "tenant_id": session["tenant"],
                "execution_id": session["executionId"], "work_item_type": WORK_TYPE, "body_sha256": digest,
                "pipeline_id": producer.contract["pipelineId"], "contract_version": producer.contract["contractVersion"],
                "release_version": producer.parameters["ReleaseVersion"]}.items()})
        for _ in range(2):
            producer.aws.call("sqs", "send-message", queue_url=queue, message_body=message["Body"])
        producer.aws.call("sqs", "change-message-visibility", queue_url=queue,
                          receipt_handle=message["ReceiptHandle"], visibility_timeout=0)
    claim = producer.poll(lambda: producer.aws.call("dynamodb", "get-item", table_name=table, consistent_read=True,
        key={"fault_id": {"S": fault_id + "#CLAIM"}}).get("Item", {}), bool)
    require(all(value(claim, key) == val for key, val in {"fault_id": fault_id + "#CLAIM", "point": point,
        "tenant_id": session["tenant"], "execution_id": session["executionId"], "body_sha256": digest,
        "work_item_type": WORK_TYPE, "delegate": DELEGATE}.items()) and bool(value(claim, "message_id")),
        "work delivery claim does not correlate to actual request and production delegate")
    status = producer.poll(lambda: producer.action("STATUS", session["tenant"], session["executionId"]),
        lambda response: response.get("executionStatus") == "WAITING_EXTERNAL" and bool(response.get("awaitCheckpoint")))
    session["first"] = status["awaitCheckpoint"]
    if scenario == "5":
        require(all(value(claim, key) == val for key, val in {
            "execution_status": "WAITING_EXTERNAL", "pipeline_id": producer.contract["pipelineId"],
            "contract_version": producer.contract["contractVersion"], "release_version": producer.parameters["ReleaseVersion"],
            "interaction_id": session["first"]["interactionId"], "await_step_id": session["first"]["stepId"],
            "commit_observation": "PRODUCTION_CONTROL_PLANE_POINT_READS"}.items()),
            "after-ACK claim lacks the matching production commit observation")
    require(session["first"].get("tenantId") == session["tenant"] and "FirstApproval" in session["first"]["stepId"],
            "work did not progress to the first generated native Await")
    native = RESOURCES.assert_single_execution(producer, session, "WAITING_EXTERNAL")
    require(value(native, "attempt", "N") == "0", "work retry substituted for the duplicate delivery oracle")
    interactions = RESOURCES.native_rows(producer, "AwaitInteractionTable", session["tenant"])
    require(len(interactions) == 1 and value(interactions[0], "interaction_id") == session["first"]["interactionId"]
            and value(interactions[0], "execution_id") == session["executionId"], "duplicate work repeated first Await admission")
    session["provider"] = producer.poll(lambda: producer.provider(session["start"]["durableExecutionName"]), bool)
    arn = owned.provider(session["provider"]["DurableExecutionArn"])
    producer.aws.capture("work-redelivery-native-consequence", {"id": scenario, "point": point, "claim": claim,
        "native": native, "interactions": interactions, "bodySha256": digest,
        "semanticProgressObserved": True, "ackAloneIsEvidence": False})
    RESOURCES.complete_remaining(producer, session)
    RESOURCES.assert_success(producer, session, arn, 1)
