"""Conformance-only derived SAM and archive binding; production source is unchanged."""
import hashlib
import importlib.util
from pathlib import Path

HANDLER = 'conformance-transition-delivery'
DELEGATE = 'org.pipelineframework.aws.durable.AwsDurableTransitionWorkerHandler'
WRAPPER = 'org/pipelineframework/awsconformance/faults/TransitionDeliveryFaultHandler'


def derived_template(source):
    text = source.decode()
    def replace_once(before, after):
        nonlocal text
        if text.count(before) != 1:
            raise ValueError('Production SAM fixture anchor missing or ambiguous: ' + before)
        text = text.replace(before, after, 1)
    replace_once('  TransitionWorkerHandler: {Type: String, Default: aws-durable-transition-worker}',
                 '  TransitionWorkerHandler: {Type: String, Default: ' + HANDLER + ', AllowedValues: [' + HANDLER + ']}')
    replace_once('Variables: {<<: *ActionEnvironment, QUARKUS_LAMBDA_HANDLER: !Ref TransitionWorkerHandler}',
                 'Variables: {<<: *ActionEnvironment, QUARKUS_LAMBDA_HANDLER: !Ref TransitionWorkerHandler, TPF_CONFORMANCE_TRANSITION_FAULT_TABLE: !Ref ConformanceTransitionFaultTable}')
    replace_once('Resources:\n', 'Resources:\n  ConformanceTransitionFaultTable:\n    Condition: FunctionTransitionConsumer\n    Type: AWS::DynamoDB::Table\n    Properties:\n      BillingMode: PAY_PER_REQUEST\n      AttributeDefinitions: [{AttributeName: fault_id, AttributeType: S}]\n      KeySchema: [{AttributeName: fault_id, KeyType: HASH}]\n      SSESpecification: {SSEEnabled: true}\n  ConformanceTransitionFaultAccess:\n    Condition: FunctionTransitionConsumer\n    Type: AWS::IAM::Policy\n    Properties:\n      PolicyName: conformance-transition-fault\n      Roles: [!Ref WorkerRole]\n      PolicyDocument:\n        Version: "2012-10-17"\n        Statement:\n          - Effect: Allow\n            Action: [dynamodb:GetItem, dynamodb:PutItem]\n            Resource: !GetAtt ConformanceTransitionFaultTable.Arn\n')
    digest = hashlib.sha256(source).hexdigest()
    replace_once('Outputs:\n', 'Outputs:\n  ConformanceTransitionFaultManifest:\n    Condition: FunctionTransitionConsumer\n    Value: !Sub |\n      {"schemaVersion":1,"fixtureOnly":true,"handler":"' + HANDLER + '","delegate":"' + DELEGATE + '","transitionVersionArn":"${TransitionWorkerVersion}","faultTable":"${ConformanceTransitionFaultTable}","productionTemplateSha256":"' + digest + '"}\n')
    return text.encode()


def verify(action_archive, source, derived):
    spec = importlib.util.spec_from_file_location('fixture_gate', Path(__file__).with_name('verify-production-conformance.py'))
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)
    classes, _ = gate.inspect_archive(Path(action_archive).read_bytes(), str(action_archive))
    for name in (WRAPPER + '.class', WRAPPER + '_Bean.class', DELEGATE.replace('.', '/') + '.class', DELEGATE.replace('.', '/') + '_Bean.class'):
        gate.require(any(value.endswith(name) for value in classes), 'Transition fixture archive lacks discovered wrapper/production delegate: ' + name)
    gate.require(derived == derived_template(source), 'Transition fixture SAM differs from exact production-derived wiring')
    return {'schemaVersion': 1, 'fixtureOnly': True, 'handler': HANDLER, 'delegate': DELEGATE,
            'productionTemplateSha256': hashlib.sha256(source).hexdigest(),
            'fixtureTemplateSha256': hashlib.sha256(derived).hexdigest()}
