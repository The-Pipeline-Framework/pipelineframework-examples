"""Conformance-only derived SAM and archive binding; production source is unchanged."""
import hashlib
import importlib.util
import re
from pathlib import Path

HANDLER = 'conformance-transition-delivery'
DELEGATE = 'org.pipelineframework.aws.durable.AwsDurableTransitionWorkerHandler'
WRAPPER = 'org/pipelineframework/awsconformance/faults/TransitionDeliveryFaultHandler'


def transition_fault_environment(text):
    """Change only the production transition worker's recognized environment form."""
    marker = '  TransitionWorkerFunction:\n'
    if text.count(marker) != 1:
        raise ValueError('Unique production TransitionWorkerFunction required')
    start = text.index(marker) + len(marker)
    following = re.search(r'(?m)^(?:  [A-Za-z][A-Za-z0-9]*:|Outputs:)', text[start:])
    if following is None:
        raise ValueError('Production transition resource boundary missing')
    end = start + following.start()
    body = text[start:end]
    legacy = 'Variables: {<<: *ActionEnvironment, QUARKUS_LAMBDA_HANDLER: !Ref TransitionWorkerHandler}'
    literal_header = '      Environment:\n        Variables:\n'
    legacy_header = '      Environment:\n        ' + legacy + '\n'
    declarations = list(re.finditer(r'(?m)^ +QUARKUS_LAMBDA_HANDLER:[^\n]*\n', body))
    if 'TPF_CONFORMANCE_TRANSITION_FAULT_TABLE' in body:
        raise ValueError('Production transition fault entry already present')
    if (body.count(legacy_header) == 1 and body.count(legacy) == 1 and literal_header not in body
            and not declarations and body.count('QUARKUS_LAMBDA_HANDLER:') == 1):
        body = body.replace(legacy, legacy[:-1] + ', TPF_CONFORMANCE_TRANSITION_FAULT_TABLE: !Ref ConformanceTransitionFaultTable}', 1)
    else:
        if body.count(literal_header) != 1 or legacy in body or len(declarations) != 1:
            raise ValueError('Literal production transition environment missing, mixed or ambiguous')
        variables_start = body.index(literal_header) + len(literal_header)
        following_field = re.search(r'(?m)^ {0,8}\S', body[variables_start:])
        variables_end = variables_start + following_field.start() if following_field else len(body)
        found = declarations[0]
        if not (variables_start <= found.start() < found.end() <= variables_end) or not re.fullmatch(
                r'''          QUARKUS_LAMBDA_HANDLER: !Ref (?:TransitionWorkerHandler|'TransitionWorkerHandler'|"TransitionWorkerHandler")\n''', found.group()):
            raise ValueError('Exact transition handler required inside Environment.Variables')
        body = body[:found.end()] + '          TPF_CONFORMANCE_TRANSITION_FAULT_TABLE: !Ref ConformanceTransitionFaultTable\n' + body[found.end():]
    return text[:start] + body + text[end:]


def derived_template(source):
    text = source.decode()
    def replace_once(before, after):
        nonlocal text
        if text.count(before) != 1:
            raise ValueError('Production SAM fixture anchor missing or ambiguous: ' + before)
        text = text.replace(before, after, 1)
    replace_once('  TransitionWorkerHandler: {Type: String, Default: aws-durable-transition-worker}',
                 '  TransitionWorkerHandler: {Type: String, Default: ' + HANDLER + ', AllowedValues: [' + HANDLER + ']}')
    text = transition_fault_environment(text)
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
