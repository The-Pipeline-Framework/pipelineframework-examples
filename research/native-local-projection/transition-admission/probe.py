"""Consume the fixed admission histories without a JVM or a network worker."""
from datetime import datetime
import hashlib
import hmac
import json
from pathlib import Path
import re
import sys

HERE = Path(__file__).resolve().parent
PROFILE = 'tpf-polyglot-transition-admission/1'
POLICY = 'tpf-polyglot-unary-policy/1'
OUTER_ENCODING = 'application/tpf-transition-envelope+json'
ENCODING = 'tpf-polyglot-research-json/1'
ROUTE = '/pipeline/worker/transitions/execute'
IDENTITY = ('pipelineId', 'contractVersion', 'releaseVersion')
ARTIFACT = ('artifactId', 'artifactDigest')
ATTEMPT = ('executionId', 'attempt', 'executionVersion', 'transitionKey')
CONTEXT = ('executionId', 'traceId')
INT_FIELDS = ('currentStepIndex', 'stopBeforeStepIndex', 'attempt', 'executionVersion', 'redriveStepIndex')
TEXT_FIELDS = (*IDENTITY, 'tenantId', 'executionId', 'traceId', 'transitionKey',
               'resultShape', 'payloadTypeId', 'payloadEncoding', 'payload', 'redriveIntent')


def timestamp(text):
    if not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z', text):
        raise ValueError('expected UTC seconds')
    return datetime.fromisoformat(text).timestamp()


def command(raw):
    try:
        value = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(value, dict) or set(value) != set(INT_FIELDS + TEXT_FIELDS):
        return None
    if any(type(value[field]) is not int for field in INT_FIELDS):
        return None
    if any(not isinstance(value[field], str) or (not value[field] and field not in CONTEXT)
           for field in TEXT_FIELDS):
        return None
    return value


class Transcript:
    def __init__(self, corpus, case):
        self.corpus = corpus
        self.target = corpus['target']
        self.capability = corpus['capability'] | case.get('capability', {})
        self.grant = corpus['grant'] | case.get('grant', {})
        self.claims = corpus['attempts'] | case.get('claims', {})
        self.requests = case.get('requests', {})
        name = case.get('projection', 'downstream-retry')
        if name not in ('downstream-retry', 'changed-order'):
            raise ValueError('unknown projection fixture')
        self.projection = (HERE.parent / 'runtime-policy/v1/generated' / name / 'projection.json').read_bytes()
        self.nonces = set()
        self.current = None
        self.disposition = False
        self.authorized = False
        self.replies = {}
        self.report = dict(diagnostics=[], physicalRequests=0, coordinatorDeliveries=0, envelopeReads=0,
                           payloadReads=0, attempts=dict(A=0, B=0, C=0), bInputs=[], contexts=[],
                           settlements=[], state='READY')

    def authenticate(self, frame):
        if any(not isinstance(frame.get(key), str) or not frame[key].strip()
               for key in ('timestamp', 'nonce', 'signature')):
            return 'MISSING_AUTH_HEADER'
        try:
            instant = timestamp(frame['timestamp'])
        except ValueError:
            return 'INVALID_TIMESTAMP'
        if abs(timestamp(self.corpus['now']) - instant) > self.corpus['toleranceSeconds']:
            return 'TIMESTAMP_OUTSIDE_WINDOW'
        body_hash = hashlib.sha256(frame['body'].encode()).hexdigest()
        preimage = '\n'.join(('POST', ROUTE, frame['timestamp'], frame['nonce'], body_hash))
        expected = hmac.digest(self.corpus['secret'].encode(), preimage.encode(), 'sha256').hex()
        if not hmac.compare_digest(expected.encode(), frame['signature'].encode()):
            return 'BAD_SIGNATURE'
        if frame['nonce'] in self.nonces:
            return 'NONCE_REPLAY'
        self.nonces.add(frame['nonce'])
        return 'OK'

    def verify_binding(self):
        advertised = self.capability
        if (advertised['protocolVersion'] != '1' or advertised['providerName'] != 'rest'
                or 'rest' not in advertised['workerProtocols']
                or OUTER_ENCODING not in advertised['payloadEncodings']
                or any(advertised.get(key) != self.target[key] for key in IDENTITY + ARTIFACT)):
            return 'UNVERIFIED_WORKER'
        if self.grant.get('profile') != POLICY:
            return 'UNSUPPORTED_POLICY_PROFILE'
        if (self.grant.get('artifactDigest') != self.target['artifactDigest']
                or self.grant.get('payloadEncoding') != ENCODING or self.grant.get('payloadTypeId') != 'ProbeValue'):
            return 'UNVERIFIED_POLICY_BINDING'
        if hashlib.sha256(self.projection).hexdigest() != self.target['artifactDigest']:
            return 'ARTIFACT_DIGEST_MISMATCH'
        return 'OK'

    def receive(self, name, claim=None):
        self.report['physicalRequests'] += 1
        frame = self.corpus['frames'][self.requests.get(name, name)]
        auth = self.authenticate(frame)
        if auth != 'OK':
            return auth
        self.report['envelopeReads'] += 1
        envelope = command(frame['body'])
        if envelope is None:
            return 'INVALID_ENVELOPE'
        if envelope['tenantId'] != self.target['tenantId']:
            return 'TENANT_FORBIDDEN'
        if any(envelope[key] != self.target[key] for key in IDENTITY):
            return 'IDENTITY_MISMATCH'
        if (envelope['currentStepIndex'] != 0 or envelope['stopBeforeStepIndex'] != -1
                or envelope['resultShape'] != 'SINGLE' or envelope['redriveIntent'] != 'REPLAY'
                or envelope['redriveStepIndex'] != -1):
            return 'UNSUPPORTED_RANGE'
        if any(not envelope[key].strip(' \t\r\n') for key in CONTEXT):
            return 'INVALID_CONTEXT'
        if claim is not None and any(envelope[key] != claim[key] for key in ATTEMPT):
            return 'ATTEMPT_MISMATCH'
        binding = self.verify_binding()
        if binding != 'OK':
            return binding
        if envelope['payloadEncoding'] != ENCODING:
            return 'UNSUPPORTED_PAYLOAD_ENCODING'
        if envelope['payloadTypeId'] != 'ProbeValue':
            return 'UNSUPPORTED_PAYLOAD_TYPE'
        self.report['payloadReads'] += 1
        try:
            payload = json.loads(envelope['payload'])
        except ValueError:
            return 'INVALID_PAYLOAD'
        if (not isinstance(payload, dict) or set(payload) != {'value'}
                or type(payload['value']) is not int or payload['value'] != 3):
            return 'INVALID_PAYLOAD'
        result = self.run_fixture(payload['value'], envelope)
        if claim is not None:
            self.replies[name] = {
                'outcome': 'COMPLETED',
                'outputPayloads': [{'payloadTypeId': 'ProbeValue', 'payloadEncoding': ENCODING,
                                    'payload': json.dumps({'value': result})}],
                'terminalOutputPublished': False, 'terminalInputPassthrough': False,
            }
        return 'COMPLETED'

    def run_fixture(self, value, envelope):
        # Model one admitted worker's observations after a first B failure.
        # The existing LOCAL binding supplies the live execution evidence.
        context = {key: envelope[key] for key in ('executionId', 'traceId')}
        self.report['contexts'].append(context)
        self.report['attempts']['A'] += 1
        value_for_b = value + 1
        self.report['attempts']['B'] += 1
        self.report['bInputs'].append(value_for_b)
        projection = json.loads(self.projection)
        if projection['policy']['retryOwner'] != 'LOCAL_SHELL' or projection['steps'][1]['retryLimit'] != 1:
            raise ValueError('worker fixture lacks its B retry policy')
        self.report['attempts']['B'] += 1
        self.report['bInputs'].append(value_for_b)
        self.report['attempts']['C'] += 1
        return value_for_b * 2 - 3

    def dispatch(self, name):
        state = self.report['state']
        initial = state == 'READY' and name == 'original'
        redrive = state == 'REMOTE_OUTCOME_UNKNOWN' and self.authorized and name == 'redrive'
        if not initial and not redrive:
            return 'REDRIVE_NOT_AUTHORIZED'
        binding = self.verify_binding()
        if binding != 'OK':
            return binding
        self.authorized = False
        self.disposition = False
        self.current = name
        self.report['state'] = 'IN_FLIGHT'
        self.report['coordinatorDeliveries'] += 1
        received = self.receive(name, self.claims[name])
        return 'DISPATCHED' if received == 'COMPLETED' else received

    def complete(self, event):
        name = event['frame']
        if name != self.current or name not in self.replies:
            return 'STALE_COMPLETION'
        if self.report['state'] == 'SETTLED':
            return 'ALREADY_SETTLED'
        if self.report['state'] != 'IN_FLIGHT':
            return 'STALE_COMPLETION'
        response = event.get('response', self.replies[name])
        if not isinstance(response, dict):
            return 'INVALID_WORKER_RESULT'
        if response.get('outcome') != 'COMPLETED':
            return 'UNSUPPORTED_WORKER_OUTCOME'
        outputs = response.get('outputPayloads')
        if (set(response) != {'outcome', 'outputPayloads', 'terminalOutputPublished', 'terminalInputPassthrough'}
                or not isinstance(outputs, list) or len(outputs) != 1 or not isinstance(outputs[0], dict)
                or set(outputs[0]) != {'payloadTypeId', 'payloadEncoding', 'payload'}
                or response.get('terminalOutputPublished') is not False
                or response.get('terminalInputPassthrough') is not False):
            return 'INVALID_WORKER_RESULT'
        output = outputs[0]
        if output.get('payloadEncoding') != ENCODING or output.get('payloadTypeId') != 'ProbeValue':
            return 'UNSUPPORTED_RESULT_ENCODING'
        try:
            value = json.loads(output.get('payload', ''))
        except (ValueError, TypeError):
            return 'INVALID_WORKER_RESULT'
        if (not isinstance(value, dict) or set(value) != {'value'} or type(value['value']) is not int
                or not -2147483648 <= value['value'] <= 2147483647):
            return 'INVALID_WORKER_RESULT'
        pin = self.claims[name]
        self.report['settlements'].append(dict(transitionKey=pin['transitionKey'],
                                               executionVersion=pin['executionVersion'],
                                               value=value['value']))
        self.report['state'] = 'SETTLED'
        return 'SETTLED'

    def apply(self, event):
        op = event['op']
        if op == 'receive':
            return self.receive(event['frame'])
        if op == 'dispatch':
            return self.dispatch(event['frame'])
        if op == 'complete':
            return self.complete(event)
        if op == 'lose-response' and self.report['state'] == 'IN_FLIGHT':
            self.report['state'] = 'REMOTE_OUTCOME_UNKNOWN'
            return 'REMOTE_OUTCOME_UNKNOWN'
        if op == 'confirm-disposition' and self.report['state'] == 'REMOTE_OUTCOME_UNKNOWN':
            if (event['authority'] != 'independent-observer' or event['frame'] != self.current
                    or event['disposition'] != 'ORIGINAL_ATTEMPT_INACTIVE'):
                return 'DISPOSITION_UNVERIFIED'
            self.disposition = True
            return 'DISPOSITION_CONFIRMED'
        if op == 'authorize-redrive' and self.report['state'] == 'REMOTE_OUTCOME_UNKNOWN':
            if event['authority'] != 'operator':
                return 'REDRIVE_FORBIDDEN'
            if self.current != 'original':
                return 'REDRIVE_NOT_AUTHORIZED'
            if not self.disposition:
                return 'DISPOSITION_REQUIRED'
            if event['prior'] != self.current or event['next'] != 'redrive':
                return 'REDRIVE_BINDING_MISMATCH'
            prior = self.claims[self.current]
            next_claim = self.claims[event['next']]
            if (next_claim['executionId'] != prior['executionId']
                    or next_claim['attempt'] <= prior['attempt']
                    or next_claim['executionVersion'] <= prior['executionVersion']
                    or next_claim['transitionKey'] == prior['transitionKey']):
                return 'REDRIVE_BINDING_MISMATCH'
            self.authorized = True
            return 'REDRIVE_AUTHORIZED'
        raise ValueError(f'unsupported transcript event: {op}')


def main(path):
    corpus = json.loads(path.read_bytes())
    if corpus['schemaVersion'] != 1 or corpus['profile'] != PROFILE:
        raise ValueError('unsupported transcript corpus')
    for case in corpus['cases']:
        model = Transcript(corpus, case)
        for event in case['events']:
            model.report['diagnostics'].append(model.apply(event))
        actual = json.dumps(model.report, sort_keys=True)
        expected = json.dumps(case['expected'], sort_keys=True)
        if actual != expected:
            print(f"TRANSITION_ADMISSION_MISMATCH:{case['id']}", file=sys.stderr)
            print(f'expected {expected}\nobserved {actual}', file=sys.stderr)
            return 1
    print(f"TRANSITION_ADMISSION_PASS:python:{len(corpus['cases'])}")
    return 0


if __name__ == '__main__':
    sys.exit(main(Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / 'vectors.json'))
