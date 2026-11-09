// Fixed transcript consumer. All state belongs to this research process.
import { createHash, createHmac, timingSafeEqual } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { isDeepStrictEqual } from 'node:util';

const PROFILE = 'tpf-polyglot-transition-admission/1';
const POLICY = 'tpf-polyglot-unary-policy/1';
const ENCODING = 'tpf-polyglot-research-json/1';
const ROUTE = '/pipeline/worker/transitions/execute';
const IDENTITY = ['pipelineId', 'contractVersion', 'releaseVersion'];
const ARTIFACT = ['artifactId', 'artifactDigest'];
const ATTEMPT = ['executionId', 'attempt', 'executionVersion', 'transitionKey'];
const CONTEXT = ['executionId', 'traceId'];
const INTS = ['currentStepIndex', 'stopBeforeStepIndex', 'attempt', 'executionVersion', 'redriveStepIndex'];
const STRINGS = [...IDENTITY, 'tenantId', 'executionId', 'traceId', 'transitionKey',
  'resultShape', 'payloadTypeId', 'payloadEncoding', 'payload', 'redriveIntent'];

const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const matches = (left, right, keys) => keys.every(key => left[key] === right[key]);

function timestamp(text) {
  const epoch = Date.parse(text);
  if (!Number.isFinite(epoch)) return NaN;
  return new Date(epoch).toISOString().replace('.000Z', 'Z') === text ? epoch / 1000 : NaN;
}

function readCommand(raw) {
  let value;
  try { value = JSON.parse(raw); } catch { return undefined; }
  if (!isObject(value) || Object.keys(value).length !== INTS.length + STRINGS.length) return undefined;
  if (!INTS.every(key => Number.isSafeInteger(value[key]))) return undefined;
  if (!STRINGS.every(key => typeof value[key] === 'string'
      && (value[key].length > 0 || CONTEXT.includes(key)))) return undefined;
  return value;
}

class Transcript {
  constructor(corpus, testCase) {
    this.corpus = corpus;
    this.target = corpus.target;
    this.capability = { ...corpus.capability, ...testCase.capability };
    this.grant = { ...corpus.grant, ...testCase.grant };
    this.claims = { ...corpus.attempts, ...testCase.claims };
    this.requests = testCase.requests ?? {};
    const name = testCase.projection ?? 'downstream-retry';
    if (!['downstream-retry', 'changed-order'].includes(name)) throw new Error('unknown projection fixture');
    const projection = new URL('../runtime-policy/v1/generated/' + name + '/projection.json', import.meta.url);
    this.projection = readFileSync(projection);
    this.nonces = new Set();
    this.disposition = false;
    this.authorized = false;
    this.replies = new Map();
    this.report = {
      diagnostics: [], physicalRequests: 0, coordinatorDeliveries: 0, envelopeReads: 0,
      payloadReads: 0, attempts: { A: 0, B: 0, C: 0 }, bInputs: [], contexts: [],
      settlements: [], state: 'READY',
    };
  }

  authenticate(frame) {
    if (!['timestamp', 'nonce', 'signature'].every(key => typeof frame[key] === 'string' && frame[key].trim())) {
      return 'MISSING_AUTH_HEADER';
    }
    const instant = timestamp(frame.timestamp);
    if (!Number.isFinite(instant)) return 'INVALID_TIMESTAMP';
    if (Math.abs(timestamp(this.corpus.now) - instant) > this.corpus.toleranceSeconds) {
      return 'TIMESTAMP_OUTSIDE_WINDOW';
    }
    const preimage = ['POST', ROUTE, frame.timestamp, frame.nonce, sha256(frame.body)].join('\n');
    const expected = Buffer.from(createHmac('sha256', this.corpus.secret).update(preimage).digest('hex'));
    const actual = Buffer.from(frame.signature);
    if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) return 'BAD_SIGNATURE';
    if (this.nonces.has(frame.nonce)) return 'NONCE_REPLAY';
    this.nonces.add(frame.nonce);
    return 'OK';
  }

  verifyBinding() {
    const capability = this.capability;
    if (capability.protocolVersion !== '1' || capability.providerName !== 'rest'
        || !capability.workerProtocols.includes('rest') || !capability.payloadEncodings.includes('json')
        || !matches(capability, this.target, [...IDENTITY, ...ARTIFACT])) return 'UNVERIFIED_WORKER';
    if (this.grant.profile !== POLICY) return 'UNSUPPORTED_POLICY_PROFILE';
    if (this.grant.artifactDigest !== this.target.artifactDigest
        || this.grant.payloadEncoding !== ENCODING || this.grant.payloadTypeId !== 'ProbeValue') {
      return 'UNVERIFIED_POLICY_BINDING';
    }
    if (sha256(this.projection) !== this.target.artifactDigest) return 'ARTIFACT_DIGEST_MISMATCH';
    return 'OK';
  }

  receive(name, claim) {
    this.report.physicalRequests++;
    const frame = this.corpus.frames[this.requests[name] ?? name];
    const auth = this.authenticate(frame);
    if (auth !== 'OK') return auth;
    this.report.envelopeReads++;
    const envelope = readCommand(frame.body);
    if (!envelope) return 'INVALID_ENVELOPE';
    if (envelope.tenantId !== this.target.tenantId) return 'TENANT_FORBIDDEN';
    if (!matches(envelope, this.target, IDENTITY)) return 'IDENTITY_MISMATCH';
    if (envelope.currentStepIndex !== 0 || envelope.stopBeforeStepIndex !== -1
        || envelope.resultShape !== 'SINGLE' || envelope.redriveIntent !== 'REPLAY'
        || envelope.redriveStepIndex !== -1) return 'UNSUPPORTED_RANGE';
    if (CONTEXT.some(key => !/[^ \t\r\n]/.test(envelope[key]))) return 'INVALID_CONTEXT';
    if (claim && !matches(envelope, claim, ATTEMPT)) return 'ATTEMPT_MISMATCH';
    const binding = this.verifyBinding();
    if (binding !== 'OK') return binding;
    if (envelope.payloadEncoding !== ENCODING) return 'UNSUPPORTED_PAYLOAD_ENCODING';
    if (envelope.payloadTypeId !== 'ProbeValue') return 'UNSUPPORTED_PAYLOAD_TYPE';
    this.report.payloadReads++;
    let payload;
    try { payload = JSON.parse(envelope.payload); } catch { return 'INVALID_PAYLOAD'; }
    if (!isObject(payload) || Object.keys(payload).length !== 1 || payload.value !== 3) return 'INVALID_PAYLOAD';
    const result = this.runFixture(payload.value, envelope);
    if (claim) this.replies.set(name, {
      outcome: 'COMPLETED',
      outputPayloads: [{ payloadTypeId: 'ProbeValue', payloadEncoding: ENCODING,
        payload: JSON.stringify({ value: result }) }],
      terminalOutputPublished: false, terminalInputPassthrough: false,
    });
    return 'COMPLETED';
  }

  runFixture(value, { executionId, traceId }) {
    // Model the worker's observations after B fails once. Live execution is covered by the LOCAL binding.
    this.report.contexts.push(Object.freeze({ executionId, traceId }));
    this.report.attempts.A++;
    const bInput = value + 1;
    this.report.attempts.B++;
    this.report.bInputs.push(bInput);
    const projection = JSON.parse(this.projection);
    if (projection.policy.retryOwner !== 'LOCAL_SHELL' || projection.steps[1].retryLimit !== 1) {
      throw new Error('worker fixture lacks its B retry policy');
    }
    this.report.attempts.B++;
    this.report.bInputs.push(bInput);
    this.report.attempts.C++;
    return bInput * 2 - 3;
  }

  dispatch(name) {
    const initial = this.report.state === 'READY' && name === 'original';
    const redrive = this.report.state === 'REMOTE_OUTCOME_UNKNOWN' && this.authorized && name === 'redrive';
    if (!initial && !redrive) return 'REDRIVE_NOT_AUTHORIZED';
    const binding = this.verifyBinding();
    if (binding !== 'OK') return binding;
    this.authorized = false;
    this.disposition = false;
    this.current = name;
    this.report.state = 'IN_FLIGHT';
    this.report.coordinatorDeliveries++;
    const received = this.receive(name, this.claims[name]);
    return received === 'COMPLETED' ? 'DISPATCHED' : received;
  }

  complete(event) {
    const name = event.frame;
    if (name !== this.current || !this.replies.has(name)) return 'STALE_COMPLETION';
    if (this.report.state === 'SETTLED') return 'ALREADY_SETTLED';
    if (this.report.state !== 'IN_FLIGHT') return 'STALE_COMPLETION';
    const response = Object.hasOwn(event, 'response') ? event.response : this.replies.get(name);
    if (!isObject(response)) return 'INVALID_WORKER_RESULT';
    if (response.outcome !== 'COMPLETED') return 'UNSUPPORTED_WORKER_OUTCOME';
    const outputs = response.outputPayloads;
    if (Object.keys(response).length !== 4 || !Array.isArray(outputs) || outputs.length !== 1
        || !isObject(outputs[0]) || Object.keys(outputs[0]).length !== 3
        || response.terminalOutputPublished !== false || response.terminalInputPassthrough !== false) {
      return 'INVALID_WORKER_RESULT';
    }
    const output = outputs[0];
    if (output.payloadEncoding !== ENCODING || output.payloadTypeId !== 'ProbeValue') {
      return 'UNSUPPORTED_RESULT_ENCODING';
    }
    let value;
    try { value = JSON.parse(output.payload); } catch { return 'INVALID_WORKER_RESULT'; }
    if (!isObject(value) || Object.keys(value).length !== 1 || !Number.isInteger(value.value)
        || value.value < -2147483648 || value.value > 2147483647) {
      return 'INVALID_WORKER_RESULT';
    }
    const { transitionKey, executionVersion } = this.claims[name];
    this.report.settlements.push({ transitionKey, executionVersion, value: value.value });
    this.report.state = 'SETTLED';
    return 'SETTLED';
  }

  apply(event) {
    switch (event.op) {
      case 'receive': return this.receive(event.frame);
      case 'dispatch': return this.dispatch(event.frame);
      case 'complete': return this.complete(event);
      case 'lose-response':
        if (this.report.state !== 'IN_FLIGHT') break;
        this.report.state = 'REMOTE_OUTCOME_UNKNOWN';
        return 'REMOTE_OUTCOME_UNKNOWN';
      case 'confirm-disposition':
        if (this.report.state !== 'REMOTE_OUTCOME_UNKNOWN') break;
        if (event.authority !== 'independent-observer' || event.frame !== this.current
            || event.disposition !== 'ORIGINAL_ATTEMPT_INACTIVE') return 'DISPOSITION_UNVERIFIED';
        this.disposition = true;
        return 'DISPOSITION_CONFIRMED';
      case 'authorize-redrive':
        if (this.report.state !== 'REMOTE_OUTCOME_UNKNOWN') break;
        if (event.authority !== 'operator') return 'REDRIVE_FORBIDDEN';
        if (this.current !== 'original') return 'REDRIVE_NOT_AUTHORIZED';
        if (!this.disposition) return 'DISPOSITION_REQUIRED';
        if (event.prior !== this.current || event.next !== 'redrive') return 'REDRIVE_BINDING_MISMATCH';
        {
          const prior = this.claims[this.current];
          const next = this.claims[event.next];
          if (next.executionId !== prior.executionId || next.attempt <= prior.attempt
              || next.executionVersion <= prior.executionVersion || next.transitionKey === prior.transitionKey) {
            return 'REDRIVE_BINDING_MISMATCH';
          }
        }
        this.authorized = true;
        return 'REDRIVE_AUTHORIZED';
    }
    throw new Error('unsupported transcript event: ' + event.op);
  }
}

const corpus = JSON.parse(readFileSync(process.argv[2] ?? new URL('./vectors.json', import.meta.url)));
if (corpus.schemaVersion !== 1 || corpus.profile !== PROFILE) throw new Error('unsupported transcript corpus');
for (const testCase of corpus.cases) {
  const model = new Transcript(corpus, testCase);
  for (const event of testCase.events) model.report.diagnostics.push(model.apply(event));
  if (!isDeepStrictEqual(model.report, testCase.expected)) {
    console.error('TRANSITION_ADMISSION_MISMATCH:' + testCase.id);
    console.error('expected', JSON.stringify(testCase.expected), 'observed', JSON.stringify(model.report));
    process.exit(1);
  }
}
console.log('TRANSITION_ADMISSION_PASS:node:' + corpus.cases.length);
