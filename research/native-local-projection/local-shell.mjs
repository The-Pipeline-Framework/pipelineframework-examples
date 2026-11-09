/** Test-only native shell. The compiler owns ordering/types/policy; this file never reads YAML. */
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';

export const POLICY_PROFILE = 'tpf-polyglot-unary-policy/1';
const policyBinding = Object.freeze({
  failureMapping: 'structured-failure/1', retryOwner: 'LOCAL_SHELL',
  resultMapping: 'explicit-value/1', cancellation: 'first-accepted-terminal/1',
  context: 'explicit-snapshot/1', recovery: 'REJECT', legacyPolicy: 'REJECT',
});

export class Diagnostic extends Error {
  constructor(code) { super(code); this.code = code; }
}
const reject = code => { throw new Diagnostic(code); };
const keys = (value, names, code) => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) reject(code);
  const actual = Object.keys(value);
  if (actual.length !== names.length || !names.every(name => actual.includes(name))) reject(code);
};
const same = (actual, expected, code) => {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) reject(code);
};
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const freeze = value => {
  if (value && typeof value === 'object') {
    Object.values(value).forEach(freeze);
    Object.freeze(value);
  }
  return value;
};
const digest = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);

// Bind data once so host getters cannot change a validated context, result or failure during copying.
function policyRecord(value, code) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) reject(code);
  let descriptors;
  try {
    descriptors = Object.getOwnPropertyDescriptors(value);
  } catch {
    reject(code);
  }
  return Object.fromEntries(Reflect.ownKeys(descriptors).map(key => {
    const descriptor = descriptors[key];
    if (typeof key !== 'string' || !descriptor.enumerable || !Object.hasOwn(descriptor, 'value')) reject(code);
    return [key, descriptor.value];
  }));
}

// Config numbers compare exactly, rather than rounding near-integer JSON into an allowed value.
// The generated artifact contains small integers. Equivalent 1.0/1e0 forms remain exact.
function exactSmallInteger(source, number) {
  const m = /^(-?)(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/.exec(source);
  if (!m || !Number.isSafeInteger(number)) reject('INVALID_NUMBER');
  const exponent = Number(m[4] ?? 0) - (m[3]?.length ?? 0);
  if (!Number.isSafeInteger(exponent) || Math.abs(exponent) > 1000) reject('INVALID_NUMBER');
  let digits = BigInt(m[2] + (m[3] ?? ''));
  if (exponent >= 0) digits *= 10n ** BigInt(exponent);
  else {
    const scale = 10n ** BigInt(-exponent);
    if (digits % scale !== 0n) reject('INVALID_NUMBER');
    digits /= scale;
  }
  if (m[1]) digits = -digits;
  if (digits !== BigInt(number)) reject('INVALID_NUMBER');
  return number;
}
export function parseProjection(bytes) {
  try {
    return JSON.parse(bytes.toString('utf8'), (_key, value, context) =>
      typeof value === 'number' ? exactSmallInteger(context.source, value) : value);
  } catch (error) {
    if (error instanceof Diagnostic) throw error;
    reject('INVALID_JSON');
  }
}

/** Pin bytes before interpreting them. A digest is provenance/integrity, not authentication. */
export function admitProjection(bytes, expectedDigest, handlers, {profile = 'tpf-polyglot-probe/0'} = {}) {
  if (!digest(expectedDigest) || hash(bytes) !== expectedDigest) reject('ARTIFACT_DIGEST_MISMATCH');
  const p = parseProjection(bytes);
  const policy = profile === POLICY_PROFILE;
  keys(p, ['schemaVersion', 'profile', 'kind', 'pipeline', 'requiredCapabilities', 'types', 'steps', 'contextKeys', 'provenance',
    ...(policy ? ['policy'] : [])], 'UNSUPPORTED_PROJECTION');
  if (p.schemaVersion !== 1) reject('UNSUPPORTED_SCHEMA');
  if ((!policy && profile !== 'tpf-polyglot-probe/0') || p.profile !== profile || p.kind !== 'compiler-projection-probe') reject('UNSUPPORTED_PROFILE');
  if (policy) {
    keys(p.policy, Object.keys(policyBinding), 'UNSUPPORTED_POLICY');
    if (Object.entries(policyBinding).some(([key, value]) => p.policy[key] !== value)) reject('UNSUPPORTED_POLICY');
  }
  keys(p.pipeline, ['id', 'inputType', 'outputType', 'transport', 'platform', 'execution'], 'UNSUPPORTED_PIPELINE');
  if (typeof p.pipeline.id !== 'string' || !p.pipeline.id) reject('INVALID_PIPELINE_ID');
  if (p.pipeline.transport !== 'LOCAL' || p.pipeline.platform !== 'COMPUTE' || p.pipeline.execution !== 'DIRECT') reject('UNSUPPORTED_EXECUTION');
  same(p.requiredCapabilities, ['pure', 'linear', 'unary', 'local', 'retry', 'cancellation', 'context'], 'UNSUPPORTED_CAPABILITY');
  same(p.contextKeys, ['executionId', 'traceId'], 'UNSUPPORTED_CONTEXT');
  if (!Array.isArray(p.types) || p.types.length < 1) reject('UNSUPPORTED_TYPE');
  const types = new Map();
  for (const type of p.types) {
    keys(type, ['id', 'fields'], 'UNSUPPORTED_TYPE');
    if (typeof type.id !== 'string' || !type.id || types.has(type.id)
        || !Array.isArray(type.fields) || !type.fields.length) reject('UNSUPPORTED_TYPE');
    const fields = new Set();
    for (const field of type.fields) {
      keys(field, ['name', 'type', 'presence', 'nullability'], 'UNSUPPORTED_TYPE');
      if (typeof field.name !== 'string' || !field.name || fields.has(field.name)
          || field.type !== 'int32' || !['REQUIRED', 'OPTIONAL'].includes(field.presence)
          || !['NON_NULL', 'NULLABLE'].includes(field.nullability)) reject('UNSUPPORTED_TYPE');
      fields.add(field.name);
    }
    types.set(type.id, type);
  }
  const type = types.get(p.pipeline.inputType);
  if (!type || p.pipeline.outputType !== type.id) reject('TYPE_MISMATCH');
  // The executable research slice is one non-null int32 value. Other emitted models
  // (e.g. PresenceProbe) are declaration evidence, not additional executable codecs.
  same(type.fields, [{name: 'value', type: 'int32', presence: 'REQUIRED', nullability: 'NON_NULL'}], 'UNSUPPORTED_TYPE');
  if (!Array.isArray(p.steps) || p.steps.length !== 3) reject('UNSUPPORTED_STEP_SHAPE');
  const ids = new Set(), bindings = new Set();
  for (const step of p.steps) {
    keys(step, ['id', 'inputType', 'outputType', 'handler', 'retryLimit'], 'UNSUPPORTED_STEP');
    if (typeof step.id !== 'string' || !step.id || ids.has(step.id)) reject('DUPLICATE_STEP');
    ids.add(step.id);
    if (step.inputType !== type.id || step.outputType !== type.id) reject('TYPE_MISMATCH');
    if (typeof step.handler !== 'string' || !Object.hasOwn(handlers, step.handler)
        || typeof handlers[step.handler] !== 'function') reject('MISSING_HANDLER');
    if (bindings.has(step.handler)) reject('AMBIGUOUS_HANDLER');
    bindings.add(step.handler);
    if (!Number.isInteger(step.retryLimit) || step.retryLimit < 0 || step.retryLimit > 2) reject('UNSUPPORTED_RETRY');
  }
  keys(p.provenance, ['dslSha256', 'bindingsSha256', 'generatorTreeSha256', 'contractsVersion', 'dslJarSha256', 'semanticModelJarSha256'], 'INVALID_PROVENANCE');
  if (p.provenance.contractsVersion !== '26.11.1'
      || Object.entries(p.provenance).some(([key, value]) => key !== 'contractsVersion' && !digest(value))) reject('INVALID_PROVENANCE');
  // Keep an admitted artifact stable even when its caller retains references.
  return freeze({projection: p, handlers: Object.fromEntries(
    p.steps.map(step => [step.handler, handlers[step.handler]]))});
}
export async function loadProjection(path, digestPath, handlers) {
  const bytes = await readFile(path);
  return admitProjection(bytes, (await readFile(digestPath, 'utf8')).trim(), handlers);
}

export function validateValue(value) {
  keys(value, ['value'], 'INVALID_VALUE');
  if (!Number.isInteger(value.value) || Object.is(value.value, -0) || value.value < -2147483648 || value.value > 2147483647) reject('INVALID_VALUE');
}
export function validateContext(context, policy = false) {
  keys(context, ['executionId', 'traceId'], 'INVALID_CONTEXT');
  if (Object.values(context).some(value => typeof value !== 'string' || (policy ? !/[^ \t\r\n]/.test(value) : !value))) reject('INVALID_CONTEXT');
}
function validateFailure(failure, stepId) {
  keys(failure, ['code', 'message', 'retryable', 'owner', 'stepId'], 'MALFORMED_FAILURE');
  if (!['TRANSIENT', 'BUSINESS_REJECTION'].includes(failure.code)) reject('UNKNOWN_FAILURE');
  if (typeof failure.message !== 'string' || typeof failure.retryable !== 'boolean' || failure.stepId !== stepId) reject('MALFORMED_FAILURE');
  if (failure.owner !== 'local-invocation-shell' || failure.retryable !== (failure.code === 'TRANSIENT')) reject('FAILURE_POLICY_CONFLICT');
}

/** Explicit carrier for structured failures. All other host throws are unclassified. */
export class StructuredFailure extends Error {
  #envelope;
  constructor(envelope) { super('Structured business function failure'); this.#envelope = envelope; }
  static envelope(error) { return #envelope in error ? error.#envelope : undefined; }
}

const sanitised = 'Unclassified business function failure';
function bindFailure(error, stepId) {
  const failure = error instanceof StructuredFailure ? policyRecord(StructuredFailure.envelope(error), 'MALFORMED_FAILURE') : {
    code: 'UNCLASSIFIED', message: sanitised, retryable: false, owner: policyBinding.retryOwner, stepId,
  };
  keys(failure, ['code', 'message', 'retryable', 'owner', 'stepId'], 'MALFORMED_FAILURE');
  if (['code', 'message', 'owner', 'stepId'].some(key => typeof failure[key] !== 'string')
      || typeof failure.retryable !== 'boolean') reject('MALFORMED_FAILURE');
  if (!['TRANSIENT', 'BUSINESS_REJECTION', 'UNCLASSIFIED'].includes(failure.code)) reject('UNKNOWN_FAILURE_CODE');
  if (failure.owner !== policyBinding.retryOwner || failure.stepId !== stepId
      || failure.retryable !== (failure.code === 'TRANSIENT')
      || (failure.code === 'UNCLASSIFIED' && failure.message !== sanitised)) reject('FAILURE_BINDING_MISMATCH');
  return Object.freeze({...failure});
}

function bindResult(returned) {
  returned = policyRecord(returned, 'INVALID_VALUE');
  if (returned.kind === 'EMPTY' && Object.keys(returned).length === 1) reject('UNSUPPORTED_EMPTY');
  keys(returned, ['kind', 'value'], 'INVALID_VALUE');
  if (returned.kind !== 'VALUE') reject('INVALID_VALUE');
  return policyRecord(returned.value, 'INVALID_VALUE');
}

/** Each awaited return goes to this shell before any downstream handler is selected. */
export async function execute(admission, input, context, {
  signal, afterReturn, beforeRetry, onTerminal, onDelivery, ownership = {legacyRetry: false, recovery: false},
} = {}) {
  const {projection, handlers} = admission;
  const policy = projection.profile === POLICY_PROFILE;
  const attempts = Object.fromEntries(projection.steps.map(step => [step.id, 0]));
  const trace = [];
  const delivery = [];
  let terminal;
  let observations = 0;
  const summary = () => ({terminal, attempts, trace, observations, delivery});
  const finish = outcome => {
    if (!terminal) {
      terminal = freeze(outcome);
      observations += 1;
      onTerminal?.(terminal);
      if (terminal.kind !== 'CANCELLED') {
        delivery.push(terminal);
        onDelivery?.(terminal);
      }
    }
    return summary();
  };
  const acceptCancel = () => finish({kind: 'CANCELLED'});
  let localContext;
  let value;
  const event = (kind, step) => trace.push({kind, stepId: step.id, ...localContext});
  const cancelled = step => {
    if (policy) return terminal?.kind === 'CANCELLED';
    if (!signal?.aborted) return false;
    event('cancelled', step);
    return true;
  };
  const cancelledResult = () => policy ? summary() : {outcome: 'CANCELLED', attempts, trace};
  try {
    const boundInput = policy ? policyRecord(input, 'INVALID_VALUE') : input;
    const boundContext = policy ? policyRecord(context, 'INVALID_CONTEXT') : context;
    validateValue(boundInput);
    validateContext(boundContext, policy);
    localContext = Object.freeze({...boundContext});
    value = {...boundInput};
    if (policy) {
      const boundOwnership = policyRecord(ownership, 'INVALID_POLICY');
      keys(boundOwnership, ['legacyRetry', 'recovery'], 'INVALID_POLICY');
      if (Object.values(boundOwnership).some(flag => typeof flag !== 'boolean')) reject('INVALID_POLICY');
      if (boundOwnership.legacyRetry || boundOwnership.recovery) reject('POLICY_OWNERSHIP_CONFLICT');
      signal?.addEventListener('abort', acceptCancel, {once: true});
      if (signal?.aborted) return acceptCancel();
    }
    for (const [index, step] of projection.steps.entries()) {
      if (cancelled(step)) return cancelledResult();
      for (;;) {
        attempts[step.id] += 1;
        event('invoke', step);
        let returned;
        try {
          // Each business function runs as a Promise in this process.
          returned = await handlers[step.handler](Object.freeze({...value}), localContext);
        } catch (error) {
          if (cancelled(step)) return cancelledResult();
          const failure = policy ? bindFailure(error, step.id) : error;
          if (!policy) validateFailure(failure, step.id);
          event('failure', step);
          if (!failure.retryable || attempts[step.id] > step.retryLimit) {
            return policy ? finish({kind: 'FAILED', failure}) : {outcome: 'FAILED', attempts, trace, failure};
          }
          event('retry', step);
          if (policy && beforeRetry) await beforeRetry(step.id);
          if (policy && cancelled(step)) return cancelledResult();
          continue;
        }
        if (cancelled(step)) return cancelledResult();
        if (policy) returned = bindResult(returned);
        validateValue(returned);
        value = {...returned};
        event('return', step);
        if (policy && index === projection.steps.length - 1) {
          finish({kind: 'COMPLETED', outcome: {kind: 'VALUE', value}});
        }
        // Test scheduling only. A final value is already committed before this hook.
        if (afterReturn) await afterReturn(step.id);
        break;
      }
    }
    if (policy) return summary();
    if (cancelled(projection.steps.at(-1))) return cancelledResult();
    return {outcome: 'COMPLETED', attempts, trace, result: value};
  } catch (error) {
    if (policy && error instanceof Diagnostic) return finish({kind: 'REJECTED', diagnostic: error.code});
    throw error;
  } finally {
    if (policy) signal?.removeEventListener('abort', acceptCancel);
  }
}
