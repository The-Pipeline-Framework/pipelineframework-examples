/** Test-only native shell. The compiler owns ordering/types/policy; this file never reads YAML. */
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';

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
export function admitProjection(bytes, expectedDigest, handlers) {
  if (!digest(expectedDigest) || hash(bytes) !== expectedDigest) reject('ARTIFACT_DIGEST_MISMATCH');
  const p = parseProjection(bytes);
  keys(p, ['schemaVersion', 'profile', 'kind', 'pipeline', 'requiredCapabilities', 'types', 'steps', 'contextKeys', 'provenance'], 'UNSUPPORTED_PROJECTION');
  if (p.schemaVersion !== 1) reject('UNSUPPORTED_SCHEMA');
  if (p.profile !== 'tpf-polyglot-probe/0' || p.kind !== 'compiler-projection-probe') reject('UNSUPPORTED_PROFILE');
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
export function validateContext(context) {
  keys(context, ['executionId', 'traceId'], 'INVALID_CONTEXT');
  if (Object.values(context).some(value => typeof value !== 'string' || !value)) reject('INVALID_CONTEXT');
}
function validateFailure(failure, stepId) {
  keys(failure, ['code', 'message', 'retryable', 'owner', 'stepId'], 'MALFORMED_FAILURE');
  if (!['TRANSIENT', 'BUSINESS_REJECTION'].includes(failure.code)) reject('UNKNOWN_FAILURE');
  if (typeof failure.message !== 'string' || typeof failure.retryable !== 'boolean' || failure.stepId !== stepId) reject('MALFORMED_FAILURE');
  if (failure.owner !== 'local-invocation-shell' || failure.retryable !== (failure.code === 'TRANSIENT')) reject('FAILURE_POLICY_CONFLICT');
}

/** Each awaited return goes to this shell before any downstream handler is selected. */
export async function execute(admission, input, context, { signal, afterReturn } = {}) {
  const {projection, handlers} = admission;
  validateValue(input);
  validateContext(context);
  const localContext = Object.freeze({...context});
  const attempts = Object.fromEntries(projection.steps.map(step => [step.id, 0]));
  const trace = [];
  let value = {...input};
  const event = (kind, step) => trace.push({kind, stepId: step.id, ...localContext});
  const cancelled = step => {
    if (!signal?.aborted) return false;
    event('cancelled', step);
    return true;
  };
  for (const step of projection.steps) {
    if (cancelled(step)) return {outcome: 'CANCELLED', attempts, trace};
    for (;;) {
      attempts[step.id] += 1;
      event('invoke', step);
      let returned;
      try {
        // No JVM, subprocess, RPC, or coordinator: a native Promise in this process.
        returned = await handlers[step.handler](Object.freeze({...value}), localContext);
      } catch (failure) {
        if (cancelled(step)) return {outcome: 'CANCELLED', attempts, trace};
        validateFailure(failure, step.id);
        event('failure', step);
        if (!failure.retryable || attempts[step.id] > step.retryLimit) {
          return {outcome: 'FAILED', attempts, trace, failure};
        }
        event('retry', step);
        continue;
      }
      if (cancelled(step)) return {outcome: 'CANCELLED', attempts, trace};
      validateValue(returned);
      value = {...returned};
      event('return', step);
      // Test scheduling hook, never generated policy or a business capability.
      if (afterReturn) await afterReturn(step.id);
      break;
    }
  }
  if (cancelled(projection.steps.at(-1))) return {outcome: 'CANCELLED', attempts, trace};
  return {outcome: 'COMPLETED', attempts, trace, result: value};
}
