import assert from 'node:assert/strict';
import test from 'node:test';
import { handlers } from './business.ts';
import { artifacts, runJourney, sha256 } from './journey.mjs';
import { admitProjection, execute } from './local-shell.mjs';

const {bytes, digests} = await artifacts();
const projectionBytes = bytes['projection.json'];
const digest = digests['projection.json'];
const context = {executionId: 'test', traceId: 'trace-test'};
const cloned = () => JSON.parse(projectionBytes);
const admission = functions => admitProjection(projectionBytes, digest, functions ?? handlers);
const diagnostic = code => error => error?.code === code;
const altered = (change, functions = handlers) => {
  const p = cloned(); change(p);
  const bytes = Buffer.from(JSON.stringify(p));
  return admitProjection(bytes, sha256(bytes), functions);
};

test('compiler artifact drives seven fixed async scenarios and overlapping contexts', async () => {
  const report = await runJourney();
  assert.equal(report.cases.length, 7);
  assert.equal(report.contexts.length, 2);
});

test('closed artifact admission fails before any handler invocation', () => {
  let calls = 0;
  const never = async () => { calls++; return {value: 0}; };
  const functions = {a: never, b: never, c: never};
  assert.throws(() => admitProjection(Buffer.concat([projectionBytes, Buffer.from(' ')]), digest, functions), diagnostic('ARTIFACT_DIGEST_MISMATCH'));
  for (const [change, code] of [
    [p => p.schemaVersion = true, 'UNSUPPORTED_SCHEMA'],
    [p => p.schemaVersion = 2, 'UNSUPPORTED_SCHEMA'],
    [p => p.profile = 'future/1', 'UNSUPPORTED_PROFILE'],
    [p => p.requiredCapabilities.push('await'), 'UNSUPPORTED_CAPABILITY'],
    [p => p.pipeline.transport = 'REST', 'UNSUPPORTED_EXECUTION'],
    [p => p.pipeline.execution = 'QUEUE_ASYNC', 'UNSUPPORTED_EXECUTION'],
    [p => p.pipeline.providerCredentials = 'forbidden', 'UNSUPPORTED_PIPELINE'],
    [p => p.contextKeys.push('coordinatorStore'), 'UNSUPPORTED_CONTEXT'],
    [p => p.steps[0].inputType = 'unknown', 'TYPE_MISMATCH'],
    [p => p.types.find(t => t.id === 'ProbeValue').fields[0].type = 'int64', 'UNSUPPORTED_TYPE'],
    [p => p.steps[0].handler = 'missing', 'MISSING_HANDLER'],
    [p => p.steps[1].handler = 'a', 'AMBIGUOUS_HANDLER'],
    [p => p.steps[1].id = 'A', 'DUPLICATE_STEP'],
    [p => p.steps[0].retryLimit = 3, 'UNSUPPORTED_RETRY'],
    [p => p.steps[0].retryLimit = true, 'UNSUPPORTED_RETRY'],
    [p => p.steps[0].query = {}, 'UNSUPPORTED_STEP'],
    [p => p.provenance.contractsVersion = 'future', 'INVALID_PROVENANCE'],
  ]) assert.throws(() => altered(change, functions), diagnostic(code));
  const nearInteger = Buffer.from(projectionBytes.toString().replace('"schemaVersion" : 1', '"schemaVersion" : 1.0000000000000001'));
  assert.throws(() => admitProjection(nearInteger, sha256(nearInteger), functions), diagnostic('INVALID_NUMBER'));
  assert.equal(calls, 0);
});

test('ordering comes from artifact; admitted policy and function bindings remain immutable', async () => {
  const reordered = altered(p => p.steps = [p.steps[1], p.steps[0], p.steps[2]]);
  assert.equal((await execute(reordered, {value: 3}, context)).result.value, 4); // B→A→C: 6→7→4
  const mutable = {...handlers};
  const admitted = admission(mutable);
  mutable.b = async () => ({value: 999});
  assert.throws(() => admitted.projection.steps.reverse(), TypeError);
  assert.throws(() => admitted.handlers.b = mutable.b, TypeError);
  assert.equal((await execute(admitted, {value: 3}, context)).result.value, 5);
});

test('malformed input/context and out-of-range results cannot reach downstream handlers', async () => {
  const invoked = [];
  const functions = Object.fromEntries(Object.entries(handlers).map(([name, fn]) => [name, async (...args) => {
    invoked.push(name); return fn(...args);
  }]));
  for (const input of [{}, {value: null}, {value: false}, {value: -0}, {value: 1.5}, {value: 2147483648}, {value: 1, extra: 2}]) {
    await assert.rejects(execute(admission(functions), input, context), diagnostic('INVALID_VALUE'));
  }
  for (const key of ['providerCredentials', 'coordinatorStore', 'tenantPolicy', 'connectionSecret', 'retryOwner']) {
    await assert.rejects(execute(admission(functions), {value: 3}, {...context, [key]: 'forbidden'}), diagnostic('INVALID_CONTEXT'));
  }
  assert.deepEqual(invoked, []);
  for (const value of [{}, {value: null}, {value: false}, {value: 2147483648}, {value: 4, extra: 1}]) {
    invoked.length = 0;
    const invalidA = {...functions, a: async () => { invoked.push('a'); return value; }};
    await assert.rejects(execute(admission(invalidA), {value: 3}, context), diagnostic('INVALID_VALUE'));
    assert.deepEqual(invoked, ['a']);
  }
  invoked.length = 0;
  await assert.rejects(execute(admission(functions), {value: 2147483647}, context), diagnostic('INVALID_VALUE'));
  assert.deepEqual(invoked, ['a']);
  invoked.length = 0;
  await assert.rejects(execute(admission(functions), {value: 1073741823}, context), diagnostic('INVALID_VALUE'));
  assert.deepEqual(invoked, ['a', 'b']);
});

test('malformed/unknown failure and conflicting authority never retry', async () => {
  const base = {code: 'TRANSIENT', message: 'test', retryable: true, owner: 'local-invocation-shell', stepId: 'A'};
  for (const [failure, code] of [[null, 'MALFORMED_FAILURE'], [new Error('oops'), 'MALFORMED_FAILURE'],
    [{...base, code: 'java.lang.Exception'}, 'UNKNOWN_FAILURE'],
    [{...base, retryable: false}, 'FAILURE_POLICY_CONFLICT'],
    [{...base, owner: 'provider'}, 'FAILURE_POLICY_CONFLICT'],
    [{...base, stepId: 'B'}, 'MALFORMED_FAILURE']]) {
    const invoked = [];
    const functions = {a: async () => { await Promise.resolve(); invoked.push('a'); throw failure; },
      b: async () => { invoked.push('b'); return {value: 0}; }, c: handlers.c};
    await assert.rejects(execute(admission(functions), {value: 3}, context), diagnostic(code));
    assert.deepEqual(invoked, ['a']);
  }
});

test('cancellation wins at an explicit A-return barrier before B can start', async () => {
  let arrived, release;
  const atReturn = new Promise(resolve => { arrived = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  const controller = new AbortController();
  const running = execute(admission(), {value: 3}, context, {signal: controller.signal,
    afterReturn: async step => { if (step === 'A') { arrived(); await gate; } }});
  await atReturn;
  controller.abort(); release();
  const result = await running;
  assert.equal(result.outcome, 'CANCELLED');
  assert.deepEqual(result.attempts, {A: 1, B: 0, C: 0});
  assert.equal(result.trace.some(e => e.kind === 'invoke' && e.stepId === 'B'), false);
});

test('comma-joined context names cannot replace individual required keys', async () => {
  const invoked = [];
  const functions = {...handlers, a: async (...args) => { invoked.push('a'); return handlers.a(...args); }};
  for (const invalid of [{'executionId,traceId': 'x'}, {executionId: 'x', other: 'y'}]) {
    await assert.rejects(execute(admission(functions), {value: 3}, invalid), diagnostic('INVALID_CONTEXT'));
  }
  assert.deepEqual(invoked, []);
});

test('numeric and parser failures produce explicit diagnostics', () => {
  for (const [source, code] of [
    ['{"schemaVersion":1e9999999999999999999999999999999999999999}', 'INVALID_NUMBER'],
    ['{', 'INVALID_JSON'],
  ]) {
    const raw = Buffer.from(source);
    assert.throws(() => admitProjection(raw, sha256(raw), handlers), diagnostic(code));
  }
  // Seed the parser's nesting failure deterministically; arbitrary JSON depth is
  // outside the trusted compiler-output boundary and varies by engine/version.
  const parse = JSON.parse;
  try {
    JSON.parse = () => { throw new RangeError('seeded parser recursion failure'); };
    assert.throws(() => admitProjection(projectionBytes, digest, handlers), diagnostic('INVALID_JSON'));
  } finally {
    JSON.parse = parse;
  }
});

test('cancellation after C returns suppresses the result; the uncancelled control completes', async () => {
  for (const cancel of [false, true]) {
    const controller = new AbortController();
    const result = await execute(admission(), {value: 3}, context, {signal: controller.signal,
      afterReturn: async step => { if (step === 'C' && cancel) controller.abort(); }});
    assert.deepEqual(result.attempts, {A: 1, B: 1, C: 1});
    assert.ok(result.trace.some(e => e.kind === 'return' && e.stepId === 'C'));
    if (cancel) {
      assert.equal(result.outcome, 'CANCELLED');
      assert.equal(Object.hasOwn(result, 'result'), false);
      assert.deepEqual(result.trace.at(-1), {kind: 'cancelled', stepId: 'C', ...context});
    } else {
      assert.equal(result.outcome, 'COMPLETED');
      assert.deepEqual(result.result, {value: 5});
    }
  }
});
