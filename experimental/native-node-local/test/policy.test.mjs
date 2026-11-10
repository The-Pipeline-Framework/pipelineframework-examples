import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { createNodeLocalRuntime, NodeLocalDiagnostic, StructuredFailure } from '@tpf/experimental-node-local';
import { handlers as business } from '../src/business.ts';
import { profile, sha256, verifyGenerated } from '../verify-project.mjs';

const readJson = path => JSON.parse(readFileSync(new URL(path, import.meta.url)));
const corpus = readJson('../../../research/native-local-projection/runtime-policy/v1/scenarios.json');
const pins = readJson('../generation-pins.json');
assert.equal(corpus.profile, profile);
assert.equal(corpus.schemaVersion, 1);

const inventory = {};
for (const [name, pin] of Object.entries(pins)) {
  const dir = new URL(`../fixtures/${name}/`, import.meta.url);
  const {receipt, projection} = verifyGenerated(dir, pin.receiptSha256);
  assert.equal(receipt.run.id, pin.run);
  const api = await import(new URL('application.ts', dir).href);
  assert.equal(api.projectionSha256, receipt.files['projection.json']);
  assert.equal(api.projectionJson, readFileSync(new URL('projection.json', dir), 'utf8'));
  inventory[name] = {api, projection};
}

inventory['downstream-retry'] = inventory.project;

function scenario(row) {
  const signal = new AbortController();
  const context = Object.hasOwn(row, 'context') ? structuredClone(row.context)
    : {executionId: row.id, traceId: `trace-${row.id}`};
  return {
    row, signal, context,
    expectedContext: structuredClone(context),
    ready: Promise.withResolvers(),
    release: Promise.withResolvers(),
    calls: [],
    observations: [],
    delivered: [],
    attempts: new Map(),
    abort() {
      signal.abort('private caller reason');
      signal.abort('duplicate cancellation');
    },
  };
}

function session(states) {
  const {row} = states[0];
  const {api, projection} = inventory[row.projection ?? 'baseline'];
  const byExecutionId = new Map(states.map(state => [state.context?.executionId, state]));
  const stateFor = context => states.length === 1 ? states[0] : byExecutionId.get(context?.executionId);
  const runtime = createNodeLocalRuntime({onObservation: event => {
    const state = stateFor(event.context);
    state.observations.push(event);
    const {gate, cancel} = state.row;
    if (cancel === 'during' && event.stepId === gate.step && (
      (gate.phase === 'after-return' && event.kind === 'RETURNED') ||
      (gate.phase === 'before-retry' && event.kind === 'RETRY_SCHEDULED')
    )) state.abort();
  }});

  const handlers = Object.fromEntries(projection.steps.map(({id, handler}) => [
    handler, async (input, context) => {
      const state = stateFor(context);
      const {row, calls, attempts} = state;
      const prior = attempts.get(id) ?? 0;
      attempts.set(id, prior + 1);
      calls.push({
        id, value: input.value, context: {...context},
        contextImmutable: !Reflect.set(context, 'traceId', 'handler mutation'),
        inputImmutable: !Reflect.set(input, 'value', -1),
      });
      if (prior === 0 && row.gate?.phase === 'in-flight' && row.gate.step === id) {
        state.ready.resolve();
        await state.release.promise;
      }
      const fault = row.faults?.[id]?.[prior];
      switch (fault?.kind) {
        case 'failure': throw new StructuredFailure(fault.envelope);
        case 'throw': throw new Error('private provider detail');
        case 'implicit': return undefined;
        case 'return': return fault.outcome;
        default: return business[handler](input, context);
      }
    },
  ]));
  return {api, projection, runtime, handlers};
}

function launch(state, application) {
  if (state.row.cancel === 'prestart') state.abort();
  const input = Object.hasOwn(state.row, 'input') ? state.row.input : {value: 3};
  return application.run(input, state.context, state.signal.signal).then(record => {
    state.delivered.push(record);
    return record;
  });
}

function verifyRun(state, record, projection) {
  const {row, calls, delivered, observations, expectedContext} = state;
  assert.deepEqual(record, row.expected.terminal);
  assert.deepEqual(delivered, [record], 'generated Promise delivers exactly one terminal record');
  assert.deepEqual(observations.filter(event => event.kind === 'TERMINAL').map(event => event.record), [record]);
  assert.deepEqual(calls.map(({id, value}) => [id, value]), row.expected.calls);
  for (const call of calls) {
    assert.deepEqual(call.context, expectedContext);
    assert.equal(call.contextImmutable, true);
    assert.equal(call.inputImmutable, true);
  }

  const attempts = new Map();
  const started = observations.filter(event => event.kind === 'ATTEMPT_STARTED');
  assert.deepEqual(started.map(({stepId, inputType, outputType, attempt}) => [stepId, inputType, outputType, attempt]),
    row.expected.calls.map(([id]) => {
      const {inputType, outputType} = projection.steps.find(step => step.id === id);
      const attempt = (attempts.get(id) ?? 0) + 1;
      attempts.set(id, attempt);
      return [id, inputType, outputType, attempt];
    }));
  for (const event of started) {
    assert.equal(event.pipelineId, projection.pipeline.id);
    assert.deepEqual(event.context, expectedContext);
  }

  if (row.gate && row.gate.phase !== 'in-flight') {
    const kind = row.gate.phase === 'after-return' ? 'RETURNED' : 'RETRY_SCHEDULED';
    const index = observations.findIndex(event => event.kind === kind && event.stepId === row.gate.step);
    assert.notEqual(index, -1, `missing ${kind} at the cancellation boundary`);
    assert.equal(observations[index + 1].kind, row.cancel === 'during' ? 'TERMINAL' : 'ATTEMPT_STARTED');
  }
}

const overlap = [];
for (const row of corpus.cases) {
  if (row.overlap) {
    overlap.push(row);
    continue;
  }
  test(row.id, {timeout: 5000}, async () => {
    const state = scenario(row);
    const {api, projection, runtime, handlers} = session([state]);

    if (Object.hasOwn(row, 'ownership')) {
      // The generated API admits no legacy ownership or recovery switch.
      assert.throws(() => createNodeLocalRuntime({ownership: row.ownership}),
        {code: 'UNSUPPORTED_RUNTIME_OPTIONS'});
      assert.deepEqual(state.calls, row.expected.calls);
      return;
    }
    if (row.artifactMutation) {
      const candidate = structuredClone(projection);
      const {path, value} = row.artifactMutation;
      const parent = path.slice(0, -1).reduce((object, key) => object[key], candidate);
      parent[path.at(-1)] = value;
      const projectionJson = JSON.stringify(candidate);
      assert.throws(() => runtime.load({schemaVersion: 1, projectionJson, projectionSha256: sha256(projectionJson), handlers}),
        error => error instanceof NodeLocalDiagnostic && error.code === row.expected.terminal.diagnostic);
      assert.deepEqual(state.calls, row.expected.calls);
      assert.deepEqual(state.observations, []);
      return;
    }

    const result = launch(state, api.createApplication(runtime, handlers));
    if (row.gate?.phase === 'in-flight') {
      try {
        await Promise.race([state.ready.promise, result.then(() => assert.fail('invocation settled before the handler gate'))]);
        if (row.cancel === 'during') {
          state.abort();
          const terminals = state.observations.filter(event => event.kind === 'TERMINAL');
          assert.deepEqual(terminals.map(event => event.record), [{kind: 'CANCELLED'}]);
          await Promise.resolve();
          assert.deepEqual(state.delivered, [], 'still waiting for the started handler to drain');
        }
      } finally {
        state.release.resolve();
      }
    }
    const record = await result;
    if (row.cancel === 'after') state.abort();
    verifyRun(state, record, projection);
  });
}

test('overlapping calls on one loaded application keep separate context snapshots', {timeout: 5000}, async () => {
  assert.equal(overlap.length, 2);
  const states = overlap.map(scenario);
  const {api, projection, runtime, handlers} = session(states);
  const application = api.createApplication(runtime, handlers);
  const results = states.map(state => launch(state, application));
  try {
    await Promise.race([
      Promise.all(states.map(state => state.ready.promise)),
      ...results.map(result => result.then(() => assert.fail('overlap settled before both handler gates'))),
    ]);
    for (const state of states) {
      state.context.executionId = 'caller mutation';
      state.context.traceId = 'caller mutation';
    }
    states[1].release.resolve();
    verifyRun(states[1], await results[1], projection);
  } finally {
    for (const state of states) state.release.resolve();
  }
  verifyRun(states[0], await results[0], projection);
});

test('changed application entrypoint runs a locally edited transformation', async () => {
  const {api} = inventory['changed-order'];
  const observations = [];
  const application = api.createApplication(createNodeLocalRuntime({onObservation: event => observations.push(event)}), {
    ...business,
    finish: ({value}) => ({kind: 'VALUE', value: {value: value - 4}}),
  });
  assert.deepEqual(await application.run({value: 3}, {executionId: 'edit', traceId: 'edit'}), {
    kind: 'COMPLETED', outcome: {kind: 'VALUE', value: {value: 3}},
  });
  assert.deepEqual(observations.filter(event => event.kind === 'ATTEMPT_STARTED').map(event => event.stepId), ['B', 'A', 'C']);
});

test('loader refuses tampered projection bytes and incomplete handler registry', () => {
  const {api} = inventory['baseline'];
  const runtime = createNodeLocalRuntime();
  const {projectionJson, projectionSha256} = api;
  assert.throws(() => runtime.load({schemaVersion: 1, projectionJson: projectionJson + ' ', projectionSha256, handlers: business}),
    {code: 'ARTIFACT_DIGEST_MISMATCH'});
  assert.throws(() => api.createApplication(runtime, {normalize: business.normalize, finish: business.finish}),
    {code: 'MISSING_HANDLER'});
});
