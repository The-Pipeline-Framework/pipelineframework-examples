/** Execute the shared live cases with actual promises and AbortController. */
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {admitProjection, Diagnostic, execute, POLICY_PROFILE, StructuredFailure} from '../../local-shell.mjs';
import {handlers as business} from './authored.ts';

const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const read = path => readFileSync(new URL(path, import.meta.url));
const inventory = Object.fromEntries(['baseline', 'changed-order', 'downstream-retry'].map(name => {
  const bytes = read(`generated/${name}/projection.json`);
  const digest = read(`generated/${name}/SHA256SUMS`).toString().split('\n')
    .find(line => line.endsWith('  projection.json')).split('  ')[0];
  assert.equal(hash(bytes), digest);
  return [name, {bytes, digest}];
}));

function launch(row) {
  const calls = [];
  const observed = [];
  const delivery = [];
  const firstTerminal = Promise.withResolvers();
  const ready = Promise.withResolvers();
  const release = Promise.withResolvers();
  const controller = new AbortController();
  const context = Object.hasOwn(row, 'context') ? structuredClone(row.context)
    : {executionId: row.id, traceId: `trace-${row.id}`};
  const cancelTwice = () => {
    controller.abort('private caller reason');
    controller.abort('a different reason');
  };
  const hold = async () => {
    ready.resolve();
    await release.promise;
  };
  const onTerminal = terminal => { observed.push(terminal); firstTerminal.resolve(); };
  const onDelivery = terminal => delivery.push(terminal.kind);
  const handlers = Object.fromEntries(Object.entries(business).map(([handler, authored]) => {
    const step = handler.toUpperCase();
    let attempt = 0;
    return [handler, async (input, snapshot) => {
      const fault = row.faults?.[step]?.[attempt];
      attempt += 1;
      const mutationBlocked = !Reflect.set(snapshot, 'traceId', 'handler mutation');
      assert.equal(Reflect.set(input, 'value', -1), false, 'attempt input must be immutable');
      calls.push({step, input: input.value, ...snapshot, mutationBlocked});
      if (attempt === 1 && row.gate?.phase === 'in-flight' && row.gate.step === step) await hold();
      switch (fault?.kind) {
        case 'failure': throw new StructuredFailure(fault.envelope);
        case 'throw': throw new Error('private provider detail');
        case 'implicit': return;
        case 'return': return fault.outcome;
        default: return authored(input, snapshot);
      }
    }];
  }));

  const {bytes, digest} = inventory[row.projection ?? 'baseline'];
  let admitted;
  try {
    if (row.artifactMutation) {
      const {path, value} = row.artifactMutation;
      const candidate = JSON.parse(bytes);
      const parent = path.slice(0, -1).reduce((current, key) => current[key], candidate);
      parent[path.at(-1)] = value;
      const altered = Buffer.from(JSON.stringify(candidate));
      admitted = admitProjection(altered, hash(altered), handlers, {profile: POLICY_PROFILE});
    } else {
      admitted = admitProjection(bytes, digest, handlers, {profile: POLICY_PROFILE});
    }
  } catch (error) {
    if (!(error instanceof Diagnostic)) throw error;
    const terminal = {kind: 'REJECTED', diagnostic: error.code};
    onTerminal(terminal);
    onDelivery(terminal);
  }

  if (row.cancel === 'prestart') cancelTwice();
  const result = admitted ? execute(admitted, Object.hasOwn(row, 'input') ? row.input : {value: 3}, context, {
    signal: controller.signal,
    ownership: row.ownership,
    afterReturn: step => row.gate?.phase === 'after-return' && row.gate.step === step ? hold() : undefined,
    beforeRetry: step => row.gate?.phase === 'before-retry' && row.gate.step === step ? hold() : undefined,
    onTerminal, onDelivery,
  }) : Promise.resolve({terminal: observed[0]});

  return {
    ready: ready.promise,
    release: release.resolve,
    mutateCaller: () => { context.executionId = 'caller mutation'; context.traceId = 'caller mutation'; },
    async cancelWhileHeld() {
      cancelTwice();
      await firstTerminal.promise;
      assert.deepEqual(observed, [{kind: 'CANCELLED'}]);
      assert.deepEqual(delivery, [], 'cancelled execution delivered while held');
    },
    async report() {
      if (row.cancel === 'after') {
        await firstTerminal.promise;
        cancelTwice();
      }
      // Await the Promise through release, so late fulfillment or rejection has actually drained.
      const {terminal} = await result;
      assert.deepEqual(observed, [terminal]);
      return {id: row.id, calls, terminal, observations: observed.length, delivery};
    },
  };
}

const corpus = JSON.parse(read('scenarios.json'));
assert.equal(corpus.profile, POLICY_PROFILE);
assert.equal(corpus.schemaVersion, 1);
const reports = new Map();
const overlap = [];
for (const row of corpus.cases) {
  const run = launch(row);
  if (row.gate) await run.ready;
  if (row.overlap) {
    overlap.push([row, run]);
    continue;
  }
  if (row.cancel === 'during') await run.cancelWhileHeld();
  run.release();
  reports.set(row.id, await run.report());
}
assert.equal(overlap.length, 2);
for (const [, run] of overlap) run.mutateCaller();
for (const [row, run] of overlap.reverse()) {
  run.release();
  reports.set(row.id, await run.report());
}
console.log(JSON.stringify({
  language: 'node', profile: POLICY_PROFILE,
  artifactSha256: Object.fromEntries(Object.entries(inventory).map(([name, entry]) => [name, entry.digest])),
  cases: corpus.cases.map(row => reports.get(row.id)),
}));
