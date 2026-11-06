/** Runnable LOCAL journey. This module uses only native functions and explicit compiler artifacts. */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { join } from 'node:path';
import { handlers } from './business.ts';
import { admitProjection, execute } from './local-shell.mjs';

export const root = fileURLToPath(new URL('.', import.meta.url));
export const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
export async function artifacts(directory = join(root, 'generated')) {
  const lines = (await readFile(join(directory, 'SHA256SUMS'), 'utf8')).trim().split('\n');
  assert.equal(lines.length, 3, 'compiler artifact inventory');
  const bytes = {};
  const digests = {};
  for (const line of lines) {
    const match = /^([a-f0-9]{64})  (models\.py|models\.ts|projection\.json)$/.exec(line);
    assert.ok(match, 'compiler artifact inventory entry');
    assert.equal(Object.hasOwn(bytes, match[2]), false, 'duplicate artifact');
    bytes[match[2]] = await readFile(join(directory, match[2]));
    digests[match[2]] = match[1];
    assert.equal(sha256(bytes[match[2]]), match[1], `artifact digest: ${match[2]}`);
  }
  return {bytes, digests};
}
export function summary(result) {
  const s = {outcome: result.outcome, attempts: result.attempts};
  if (result.outcome === 'COMPLETED') s.result = result.result;
  if (result.outcome === 'FAILED') s.failureCode = result.failure.code;
  return s;
}
export async function runJourney(directory) {
  const {bytes, digests} = await artifacts(directory);
  const scenarioBytes = await readFile(join(root, 'scenarios.json'));
  const scenarios = JSON.parse(scenarioBytes);
  const cases = [];
  for (const scenario of scenarios.cases) {
    let faults = 0;
    const functions = Object.fromEntries(Object.entries(handlers).map(([name, fn]) => [name, async (value, context) => {
      await Promise.resolve();
      if (scenario.fault?.step.toLowerCase() === name && faults++ < scenario.fault.times) {
        throw {code: scenario.fault.code, message: 'deterministic test failure',
          retryable: scenario.fault.code === 'TRANSIENT', owner: 'local-invocation-shell',
          stepId: scenario.fault.step};
      }
      return fn(value, context);
    }]));
    const admitted = admitProjection(bytes['projection.json'], digests['projection.json'], functions);
    const controller = new AbortController();
    if (scenario.cancelBefore) controller.abort();
    const result = await execute(admitted, scenario.input, {executionId: scenario.id, traceId: `trace-${scenario.id}`}, {
      signal: controller.signal,
      afterReturn: async step => { if (step === scenario.cancelAfter) controller.abort(); },
    });
    const actual = summary(result);
    assert.deepEqual(actual, scenario.expected, scenario.id);
    cases.push({id: scenario.id, ...actual});
  }
  // Both executions must enter A before either can advance. Each function observes its
  // own actual invocation context; the fixed rows supply the independent expected IDs.
  let entered = 0, release;
  const barrier = new Promise(resolve => { release = resolve; });
  const contexts = await Promise.all(scenarios.contexts.map(async scenario => {
    const observations = [];
    const functions = Object.fromEntries(Object.entries(handlers).map(([name, fn]) => [name, async (value, context) => {
      if (name === 'a') { if (++entered === scenarios.contexts.length) release(); await barrier; }
      observations.push({step: name.toUpperCase(), executionId: context.executionId, traceId: context.traceId});
      return fn(value, context);
    }]));
    const admitted = admitProjection(bytes['projection.json'], digests['projection.json'], functions);
    const result = await execute(admitted, scenario.input, {executionId: scenario.executionId, traceId: scenario.traceId});
    assert.equal(result.outcome, 'COMPLETED');
    assert.deepEqual(result.result, scenario.expected);
    assert.deepEqual(observations, ['A', 'B', 'C'].map(step => ({step, executionId: scenario.executionId, traceId: scenario.traceId})));
    return {executionId: scenario.executionId, traceId: scenario.traceId, result: result.result, observations};
  }));
  return {language: 'node', profile: 'tpf-polyglot-probe/0', projectionSha256: digests['projection.json'],
    scenariosSha256: sha256(scenarioBytes), cases, contexts};
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  console.log(JSON.stringify(await runJourney(process.argv[2]), null, 2));
}
