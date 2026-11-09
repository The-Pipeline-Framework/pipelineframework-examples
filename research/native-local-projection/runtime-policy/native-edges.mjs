import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { handlers } from '../business.ts';
import { sha256, summary } from '../journey.mjs';
import { Diagnostic, admitProjection, execute } from '../local-shell.mjs';

const read = path => readFile(new URL(path, import.meta.url));
const provenance = JSON.parse(await read('./PROVENANCE.json'));
const projections = {
  baseline: [await read('../generated/projection.json'), provenance.baselineProjectionSha256],
  'downstream-retry': [await read('./downstream-retry-projection.json'), provenance.downstreamRetry.projectionSha256],
};
const {cases} = JSON.parse(await read('./edge-scenarios.json'));
const report = {};
for (const row of cases) {
  const attempts = {A: 0, B: 0, C: 0};
  const functions = Object.fromEntries(Object.entries(handlers).map(([name, fn]) => [name, async (value, context) => {
    const step = name.toUpperCase();
    const attempt = ++attempts[step];
    const fault = row.fault;
    if (step !== fault.step || attempt > fault.times) return fn(value, context);
    if (fault.code === 'NULL_RESULT') return null;
    throw {code: fault.code, message: 'fixture failure', retryable: fault.code === 'TRANSIENT',
      owner: 'local-invocation-shell', stepId: step};
  }]));
  try {
    const [bytes, digest] = projections[row.projection];
    let admitted = admitProjection(bytes, digest, functions);
    if (row.recoverOnFailure) {
      // An intentional unsupported-feature negative, separate from both compiler artifacts.
      const unsupported = JSON.parse(bytes);
      unsupported.steps.find(step => step.id === row.fault.step).recoverOnFailure = true;
      const altered = Buffer.from(JSON.stringify(unsupported));
      admitted = admitProjection(altered, sha256(altered), functions);
    }
    const result = await execute(admitted, {value: 3}, {executionId: row.id, traceId: 'trace-edge'});
    assert.deepEqual(result.attempts, attempts, row.id);
    report[row.id] = summary(result);
  } catch (error) {
    if (!(error instanceof Diagnostic)) throw error;
    report[row.id] = {diagnostic: error.code, attempts};
  }
}
console.log(JSON.stringify(report));
