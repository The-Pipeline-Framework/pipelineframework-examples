/** Consume an explicitly supplied compiler output, e.g. a regenerated B→A→C projection. */
import assert from 'node:assert/strict';
import { handlers } from './business.ts';
import { artifacts } from './journey.mjs';
import { admitProjection, execute } from './local-shell.mjs';

const [directory, value = '3', expected] = process.argv.slice(2);
assert.ok(directory, 'usage: node run_once.mjs COMPILER_OUTPUT_DIRECTORY [INPUT] [EXPECTED]');
const {bytes, digests} = await artifacts(directory);
const admitted = admitProjection(bytes['projection.json'], digests['projection.json'], handlers);
const result = await execute(admitted, {value: Number(value)}, {executionId: 'changed-dsl', traceId: 'changed-dsl-trace'});
assert.equal(result.outcome, 'COMPLETED');
if (expected !== undefined) assert.equal(result.result.value, Number(expected));
console.log(JSON.stringify({projectionSha256: digests['projection.json'],
  stepOrder: admitted.projection.steps.map(step => step.id), result: result.result, attempts: result.attempts}, null, 2));
