import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';

import {admitProjection, execute, POLICY_PROFILE, StructuredFailure} from '../../local-shell.mjs';
import {handlers} from './authored.ts';

const bytes = readFileSync(new URL('generated/baseline/projection.json', import.meta.url));
const digest = createHash('sha256').update(bytes).digest('hex');
const context = {executionId: 'execution', traceId: 'trace'};
const value = {value: 3};
const run = (input, suppliedContext, a = handlers.a) =>
  execute(admitProjection(bytes, digest, {...handlers, a}, {profile: POLICY_PROFILE}), input, suppliedContext);

test('context and canonical input reject accessors before calling A', async () => {
  for (const [input, suppliedContext, diagnostic] of [
    [value, {...context, get executionId() { throw new Error('unexpected accessor'); }}, 'INVALID_CONTEXT'],
    [{get value() { throw new Error('unexpected accessor'); }}, context, 'INVALID_VALUE'],
  ]) {
    const report = await run(input, suppliedContext);
    assert.deepEqual(report.terminal, {kind: 'REJECTED', diagnostic});
    assert.deepEqual(report.attempts, {A: 0, B: 0, C: 0});
  }
});

test('result and failure accessors cannot change validated data before handoff', async () => {
  const privateDetail = () => { throw new Error('unexpected accessor'); };
  const result = await run(value, context, async () => ({kind: 'VALUE', value: {get value() { return privateDetail(); }}}));
  assert.deepEqual(result.terminal, {kind: 'REJECTED', diagnostic: 'INVALID_VALUE'});
  assert.deepEqual(result.attempts, {A: 1, B: 0, C: 0});

  const failure = await run(value, context, async () => {
    throw new StructuredFailure({
      code: 'UNCLASSIFIED', owner: 'LOCAL_SHELL', stepId: 'A', retryable: false,
      get message() { return privateDetail(); },
    });
  });
  assert.deepEqual(failure.terminal, {kind: 'REJECTED', diagnostic: 'MALFORMED_FAILURE'});
  assert.deepEqual(failure.attempts, {A: 1, B: 0, C: 0});
});
