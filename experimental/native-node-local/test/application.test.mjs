import assert from 'node:assert/strict';
import { test } from 'node:test';
import { localApplication } from '../src/application.ts';

test('runs the generated application and observes the compiler-owned order', async () => {
  const observations = [];
  const application = localApplication({onObservation: event => observations.push(event)});
  const result = await application.run({value: 3}, {executionId: 'application-test', traceId: 'local'});
  assert.deepEqual(result, {kind: 'COMPLETED', outcome: {kind: 'VALUE', value: {value: 5}}});
  assert.deepEqual(observations.filter(event => event.kind === 'ATTEMPT_STARTED')
    .map(({stepId, inputType, outputType, attempt}) => [stepId, inputType, outputType, attempt]), [
    ['A', 'InputValue', 'FirstValue', 1],
    ['B', 'FirstValue', 'SecondValue', 1],
    ['C', 'SecondValue', 'OutputValue', 1],
  ]);
  assert.deepEqual(observations.filter(event => event.kind === 'TERMINAL').map(event => event.record), [result]);
});
