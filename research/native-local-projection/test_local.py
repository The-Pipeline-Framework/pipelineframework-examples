"""Independent Python boundary challenges against the actual compiler artifact."""
import asyncio
import hashlib
import json
import unittest
from unittest import mock
from business import handlers
from journey import artifacts, run_journey
from local_shell import Diagnostic, Failure, admit_projection, execute

RAW, DIGESTS = artifacts()
PROJECTION, DIGEST = RAW['projection.json'], DIGESTS['projection.json']
CONTEXT = {'executionId': 'test', 'traceId': 'trace-test'}


def admission(functions=None):
    return admit_projection(PROJECTION, DIGEST, handlers if functions is None else functions)


def altered(change, functions=None):
    projection = json.loads(PROJECTION)
    change(projection)
    raw = json.dumps(projection).encode()
    return admit_projection(raw, hashlib.sha256(raw).hexdigest(), handlers if functions is None else functions)


class LocalProof(unittest.IsolatedAsyncioTestCase):
    def diagnostic(self, code):
        return self.assertRaisesRegex(Diagnostic, f'^{code}$')

    async def test_fixed_scenarios_and_overlapping_contexts(self):
        report = await run_journey()
        self.assertEqual(len(report['cases']), 7)
        self.assertEqual(len(report['contexts']), 2)

    async def test_closed_admission_before_invocation(self):
        calls = []

        async def never(value, context):
            calls.append('called')
            return {'value': 0}

        functions = dict.fromkeys(['a', 'b', 'c'], never)
        with self.diagnostic('ARTIFACT_DIGEST_MISMATCH'):
            admit_projection(PROJECTION + b' ', DIGEST, functions)
        changes = [
            (lambda p: p.update(schemaVersion=True), 'UNSUPPORTED_SCHEMA'),
            (lambda p: p.update(schemaVersion=2), 'UNSUPPORTED_SCHEMA'),
            (lambda p: p.update(profile='future/1'), 'UNSUPPORTED_PROFILE'),
            (lambda p: p['requiredCapabilities'].append('await'), 'UNSUPPORTED_CAPABILITY'),
            (lambda p: p['pipeline'].update(transport='REST'), 'UNSUPPORTED_EXECUTION'),
            (lambda p: p['pipeline'].update(execution='QUEUE_ASYNC'), 'UNSUPPORTED_EXECUTION'),
            (lambda p: p['pipeline'].update(providerCredentials='no'), 'UNSUPPORTED_PIPELINE'),
            (lambda p: p['contextKeys'].append('coordinatorStore'), 'UNSUPPORTED_CONTEXT'),
            (lambda p: p['steps'][0].update(inputType='unknown'), 'TYPE_MISMATCH'),
            (lambda p: next(t for t in p['types'] if t['id'] == 'ProbeValue')['fields'][0].update(type='int64'), 'UNSUPPORTED_TYPE'),
            (lambda p: p['steps'][0].update(handler='missing'), 'MISSING_HANDLER'),
            (lambda p: p['steps'][1].update(handler='a'), 'AMBIGUOUS_HANDLER'),
            (lambda p: p['steps'][1].update(id='A'), 'DUPLICATE_STEP'),
            (lambda p: p['steps'][0].update(retryLimit=3), 'UNSUPPORTED_RETRY'),
            (lambda p: p['steps'][0].update(retryLimit=True), 'UNSUPPORTED_RETRY'),
            (lambda p: p['steps'][0].update(query={}), 'UNSUPPORTED_STEP'),
            (lambda p: p['provenance'].update(contractsVersion='future'), 'INVALID_PROVENANCE'),
        ]
        for change, code in changes:
            with self.subTest(code=code), self.diagnostic(code):
                altered(change, functions)
        raw = PROJECTION.replace(b'"schemaVersion" : 1', b'"schemaVersion" : 1.0000000000000001')
        with self.diagnostic('INVALID_NUMBER'):
            admit_projection(raw, hashlib.sha256(raw).hexdigest(), functions)
        self.assertEqual(calls, [])

    async def test_order_and_immutable_admission(self):
        def reorder(p):
            p['steps'] = [p['steps'][1], p['steps'][0], p['steps'][2]]
        self.assertEqual((await execute(altered(reorder), {'value': 3}, CONTEXT))['result'], {'value': 4})
        mutable = dict(handlers)
        admitted = admission(mutable)

        async def incorrect(value, context):
            return {'value': 999}

        mutable['b'] = incorrect
        with self.assertRaises(TypeError):
            admitted.handlers['b'] = incorrect
        with self.assertRaises(TypeError):
            admitted.projection['steps'][0]['retryLimit'] = 0
        self.assertEqual((await execute(admitted, {'value': 3}, CONTEXT))['result'], {'value': 5})

    async def test_bad_input_context_and_output_stop_downstream(self):
        called = []

        def wrap(name, fn):
            async def call(value, context):
                called.append(name)
                return await fn(value, context)
            return call

        functions = {name: wrap(name, fn) for name, fn in handlers.items()}
        for value in [{}, {'value': None}, {'value': False}, {'value': 1.5}, {'value': 2147483648}, {'value': 1, 'extra': 2}]:
            with self.diagnostic('INVALID_VALUE'):
                await execute(admission(functions), value, CONTEXT)
        for key in ['providerCredentials', 'coordinatorStore', 'tenantPolicy', 'connectionSecret', 'retryOwner']:
            with self.diagnostic('INVALID_CONTEXT'):
                await execute(admission(functions), {'value': 3}, {**CONTEXT, key: 'no'})
        self.assertEqual(called, [])
        for value in [{}, {'value': None}, {'value': False}, {'value': 2147483648}, {'value': 4, 'extra': 1}]:
            called.clear()

            async def invalid(value_in, context):
                called.append('a')
                return value

            with self.diagnostic('INVALID_VALUE'):
                await execute(admission({**functions, 'a': invalid}), {'value': 3}, CONTEXT)
            self.assertEqual(called, ['a'])
        for input_value, expected_calls in [(2147483647, ['a']), (1073741823, ['a', 'b'])]:
            called.clear()
            with self.diagnostic('INVALID_VALUE'):
                await execute(admission(functions), {'value': input_value}, CONTEXT)
            self.assertEqual(called, expected_calls)

    async def test_failure_validation_before_retry(self):
        base = {'code': 'TRANSIENT', 'message': 'test', 'retryable': True, 'owner': 'local-invocation-shell', 'stepId': 'A'}
        for envelope, code in [(None, 'MALFORMED_FAILURE'),
                               ({**base, 'code': 'java.lang.Exception'}, 'UNKNOWN_FAILURE'),
                               ({**base, 'retryable': False}, 'FAILURE_POLICY_CONFLICT'),
                               ({**base, 'owner': 'provider'}, 'FAILURE_POLICY_CONFLICT'),
                               ({**base, 'stepId': 'B'}, 'MALFORMED_FAILURE')]:
            called = []

            async def failure(value, context):
                await asyncio.sleep(0)
                called.append('a')
                raise Failure(envelope)

            async def downstream(value, context):
                called.append('b')
                return value

            with self.diagnostic(code):
                await execute(admission({'a': failure, 'b': downstream, 'c': handlers['c']}), {'value': 3}, CONTEXT)
            self.assertEqual(called, ['a'])

    async def test_cancellation_at_A_return_barrier(self):
        arrived, gate, cancel = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def after_return(step):
            if step == 'A':
                arrived.set()
                await gate.wait()

        running = asyncio.create_task(execute(admission(), {'value': 3}, CONTEXT,
                                              cancelled=cancel.is_set, after_return=after_return))
        await arrived.wait()
        cancel.set()
        gate.set()
        result = await running
        self.assertEqual(result['outcome'], 'CANCELLED')
        self.assertEqual(result['attempts'], {'A': 1, 'B': 0, 'C': 0})
        self.assertFalse(any(e['kind'] == 'invoke' and e['stepId'] == 'B' for e in result['trace']))


    async def test_number_and_parser_exception_diagnostics(self):
        for raw, code in [
            (b'{"schemaVersion":1e9999999999999999999999999999999999999999}', 'INVALID_NUMBER'),
            (b'{', 'INVALID_JSON'),
        ]:
            with self.subTest(code=code), self.diagnostic(code):
                admit_projection(raw, hashlib.sha256(raw).hexdigest(), handlers)
        # Qualify exception mapping without depending on a particular Python
        # version's recursion limit or allocating adversarial nested structures.
        with mock.patch('local_shell.json.loads', side_effect=RecursionError('seeded parser failure')):
            with self.diagnostic('INVALID_JSON'):
                admit_projection(PROJECTION, DIGEST, handlers)

    async def test_cancellation_after_final_return_and_uncancelled_control(self):
        for should_cancel in [False, True]:
            cancel = asyncio.Event()

            async def after_return(step):
                if step == 'C' and should_cancel:
                    cancel.set()

            result = await execute(admission(), {'value': 3}, CONTEXT,
                                   cancelled=cancel.is_set, after_return=after_return)
            self.assertEqual(result['attempts'], {'A': 1, 'B': 1, 'C': 1})
            self.assertTrue(any(e['kind'] == 'return' and e['stepId'] == 'C' for e in result['trace']))
            if should_cancel:
                self.assertEqual(result['outcome'], 'CANCELLED')
                self.assertNotIn('result', result)
                self.assertEqual(result['trace'][-1], {'kind': 'cancelled', 'stepId': 'C', **CONTEXT})
            else:
                self.assertEqual(result['outcome'], 'COMPLETED')
                self.assertEqual(result['result'], {'value': 5})


if __name__ == '__main__':
    unittest.main()
