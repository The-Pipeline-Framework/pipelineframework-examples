"""CPython mechanism probes, not a TPF executor or policy conformance substitute.

Each test isolates a language behavior the future runtime must account for. No
pipeline graph, retries, generated binding or production runtime lives here.
"""
import asyncio
from contextvars import ContextVar
from dataclasses import FrozenInstanceError, dataclass
import inspect
from types import MappingProxyType
import unittest


class PythonMechanisms(unittest.IsolatedAsyncioTestCase):
    async def test_unshielded_waiter_cancellation_reaches_handler(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def handler():
            started.set()
            await release.wait()

        task = asyncio.create_task(handler())

        async def caller():
            await task

        waiter = asyncio.create_task(caller())
        await started.wait()
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertTrue(task.cancelled(), 'plain await violates handler-drain isolation')

    async def test_shield_preserves_handler_through_repeated_waiter_cancellation(self):
        started, release, detached = asyncio.Event(), asyncio.Event(), asyncio.Event()
        completed = []

        async def handler():
            started.set()
            await release.wait()
            completed.append('handler settled')
            return 17

        task = asyncio.create_task(handler())

        async def caller():
            # Mechanism only: the runtime must separately arbitrate its terminal record.
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                detached.set()
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        continue
                raise

        waiter = asyncio.create_task(caller())
        await started.wait()
        waiter.cancel()
        await detached.wait()
        self.assertFalse(task.cancelled())
        waiter.cancel()
        await asyncio.sleep(0)
        self.assertFalse(waiter.done(), 'caller must not escape before handler drains')
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertEqual(task.result(), 17)
        self.assertEqual(completed, ['handler settled'])

    async def test_contextvars_are_task_local_but_not_an_immutable_input_snapshot(self):
        current = ContextVar('invocation')
        mutable = {'executionId': 'original', 'traceId': 'trace'}
        token = current.set(mutable)
        gate = asyncio.Event()

        async def read_context():
            await gate.wait()
            return current.get()['executionId']

        first = asyncio.create_task(read_context())
        mutable['executionId'] = 'mutated'
        current.set({'executionId': 'other', 'traceId': 'trace-other'})
        second = asyncio.create_task(read_context())
        gate.set()
        self.assertEqual(await first, 'mutated', 'task context copies references, not values')
        self.assertEqual(await second, 'other')
        current.reset(token)

    async def test_snapshot_copy_isolated_from_caller_mutation(self):
        caller = {'executionId': 'first', 'traceId': 'trace'}
        snapshot = MappingProxyType(dict(caller))
        caller['executionId'] = 'changed'
        self.assertEqual(snapshot['executionId'], 'first')
        with self.assertRaises(TypeError):
            snapshot['executionId'] = 'forbidden'

    async def test_synchronous_return_needs_an_explicit_scheduling_boundary(self):
        events = []
        asyncio.get_running_loop().call_soon(events.append, 'scheduled cancellation')
        value = 3
        events.append('sync return')
        self.assertEqual(events, ['sync return'])
        await asyncio.sleep(0)
        self.assertEqual(events, ['sync return', 'scheduled cancellation'])
        self.assertEqual(value, 3)

    async def test_coroutine_observer_is_lazy_and_can_be_discarded_without_execution(self):
        observed = []

        async def observer():
            observed.append('executed')

        returned = observer()
        self.assertTrue(inspect.iscoroutine(returned))
        self.assertEqual(observed, [])
        returned.close()  # Discarding without close would leak an unawaited coroutine warning.
        self.assertEqual(observed, [])

    async def test_native_task_observer_failure_can_be_consumed_without_awaiting(self):
        consumed = asyncio.Event()
        errors = []

        async def observer():
            raise RuntimeError('observer failure')

        task = asyncio.create_task(observer())

        def consume(done):
            errors.append(type(asyncio.Task.exception(done)))
            consumed.set()

        asyncio.Task.add_done_callback(task, consume)
        await consumed.wait()
        self.assertEqual(errors, [RuntimeError])

    async def test_cancelled_observer_task_requires_a_cancelled_guard(self):
        async def observer():
            await asyncio.sleep(0)

        task = asyncio.create_task(observer())
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(asyncio.Task.cancelled(task))
        with self.assertRaises(asyncio.CancelledError):
            asyncio.Task.exception(task)

    async def test_custom_awaitable_can_be_ignored_without_invoking_user_code(self):
        called = []

        class CustomAwaitable:
            def __await__(self):
                called.append('user code')
                yield

        returned = CustomAwaitable()
        self.assertFalse(inspect.iscoroutine(returned))
        self.assertNotIsInstance(returned, asyncio.Task)
        self.assertEqual(called, [])

    async def test_handler_cancelled_error_is_not_an_ordinary_exception(self):
        self.assertFalse(issubclass(asyncio.CancelledError, Exception))
        self.assertTrue(issubclass(asyncio.CancelledError, BaseException))
        # Runtime must classify handler-raised cancellation separately from its own token.
        # A broad except Exception cannot preserve terminal accounting for this carrier.

    async def test_async_observer_forms_can_be_rejected_before_calling_them(self):
        async def function_observer(event):
            raise AssertionError('must not execute')

        class ObjectObserver:
            async def __call__(self, event):
                raise AssertionError('must not execute')

        self.assertTrue(inspect.iscoroutinefunction(function_observer))
        self.assertTrue(inspect.iscoroutinefunction(ObjectObserver.__call__))
        self.assertFalse(inspect.iscoroutinefunction(lambda event: None))

    async def test_handler_boundary_can_separate_raised_and_self_cancel_from_outer_cancel(self):
        async def raises_cancel():
            raise asyncio.CancelledError('private handler detail')

        async def cancels_itself():
            asyncio.current_task().cancel()
            await asyncio.sleep(0)

        async def classify(handler):
            # This is a language-carrier probe, not a failure policy implementation.
            try:
                await handler()
            except asyncio.CancelledError:
                return 'handler cancellation carrier'
            self.fail('handler must reach its cancellation point')

        for handler in (raises_cancel, cancels_itself):
            with self.subTest(handler=handler.__name__):
                task = asyncio.create_task(classify(handler))
                self.assertEqual(await asyncio.shield(task), 'handler cancellation carrier')
                self.assertFalse(task.cancelled())

    async def test_integer_bool_and_float_need_exact_validation(self):
        self.assertTrue(isinstance(True, int))
        self.assertIsNot(type(True), int)
        self.assertIs(type(3), int)
        self.assertEqual(-0.0, 0)
        self.assertIsNot(type(-0.0), int)
        self.assertGreater(2**80, 2147483647)

    async def test_frozen_nominal_records_distinguish_identical_field_shapes(self):
        @dataclass(frozen=True, slots=True)
        class Input:
            value: int

        @dataclass(frozen=True, slots=True)
        class Output:
            value: int

        value = Input(3)
        self.assertNotEqual(value, Output(3))
        with self.assertRaises(FrozenInstanceError):
            value.value = 4
        # Dataclass annotations alone do not validate runtime carriers.
        self.assertEqual(Input(True).value, True)


if __name__ == '__main__':
    unittest.main()
