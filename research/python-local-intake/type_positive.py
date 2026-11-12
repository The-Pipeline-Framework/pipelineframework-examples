from collections.abc import Coroutine, Generator
from typing import Never

from typing_shapes import Handler, InputValue, InvocationContext, OutputValue, Registry, Value


def sync_handler(value: InputValue, context: InvocationContext) -> Value[OutputValue]:
    return Value(OutputValue(value.value + 1))


async def async_handler(value: InputValue, context: InvocationContext) -> Value[OutputValue]:
    return Value(OutputValue(value.value + 1))


sync: Handler = sync_handler
asynchronous: Handler = async_handler
registry = Registry(sync)


class ForeignCoroutine(Coroutine[None, None, Value[OutputValue]]):
    """A static match for Coroutine that is not a native coroutine object."""

    def __await__(self) -> Generator[None, None, Value[OutputValue]]:
        yield
        return Value(OutputValue(3))

    def send(self, value: None, /) -> Never:
        raise StopIteration(Value(OutputValue(3)))

    def throw(self, *args: object) -> Never:
        raise AssertionError('the runtime must reject this carrier before driving it')

    def close(self) -> None:
        pass


def custom_coroutine_handler(value: InputValue, context: InvocationContext) -> ForeignCoroutine:
    return ForeignCoroutine()


static_match_only: Handler = custom_coroutine_handler
