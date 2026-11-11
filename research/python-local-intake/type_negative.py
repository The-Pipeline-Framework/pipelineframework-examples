"""Intentional static errors; runtime guards still own int32 range and bool rejection."""
from typing_shapes import Handler, InputValue, InvocationContext, OutputValue, Registry, Value


def wrong_result(value: InputValue, context: InvocationContext) -> Value[InputValue]:
    return Value(value)


wrong: Handler = wrong_result  # expect: assignment
missing = Registry()  # expect: call-arg
missing_value = InputValue()  # expect: call-arg
null_value = InputValue(None)  # expect: arg-type
string_value = OutputValue('3')  # expect: arg-type
mutated = OutputValue(3)
mutated.value = 4  # expect: misc


def wrong_input(value: OutputValue, context: InvocationContext) -> Value[OutputValue]:
    return Value(value)


async def wrong_async_result(value: InputValue, context: InvocationContext) -> Value[InputValue]:
    return Value(value)


wrong_argument: Handler = wrong_input  # expect: assignment
wrong_coroutine: Handler = wrong_async_result  # expect: assignment


from collections.abc import Generator


class ForeignAwaitable:
    def __await__(self) -> Generator[None, None, Value[OutputValue]]:
        yield
        return Value(OutputValue(3))


def custom_return(value: InputValue, context: InvocationContext) -> ForeignAwaitable:
    return ForeignAwaitable()


wrong_custom: Handler = custom_return  # expect: assignment
