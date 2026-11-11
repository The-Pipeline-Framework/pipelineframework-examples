from typing_shapes import Handler, InputValue, InvocationContext, OutputValue, Registry, Value


def sync_handler(value: InputValue, context: InvocationContext) -> Value[OutputValue]:
    return Value(OutputValue(value.value + 1))


async def async_handler(value: InputValue, context: InvocationContext) -> Value[OutputValue]:
    return Value(OutputValue(value.value + 1))


sync: Handler = sync_handler
asynchronous: Handler = async_handler
registry = Registry(sync)
