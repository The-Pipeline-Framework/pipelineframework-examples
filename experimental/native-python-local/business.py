"""Authored handlers for the generated Python LOCAL application."""
from generated.project.application import HandlerRegistry, InvocationContext, Value
from generated.project.models import FirstValue, InputValue, OutputValue, SecondValue


def normalize(record: InputValue, context: InvocationContext) -> Value[FirstValue]:
    return Value(FirstValue(record.value + 1))


async def apply(record: FirstValue, context: InvocationContext) -> Value[SecondValue]:
    return Value(SecondValue(record.value * 2))


def finish(record: SecondValue, context: InvocationContext) -> Value[OutputValue]:
    value = record.value
    return Value(OutputValue(value - 3))


handlers: HandlerRegistry = {"normalize": normalize, "apply": apply, "finish": finish}
