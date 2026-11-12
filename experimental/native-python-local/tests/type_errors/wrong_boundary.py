from generated.project.application import HandlerRegistry, InvocationContext, Value
from generated.project.models import FirstValue, InputValue, SecondValue


def wrong_input(record: FirstValue, context: InvocationContext) -> Value[FirstValue]:
    return Value(record)


def wrong_result(record: InputValue, context: InvocationContext) -> Value[SecondValue]:
    return Value(SecondValue(record.value))


def check(registry: HandlerRegistry) -> None:
    registry["normalize"] = wrong_input
    registry["normalize"] = wrong_result
