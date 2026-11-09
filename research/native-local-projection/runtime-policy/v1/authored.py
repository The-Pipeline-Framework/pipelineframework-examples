"""Turn a typed business value into the policy profile's explicit result carrier."""
from typing import Awaitable, Callable, Literal, TypedDict

from business import handlers as business
from generated.baseline.models import ProbeContext, ProbeValue


class ValueResult(TypedDict):
    kind: Literal['VALUE']
    value: ProbeValue


BusinessHandler = Callable[[ProbeValue, ProbeContext], Awaitable[ProbeValue]]
PolicyHandler = Callable[[ProbeValue, ProbeContext], Awaitable[ValueResult]]


def bind(handler: BusinessHandler) -> PolicyHandler:
    async def invoke(value: ProbeValue, context: ProbeContext) -> ValueResult:
        return {'kind': 'VALUE', 'value': await handler(value, context)}
    return invoke


handlers = {'a': bind(business['a']), 'b': bind(business['b']), 'c': bind(business['c'])}
