"""Authored Python functions using the compiler-produced model declarations."""
import asyncio
from generated.models import ProbeValue, ProbeContext, HandlerRegistry


async def a(value: ProbeValue, context: ProbeContext) -> ProbeValue:
    await asyncio.sleep(0)
    return {'value': value['value'] + 1}


async def b(value: ProbeValue, context: ProbeContext) -> ProbeValue:
    await asyncio.sleep(0)
    return {'value': value['value'] * 2}


async def c(value: ProbeValue, context: ProbeContext) -> ProbeValue:
    await asyncio.sleep(0)
    return {'value': value['value'] - 3}


handlers: HandlerRegistry = {'a': a, 'b': b, 'c': c}
