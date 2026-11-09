from generated.baseline.models import ProbeContext, ProbeValue


async def a(value: ProbeValue, _context: ProbeContext) -> ProbeValue:
    return {'value': str(value['value'])}
