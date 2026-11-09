"""Run the same compiler-produced LOCAL artifact in one Python process."""
import asyncio
import hashlib
import json
from pathlib import Path
import sys

from business import handlers
from local_shell import Failure, admit_projection, execute

ROOT = Path(__file__).resolve().parent


def artifacts(directory=ROOT / 'generated'):
    directory = Path(directory)
    lines = (directory / 'SHA256SUMS').read_text().strip().splitlines()
    assert len(lines) == 3, 'compiler artifact inventory'
    raw, digests = {}, {}
    for line in lines:
        digest, name = line.split('  ')
        assert name in ['projection.json', 'models.ts', 'models.py'] and name not in raw
        raw[name] = (directory / name).read_bytes()
        digests[name] = digest
        assert hashlib.sha256(raw[name]).hexdigest() == digest, f'artifact digest: {name}'
    return raw, digests


def summary(result):
    value = {'outcome': result['outcome'], 'attempts': result['attempts']}
    if result['outcome'] == 'COMPLETED':
        value['result'] = result['result']
    if result['outcome'] == 'FAILED':
        value['failureCode'] = result['failure']['code']
    return value


async def run_journey(directory=ROOT / 'generated'):
    raw, digests = artifacts(directory)
    scenario_bytes = (ROOT / 'scenarios.json').read_bytes()
    scenarios = json.loads(scenario_bytes)
    cases = []
    for scenario in scenarios['cases']:
        faults = 0

        def wrap(name, fn):
            async def call(value, context):
                nonlocal faults
                await asyncio.sleep(0)
                fault = scenario.get('fault', {})
                if fault.get('step', '').lower() == name:
                    faults += 1
                    if faults <= fault['times']:
                        raise Failure({'code': fault['code'], 'message': 'deterministic test failure',
                                       'retryable': fault['code'] == 'TRANSIENT',
                                       'owner': 'local-invocation-shell', 'stepId': fault['step']})
                return await fn(value, context)
            return call

        admitted = admit_projection(raw['projection.json'], digests['projection.json'],
                                    {name: wrap(name, fn) for name, fn in handlers.items()})
        cancel = asyncio.Event()
        if scenario.get('cancelBefore'):
            cancel.set()

        async def after_return(step):
            if step == scenario.get('cancelAfter'):
                cancel.set()

        result = await execute(admitted, scenario['input'],
                               {'executionId': scenario['id'], 'traceId': f"trace-{scenario['id']}"},
                               cancelled=cancel.is_set, after_return=after_return)
        actual = summary(result)
        assert actual == scenario['expected'], scenario['id']
        cases.append({'id': scenario['id'], **actual})

    entered = 0
    barrier = asyncio.Event()

    async def context_case(scenario):
        observations = []

        def wrap(name, fn):
            async def call(value, context):
                nonlocal entered
                if name == 'a':
                    entered += 1
                    if entered == len(scenarios['contexts']):
                        barrier.set()
                    await barrier.wait()
                observations.append({'step': name.upper(), 'executionId': context['executionId'],
                                     'traceId': context['traceId']})
                return await fn(value, context)
            return call

        admitted = admit_projection(raw['projection.json'], digests['projection.json'],
                                    {name: wrap(name, fn) for name, fn in handlers.items()})
        context = {'executionId': scenario['executionId'], 'traceId': scenario['traceId']}
        result = await execute(admitted, scenario['input'], context)
        assert result['outcome'] == 'COMPLETED' and result['result'] == scenario['expected']
        assert observations == [{'step': step, **context} for step in ['A', 'B', 'C']]
        return {**context, 'result': result['result'], 'observations': observations}

    contexts = await asyncio.gather(*(context_case(row) for row in scenarios['contexts']))
    return {'language': 'python', 'profile': 'tpf-polyglot-probe/0',
            'projectionSha256': digests['projection.json'],
            'scenariosSha256': hashlib.sha256(scenario_bytes).hexdigest(),
            'cases': cases, 'contexts': contexts}


if __name__ == '__main__':
    print(json.dumps(asyncio.run(run_journey(Path(sys.argv[1]) if len(sys.argv) > 1
                                           else ROOT / 'generated')), indent=2))
