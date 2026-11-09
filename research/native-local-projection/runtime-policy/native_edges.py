"""Observe the same policy edges in the Python shell."""
import asyncio
from hashlib import sha256
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from business import handlers
from journey import summary
from local_shell import Diagnostic, Failure, admit_projection, execute


async def main():
    provenance = json.loads((HERE / 'PROVENANCE.json').read_bytes())
    projections = {
        'baseline': ((HERE.parent / 'generated/projection.json').read_bytes(), provenance['baselineProjectionSha256']),
        'downstream-retry': ((HERE / 'downstream-retry-projection.json').read_bytes(),
                             provenance['downstreamRetry']['projectionSha256']),
    }
    cases = json.loads((HERE / 'edge-scenarios.json').read_bytes())['cases']
    report = {}
    for row in cases:
        attempts = dict.fromkeys(('A', 'B', 'C'), 0)

        def wrap(name, handler):
            step = name.upper()

            async def call(value, context):
                attempts[step] += 1
                fault = row['fault']
                if step != fault['step'] or attempts[step] > fault['times']:
                    return await handler(value, context)
                if fault['code'] == 'NULL_RESULT':
                    return None
                raise Failure({'code': fault['code'], 'message': 'fixture failure',
                               'retryable': fault['code'] == 'TRANSIENT', 'owner': 'local-invocation-shell',
                               'stepId': step})

            return call

        functions = {name: wrap(name, handler) for name, handler in handlers.items()}
        try:
            raw, digest = projections[row['projection']]
            admitted = admit_projection(raw, digest, functions)
            if row.get('recoverOnFailure'):
                # An intentional unsupported-feature negative, separate from both compiler artifacts.
                unsupported = json.loads(raw)
                next(step for step in unsupported['steps'] if step['id'] == row['fault']['step'])['recoverOnFailure'] = True
                altered = json.dumps(unsupported).encode()
                admitted = admit_projection(altered, sha256(altered).hexdigest(), functions)
            result = await execute(admitted, {'value': 3}, {'executionId': row['id'], 'traceId': 'trace-edge'})
            if result['attempts'] != attempts:
                raise ValueError(f"reported attempts differ from observed handler calls: {row['id']}")
            report[row['id']] = summary(result)
        except Diagnostic as error:
            report[row['id']] = {'diagnostic': error.code, 'attempts': attempts}
    print(json.dumps(report))


if __name__ == '__main__':
    asyncio.run(main())
