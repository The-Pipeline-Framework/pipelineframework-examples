"""Execute the shared live cases with actual coroutines and asyncio cancellation signals."""
import asyncio
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.append(str(HERE.parents[1]))
# The generated models belong to a pinned inventory that must stay clean on direct runs.
sys.dont_write_bytecode = True

from authored import handlers as business
from local_shell import Diagnostic, Failure, POLICY_PROFILE, admit_projection, execute


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def read_inventory():
    inventory = {}
    for name in ('baseline', 'changed-order', 'downstream-retry'):
        fixture = HERE / 'generated' / name
        raw = (fixture / 'projection.json').read_bytes()
        digests = dict(line.split('  ')[::-1] for line in (fixture / 'SHA256SUMS').read_text().splitlines())
        require(hashlib.sha256(raw).hexdigest() == digests['projection.json'], f'{name} digest mismatch')
        inventory[name] = (raw, digests['projection.json'])
    return inventory


class Run:
    def __init__(self, row, inventory):
        self.row = row
        self.calls = []
        self.observed = []
        self.delivery = []
        self.first_terminal = asyncio.Event()
        self.ready = asyncio.Event()
        self.release = asyncio.Event()
        self.cancel_event = asyncio.Event()
        self.context = row.get('context', {'executionId': row['id'], 'traceId': f"trace-{row['id']}"})
        if type(self.context) is dict:
            self.context = self.context.copy()
        raw, digest = inventory[row.get('projection', 'baseline')]
        try:
            if 'artifactMutation' in row:
                mutation = row['artifactMutation']
                candidate = json.loads(raw)
                parent = candidate
                for key in mutation['path'][:-1]:
                    parent = parent[key]
                parent[mutation['path'][-1]] = mutation['value']
                raw = json.dumps(candidate).encode()
                digest = hashlib.sha256(raw).hexdigest()
            admission = admit_projection(raw, digest, {name: self.bind(name, authored)
                                                       for name, authored in business.items()},
                                         profile=POLICY_PROFILE)
        except Diagnostic as error:
            terminal = {'kind': 'REJECTED', 'diagnostic': error.code}
            self.on_terminal(terminal)
            self.on_delivery(terminal)
            self.result = None
        else:
            if row.get('cancel') == 'prestart':
                self.cancel_twice()
            self.result = asyncio.create_task(execute(
                admission, row.get('input', {'value': 3}), self.context,
                cancel_event=self.cancel_event,
                after_return=self.after_return, before_retry=self.before_retry,
                on_terminal=self.on_terminal, on_delivery=self.on_delivery,
                **({'ownership': row['ownership']} if 'ownership' in row else {}),
            ))

    def on_terminal(self, terminal):
        self.observed.append(dict(terminal))
        self.first_terminal.set()

    def on_delivery(self, terminal):
        self.delivery.append(terminal['kind'])

    async def hold(self):
        self.ready.set()
        await self.release.wait()

    def at_gate(self, phase, step):
        return self.row.get('gate') == {'phase': phase, 'step': step}

    async def after_return(self, step):
        if self.at_gate('after-return', step):
            await self.hold()

    async def before_retry(self, step):
        if self.at_gate('before-retry', step):
            await self.hold()

    def bind(self, handler, authored):
        step = handler.upper()
        attempts = 0

        async def invoke(value, snapshot):
            nonlocal attempts
            faults = self.row.get('faults', {}).get(step, [])
            fault = faults[attempts] if attempts < len(faults) else {}
            attempts += 1
            mutation_blocked = False
            try:
                snapshot['traceId'] = 'handler mutation'
            except TypeError:
                mutation_blocked = True
            try:
                value['value'] = -1
            except TypeError:
                pass
            else:
                raise RuntimeError('attempt input must be immutable')
            self.calls.append({'step': step, 'input': value['value'], **snapshot,
                               'mutationBlocked': mutation_blocked})
            if attempts == 1 and self.at_gate('in-flight', step):
                await self.hold()
            match fault.get('kind'):
                case 'failure':
                    raise Failure(fault['envelope'])
                case 'throw':
                    raise RuntimeError('private provider detail')
                case 'implicit':
                    pass
                case 'return':
                    return fault['outcome']
                case _:
                    return await authored(value, snapshot)
        return invoke

    def cancel_twice(self):
        self.cancel_event.set()
        self.cancel_event.set()

    def mutate_caller(self):
        self.context.update(executionId='caller mutation', traceId='caller mutation')

    async def cancel_while_held(self):
        self.cancel_twice()
        await self.first_terminal.wait()
        require(self.observed == [{'kind': 'CANCELLED'}] and self.delivery == [],
                'cancelled execution did not settle correctly while held')

    async def report(self):
        if self.row.get('cancel') == 'after':
            await self.first_terminal.wait()
            self.cancel_twice()
        # Keep waiting after cancellation observation until the late coroutine has drained.
        terminal = (await self.result)['terminal'] if self.result is not None else self.observed[0]
        require(self.observed == [terminal], 'terminal observation and outcome disagree')
        return {'id': self.row['id'], 'calls': self.calls, 'terminal': terminal,
                'observations': len(self.observed), 'delivery': self.delivery}


async def main():
    corpus = json.loads((HERE / 'scenarios.json').read_bytes())
    require(corpus['profile'] == POLICY_PROFILE and corpus['schemaVersion'] == 1, 'unsupported case corpus')
    inventory = read_inventory()
    reports, overlap = {}, []
    for row in corpus['cases']:
        run = Run(row, inventory)
        if 'gate' in row:
            await run.ready.wait()
        if row.get('overlap'):
            overlap.append(run)
            continue
        if row.get('cancel') == 'during':
            await run.cancel_while_held()
        run.release.set()
        reports[row['id']] = await run.report()
    require(len(overlap) == 2, 'expected two overlapping live invocations')
    for run in overlap:
        run.mutate_caller()
    for run in reversed(overlap):
        run.release.set()
        reports[run.row['id']] = await run.report()
    print(json.dumps({'language': 'python', 'profile': POLICY_PROFILE,
                      'artifactSha256': {name: digest for name, (_, digest) in inventory.items()},
                      'cases': [reports[row['id']] for row in corpus['cases']]}))


if __name__ == '__main__':
    asyncio.run(asyncio.wait_for(main(), timeout=15))
