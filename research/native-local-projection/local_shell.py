"""Independent, test-only Python shell for the compiler's closed native projection.

There is no DSL parser, provider, coordinator, subprocess, JVM, or transport here.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from types import MappingProxyType


POLICY_PROFILE = 'tpf-polyglot-unary-policy/1'
POLICY_BINDING = MappingProxyType({
    'failureMapping': 'structured-failure/1', 'retryOwner': 'LOCAL_SHELL',
    'resultMapping': 'explicit-value/1', 'cancellation': 'first-accepted-terminal/1',
    'context': 'explicit-snapshot/1', 'recovery': 'REJECT', 'legacyPolicy': 'REJECT',
})
DEFAULT_OWNERSHIP = object()


class Diagnostic(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def require(condition, code):
    if not condition:
        raise Diagnostic(code)


def keys(value, names, code):
    require(type(value) is dict and set(value) == set(names), code)


def digest(value):
    return (type(value) is str and len(value) == 64
            and all(c in '0123456789abcdef' for c in value))


def parse_projection(raw):
    def integer_decimal(token):
        try:
            number = Decimal(token)
            require(number == number.to_integral_value() and abs(number) <= 2**53 - 1,
                    'INVALID_NUMBER')
            return int(number)
        except ArithmeticError as error:
            raise Diagnostic('INVALID_NUMBER') from error

    try:
        return json.loads(raw.decode('utf-8'), parse_float=integer_decimal,
                          parse_constant=lambda _: require(False, 'INVALID_NUMBER'))
    except (ValueError, UnicodeError, RecursionError) as error:
        raise Diagnostic('INVALID_JSON') from error


@dataclass(frozen=True)
class Admission:
    projection: object
    handlers: object


def freeze(value):
    if type(value) is dict:
        return MappingProxyType({key: freeze(item) for key, item in value.items()})
    if type(value) is list:
        return tuple(freeze(item) for item in value)
    return value


def admit_projection(raw, expected_digest, handlers, *, profile='tpf-polyglot-probe/0'):
    """Check byte identity first. The digest establishes integrity, not authentication."""
    require(digest(expected_digest) and hashlib.sha256(raw).hexdigest() == expected_digest,
            'ARTIFACT_DIGEST_MISMATCH')
    p = parse_projection(raw)
    policy = profile == POLICY_PROFILE
    fields = ['schemaVersion', 'profile', 'kind', 'pipeline', 'requiredCapabilities',
              'types', 'steps', 'contextKeys', 'provenance']
    if policy:
        fields.append('policy')
    keys(p, fields, 'UNSUPPORTED_PROJECTION')
    require(type(p['schemaVersion']) is int and p['schemaVersion'] == 1, 'UNSUPPORTED_SCHEMA')
    require((policy or profile == 'tpf-polyglot-probe/0') and p['profile'] == profile
            and p['kind'] == 'compiler-projection-probe', 'UNSUPPORTED_PROFILE')
    if policy:
        require(p['policy'] == POLICY_BINDING, 'UNSUPPORTED_POLICY')
    pipeline = p['pipeline']
    keys(pipeline, ['id', 'inputType', 'outputType', 'transport', 'platform', 'execution'],
         'UNSUPPORTED_PIPELINE')
    require(type(pipeline['id']) is str and bool(pipeline['id']), 'INVALID_PIPELINE_ID')
    require((pipeline['transport'], pipeline['platform'], pipeline['execution'])
            == ('LOCAL', 'COMPUTE', 'DIRECT'), 'UNSUPPORTED_EXECUTION')
    require(p['requiredCapabilities'] == ['pure', 'linear', 'unary', 'local', 'retry',
                                          'cancellation', 'context'], 'UNSUPPORTED_CAPABILITY')
    require(p['contextKeys'] == ['executionId', 'traceId'], 'UNSUPPORTED_CONTEXT')
    require(type(p['types']) is list and len(p['types']) >= 1, 'UNSUPPORTED_TYPE')
    types = {}
    for canonical in p['types']:
        keys(canonical, ['id', 'fields'], 'UNSUPPORTED_TYPE')
        require(type(canonical['id']) is str and bool(canonical['id'])
                and canonical['id'] not in types and type(canonical['fields']) is list
                and bool(canonical['fields']), 'UNSUPPORTED_TYPE')
        fields = set()
        for field in canonical['fields']:
            keys(field, ['name', 'type', 'presence', 'nullability'], 'UNSUPPORTED_TYPE')
            require(type(field['name']) is str and bool(field['name'])
                    and field['name'] not in fields and field['type'] == 'int32'
                    and field['presence'] in ['REQUIRED', 'OPTIONAL']
                    and field['nullability'] in ['NON_NULL', 'NULLABLE'], 'UNSUPPORTED_TYPE')
            fields.add(field['name'])
        types[canonical['id']] = canonical
    require(type(pipeline['inputType']) is str and pipeline['inputType'] in types, 'TYPE_MISMATCH')
    canonical = types[pipeline['inputType']]
    require(pipeline['outputType'] == canonical['id'], 'TYPE_MISMATCH')
    # Additional emitted models establish declaration coverage only; executable values
    # in this bounded research slice have exactly one required non-null int32 field.
    require(canonical['fields'] == [{'name': 'value', 'type': 'int32',
                                    'presence': 'REQUIRED', 'nullability': 'NON_NULL'}],
            'UNSUPPORTED_TYPE')
    require(type(p['steps']) is list and len(p['steps']) == 3, 'UNSUPPORTED_STEP_SHAPE')
    ids, bindings = set(), set()
    for step in p['steps']:
        keys(step, ['id', 'inputType', 'outputType', 'handler', 'retryLimit'], 'UNSUPPORTED_STEP')
        require(type(step['id']) is str and bool(step['id']) and step['id'] not in ids,
                'DUPLICATE_STEP')
        ids.add(step['id'])
        require(step['inputType'] == canonical['id'] == step['outputType'], 'TYPE_MISMATCH')
        require(type(step['handler']) is str and callable(handlers.get(step['handler'])),
                'MISSING_HANDLER')
        require(step['handler'] not in bindings, 'AMBIGUOUS_HANDLER')
        bindings.add(step['handler'])
        require(type(step['retryLimit']) is int and 0 <= step['retryLimit'] <= 2,
                'UNSUPPORTED_RETRY')
    keys(p['provenance'], ['dslSha256', 'bindingsSha256', 'generatorTreeSha256',
                          'contractsVersion', 'dslJarSha256', 'semanticModelJarSha256'],
         'INVALID_PROVENANCE')
    require(p['provenance']['contractsVersion'] == '26.11.1'
            and all(digest(v) for k, v in p['provenance'].items() if k != 'contractsVersion'),
            'INVALID_PROVENANCE')
    return Admission(freeze(p), MappingProxyType({
        step['handler']: handlers[step['handler']] for step in p['steps']}))


def load_projection(path, digest_path, handlers):
    return admit_projection(Path(path).read_bytes(), Path(digest_path).read_text().strip(), handlers)


def validate_value(value):
    keys(value, ['value'], 'INVALID_VALUE')
    require(type(value['value']) is int and -2147483648 <= value['value'] <= 2147483647,
            'INVALID_VALUE')


def validate_context(context, policy=False):
    keys(context, ['executionId', 'traceId'], 'INVALID_CONTEXT')
    require(all(type(v) is str and (bool(v.strip(' \t\r\n')) if policy else bool(v))
                for v in context.values()), 'INVALID_CONTEXT')


class Failure(Exception):
    """Native exception wrapping a portable failure record, never a Java class name."""
    def __init__(self, envelope):
        super().__init__('probe failure')
        self.envelope = envelope


def validate_failure(failure, step_id):
    keys(failure, ['code', 'message', 'retryable', 'owner', 'stepId'], 'MALFORMED_FAILURE')
    require(failure['code'] in ['TRANSIENT', 'BUSINESS_REJECTION'], 'UNKNOWN_FAILURE')
    require(type(failure['message']) is str and type(failure['retryable']) is bool
            and failure['stepId'] == step_id, 'MALFORMED_FAILURE')
    require(failure['owner'] == 'local-invocation-shell'
            and failure['retryable'] == (failure['code'] == 'TRANSIENT'),
            'FAILURE_POLICY_CONFLICT')


SANITISED = 'Unclassified business function failure'


def bind_failure(error, step_id):
    failure = error.envelope if isinstance(error, Failure) else {
        'code': 'UNCLASSIFIED', 'message': SANITISED, 'retryable': False,
        'owner': POLICY_BINDING['retryOwner'], 'stepId': step_id,
    }
    keys(failure, ['code', 'message', 'retryable', 'owner', 'stepId'], 'MALFORMED_FAILURE')
    require(all(type(failure[key]) is str for key in ['code', 'message', 'owner', 'stepId'])
            and type(failure['retryable']) is bool, 'MALFORMED_FAILURE')
    require(failure['code'] in ['TRANSIENT', 'BUSINESS_REJECTION', 'UNCLASSIFIED'],
            'UNKNOWN_FAILURE_CODE')
    require(failure['owner'] == POLICY_BINDING['retryOwner'] and failure['stepId'] == step_id
            and failure['retryable'] == (failure['code'] == 'TRANSIENT')
            and (failure['code'] != 'UNCLASSIFIED' or failure['message'] == SANITISED),
            'FAILURE_BINDING_MISMATCH')
    return dict(failure)


def bind_result(returned):
    require(returned != {'kind': 'EMPTY'}, 'UNSUPPORTED_EMPTY')
    keys(returned, ['kind', 'value'], 'INVALID_VALUE')
    require(returned['kind'] == 'VALUE', 'INVALID_VALUE')
    return returned['value']


async def execute(admission, input_value, context, *, cancelled=lambda: False,
                  after_return=None, cancel_event=None, before_retry=None,
                  on_terminal=None, on_delivery=None, ownership=DEFAULT_OWNERSHIP):
    """Return to the shell after each awaited native coroutine before choosing the next step."""
    projection, handlers = admission.projection, admission.handlers
    policy = projection['profile'] == POLICY_PROFILE
    attempts = {step['id']: 0 for step in projection['steps']}
    trace = []
    delivery = []
    terminal = None
    observations = 0
    watcher = None

    def summary():
        return {'terminal': terminal, 'attempts': attempts, 'trace': trace,
                'observations': observations, 'delivery': delivery}

    def finish(outcome):
        nonlocal terminal, observations
        if terminal is None:
            terminal = outcome
            observations += 1
            if on_terminal:
                on_terminal(freeze(terminal))
            if terminal['kind'] != 'CANCELLED':
                delivery.append(terminal)
                if on_delivery:
                    on_delivery(freeze(terminal))
        return summary()

    async def observe_cancel():
        await cancel_event.wait()
        finish({'kind': 'CANCELLED'})

    def event(kind, step):
        trace.append({'kind': kind, 'stepId': step['id'], **local_context})

    def cancellation(step):
        if policy:
            if cancel_event is not None and cancel_event.is_set():
                finish({'kind': 'CANCELLED'})
            return terminal is not None and terminal['kind'] == 'CANCELLED'
        if cancelled():
            event('cancelled', step)
            return True
        return False

    def cancelled_result():
        return summary() if policy else {'outcome': 'CANCELLED', 'attempts': attempts, 'trace': trace}

    try:
        validate_value(input_value)
        validate_context(context, policy)
        local_context = MappingProxyType(dict(context))
        value = dict(input_value)
        if policy:
            if ownership is DEFAULT_OWNERSHIP:
                ownership = {'legacyRetry': False, 'recovery': False}
            keys(ownership, ['legacyRetry', 'recovery'], 'INVALID_POLICY')
            require(all(type(flag) is bool for flag in ownership.values()), 'INVALID_POLICY')
            require(not any(ownership.values()), 'POLICY_OWNERSHIP_CONFLICT')
            if cancel_event is not None:
                if cancel_event.is_set():
                    return finish({'kind': 'CANCELLED'})
                watcher = asyncio.create_task(observe_cancel())

        for index, step in enumerate(projection['steps']):
            if cancellation(step):
                return cancelled_result()
            while True:
                attempts[step['id']] += 1
                event('invoke', step)
                try:
                    returned = await handlers[step['handler']](MappingProxyType(value), local_context)
                except Exception as error:
                    if cancellation(step):
                        return cancelled_result()
                    if policy:
                        failure = bind_failure(error, step['id'])
                    else:
                        require(isinstance(error, Failure), 'MALFORMED_FAILURE')
                        failure = error.envelope
                        validate_failure(failure, step['id'])
                    event('failure', step)
                    if not failure['retryable'] or attempts[step['id']] > step['retryLimit']:
                        if policy:
                            return finish({'kind': 'FAILED', 'failure': failure})
                        return {'outcome': 'FAILED', 'attempts': attempts, 'trace': trace,
                                'failure': failure}
                    event('retry', step)
                    if policy and before_retry:
                        await before_retry(step['id'])
                    if policy and cancellation(step):
                        return cancelled_result()
                    continue
                if cancellation(step):
                    return cancelled_result()
                if policy:
                    returned = bind_result(returned)
                validate_value(returned)
                value = dict(returned)
                event('return', step)
                if policy and index == len(projection['steps']) - 1:
                    finish({'kind': 'COMPLETED', 'outcome': {'kind': 'VALUE', 'value': value}})
                # Test scheduling only. A final value is already committed before this hook.
                if after_return:
                    await after_return(step['id'])
                break
        if policy:
            return summary()
        if cancellation(projection['steps'][-1]):
            return cancelled_result()
        return {'outcome': 'COMPLETED', 'attempts': attempts, 'trace': trace, 'result': value}
    except Diagnostic as error:
        if policy:
            return finish({'kind': 'REJECTED', 'diagnostic': error.code})
        raise
    finally:
        if watcher is not None:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
