"""Run the fixed cross-language scenarios through compiler output and the installed wheel."""
import asyncio
from collections import Counter
from dataclasses import FrozenInstanceError, asdict, replace
from functools import cache
from hashlib import sha256
import importlib
import json
from pathlib import Path
import unittest

from tpf_experimental_python_local import (
    CancellationToken,
    PythonLocalDiagnostic,
    StructuredFailure,
    create_python_local_runtime,
)

from verify_project import FIXTURES, PROFILE, fixture_pin, verify_output

CORPUS = Path(__file__).resolve().parents[3] / "research" / "native-local-projection" / "runtime-policy"
OPERATIONS = {"A": lambda value: value + 1, "B": lambda value: value * 2, "C": lambda value: value - 3}


def load_cases(relative_path, digest):
    raw = (CORPUS / relative_path).read_bytes()
    if sha256(raw).hexdigest() != digest:
        raise ValueError(f"Shared corpus digest differs for {relative_path}.")
    return json.loads(raw)


@cache
def generation(name):
    fixture = "project" if name == "downstream-retry" else name
    pin = fixture_pin(fixture)
    directory = FIXTURES / fixture / "generated"
    projection = verify_output(directory, pin["files"], pin["inputs"])
    api = importlib.import_module(f"fixtures.{fixture}.generated.application")
    models = importlib.import_module(f"fixtures.{fixture}.generated.models")
    raw = (directory / "projection.json").read_bytes()
    if raw != api.PROJECTION_BYTES or sha256(raw).hexdigest() != api.PROJECTION_SHA256:
        raise ValueError(f"Embedded projection differs from the {fixture} generation.")
    return api, models, projection


def check_frozen(value, field, replacement):
    try:
        setattr(value, field, replacement)
    except FrozenInstanceError:
        return True
    return False


def terminal_payload(terminal):
    payload = asdict(terminal)
    if terminal.kind == "FAILED":
        failure = payload["failure"]
        failure["stepId"] = failure.pop("step_id")
    return payload


class Case:
    def __init__(self, row):
        self.row = row
        self.token = CancellationToken()
        self.expected_context = row.get("context", {"executionId": row["id"], "traceId": f"trace-{row['id']}"})
        self.ready = asyncio.Event()
        self.release = asyncio.Event()
        self.attempts = Counter()
        self.calls = []
        self.observations = []
        self.delivered = []

    def observe(self, event):
        self.observations.append(event)
        gate = self.row.get("gate")
        if not gate or self.row.get("cancel") != "during" or event.get("stepId") != gate["step"]:
            return
        if (gate["phase"], event["kind"]) in {
            ("after-return", "RETURNED"), ("before-retry", "RETRY_SCHEDULED"),
        }:
            self.cancel()

    def cancel(self):
        self.token.cancel()
        self.token.cancel()

    async def run(self, application, api):
        if self.row.get("cancel") == "prestart":
            self.cancel()
        raw_input = self.row.get("input", {"value": 3})
        input_record = api.ApplicationInput(**raw_input) if isinstance(raw_input, dict) else raw_input
        raw_context = self.expected_context
        self.context = (
            api.InvocationContext(**raw_context)
            if type(raw_context) is dict and raw_context.keys() == {"executionId", "traceId"}
            else raw_context
        )
        terminal = await application.run(input_record, self.context, self.token)
        self.delivered.append(terminal)
        return terminal

    async def invoke(self, api, model, step, record, context):
        step_id = step["id"]
        prior = self.attempts[step_id]
        self.attempts[step_id] += 1
        self.calls.append((
            step_id, record.value, asdict(context),
            check_frozen(context, "traceId", "handler mutation"),
            check_frozen(record, "value", -1),
        ))
        gate = self.row.get("gate")
        if prior == 0 and gate == {"phase": "in-flight", "step": step_id}:
            self.ready.set()
            await self.release.wait()
        faults = self.row.get("faults", {}).get(step_id, [])
        if prior >= len(faults):
            return api.Value(model(OPERATIONS[step_id](record.value)))
        fault = faults[prior]
        match fault["kind"]:
            case "failure":
                raise StructuredFailure(fault["envelope"])
            case "throw":
                raise RuntimeError("private provider detail")
            case "implicit":
                return None
            case "return":
                outcome = fault["outcome"]
                if isinstance(outcome, dict) and outcome.get("kind") == "VALUE" and "value" in outcome:
                    raw = outcome["value"]
                    return api.Value(model(**raw) if isinstance(raw, dict) else raw)
                return outcome
            case other:
                raise AssertionError(f"Unexpected fault {other}.")


def session(cases):
    api, models, projection = generation(cases[0].row.get("projection", "baseline"))
    contexts = {case.expected_context["executionId"]: case for case in cases} if len(cases) > 1 else {}

    def case_for(context):
        return cases[0] if len(cases) == 1 else contexts[context.executionId]

    def observe(event):
        case_for(event["context"]).observe(event)

    def handler_for(step):
        model = getattr(models, step["outputType"])

        async def handler(record, context):
            return await case_for(context).invoke(api, model, step, record, context)

        return handler

    handlers = {step["handler"]: handler_for(step) for step in projection["steps"]}
    return api, projection, create_python_local_runtime(on_observation=observe), handlers


class BindingCapture:
    """Let the generated entrypoint supply the real nominal carrier and model registry."""
    def load(self, binding):
        return binding


def mutated_binding(api, handlers, mutation):
    binding = api.create_application(BindingCapture(), handlers)
    candidate = json.loads(binding.projection_bytes)
    parent = candidate
    for key in mutation["path"][:-1]:
        parent = parent[key]
    parent[mutation["path"][-1]] = mutation["value"]
    raw = json.dumps(candidate).encode()
    return replace(binding, projection_bytes=raw, projection_sha256=sha256(raw).hexdigest())


class Conformance(unittest.IsolatedAsyncioTestCase):
    def assert_terminal(self, case, terminal):
        self.assertEqual(case.delivered, [terminal], "generated awaitable delivers exactly one terminal record")
        self.assertEqual(
            [event["record"] for event in case.observations if event["kind"] == "TERMINAL"],
            [terminal],
        )

    def verify_policy(self, case, terminal, projection):
        row = case.row
        self.assertEqual(terminal_payload(terminal), row["expected"]["terminal"])
        self.assert_terminal(case, terminal)
        self.assertEqual([[step, value] for step, value, *_ in case.calls], row["expected"]["calls"])
        for _, _, context, context_frozen, input_frozen in case.calls:
            self.assertEqual(context, case.expected_context)
            self.assertTrue(context_frozen and input_frozen)

        steps = {step["id"]: step for step in projection["steps"]}
        attempts = Counter()
        expected_starts = []
        for step_id, _ in row["expected"]["calls"]:
            attempts[step_id] += 1
            step = steps[step_id]
            expected_starts.append((step_id, step["inputType"], step["outputType"], attempts[step_id]))
        started = [event for event in case.observations if event["kind"] == "ATTEMPT_STARTED"]
        self.assertEqual(
            [(event["stepId"], event["inputType"], event["outputType"], event["attempt"]) for event in started],
            expected_starts,
        )
        for event in started:
            self.assertEqual(event["pipelineId"], projection["pipeline"]["id"])
            self.assertEqual(asdict(event["context"]), case.expected_context)

        gate = row.get("gate")
        if gate and gate["phase"] != "in-flight":
            kind = {"after-return": "RETURNED", "before-retry": "RETRY_SCHEDULED"}[gate["phase"]]
            observations = case.observations
            position = next(i for i, event in enumerate(observations)
                            if event["kind"] == kind and event["stepId"] == gate["step"])
            self.assertEqual(
                observations[position + 1]["kind"],
                "TERMINAL" if row.get("cancel") == "during" else "ATTEMPT_STARTED",
            )

    async def verify_policy_case(self, row):
        case = Case(row)
        api, projection, runtime, handlers = session([case])
        if "ownership" in row:
            # The current factory rejects the option itself; the old subscriber probe
            # accepted its name and separately validated its value and policy claims.
            with self.assertRaises(PythonLocalDiagnostic) as error:
                create_python_local_runtime(ownership=row["ownership"])
            self.assertEqual(error.exception.code, "UNSUPPORTED_RUNTIME_OPTIONS")
            legacy_diagnostic = "INVALID_POLICY" if row["ownership"] is None else "POLICY_OWNERSHIP_CONFLICT"
            self.assertEqual(row["expected"]["terminal"], {"kind": "REJECTED", "diagnostic": legacy_diagnostic})
            self.assertEqual(case.calls, row["expected"]["calls"])
            self.assertEqual(case.observations, [])
            return
        if "artifactMutation" in row:
            with self.assertRaises(PythonLocalDiagnostic) as error:
                runtime.load(mutated_binding(api, handlers, row["artifactMutation"]))
            self.assertEqual(error.exception.code, row["expected"]["terminal"]["diagnostic"])
            self.assertEqual(case.calls, row["expected"]["calls"])
            self.assertEqual(case.observations, [])
            return

        application = api.create_application(runtime, handlers)
        result = asyncio.create_task(case.run(application, api))
        gate = row.get("gate")
        if gate and gate["phase"] == "in-flight":
            try:
                await asyncio.wait_for(case.ready.wait(), timeout=5)
                self.assertFalse(result.done())
                if row.get("cancel") == "during":
                    case.cancel()
                    records = [terminal_payload(event["record"]) for event in case.observations if event["kind"] == "TERMINAL"]
                    self.assertEqual(records, [row["expected"]["terminal"]])
                    await asyncio.sleep(0)
                    self.assertEqual(case.delivered, [], "handler work must drain before delivery")
            finally:
                case.release.set()
        terminal = await asyncio.wait_for(result, timeout=5)
        if row.get("cancel") == "after":
            case.cancel()
        self.verify_policy(case, terminal, projection)

    async def verify_overlap(self, cases):
        api, projection, runtime, handlers = session(cases)
        application = api.create_application(runtime, handlers)
        results = [asyncio.create_task(case.run(application, api)) for case in cases]
        left, right = cases
        left_result, right_result = results
        try:
            await asyncio.wait_for(asyncio.gather(left.ready.wait(), right.ready.wait()), timeout=5)
            self.assertTrue(all(not task.done() for task in results))
            for case in cases:
                object.__setattr__(case.context, "executionId", "caller mutation")
                object.__setattr__(case.context, "traceId", "caller mutation")
            right.release.set()
            self.verify_policy(right, await asyncio.wait_for(right_result, timeout=5), projection)
            self.assertFalse(left_result.done())
        finally:
            for case in cases:
                case.release.set()
        self.verify_policy(left, await asyncio.wait_for(left_result, timeout=5), projection)

    async def test_shared_policy_corpus(self):
        corpus = load_cases("v1/scenarios.json", "bc11ab255dec48f20eb186f4c909b50b24a2cc3644f7a44a682755a777c3df9f")
        self.assertEqual((corpus["schemaVersion"], corpus["profile"]), (1, PROFILE))
        self.assertEqual(len(corpus["cases"]), 51)
        overlap = []
        for row in corpus["cases"]:
            with self.subTest(case=row["id"]):
                if row.get("overlap"):
                    overlap.append(Case(row))
                else:
                    await self.verify_policy_case(row)
        self.assertEqual(len(overlap), 2)
        await self.verify_overlap(overlap)

    async def verify_edge(self, row):
        fault = row["fault"]
        step, code = fault["step"], fault["code"]
        injected = (
            {"kind": "return", "outcome": None}
            if code == "NULL_RESULT"
            else {"kind": "failure", "envelope": {
                "code": code, "message": "fixture failure", "retryable": code == "TRANSIENT",
                "owner": "LOCAL_SHELL", "stepId": step,
            }}
        )
        case = Case(row | {"faults": {step: [injected] * fault["times"]}})
        api, projection, runtime, handlers = session([case])
        if row.get("recoverOnFailure"):
            index = next(i for i, item in enumerate(projection["steps"]) if item["id"] == step)
            binding = mutated_binding(api, handlers, {"path": ["steps", index, "recoverOnFailure"], "value": True})
            with self.assertRaises(PythonLocalDiagnostic) as error:
                runtime.load(binding)
            actual = {"diagnostic": error.exception.code}
            self.assertEqual(case.observations, [])
        else:
            terminal = await asyncio.wait_for(case.run(api.create_application(runtime, handlers), api), timeout=5)
            self.assert_terminal(case, terminal)
            match terminal.kind:
                case "COMPLETED":
                    actual = {"outcome": terminal.kind, "result": asdict(terminal.outcome.value)}
                case "FAILED":
                    actual = {"outcome": terminal.kind, "failureCode": terminal.failure.code}
                case "REJECTED":
                    actual = {"diagnostic": terminal.diagnostic}
                    if code == "UNKNOWN":
                        # The older corpus predates v1's specific unknown-code diagnostic.
                        self.assertEqual(terminal.diagnostic, "UNKNOWN_FAILURE_CODE")
                        actual["diagnostic"] = "UNKNOWN_FAILURE"
                case other:
                    self.fail(f"Unexpected terminal {other}.")
        actual["attempts"] = {step_id: case.attempts[step_id] for step_id in OPERATIONS}
        self.assertEqual(actual, row["native"])

    async def test_shared_native_edges(self):
        corpus = load_cases("edge-scenarios.json", "e792632a640b9148e956e56661e7ea60bd56a204198f4e6c7387eceb66a244ba")
        self.assertEqual(len(corpus["cases"]), 5)
        for row in corpus["cases"]:
            with self.subTest(case=row["id"]):
                await self.verify_edge(row)


if __name__ == "__main__":
    unittest.main()
