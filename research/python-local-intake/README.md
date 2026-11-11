# Python LOCAL intake probes

These tests isolate CPython behavior needed by the experimental Python runtime.
They do not implement a pipeline executor or qualify Python runtime conformance.
The existing 51-case policy corpus and five native edge cases remain unchanged.
The runtime package must eventually pass those cases over installed wheel bytes.

Pull the pinned CPython 3.12.15 image before going offline:

```sh
docker pull python@sha256:dddfd7e07f9d15aeeca61529320492139d21cac7f0070c00609243e51e4e0016
```

Run the mechanism probes from the repository root without Java, Node or network access:

```sh
docker run --rm --pull=never --network none \
  -v "$PWD:/work:ro" -w /work \
  python@sha256:dddfd7e07f9d15aeeca61529320492139d21cac7f0070c00609243e51e4e0016 \
  python research/python-local-intake/run_probes.py
```

The runner gives the probe process 30 seconds to finish. The mechanism tests use
events and explicit scheduling barriers. They demonstrate:

- Unshielded task cancellation propagates into the awaited handler. Shielding
  isolates that handler, but the caller still needs deliberate draining and
  separate terminal arbitration, including repeated cancellation requests.
- Context variables isolate task bindings but retain references to mutable
  values. Copying validated context before exposing an immutable view is needed.
- Synchronous returns do not yield automatically. Python coroutine observers are
  lazy; discarded coroutines need closing, and started task failures need consuming.
- `CancelledError` escapes `except Exception`; booleans are Python integers;
  dataclass annotations do not validate values. The runtime must handle each
  explicitly under the existing profile.

These are mechanism observations, not a substitute for shared expected outcomes.
Intake decisions, exact evidence and remaining gates belong in
[runtime #85](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/issues/85).

The candidate nominal type shapes are test-only fixtures, not generated artifacts
or a Python runtime API. Check ordinary and async handler assignment, wrong input
and output identity, missing registry members and immutable models with:

```sh
python3.12 -m venv /tmp/tpf-python-intake-venv
/tmp/tpf-python-intake-venv/bin/python -m pip install \
  -r research/python-local-intake/requirements-test.txt
/tmp/tpf-python-intake-venv/bin/python research/python-local-intake/check_types.py
```

The checker is pinned mypy 1.18.2, which runs without Node or Java. The checker
rejects subclasses of the `@final` models, but Python permits them at runtime.
It also treats `bool` as an `int`. The future runtime must validate exact model
and primitive identity and int32 bounds. The positive fixture shows that mypy
accepts a custom `Coroutine` subclass. The runtime must check native coroutine
identity before awaiting a handler result. These probes qualify the candidate
type shapes and Python mechanisms.
