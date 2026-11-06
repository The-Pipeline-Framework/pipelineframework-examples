# A compiler-produced pipeline in one native process

This research example consumes the compiler-owned research tool's **explicitly supplied generated
artifact** and calls three asynchronous functions in one Node or Python process:

```text
{value: 3} → A adds 1 → {value: 4} → B doubles → {value: 8} → C subtracts 3 → {value: 5}
```

Start with [business.ts](business.ts) or [business.py](business.py). These are the
small authored business functions. Their types come from the compiler-produced
[models.ts](generated/models.ts) and [models.py](generated/models.py). The native
shell reads [projection.json](generated/projection.json) to select the functions,
their order, logical type identities and allowed retry counts. It never reads YAML.

This is the bounded proof requested by [examples #27](https://github.com/The-Pipeline-Framework/pipelineframework-examples/issues/27),
building on the provisional [contracts #52 profile](https://github.com/The-Pipeline-Framework/pipelineframework-contracts/pull/55)
and the test-only compiler projection for [compiler #46](https://github.com/The-Pipeline-Framework/pipelineframework-compiler/issues/46).
These shells are handwritten research scaffolding, not public native runtime
libraries or delivered Node/Python parity. They add no Maven dependency or module;
the existing examples continue consuming their released artifacts.

## Run and inspect

Contributor tools: Node 22.23 or later, Python 3.11 or later, npm. No Java process,
coordinator, Cloud account, RPC or provider is needed for these native commands.
From this directory:

```sh
npm ci --ignore-scripts --no-audit --no-fund
npm test
node journey.mjs
python3 -B journey.py
```

`npm test` runs the actual pinned TypeScript 5.9.3 checker, requires five specific
negative type diagnostics, runs Node and Python boundary tests, and compares each
language's actual journey report with the pinned Java research reference. Focused
contributor-tool tests require these evidence checks to reject drift under optimized
Python as well. Merely
running `.ts` with Node strips types; **the separate `tsc` check supplies semantic
type diagnostics**. Python imports generated `TypedDict` declarations and performs
runtime validation; no Python static checker was exercised.

The terminal report names each outcome, A/B/C invocation count and result. Seven
fixed scenarios cover success, two transient A failures followed by success, retry
exhaustion, nonretryable business rejection at A and B, cancellation after A and
pre-cancellation. In the transient example A runs three times; B and C each run
once. After rejection or cancellation at A, B and C run zero times. These are actual
Promise/coroutine calls, not a simulated count calculator.

Two other executions enter A at a shared barrier. Each A, B and C observes its own
execution and trace IDs. The expected results and identities are fixed independently
in [scenarios.json](scenarios.json); they are not copied from the observed context.

## What happens between A returning and B starting

Read the short loop in [local-shell.mjs](local-shell.mjs) or [local_shell.py](local_shell.py).

1. Admission checks the supplied artifact's SHA-256 and closed research schema,
   profile, capabilities, logical types, handlers and policy before invoking A.
   The admitted projection and handler bindings become immutable.
2. The shell awaits A in the current native process. A owns only its typed calculation.
3. The shell validates A's returned canonical value before selecting B from the
   compiler's ordered step list. A missing, null, boolean, fractional or overflowing
   `int32` result stops the flow. There is no coercion into a convenient host value.
4. A structured `TRANSIENT` failure may retry under the declared limit. A
   `BUSINESS_REJECTION` is nonretryable. Unknown/malformed failures and conflicting
   owners stop with explicit diagnostics. `local-invocation-shell` is the retry
   owner fixed by provisional profile 0; `retryLimit` comes from the compiler probe's
   research binding input, not new production DSL retry syntax.
5. Cancellation is checked at invocation/return boundaries and before B. A
   deterministic A-return barrier proves cancellation wins before B starts. This
   does not promise interruption of an arbitrary running business function.
6. B receives only the validated value and immutable `{executionId, traceId}` context.
   Provider credentials, connection secrets, tenant policy, retry authority and
   coordinator-store authority are rejected as extra context keys.

The trace is in-memory research evidence. No durable state, transport acknowledgement,
cache, persistence, Query capture, Command effect, replay, provider or coordinator is
hidden between the functions. Adding any such responsibility requires a separately
accepted contract and owner.

## Artifact origin and drift checks

The files in `generated/` are copied **byte for byte** from the compiler-owned
`conformance/polyglot-projection-v0/generated/` output. [PROVENANCE.json](PROVENANCE.json)
records its source revision, whether that checkout was modified at capture, exact
source-file fingerprints and artifact hashes. The projection pins the DSL bytes,
binding-input bytes, generator sources and released DSL/semantic-model JARs.
`SHA256SUMS` pins the generated JSON and declarations. `java-reference.sha256` pins
the separately generated Java reference report. `check_journey.py` verifies this
inventory and requires identical report values, projection digest and scenario digest
across Java, Node and Python.

The Java reference is an independent, handwritten `CompletableFuture` research
shell over the same generated projection and fixed scenarios. It is not execution
through Java's production `PipelineRunnerCore`/Quarkus runtime and does not establish
production policy equivalence. The earlier contracts probe additionally checks
released Java serialization/signature contracts. Neither result is a parity score.

To regenerate, first run the producer in an explicitly chosen compiler checkout;
that contributor-side action currently requires Java and released Maven artifacts:

```sh
# In the compiler checkout (not this examples repository):
python3 conformance/polyglot-projection-v0/run.py

# Back in this directory, explicitly import/check that producer's exact output:
python3 sync_fixture.py --compiler-repo /absolute/path/to/pipelineframework-compiler
python3 sync_fixture.py --compiler-repo /absolute/path/to/pipelineframework-compiler --check
npm test
```

`sync_fixture.py` never invents or rewrites a projection. It first checks that the
current producer sources and input bytes match the projection's fingerprints; a
source edit requires genuine regeneration before copying. `--check` reports drift
without overwriting the fixture. Native CI verifies the committed inventory and
journeys; compiler CI owns real DSL validation, generation and regeneration drift.

A digest supplies integrity relative to the explicitly trusted inventory; it is not
authentication or a signed Release identity. This bounded loader accepts trusted,
compiler-emitted JSON. Arbitrary adversarial JSON, including duplicate-object-key
ambiguity, is not qualified as a production artifact-admission protocol.

## Change the DSL and execute the changed artifact

The compiler probe includes a real changed DSL fixture whose step order is B→A→C.
After generating it with the compiler's `--projection-only` mode, consume its explicit
output here with **the same business functions**:

```sh
# In compiler checkout:
python3 conformance/polyglot-projection-v0/run.py --projection-only \
  --dsl conformance/polyglot-projection-v0/fixtures/changed-order.yaml \
  --output conformance/polyglot-projection-v0/generated-changed-order

# In this directory; 3 doubles to 6, adds 1, subtracts 3, producing 4:
node run_once.mjs /absolute/path/to/compiler/conformance/polyglot-projection-v0/generated-changed-order 3 4
```

The baseline `journey.mjs`/`journey.py` corpus expects A→B→C and result 5. `run_once.mjs`
is the explicit changed-artifact entry point; it checks all generated hashes and
executes the supplied ordering. It does not rebuild or parse DSL.

## Scope and remaining gaps

The executable slice is exactly three pure linear unary steps over one required,
non-null `int32` field named `value`, `COMPUTE` + `LOCAL` + `DIRECT`, retry limits 0–2
and the two allowed context keys. Additional emitted `PresenceProbe` fields exercise
generated required/optional and nullable/non-null declarations under TypeScript's
`exactOptionalPropertyTypes`; they are not additional executable codecs. Unsupported
features fail explicitly rather than disappearing during execution.

This proves one native-process LOCAL research journey and inspectable compiler output.
The full no-local-JVM changed-project generation/build workflow remains **GAP**: no
remote build service, authentication, source/dependency upload, immutable Release
provenance or offline regeneration is implemented here. A prebuilt fixture does not
close that requirement.

Connectors/plugins, streaming/backpressure, durable Await/restart, Query capture/replay,
Commands/effect recovery, FUNCTION, distributed REST/gRPC workers, QUEUE_ASYNC,
authorisation, durable cancellation and production runtime integration remain outside
this bounded increment. Streaming, Connectors and durable Await remain product
requirements. No issue, release or deployment is completed by this proof.
