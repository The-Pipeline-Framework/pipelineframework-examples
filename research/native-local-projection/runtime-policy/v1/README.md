# Run the unary policy binding

The compiler supplies the order and retry budget. The authored functions supply the arithmetic.
With the baseline projection, input 3 goes through A, B, C and produces 5. The reordered DSL
makes the same functions run B, A, C and produce 4. A third artifact gives B one retry. When B
fails once, it receives 4 again and A is called once.

This is the live binding for the candidate `tpf-polyglot-unary-policy/1` profile. Its
[shared case inputs and expected answers](scenarios.json) cover actual calls and settlement.
The [contracts model](https://github.com/The-Pipeline-Framework/pipelineframework-contracts/issues/52)
also covers hypothetical configurations such as nullable whole results.

## Run locally

From `research/native-local-projection`:

```sh
npm ci --ignore-scripts --no-audit --no-fund
npm run test:policy
```

Use Node 22.23 or later, npm and Python 3.11 or later. This runs actual promises and coroutines,
checks generated types with TypeScript and Pyright, and requires Pyright to reject an authored
function that returns a string where the compiler declares an integer. The check uses supplied
artifacts and requires no local JVM or coordinator.

To compare the three bindings, also make a JDK 21 available and run:

```sh
python3 -B runtime-policy/v1/check_policy_binding.py
```

This resolves and verifies the pinned [released Java artifacts](../PROVENANCE.json) in the
worktree's Maven cache. Java runs through `PipelineRunnerCore`, `PipelineStepExecutor` and
`StepOneToOne`. The profile supplies a strict predicate to the released retry loop. The
[original comparison](../check_runtime_policy.py) remains the control for Java's unprofiled
classification and recovery.

## Follow an invocation

Start with [authored.ts](authored.ts) or [authored.py](authored.py). Each adapter takes the result
from the [existing TypeScript](../../business.ts) or [Python](../../business.py) business function
and wraps it in an explicit `VALUE` carrier. The shell validates the carrier and its required,
non-null `int32` record before dispatching the next step.

- A validated `TRANSIENT` failure may spend the current step's projected budget. Exhaustion
  preserves the last complete envelope. Business rejection is terminal. A raw host throw becomes
  `UNCLASSIFIED` with a fixed message, including a Java throw bearing the legacy nonretryable
  marker. Malformed or conflicting structured carriers reject with a protocol diagnostic.
- The binding rejects independently active legacy retry or configured recovery before business
  code starts. A null, implicit or untagged result rejects. The profile recognises `EMPTY` and
  rejects it for this unary boundary.
- Cancellation cases hold a real invocation, a validated A return, or a pending retry. Node
  uses an `AbortSignal`, Python an `asyncio.Event`, and Java cancels an actual Mutiny
  subscription. The Java retry barrier holds a task on Mutiny's scheduler. The probes release
  those barriers to check whether late work changes the terminal winner or is delivered to
  the caller. Separate controls release the same barriers without cancelling.
- Two invocations overlap at A, then retry after the caller objects have been mutated. Each
  invocation receives its own immutable `executionId` and `traceId` on every attempt. The
  check preserves the exact identifier text, including non-ASCII whitespace, and rejects
  context with any other fields.

The [Java binding](PolicyBinding.java), [Node shell](../../local-shell.mjs) and
[Python shell](../../local_shell.py) implement the mapping. Each live driver observes actual
calls and counts terminal observations separately from application delivery. The comparison
uses the expected values supplied in `scenarios.json` as its oracle.

## Artifact origin and limits

The three directories under [generated](generated) contain the compiler's output bytes.
[PROVENANCE.json](PROVENANCE.json) pins the research producer source, DSL and binding inputs, and each
artifact's digest. To regenerate, use the examples checkout and its
[three generation commands](../../../polyglot-projection-v0/README.md#generate-the-explicit-unary-policy-binding),
then compare its `generated-policy/` directory with this directory's `generated/`.
The producer tests also verify these copies and all pinned source/input bytes.
`importedFrom` preserves the original compiler-PR provenance; no compiler checkout
or unmerged compiler PR is required. The
examples runner checks those pinned bytes and reads the artifact to select the functions.

This proof qualifies one pure, linear, unary LOCAL research slice over trusted compiler JSON.
It leaves adversarial artifact parsing, signed Release identity, nullable whole results,
changed-project generation without a local JVM, streaming, Connectors, durable Await, remote
workers and production runtime integration open.
