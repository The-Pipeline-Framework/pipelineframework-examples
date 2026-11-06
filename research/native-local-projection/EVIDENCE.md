# Native LOCAL projection research evidence

Acceptance sources read directly on 6 November 2026: examples #27, contracts #52,
compiler #46, the contracts #55 `conformance/polyglot-v0/README.md` and `EVIDENCE.md`,
and the lead/reviewer obligations P1–P8. The bounded implementation does not accept
production parity or the full no-local-JVM authoring workflow. Independent adequacy
belongs to the lead and `projection_gate_review`, not the shell author.

## Revisions and actual results

Examples baseline: `c0af82a11d7ed9d7e6cf6d41dc05c4865f026f93`.
Exact producer revision/source fingerprints and generated artifact inventory live in
[PROVENANCE.json](PROVENANCE.json). Compiler-owned source fingerprint:
`7cddd1e0f5db40ed04d02899c6ec0aac7b51ce0119f5f320ef999a69ac4e52f7`.
Baseline projection SHA-256:
`fc8f31fd3b89b7cf85cfa50769d6775a372a8373c98405f2551456531a7e047a`.
Shared scenario SHA-256:
`aa3da5ea64b045d4dc818ce9daa5b357b074894e2183dbaa12ae8342d23d2033`.
Java research reference SHA-256:
`2f14a2bd88bf01dd93a353c4e7e5a623570dcab20cf6ae4d90c66a74cea50412`.

Run from this directory:

```sh
npm ci --ignore-scripts --no-audit --no-fund
npm test
python3 sync_fixture.py --compiler-repo /absolute/path/to/compiler --check
node run_once.mjs /absolute/path/to/compiler/conformance/polyglot-projection-v0/generated-changed-order 3 4
```

All passed locally on 6 November 2026 with Node 26.8.2, Python 3.14.6 and actual
TypeScript 5.9.3. Local tests completed in about four seconds; this is not billed CI
time. The dedicated native CI job invokes the same `npm test` with Node 22.23.4
and the runner's Python; its run, rather than this local report, owns CI evidence.
No Maven command was needed or run for the examples changes. Compiler-owned
contributor regeneration separately uses released Java DSL/model artifacts.

| Obligation | Executed evidence | Limit |
| --- | --- | --- |
| P1 Compiler authority | Exact producer artifacts copied and `sync_fixture.py --check` passed; real changed DSL artifact executes B→A→C, result 4 | Compiler owns v3 loading/normalization/validation and its own regeneration tests |
| P2 Typed bindings | Authored TypeScript checked against emitted models; five named diagnostics reject missing/wrong handlers, missing required field, null optional non-null field and explicit undefined | Python declarations imported and runtime-checked; no Python static checker claim |
| P3 Closed admission | Node/Python reject tampered digest and matching-digest incompatible schema/profile/capability/type/handler/policy/context before any function; admitted projection/bindings immutable | Trusted compiler JSON/inventory, not arbitrary adversarial JSON or authenticated Release admission |
| P4 Actual LOCAL | Seven fixed expected scenarios execute native async functions in one process; trace/order and int32 results checked | Handwritten test-only shells, no public runtime |
| P5 Java comparison | Both actual reports exactly equal pinned Java report except language: all seven outcomes/counts/results and two forced-overlap context observations | Java reference is a handwritten CompletableFuture probe, not production runtime equivalence |
| P6 Boundary failures | Malformed/null/boolean/overflowing output stops downstream; malformed/unknown/conflicting failure never retries; overflow after A and B checked | Executable canonical subset is only required non-null int32.value |
| P7 Context/cancellation | Each A/B/C observes fixed expected execution/trace IDs under forced overlap; authority keys reject; explicit A-return barrier proves cancellation before B | In-process context and cooperative boundary cancellation only |
| P8 Provenance/gaps | Artifact/source/dependency hashes, fixed scenarios and CI invocation committed; unchanged authored functions consume genuine changed-DSL output | Full changed-project generation without local Java remains GAP |

The real changed-order artifact digest is
`4636b5ee5895c5ab13b9e4a6a610bd9647e399a559e63f9c44325bfea1e354b1`.
`run_once.mjs` read that exact compiler-generated output and reported step order
`[B,A,C]`, result `{value:4}`, counts `{B:1,A:1,C:1}`. It did not mutate or regenerate
its own artifact. Baseline corpus results remain 5, so a baseline-only hardcoded
step order would fail this independent integration oracle.

## Review and material limits

Six Node and six Python focused tests passed, plus actual positive TypeScript
compilation and five targeted compile-time rejection checks. These counts identify
the executed harness; they are not a coverage or parity score. Java/Node/Python
outcome comparison is exact, not an assertion that each language printed PASS.

The dedicated CI job changes no canonical Maven module graph, profile, dependency,
publication or deployment. Handwritten shell policy and compiler research binding
inputs are disclosed in the README. No provider/coordinator responsibility was
absorbed. No network, JVM executor or subprocess exists in either native execution
loop; contributor test/origin-check scripts do launch native tools and read Git.

The bound examples GitNexus index matched the baseline above, indexed
6 November 2026. Query and pre-edit CI-file impact both failed with a LadybugDB
read-only shadow-page error, returning UNKNOWN rather than low impact. No index
rebuild was authorised or performed. Existing source was inspected directly: only a
new independent CI job is added; the research files are new unindexed symbols.
The subsequent external overlay change analysis succeeded: 233 changed symbols,
17 affected flows and 33 files, with a CRITICAL summary and `partial:true`. A second
run retained the same partial state. The affected flows all belong to the new
research shell/admission/journey functions. Unmapped entries are the unchanged
`CLAUDE.md → AGENTS.md` symlink and the two exact generated checksum files. The
source and checksum inventory checks above cover those descriptors directly; this
is disclosed fallback evidence, not a clean graph verdict. Pre-edit graph impact
remains unavailable rather than zero. Later overlay taint queries on both native
shells returned no modelled findings. Node control-dependence evidence was checked
against its rejection guards; Python returned no dependence edges with an explicit
coverage warning, so Python guard behaviour relies on source review and executable
tests. These results are bounded analysis, not a security or full PDG qualification.

Duplicate-key handling for arbitrary JSON remains unqualified: the generated fixture
contains unique object keys and admission verifies its exact trusted bytes. A digest
is integrity relative to a trusted inventory, not producer authentication. Streaming,
Connectors, durable Await, distributed execution, Query/Command semantics, release
signing and no-local-JVM regeneration remain separate required gates.

## PR feedback checks

Feedback on examples PR #35 was verified against candidate
`169d8c5ba0338f338f9493122c7e9986090f3dee`. The focused follow-up passed nine Node
checks and ten Python checks (eight shell checks plus two contributor-tool checks),
as well as the unchanged positive TypeScript check, five required diagnostics and
exact seven-case/two-context Java report comparison.

The added cases reject a comma-joined substitute for the two required context keys,
map extreme numeric/parser-recursion exceptions into explicit diagnostics, and
exercise cancellation after C returns alongside an uncancelled result-5 control.
Actual malformed JSON and extreme numeric inputs exercise native parser failures.
Deterministically injected Python `RecursionError` and Node `RangeError` verify
exception-to-diagnostic mapping without allocating arbitrarily deep inputs or
claiming engine-dependent parser-depth qualification.

The source-origin and Java-comparison tools now use explicit validation, preserving
checks under `python3 -O` and `PYTHONOPTIMIZE`. Optimized-process regression checks
require successful unchanged controls, rejection of altered copied output and
provenance, and rejection of a deliberately wrong Java expected result even after
its integrity hashes are updated. The origin-tool regression substitutes only Git
metadata in an isolated synthetic producer fixture; source/output comparisons and
the real command-line tool execute unchanged. This does not add compiler evidence.
`sourceModifiedAtCapture` now measures the entire compiler checkout, matching its
description. README and evidence changed-order commands use the same output folder.
