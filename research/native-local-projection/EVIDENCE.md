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
`856386df2856b2ad95d4e1ab43775273731ada8ac0de6b0cceb8f356cba94fa7`.
Baseline projection SHA-256:
`aeef5a95f098d5f8af294b49feb6cf0a8b04db5fad0d8222d0a608ea6ea7d39b`.
Shared scenario SHA-256:
`aa3da5ea64b045d4dc818ce9daa5b357b074894e2183dbaa12ae8342d23d2033`.
Java research reference SHA-256:
`78f8abc6322d5e346ee069e6ec0f48204b96ad8e86263b0cb4e2985fe5c0db6a`.

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
`ce4aa482c62bcdb36fba74655744cc40fe50f2f76b40dda8b29cf7e3db760b7d`.
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
