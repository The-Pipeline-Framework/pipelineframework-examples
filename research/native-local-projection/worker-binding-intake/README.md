# Prepare a native worker binding

This executable intake checks which existing Java contracts can carry a native
application, and which compiler/runtime integrations still have to be built.
It supports the bounded work in [runtime #82](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/issues/82).
It does not start a worker server or a coordinator.

The existing LOCAL examples run the whole pipeline in one Node or Python process.
The next binding will put that same process behind an authenticated worker endpoint:

```text
Developer → deployed Java coordinator → deployed Node or Python worker
                                          A → B → C, all native LOCAL
```

The coordinator owns durable admission and settlement. The native runtime owns
the admitted local function calls. Losing the developer's connection must not
cancel admitted work. A development laptop does not host the coordinator or need
an inbound worker connection.

## Run the intake

From `research/native-local-projection`, with JDK 21, Node 22 and Python available:

```sh
python3 -B transition-admission/run.py
python3 -B worker-binding-intake/run.py
```

The first command retains the accepted Java, Node and Python authentication,
admission and failed-result cases. The second probes the released Java carrier
and release contracts using each language's real generated baseline files. It
does not execute new Node/Python payload decoders. Dependencies resolve into this worktree's isolated Maven
repository and are checked against the existing pinned release digests.

These are contributor checks. Requiring Java here does not change the Java-free
Node/Python application development requirement.

The release probe builds temporary fixture archives from generated bindings,
models, projection and business source. Its coordinator Contract is explicitly
synthetic. These archives omit a packaged native runtime and dependency closure;
they are not deployable releases. The Node and Python baseline examples also have
different pipeline IDs, so their successful checks do not prove that the compiler
has emitted one shared coordinator Contract.

| Check | What it establishes |
| --- | --- |
| Four nominal IDs carrying the same payload string | Existing request/result carriers preserve identity and bytes; they do not check the expected boundary type. |
| Research payload passed to the legacy Java codec | It rejects unsupported encoding/type instead of providing native decoding. |
| Separate archive and Compiled Truth fixture | The released resolver verifies both digests and preserves the projection resource. |
| Changed archive or carrier bytes under the original digest | The released resolver rejects the mismatch. |
| Substituted carrier with its own valid digest | Per-artifact validation succeeds; the probe's explicit projection comparison rejects the inconsistent pair. |
| Two deployable owners for the same steps | Existing release validation rejects ambiguous ownership. |
| Ordinary Java capabilities with a bounded archive | Existing release validation rejects the missing protocols; the compiler must emit honest bounded capabilities. |

## Keep the selected boundaries separate

| Boundary | Intake choice |
| --- | --- |
| Business execution | Keep `tpf-polyglot-unary-policy/1`: finite linear unary execution, required non-null int32 records, explicit context and one local retry owner. |
| Worker envelope | Reuse the released REST command and COMPLETED envelope. Its outer encoding remains `application/tpf-transition-envelope+json`. |
| Application payload | Carry the compiler-owned logical type ID and exact payload bytes. Select an inner encoding through trusted deployment configuration; the outer encoding does not select a decoder. |
| Failure result | Retain the explicit `tpf-polyglot-failed-result/1` selection and fixed diagnostics from ADR-0088. Do not route it through the legacy Java exception-class decoder. |
| Release | Use the existing Release Descriptor with independently verified runnable and descriptor-named Compiled Truth artifacts. No parallel native release manifest. |
| Deployment | Use one explicitly paired coordinator/worker endpoint per verified binding. Preserve HTTPS, request signatures, freshness, nonce checks and retained attempt authority. |

The old `tpf-polyglot-research-json/1` / `ProbeValue` transcript remains a research
fixture. Passing it does not allocate a production encoding for the four generated
types. The new probe demonstrates carrier compatibility; it does not install a
decoder or widen an accepted profile. The payload binding must check the expected
type at each boundary, not merely that a type name appears somewhere in the model.

## What the integration must supply

Native generation currently emits application bindings, models and a projection.
Worker packaging also needs compiler-produced `pipeline-contract.json` and a
descriptor-named Compiled Truth carrier containing the exact native projection.
The coordinator and worker must use the same generated contract. A probe fixture
is not a replacement for that compiler output.

The ordinary Java contract advertises worker protocols beyond this bounded REST
binding. The native archive must not advertise gRPC or SQS simply to pass release
validation. The compiler owns the bounded contract/capability projection; the
runtime retains its exact-contract and whole-bundle checks.

The binding must associate the executable actually loaded by the worker with the
verified runnable bytes, and the projection actually consumed by its runtime with
the verified Compiled Truth resource. Checking an advertised digest, or hashing
an unrelated downloaded archive, is insufficient. Use separate immutable Release
versions for the Node and Python whole-bundle deployments; do not assign every
step to two deployable owners in one Release.

The intake decisions and dated source evidence live in [runtime #82](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/issues/82).
[ADR-0088](https://github.com/The-Pipeline-Framework/pipelineframework/blob/main/docs/decisions/0088-polyglot-contract-qualification-precedes-runtime-selection.md)
remains the accepted authority for profiles, release trust, failure mapping and
uncertain remote outcomes.

## Limits

An envelope round trip does not prove application decoding. A valid descriptor
does not prove that a native process runs its declared artifact. A local fixture
does not prove deployed authentication, durable settlement or restart recovery.
Those remain acceptance gates for the real paired binding.

This intake adds no production runtime, server, coordinator, public Provider SPI
or release publication. Streaming, Connectors and durable Await remain broader
product requirements. The bounded unary worker is not the product MVP.
