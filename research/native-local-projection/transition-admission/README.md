# Admit and settle a pinned unary transition

The research profile `tpf-polyglot-transition-admission/1` examines one whole-bundle
worker boundary for the A → B → C fixture. The existing
[unary policy binding](../runtime-policy/v1/README.md) exercises the LOCAL shell with
real functions. This probe uses fixed request bytes and an ordered transcript to
qualify the authority handoff around that shell.

## Run

From `research/native-local-projection`, use Node 22, Python 3.11 and JDK 21.

```sh
python3 -B transition-admission/run.py
```

Use `--native-only` to exercise the Node and Python consumers without Java.
The combined command resolves the released Java dependencies into the examples
worktree's isolated Maven repository and verifies their pinned digests. It
checks the fixed observations in [vectors.json](vectors.json), then alters the
signed bytes, an expected decode count, a disposition event and a completion
event. Each language must fail at the named case. A process crash fails the check.

## Released wire and candidate admission

The Java consumer uses `TransitionWorkerSignature`, `RestTransitionWorkerProtocol`,
`PipelineWorkerCapability`, `TransitionCommandEnvelope` and `TransitionWireResult`
from Contracts 26.11.1. Node and Python independently compute the HMAC from the
same bytes. The fixed signatures were calculated with OpenSSL using a public
fixture key. Request authentication covers method, route, timestamp, nonce and
SHA-256 of the exact body bytes.

The released REST resource checks freshness and signature, then consumes the
nonce before parsing the command envelope. The worker service checks the
pipeline, contract and release before decoding the application payload. Registered
REST selection checks the runnable artifact identity and digest at the coordinator.
The capability advertisement describes the outer envelope encoding. The command
carries the inner payload encoding and type. These checks have separate meanings.
[REST resource](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/blob/60dce3e400f51656631230ab9fe910ecb9a6c972/runtime/src/main/java/org/pipelineframework/orchestrator/RestTransitionWorkerResource.java#L83-L211)
· [Worker execution](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/blob/60dce3e400f51656631230ab9fe910ecb9a6c972/runtime/src/main/java/org/pipelineframework/PipelineExecutionService.java#L497-L509)
· [Registered selection](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/blob/60dce3e400f51656631230ab9fe910ecb9a6c972/runtime/src/main/java/org/pipelineframework/orchestrator/RegisteredRestWorkerTargets.java#L114-L200)

The test host supplies a trusted tenant, target and profile grant out of band.
The grant binds the selected unary policy to the real hash of the
`downstream-retry` projection. The probe compares that hash with the loaded
projection bytes and includes a substituted projection case. The grant is a
research fixture outside the released capability and envelope fields. A packaged
Release's provenance and deployed trust configuration require their own evidence.

The profile accepts the one required non-null int32 fixture record and a SINGLE,
whole-bundle result. It qualifies the separately named inner candidate encoding
`tpf-polyglot-research-json/1` and logical type `ProbeValue`. Legacy Java payload
tags and encodings reject before application decode. A legacy failure response
or incompatible output encoding cannot settle this profile. Failure mapping
within the LOCAL unary shell remains covered by the existing binding. Tenant
authority and worker authentication material stay in the host. The business
snapshot carries only `executionId` and `traceId`. Empty or ASCII-space, tab,
carriage-return or newline-only identifiers reject before payload decode.
Other identifier text is preserved exactly.

## Owners and observations

| Operation | Owner | Observation |
| --- | --- | --- |
| Validate raw request and consume nonce | Receiving host | A failed signature leaves the nonce available. A valid signature consumes it even if the envelope then fails validation. |
| Choose compatible worker and pin the attempt | Coordinator | An incompatible profile or artifact cannot cause a payload read or business dispatch. |
| Run the admitted bundle | Worker LOCAL shell | The modeled observations after a first B failure are A1, B2, C1. Both B observations carry the accepted input. The existing LOCAL binding exercises the actual calls and retry. |
| Invoke one remotely placed step | Callee shell in the proposed remote step binding | The caller has no additional logical retry budget. This separate boundary requires its own qualification. |
| Record an uncertain response and permit re-drive | Java coordinator | A lost response leaves `REMOTE_OUTCOME_UNKNOWN`. Independent evidence of the original attempt's disposition and an operator grant bound to that attempt precede a fresh request. |
| Settle a result | Java coordinator | A response belongs to the attempt associated with its dispatch. A stale or duplicate completion leaves the authoritative settlement unchanged. |

The uncertain-outcome transcript requires disposition confirming that the original
attempt is inactive. A worker advertisement cannot supply that evidence. A
fixture supplies a fresh signed request for one explicitly authorized re-drive.
The claim keeps the execution identifier, advances the attempt and execution
version, and changes the transition key. The worker compares all four claim
fields against the authenticated envelope before payload decode. Each dispatch
clears the prior disposition. A lost re-drive remains uncertain in this
one-re-drive profile. The same functions can run again. The report counts physical
requests, coordinator deliveries, application payload reads and logical attempts
separately. [ADR-0014](https://github.com/The-Pipeline-Framework/pipelineframework/blob/main/docs/decisions/0014-remote-transition-outcome-uncertainty.md)
· [Transition key construction](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/blob/60dce3e400f51656631230ab9fe910ecb9a6c972/runtime/src/main/java/org/pipelineframework/ClaimedSegment.java#L41-L83)

The test clock, nonce set, host grants and attempt records live in the transcript
process. Response association comes from the modeled dispatch context. Deployed
REST authentication, transport response protection, durable recovery and actual
distributed execution remain separate qualification gates.

## Compatibility and stopping rule

The fixture format, unary policy profile, worker capability version, inner
payload encoding and Release identity have separate versions. An incompatible
combination stops at its owning boundary without trying a different decoder or
adding another retry owner. A profile extension requires shared contract review
and fixed cases in every claimed language. The research increment ends when the
three consumers agree on the admission and settlement observations, including
the rejection paths. The remaining developer workflow and product acceptance
are tracked in [contracts #52](https://github.com/The-Pipeline-Framework/pipelineframework-contracts/issues/52)
and [epic #1055](https://github.com/The-Pipeline-Framework/pipelineframework/issues/1055).
