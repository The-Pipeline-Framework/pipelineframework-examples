# Polyglot Proof (ADR-0092)

Executable compatibility proof for ADR-0092 (*Java Execution Engine + Foreign Function Host + Native `tpf` CLI*) over released/snapshot TPF compiler and runtime artifacts with **zero user Java domain classes**.

## What This Proof Verifies

1. **Class-Free Canonical v3 Compilation Parity (`PolyglotCompiler`):**
   - Compiles canonical v3 `pipeline.yaml` definitions containing multi-field records, nested types, repeated lists, constrained wrappers (`allowedValues`), `int64` (`> Number.MAX_SAFE_INTEGER`), exact `decimal` arithmetic (`123456.789012` → `123477.289012` without float64 loss), `bool`, `uuid`, `timestamp`, and optional/nullable fields.
   - Proves TypeScript and Python targets emit byte-identical class-free `META-INF/pipeline/pipeline-contract.json` contracts with zero Java `runtimeClass` leaks.
2. **Node (TypeScript) vs Python Cross-Language Equivalence (`1:1`, `1:N`, `N:1`):**
   - Runs real generated TypeScript (`host.ts` / `types.ts`) and Python (`host.py` / `types.py`) subprocesses through TPF's `QueueAsyncCoordinator`, `PipelineExecutionService`, `CanonicalFunctionHostStep`, `CanonicalFunctionHostOneToManyStep`, and `CanonicalFunctionHostManyToOneStep`.
   - Proves bit-for-bit identical canonical outputs (including 64-bit `int64` above `Number.MAX_SAFE_INTEGER` and exact decimal arithmetic), identical `BUSINESS_REJECTION` terminal failures (`failedStepIndex`, `failedCommandId`, `INVALID_INVOICE` error classification, zero retries), identical `TRANSIENT` retry + sweep completion, and `ForeignDispatchIntentStore` pre-send reservation on uncertain outcomes.
3. **Tier-2 Custom Polyglot Connectors (`PolyglotConnectorProvider`):**
   - Runs both Node and Python custom `Query` (`fx.quote` v1) and `Command` (`ledger.post` v1 with `reconciliationSupported: true`) connectors through TPF's `QueryStepSupport`, `CommandStepSupport`, and `ConnectorBindingRegistry`.
   - Proves Query capture replay (`0` duplicate queries for the same execution/input), Command `RETURN_RECORDED` replay (`0` duplicate dispatches), mid-dispatch `AMBIGUOUS` recovery via `/reconcile` (`CONFIRMED_SUCCEEDED` with `0` duplicate dispatches), and host-owned secret isolation (both `fx.quote` and `ledger.post` receive their connector secrets while pure functions see none in environment or context).
4. **Crash-Safe Local Disk Store Restart Recovery & DynamoDB Equivalence (`PolyglotDynamoEquivalenceIT`):**
   - Proves crash/restart recovery across both durable `QUEUED` admission before dispatch (zero premature invocations; ordinary sweep completes once after restart) and durable `WAITING_EXTERNAL` Await (persisted Await request payload decoded and resumed after restart without re-running prior steps).
   - Runs the same multi-field polyglot streaming and failure workflows against both the disk-backed store (modeling `tpf dev` durability) and TPF's production `DynamoExecutionStateStore` on LocalStack DynamoDB, verifying identical state transitions, version monotonicity, failure metadata, and canonical output payloads.
5. **Tier-3 Mixed-Topology Pipeline (`Tier3MixedTopologyProofTest`):**
   - Combines TPF's JVM ecosystem `HttpConnector` (`http.client` v1 over loopback TLS with host-owned bearer credentials and `Idempotency-Key`), a live Node (TypeScript) foreign function host, and a live Python foreign function host inside one pipeline (`tier3-mixed-ingress-pipeline.yaml`).
   - Proves polyglot `SIDE_EFFECT` pass-through semantics (Node audit executes without mutating the downstream canonical payload), canonical v3 union branching (`express` vs `standard` variants routed in Python), cross-pipeline `CheckpointPublicationService` handoff into an independently owned downstream settlement pipeline (`tier3-downstream-settlement-pipeline.yaml`), exact `int64 > 2^53` + `decimal` fidelity across the handoff boundary, and zero credential leakage to Node or Python hosts.

## Prerequisites & Running

- **Node.js** with default TypeScript type stripping (`>= 22.18` / `23.6+`) on `PATH`.
- **Python `>= 3.10`** (`python3`) on `PATH`.
- **Docker** (for LocalStack DynamoDB in `PolyglotDynamoEquivalenceIT`).
- Locally, missing Node/Python runtimes skip those tests via JUnit assumptions; under `CI=true`, missing runtimes fail closed.

```sh
./mvnw -pl polyglot-proof verify -Dmaven.repo.local="$PWD/.m2/repository"
```
