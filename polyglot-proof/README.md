# Polyglot Proof (ADR-0092)

Executable compatibility proof for ADR-0092 (*Java Execution Engine + Foreign Function Host + Native `tpf` CLI*) over released/snapshot TPF compiler and runtime artifacts with **zero user Java domain classes**.

## What This Proof Verifies

1. **Class-Free Canonical v3 Compilation Parity (`PolyglotCompiler`):**
   - Compiles canonical v3 `pipeline.yaml` definitions containing multi-field records, nested types, repeated lists, constrained wrappers (`allowedValues`), `int64` (`> Number.MAX_SAFE_INTEGER`), 29-digit `decimal`, `bool`, `uuid`, `timestamp`, and optional/nullable fields.
   - Proves TypeScript and Python targets emit byte-identical class-free `META-INF/pipeline/pipeline-contract.json` contracts with zero Java `runtimeClass` leaks.
2. **Node (TypeScript) vs Python Cross-Language Equivalence (`1:1`, `1:N`, `N:1`):**
   - Runs real generated TypeScript (`host.ts` / `types.ts`) and Python (`host.py` / `types.py`) subprocesses through TPF's `QueueAsyncCoordinator`, `PipelineExecutionService`, `CanonicalFunctionHostStep`, `CanonicalFunctionHostOneToManyStep`, and `CanonicalFunctionHostManyToOneStep`.
   - Proves bit-for-bit identical canonical outputs (including 64-bit `int64` and 29-digit `decimal` precision), identical `BUSINESS_REJECTION` terminal failures (`failedStepIndex`, `failedCommandId`, zero retries), identical `TRANSIENT` retry + sweep completion, and `ForeignDispatchIntentStore` pre-send reservation on uncertain outcomes.
3. **Tier-2 Custom Polyglot Connectors (`PolyglotConnectorProvider`):**
   - Runs both Node and Python custom `Query` (`fx.quote` v1) and `Command` (`ledger.post` v1 with `reconciliationSupported: true`) connectors through TPF's `QueryStepSupport`, `CommandStepSupport`, and `ConnectorBindingRegistry`.
   - Proves Query capture replay (`0` duplicate queries), Command `RETURN_RECORDED` replay (`0` duplicate dispatches), host-owned secret isolation (pure functions never receive connector secrets), and mid-dispatch `AMBIGUOUS` recovery via `/reconcile`.
4. **Crash-Safe Local Disk Store vs Real DynamoDB Equivalence (`PolyglotDynamoEquivalenceIT`):**
   - Runs the same multi-field polyglot streaming and failure workflows against both a crash-safe disk-backed store (modeling `tpf dev` restart recovery across durable `WAITING_EXTERNAL` Await) and TPF's production `DynamoExecutionStateStore` on LocalStack DynamoDB, verifying identical state transitions, version monotonicity, failure metadata, and canonical output payloads.

## Running

```sh
./mvnw -pl polyglot-proof verify -Dmaven.repo.local="$PWD/.m2/repository"
```
