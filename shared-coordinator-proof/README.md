# Shared Coordinator packaged proof

This proof runs two independently compiled applications through a shared Coordinator. Its three tests cover classpath isolation, production package bootstrap, and execution through a Coordinator restart.

## Packaged topology

- `application-a1` and `application-a2`: the same expense pipeline identity and canonical namespace, independently compiled artifacts. Business results distinguish A1 from A2. An expense request becomes a pending approval; a pure, request-aware completion projector joins an approval reply to the retained pending request; the final step produces a receipt.
- `application-b`: an unrelated parcel pipeline with four ordinary steps, different Java types and incompatible canonical definitions. `Prepare` and `Finish` deliberately overlap A's authored names.
- `coordinator`: framework runtime dependency only. It has no Maven dependency on any application fixture, generated application metadata or test classes.
- `proof-tests`: launches each worker and the Coordinator in separate Java processes using only that module's packaged JAR and its copied runtime dependency closure. It does not put application artifacts on its own test classpath.

The released annotation compiler owns generated Await adapters, step definitions, ordering and canonical contracts. The test reads the generated artifacts; no fixture duplicates the compiler's step order.

## Scaffold verification

From the repository root:

```sh
./mvnw -B -Dmaven.repo.local="$PWD/.m2/repository" \
  -Dpipelineframework.bom.version=26.11.2 \
  -Dpipelineframework.compiler.version=26.11.2 \
  -Dpipelineframework.runtime.version=26.11.2 \
  -pl shared-coordinator-proof/application-a1,shared-coordinator-proof/application-a2,shared-coordinator-proof/application-b,shared-coordinator-proof/coordinator,shared-coordinator-proof/proof-tests \
  -Dit.test=PackagedScaffoldIsolationIT -am clean verify
```

`PackagedScaffoldIsolationIT` records the exact child-process classpaths, compiled contracts, artifact and contract byte digests under `proof-tests/target/scaffold-evidence`. Each child uses a fresh process. The Coordinator must fail to load every class in any fixture JAR, including generated adapters and explicit markers; it must not resolve application descriptors or marker resources. This is stronger than replacing only a thread context class loader while application classes remain reachable from a parent loader.

Use exact compatibility-set component candidates for final integration. The repository's ordinary development defaults follow the active snapshot BOM; the command above deliberately checks the immutable 26.11.2 compiler baseline. Any local preview coordinate is developmental evidence only, never a substitute for published candidate compatibility results.

## Production package bootstrap

The four application modules also build Quarkus production packages under `target/quarkus-app`. `PackagedBootstrapIT` launches three independent workers and one Coordinator with the real CDI assembly and existing REST resources. Each worker must reject an unsigned capability request and return its exact compiled pipeline/contract, configured release, and actual application artifact digest for a signed capability request. The Coordinator uses `QUEUE_ASYNC`, registered REST targets and serialized payload materialization; it must serve an authenticated pending-interaction query while every fixture class and generated application resource is absent. The test records the physical packaged JAR paths and SHA-256 digests in `proof-tests/target/bootstrap-evidence/packaged-artifacts.json` and always stops its child processes.

This gate requires matching R1 runtime and deployment artifacts. Published 26.11.2 rejects the new serialized-materialization configuration, so the immutable baseline command above intentionally runs only the scaffold gate. Run the full canonical reactor with the exact compatible component set to include bootstrap. No configuration-validation bypass is part of the proof. Bootstrap establishes production wiring and package isolation only; it does not establish transition execution or recovery.

## Execution and restart gate

`PackagedExecutionRecoveryIT` runs all four production packages against a disposable local persistent store, with no application types on the Coordinator classpath. It requires the complete matching R1 runtime/contracts/compiler set, including physical canonical boundary metadata; bootstrap-only previews cannot satisfy it. The test is not disabled when a container runtime is unavailable: missing required persistence fails this gate.

`ProofReleaseSetup` is fixture-only deployment setup. It validates descriptors against exact compiled artifact bytes, records original immutable entry bindings through the existing registry, and uses conditional activation operations. It adds no production management endpoint. Application execution and completion use production control-plane and worker REST paths. Worker subprocess configuration contains no persistence connection, general administration credential, or Coordinator signing key; inherited provider/configuration variables are removed.

The scenario retains an A1 Await across a Coordinator restart and conditional activation of A2, then checks A1/A2/B business markers, reused client keys in separate scopes, tenant isolation, and malformed canonical bytes with legitimate fingerprints. The test writes `execution-evidence/execution-proof.json` only after all assertions pass; source presence or compilation is not acceptance evidence. The runtime suites cover the separate failure windows around completion admission and commit, including lost replies, early callbacks, stale bindings, and timeout races. The packaged scenario tests restart while an interaction is waiting.
