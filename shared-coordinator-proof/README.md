# Shared Coordinator packaged proof

This proof runs two independently compiled applications through a shared Coordinator. Its four tests cover classpath isolation, shared archive inspection, production package bootstrap, and execution through a Coordinator restart.

## Packaged topology

- `application-a1` and `application-a2`: the same expense pipeline identity and canonical namespace, independently compiled artifacts. Business results distinguish A1 from A2. An expense request becomes a pending approval; a pure, request-aware completion projector joins an approval reply to the retained pending request; the final step produces a receipt.
- `application-b`: an unrelated parcel pipeline with four ordinary steps, different Java types and incompatible canonical definitions. `Prepare` and `Finish` deliberately overlap A's authored names.
- `coordinator`: framework runtime dependency only. It has no Maven dependency on any application fixture, generated application metadata or test classes.
- `proof-tests`: launches each worker from its Lambda ZIP and the Coordinator from its Quarkus JVM package. The local HTTP bridge turns loopback requests into Lambda HTTP events and invokes the packaged Quarkus stream handler. It does not put application artifacts on its own test classpath.

The released annotation compiler owns generated Await adapters, step definitions, ordering and canonical contracts. The test reads the generated artifacts; no fixture duplicates the compiler's step order.

## Scaffold verification

From the repository root:

```sh
./mvnw -B -Dmaven.repo.local="$PWD/.m2/repository" \
  -pl shared-coordinator-proof/proof-tests -am clean verify
```

`PackagedScaffoldIsolationIT` records the exact child-process classpaths, compiled contracts, artifact and contract byte digests under `proof-tests/target/scaffold-evidence`. Each child uses a fresh process. The Coordinator must fail to load every class in any fixture JAR, including generated adapters and explicit markers; it must not resolve application descriptors or marker resources. This is stronger than replacing only a thread context class loader while application classes remain reachable from a parent loader.

Use the repository's active component versions or the exact coordinated candidates for integration. Local preview coordinates provide local evidence.

## Production package bootstrap

The expense and parcel modules build `target/function.zip` and generate their worker-only Release descriptors with the Release Maven plugin. The Coordinator builds a separate `target/quarkus-app`. `PackagedBootstrapIT` checks unsigned and signed capability calls through the Lambda event bridge. It records the physical package paths and SHA-256 digests under `proof-tests/target/bootstrap-evidence`.

`PackagedSharedArchivesIT` inspects the shared Action and Await lifecycle Lambda ZIPs, including their nested libraries, for fixture classes and compiled pipeline metadata. The runtime tests cover IAM signing and alias verification with controlled AWS responses. The loopback runner supplies the Lambda event envelope and exercises the TPF signature and authority checks. Live IAM and provider replacement acceptance need a deployed proof.

## Execution and restart gate

`PackagedExecutionRecoveryIT` runs all four production packages against a disposable local persistent store, with no application types on the Coordinator classpath. It requires compatible R2 runtime packaging and the R1 compiler and contracts. Physical canonical boundary metadata comes from the compiled applications. The test is not disabled when a container runtime is unavailable: missing required persistence fails this gate.

`ProofReleaseSetup` is fixture-only deployment setup. It validates descriptors against exact compiled artifact bytes, records original immutable entry bindings through the existing registry, and uses conditional activation operations. It adds no production management endpoint. Application execution and completion use production control-plane and worker REST paths. Worker subprocess configuration contains no persistence connection, general administration credential, or Coordinator signing key; inherited provider/configuration variables are removed.

The scenario retains an A1 Await across a Coordinator restart and conditional activation of A2, then checks A1/A2/B business markers, reused client keys in separate scopes, tenant isolation, and malformed canonical bytes with legitimate fingerprints. The test writes `execution-evidence/execution-proof.json` only after all assertions pass; source presence or compilation is not acceptance evidence. The runtime suites cover the separate failure windows around completion admission and commit, including lost replies, early callbacks, stale bindings, and timeout races. The packaged scenario tests restart while an interaction is waiting.
