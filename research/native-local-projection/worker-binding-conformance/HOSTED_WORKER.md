# Run the packaged Node and Python workers over HTTPS

This contributor check runs the same three-step, four-record compiler
fixture in separate Node.js and Python HTTPS worker processes. The installed
Java runtime admits the typed input through the hosted admission resource,
passes the enqueued work item through the ordinary `QUEUE_ASYNC` coordinator,
and reads the terminal state from DynamoDB Local. The authored functions
are the same functions used by the native LOCAL examples.

## Run

Use JDK 21, Node.js 22.23 or later, Python 3.14, OpenSSL 3, and a disposable
DynamoDB Local instance with its database files enabled. Bind the instance
to loopback. The probe creates and deletes uniquely named tables.

```sh
docker run --rm -p 127.0.0.1:8000:8000 \
  amazon/dynamodb-local@sha256:d89f8fcc6b1a39cb35976c248ed42a28c66ae00dc043099210f5571e42648ab4 \
  -jar DynamoDBLocal.jar -dbPath /tmp -sharedDb
```

From the examples repository, use the [artifact pins format for the
Release check](RELEASE_ATTEMPT.md). The Maven runtime JAR must contain the
hosted typed-admission path. Include its Maven dependency JARs and the
Release producer and resolver JARs in `javaClasspath`. Use the packaged
Node worker tarball and Python worker wheel from the same runtime candidate.

```sh
python3.14 -B research/native-local-projection/worker-binding-conformance/run_hosted_worker.py \
  /path/to/artifact-pins.json http://127.0.0.1:8000
```

Retain the pins, candidate source revisions and stdout together. Each
language reports the verified Release receipts, negative checks, durable
terminal states and observed business calls, followed by
`NATIVE_HOSTED_WORKER_PASS`.

In a coordinated compatibility set that includes the runtime and examples
candidates, this repository's suite manifest requests the same proof.
The coordinator packages both native workers from the tested runtime tree
and passes the archived packages and selected Maven repository to the
examples suite. The suite verifies the runtime candidate manifest and
native receipts, then resolves the Java classpaths from the immutable
coordinates. For a compiler built in the set, the suite verifies the
selected compiler manifest, source SHA and installed JAR digest. For a
compiler retained from the baseline, the coordinator verifies the JAR
before archiving the Maven repository and the suite records the local
JAR digest. An archived set can be rerun through the same entry point:

```sh
TPF_RESOLVED_SET=/path/to/inputs/resolved-set.json \
TPF_NATIVE_HOST_PACKAGES=/path/to/inputs/native-host-packages \
MAVEN_ARGS=-Dmaven.repo.local=/path/to/extracted/candidate-repository \
  bash research/native-local-projection/worker-binding-conformance/run_hosted_suite.sh
```

## Evidence

- The compiler emits identical Contracts and language-specific Compiled
  Truth. The existing Release producer and verifier bind the packaged
  runnable and exact projection before launching each worker.
- The Java client uses ordinary certificate and hostname verification
  with an ephemeral fixture CA. Its hosted admission rejects an untrusted
  CA, wrong hostname and worker capability response with a mismatched
  runnable digest. It also rejects a separately prepared coordinator whose
  Compiled Truth differs while its Contract, Release and runnable digest
  match the real worker. The HTTPS capability response identifies the
  projection validated at worker startup through
  `X-TPF-Worker-Projection-Digest`; the ordinary capability JSON stays unchanged.
  The workers reject invalid HMAC, expired timestamps,
  repeated nonces, wrong Release or range, and wrong type, encoding or
  int32 payload. The test observes zero business calls for those
  rejections and confirms that replaying a valid signed POST adds no calls.
- The coordinator persists the original typed input and settles successful
  output plus each portable terminal failure. The result endpoint returns
  the original typed output, and a fresh state-store instance reads the
  same terminal record. Duplicate deliveries create no additional business
  calls, retries or dead letters. The observed two attempts at step B for
  transient exhaustion come from the admitted native step policy and
  fixture-injected faults in the handler wrapper.
- After stopping the worker process, a fresh submission fails readiness.
  The original admission receipt remains available for both identical
  replay and read-only inquiry.

The fixture wires installed Java runtime objects directly and captures
queue deliveries and dead letters in the probe. The worker HTTPS requests
and DynamoDB transactions use real network connections. This check covers
the local coordinator-to-worker path. Hosted REST deployment, external queue
infrastructure, deployed worker failure recovery and HA remain separate
acceptance work under
[runtime #82](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/issues/82).

## Packaged coordinator ingress and recovery suite

Use the same immutable inputs and command above with `TPF_PACKAGED_INGRESS=1`
(or pass `--packaged-ingress` to `run_hosted_suite.sh`) and the AWS CLI on
PATH. The runner creates test tables in its disposable DynamoDB Local
container. It builds a Quarkus fast JAR with the generated Contract, a CDI
producer for the prepared Release binding, and an external observation hook on
the generic Java transition dispatch counter (`tpf.orchestrator.transition.dispatched.total`),
then verifies the embedded runtime JAR against the candidate digest. Before
running the native cases, an ordinary Java positive control confirms the
external counter increments on generic Java dispatch; every native case asserts
a zero increase across restarts and binding failures.

For each worker language, the runner exercises:

- **Direct-HTTPS ingress and terminal retention:** registers the Release over
  HTTP, checks that denied admission leaves no execution or receipt, verifies
  the typed result and worker execution/trace IDs, kills the coordinator after
  settlement, and with the worker offline retrieves the original Release,
  receipt, and result and exercises same-key replay and conflict.
- **Coordinator death after admission before intent:** holds the committed
  admission transaction acknowledgement and fences execution claims before they
  reach DynamoDB, confirms `QUEUED` v0 with zero intent rows and zero worker
  calls, sends `SIGKILL`, fences and discards the dead process's proxy, and
  starts a fresh coordinator whose ordinary sweep claims the retained row,
  persists dispatch intent before the single execute POST, and settles the
  typed result.
- **Coordinator death after intent:** verifies the committed `RUNNING` v1 lease
  and immutable dispatch intent before forwarding the single execute POST,
  captures and holds the valid worker completion after all three handler calls,
  sends `SIGKILL`, discards the response channel, and restarts a fresh
  coordinator whose ordinary lease-expiry sweep settles `REMOTE_OUTCOME_UNKNOWN`
  (v2, attempt 0, intent preserved, no typed result, zero resend or DLQ).
- **Stale settlement authority:** keeps the coordinator alive while holding
  lease renewals and the valid worker completion until ordinary sweep commits
  `REMOTE_OUTCOME_UNKNOWN`, releases the worker completion first to observe the
  late settlement fail its DynamoDB conditional write, and only then releases
  the renewal gate.
- **Worker death before and after business entry:** either withholds the
  execute request body after verified TLS handshake before `SIGKILL` (proving
  zero business calls before worker restart) or pauses after a flushed
  `normalize(3)` witness before `apply` and sends `SIGKILL`, verifying
  `REMOTE_OUTCOME_UNKNOWN` with zero resend or DLQ in both cases.
- **Dropped response, corrupted active response, and generic execute 503:**
  captures a valid worker completion and drops the response or replaces it on
  the original channel with invalid JSON or an incompatible typed output, or
  short-circuits a plain HTTP `503` after durable intent without forwarding,
  verifying `REMOTE_OUTCOME_UNKNOWN` with zero resend or DLQ.
- **Duplicate CDI delivery and duplicate worker request:** holds the committed
  admission acknowledgement so the ordinary sweep claim races the immediate
  event claim unchanged against DynamoDB (exact one successful claim, one
  conditional failure, one execute POST, and known `SUCCEEDED`), then replays
  the captured signed execute POST and nonce directly against the running
  worker (HTTP `401`, zero additional handler calls, settled result unchanged).
- **Client disconnect before receipt and coordinator death after settlement:**
  drops the submitting client socket after durable receipt commit and
  reconnects from a separate zero-JVM client process (`client.mjs` or
  `client.py`) using the same key without creating a second dispatch;
  separately holds the committed terminal `SUCCEEDED` acknowledgement until
  `SIGKILL` and verifies the original Release, receipt, and typed result
  survive restart with the worker offline.
- **Wrong Release, missing/replaced binding, and stale worker isolation:**
  verifies direct signed commands carrying a wrong Release fail before business
  entry (`400`, zero rows/calls), and verifies that restarting over a retained
  unclaimed execution with a missing binding, a replacement Release binding, or
  a stale worker follows the pre-intent failure oracle (`WAIT_RETRY` v2/a1 to
  `FAILED` v4/a1, zero dispatch intent, zero worker calls, zero Java fallback,
  original receipt retained).
- **Ingress validation, readiness rejections, portable failures, and distinct
  keys:** verifies missing/wrong bearer tokens (`401`), unregistered scopes
  (`404`), malformed/invalid inputs (`400`), oversized >16 KiB paired payloads
  (`413`), and offline/wrong-artifact/wrong-projection/untrusted-CA/wrong-hostname
  workers (`503`) leave zero durable rows and zero calls; verifies
  `TRANSIENT_EXHAUSTED`, `BUSINESS_REJECTION`, and `UNCLASSIFIED` portable
  failures (`FAILED` v2/a0, step index `1`, closed diagnostics, exact worker
  call counts) plus independent executions for distinct keys under
  `SERVER_KEY_ONLY` and `409` conflict on changed input or Release.

### Standalone zero-JVM client exercise

Against any running packaged coordinator (no local JVM or inbound listener
required on the client), submit, look up a receipt, or wait for terminal
status and typed result with either Node.js or Python:

```sh
node research/native-local-projection/worker-binding-conformance/client.mjs \
  submit http://127.0.0.1:<port> <token> <tenant> <pipeline> <clientKey> \
  <contractVersion> <releaseVersion> <inputType> 3
node research/native-local-projection/worker-binding-conformance/client.mjs \
  lookup http://127.0.0.1:<port> <token> <tenant> <pipeline> <clientKey>
node research/native-local-projection/worker-binding-conformance/client.mjs \
  wait http://127.0.0.1:<port> <token> <tenant> <executionId> 30
```

Replace `node .../client.mjs` with `python3.14 -B .../client.py` for the
equivalent Python client. Cloud deployment, external durable brokers, Await,
streaming, Connectors, plugins, and HA remain separate work under
[runtime #82](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/issues/82).
