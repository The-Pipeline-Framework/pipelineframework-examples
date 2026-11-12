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
