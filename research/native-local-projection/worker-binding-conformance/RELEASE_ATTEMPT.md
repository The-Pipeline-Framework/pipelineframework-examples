# Qualify a native Release and attempt binding

This contributor check prepares a schema-1 Pipeline Release in Java, pins a
typed whole-bundle transition command, then runs the verified application in
a separate Node.js or Python process. Both processes exercise the same
compiler-generated three-step pipeline and packaged native kernels. The
authored functions receive values 3, 4 and 8 in order, each with the pinned
execution and trace IDs. The final payload is `OutputValue` with `{"value":5}`
under `tpf-polyglot-int32-json/1`.

## Run with pinned artifacts

Use JDK 21, Node.js 22.23 or later and Python 3.14. From the examples
repository, run:

```sh
python3.14 -B research/native-local-projection/worker-binding-conformance/run_release_attempt.py \
  /path/to/artifact-pins.json
```

Use the [same artifact pins format as the payload check](README.md). The
`javaClasspath` must include the runtime JAR containing
`NativeWorkerReleaseBinding`, the Release resolver and producer JARs, and
their Maven dependencies. Supply the compiler with native worker generation
and the npm tarball and wheel with the expected-projection runtime option.
The runner checks SHA-256 before using any of the artifacts. Keep the pins,
candidate source revisions and stdout together. Each language prints a JSON
receipt with Release, Contract, artifact and projection identities, rejection
diagnostics and observed handler calls, then prints
`NATIVE_WORKER_RELEASE_ATTEMPT_PASS`.
The descriptor digest includes the temporary local artifact URIs and is
specific to that run.

## What the check exercises

- The compiler emits the real application, models, Contract and Compiled
  Truth for each language. Both Contracts have identical bytes and each
  language has its own Release versions. Node uses the
  interpreter's TypeScript stripping for the generated modules and authored
  functions. Python copies the generated files and authored functions byte
  for byte and preserves their existing imports.
- The runnable contains these modules and all the importable files from the
  pinned kernel tarball or wheel. The existing Release producer packages the
  runnable archive and binds it to the original compiler carrier. The
  deployment target supplies the trusted descriptor and runnable digests,
  scoped worker identity, coordinator Contract, and separate outer, payload,
  execution and failed-result profiles.
- Java exercises descriptor replacement under old pins, changed business,
  kernel, carrier and prepared-tree bytes, wrong tenant, pipeline, Contract,
  Release, range, type, profile and attempt identity. An old command is
  rejected by the replacement Release binding while its original pin still
  accepts it.
- The repository-owned bootstrap uses the prepared tree as its import source.
  The process runs again from a directory containing shadow application and
  runtime modules with attempted ambient preload and search path settings.
  The launcher supplies an absolute interpreter, a restricted environment
  and, for Python, isolated startup without site packages. The functions and
  output are identical to the first run. Both launchers report their imported
  file locations relative to the verified tree and check for an ambient-load
  marker.
- An independent compiler invocation with the same logical Contract emits a
  different projection and a matching embedded hash. Its generated
  application passes ordinary native load. With the exact expected bytes
  supplied from verified Compiled Truth, the packaged native factory rejects
  the different projection with `TRUSTED_PROJECTION_MISMATCH` and zero
  business calls.

The proof trusts the interpreter, its standard library, the bootstrap and
the host's control of the prepared tree between verification and execution.
This is contributor evidence for Release and command binding. Deployment
isolation, request authentication, active lease fencing, durable settlement
and recovery remain acceptance work for
[runtime #82](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/issues/82).
