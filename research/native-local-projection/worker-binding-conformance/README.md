# Qualify the native worker payload binding

This contributor check exercises the experimental inner payload encoding
`tpf-polyglot-int32-json/1` through packaged Java, Node.js and Python APIs. It
uses one authored three-step pipeline for both native worker generation targets.
Each target produces a coordinator Contract and a Compiled Truth archive. The
check requires identical Contract bytes and the exact generated Contract and
projection bytes in each archive.

The fixture has four distinct logical type IDs along the path
`InputValue → FirstValue → SecondValue → OutputValue`. Each record has the
same required, non-null signed int32 field, so the payload alone cannot identify
its boundary. The probes supply the expected type from the generated Contract
before decoding. They check every wrong known boundary pair, unknown type IDs,
and legacy, research and outer encodings. The binding checks type and encoding
before parsing malformed payload bytes. Java also checks that the
legacy decoder still handles a legacy value and rejects the new encoding.

Valid payloads encode to compact decimal JSON such as `{"value":13}`. Decoding
accepts JSON whitespace and an escaped spelling of the field name. It rejects
decimal or exponent number tokens, overflow, missing or null fields, duplicate
members (including escaped spellings), extra fields, nested values and trailing
JSON. The integer token `-0` decodes as zero and re-encodes as `{"value":0}`.

## Run with pinned packages

Use JDK 21, Node.js 22 with npm, and Python 3.14. From the examples repository,
run the check with a JSON file listing the artifacts and their SHA-256 digests.

```sh
python3.14 -B research/native-local-projection/worker-binding-conformance/run.py \
  /path/to/artifact-pins.json
```

The pins file has four fields.

| Field | Contents |
| --- | --- |
| `compilerClasspath` | Ordered array with the packaged compiler JAR containing `NativeWorkerCompiler`, followed by its Maven dependency JARs. |
| `javaClasspath` | Ordered array with the packaged `org.pipelineframework:pipelineframework` runtime JAR, followed by its Maven dependency JARs, including the candidate runtime-serialization artifact. |
| `nodePackage` | The `@tpf/experimental-node-local` npm tarball containing the new binding. |
| `pythonWheel` | The `tpf-experimental-python-local` wheel containing the new binding. |

Every file entry has a `path` (absolute or relative to the pins file) and a
lowercase, 64-digit `sha256`. Use the dependency classpaths from the exact
compiler and runtime candidate builds. For native artifacts from GitHub Actions,
use the existing consumers' [Node.js downloader](../../../experimental/native-node-local/fetch-runtime.mjs)
and [Python downloader](../../../experimental/native-python-local/fetch_runtime.py)
with the candidate's successful run, source commit and package digest. Local
contributor runs can use packages built by the same owner workflows' Maven,
npm and wheel commands.

The runner verifies every digest, invokes the pinned compiler and installs the
native packages offline into a temporary directory. A complete run reports
`WORKER_METADATA_CONFORMANCE_PASS` and one
`WORKER_PAYLOAD_CONFORMANCE_PASS` for each of Java, Node.js and Python. Keep the
pins and source commit identities with the result to reproduce it. The existing
owner workflows verify native packages and Maven compatibility independently.
Run this shared check with all four candidate inputs together.

## Proof boundary

The gate covers generated metadata, its association with the two Compiled Truth
archives, and the inner payload codec at the four typed boundaries. The earlier
[release intake](../worker-binding-intake/README.md) covers the released carrier
and artifact resolver. The [transition admission suite](../transition-admission/README.md)
covers the existing modeled admission and settlement cases. Installed worker
readiness, authenticated network calls, real QUEUE_ASYNC settlement and recovery
remain acceptance work for [runtime #82](https://github.com/The-Pipeline-Framework/pipelineframework-runtime/issues/82).
