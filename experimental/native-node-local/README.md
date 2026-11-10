# Run a native Node LOCAL pipeline

This experimental application uses the Java compiler's generated TypeScript API and an
installed Node runtime candidate. The runtime calls the three authored functions in one
Node process. Input `3` runs through A, B and C and returns `5`. The runner prints attempt
and terminal observations with the compiler's logical step and type identifiers.

The profile covers pure, linear, unary `int32` records, direct execution on COMPUTE, and
the supported retry, failure, cancellation and invocation context rules. Streaming,
Connectors, durable Await, remote workers and production deployment remain outside this
slice.

## Install and run

Use Node 22.23.0 or later, npm and an authenticated GitHub CLI 2.100 or later. From this directory:

```sh
node fetch-runtime.mjs
cp -R fixtures/project generated/project
npm ci --ignore-scripts --no-audit --no-fund
npm install --no-save --package-lock=false --ignore-scripts --no-audit --no-fund \
  ./generated/runtime/tpf-experimental-node-local-0.0.0.tgz
npm test
npm start -- 3
npm run test:conformance
```

The private runtime package comes from the exact workflow run and head recorded in
[runtime-candidate.json](runtime-candidate.json). The download checks the run identity and
tarball SHA-256 before making the file available to npm. GitHub retains this candidate for
14 days. If it expires, qualify another exact runtime run and update the pin before
installing. Keep an already verified tarball to repeat the installation offline. Once
dependencies and generated files are in place, test, run and debug require only Node and npm.

The [fixture receipts](fixtures/) record the application inputs, compiler commit, compiler
JAR and dependency hashes, profile and output file hashes. These fixtures preserve real
remote generation results for regression testing. Their hashes check byte integrity
against the pinned receipts. They carry no signed Release identity.

## Edit and regenerate

The application inputs are [pipeline.yaml](pipeline.yaml) and
[native-bindings.json](native-bindings.json). Start from a branch of your own Actions-enabled
repository with the pinned
[caller workflow](https://github.com/The-Pipeline-Framework/pipelineframework-compiler/blob/f5c408de97831f4d4a4e6c49bc987ceb418dcd09/.github/workflows/tpf-node-generation.yml)
registered on its default branch as `.github/workflows/tpf-node-generation.yml`. The
reusable workflow runs Java on the build host.

1. Switch the first two steps in the YAML to B, then A. Keep the type chain
   `InputValue → FirstValue → SecondValue → OutputValue` by assigning B the
   `InputValue → FirstValue` edge and A the `FirstValue → SecondValue` edge. Make those
   same binding type changes in `native-bindings.json`.
2. In [src/business.ts](src/business.ts), change `finish` to subtract 4. Commit and push
   the revision to the repository and branch you intend to submit. The compiler consumes
   the committed YAML and bindings. Node consumes the TypeScript function locally.
3. Get the Node generation client at the
   [pinned client revision](https://github.com/The-Pipeline-Framework/pipelineframework-compiler/tree/bef9a0a655d7c083e8e2d6cf94077a647b299186).
   Keep the client and its adjacent scripts together. The reusable compiler workflow stays
   pinned to `9b9172c8b4fd98b664bd14eb9b22f72dddb2b92f`. From the example repository
   root, use a new output directory for this request.

```sh
node /path/to/pinned/compiler/scripts/remote-node-generation.mjs \
  YOUR-OWNER/YOUR-REPOSITORY YOUR-BRANCH "$(git rev-parse HEAD)" \
  9b9172c8b4fd98b664bd14eb9b22f72dddb2b92f \
  experimental/native-node-local/generated/reordered experimental/native-node-local
```

The client checks the pushed revision, build run, compiler pin, inputs and returned file
hashes. When it confirms successful verification, move the old `generated/project` aside
and rename `generated/reordered` to `generated/project`. From the example directory,
run `npm run typecheck` and `npm start -- 3`. The edited pipeline should observe
B → A → C and return `3`. To keep using `npm test` with the edit, update the application
test's expected output and step order to match.

If the build host cannot be reached, check `gh auth status` and the repository's Actions
access. If the client reports a compiler diagnostic, correct the inputs, commit and push,
then submit a fresh request with a new output directory. Missing or stale generated files
fail local verification. A mismatched receipt or output digest calls for a fresh verified
download before replacing the local files.

## Debug a handler

After `npm run typecheck`, launch Node's debugger from the example directory.

```sh
node inspect run.mjs 3
```

Wait for `Break on start`, then enter `sb('business.ts', 15)` and `cont` to stop in
`finish`. Enter `exec value` to inspect the input and `cont` to complete the run.
An IDE can attach to `node --inspect-brk run.mjs 3` and set the same TypeScript breakpoint.

## Compare policy behavior

`npm run test:conformance` reads the
[shared fixed cases](../../research/native-local-projection/runtime-policy/v1/scenarios.json)
used by the Java, Node and Python research probes. It exercises the installed package and
three genuine compiler generations.

| Fixture | Order and policy | Verified generation |
| --- | --- | --- |
| `project` | A → B → C, B gets one retry | [Run 42876799438](https://github.com/The-Pipeline-Framework/pipelineframework-compiler/actions/runs/42876799438) |
| `baseline` | A → B → C, A gets two retries | [Run 42877401995](https://github.com/The-Pipeline-Framework/pipelineframework-compiler/actions/runs/42877401995) |
| `changed-order` | B → A → C | [Run 42877158151](https://github.com/The-Pipeline-Framework/pipelineframework-compiler/actions/runs/42877158151) |

The adapter asserts the shared expected business calls and terminal winners. The generated
Promise also delivers a `CANCELLED` record after any started handler drains. The research
subscriber delivery path suppresses cancellation delivery. At the return and retry
boundaries the package's synchronous observations let the test issue cancellation before
the next attempt. The three legacy ownership inputs exercise rejection of the unsupported
runtime option because the generated application API has no legacy policy switch. Loader
rejections are errors at load time, before any business call. The existing research probes
still run their original delivery and ownership assertions.
