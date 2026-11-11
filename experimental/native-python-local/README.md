# Run a native Python LOCAL pipeline

This example uses the Java compiler's generated Python API and an installed experimental
Python runtime wheel. Input `3` runs through A, B and C in one Python process and returns
`5`. The runner prints attempts with the compiler's logical step and type IDs and the
terminal outcome.

The `tpf-polyglot-unary-policy/1` profile covers three pure, linear, unary steps with
distinct `int32` record types, direct COMPUTE execution, and the admitted retry, failure,
cancellation and context rules. Streaming, Connectors, durable Await, and distributed
execution are outside this experimental profile.

## Install and run

Use CPython 3.14.10 on Linux for the qualification baseline. Use an authenticated GitHub
CLI 2.100 or newer to retrieve the runtime wheel. From this directory:

```sh
python -m venv .venv
. .venv/bin/activate
python -m fetch_runtime
python -m pip install --no-index --no-deps generated/runtime/*.whl
python -m pip install -r requirements-test.txt
cp -R fixtures/project/generated generated/project
python -m verify_project
python tests/check_types.py
python -m unittest discover -s tests -v
python -m run 3
```

The download checks the [pinned runtime candidate](runtime-candidate.json) against the
workflow run, source commit, artifact checksum and wheel SHA-256. GitHub retains runtime
candidates for 14 days. Keep a verified wheel for repeat installation, or use a newly
qualified exact workflow run and wheel digest if the artifact expires. Once installed,
the application, tests and debugger run offline with Python alone. The test requirements
include mypy for the positive and negative handler type checks.

The checked-in regression outputs were produced locally by the pinned Java compiler.
Their compiler, dependency, input and output digests are recorded in
[fixture provenance](fixtures/provenance.json). A live remote generation yields a
verified generation receipt with its exact committed inputs and artifact identities.
These hashes and receipts verify integrity and provenance for the experiment and
carry no signed Release identity.

## Edit and regenerate

1. Copy the example to your own Actions-enabled repository. Register the
   [compiler's caller recipe](https://github.com/The-Pipeline-Framework/pipelineframework-compiler/blob/6c8f905f55ace22fad1bc5ccdb61fc17a27728e3/README.md#set-up-the-generation-workflow)
   as `.github/workflows/tpf-node-generation.yml` on the default branch and on the
   branch you submit. Pin the reusable workflow to
   `6c8f905f55ace22fad1bc5ccdb61fc17a27728e3` and forward its `target` input.
2. In [pipeline.yaml](pipeline.yaml), switch the first two steps to B, then A. Assign B
   `InputValue → FirstValue` and A `FirstValue → SecondValue`. Make the same type
   changes in [native-bindings.json](native-bindings.json) and the handler signatures
   and result constructors in [business.py](business.py). Change `finish` to subtract
   4. Commit and push the project revision.
3. Check out the compiler at the same pinned commit to obtain its Python generation
   client and adjacent target manifest. Run from your application repository root,
   passing the repository-relative project directory. For a project at the root, use
   `.` and write to `generated/reordered`.

```sh
project=experimental/native-python-local
mkdir -p "$project/generated"
python /path/to/pinned/compiler/scripts/remote_node_generation.py \
  OWNER/REPOSITORY BRANCH "$(git rev-parse HEAD)" \
  6c8f905f55ace22fad1bc5ccdb61fc17a27728e3 "$project/generated/reordered" "$project" --target python
```

The client submits the committed YAML and binding files to the build host. It checks the
requested revision, compiler pin, source inputs and returned file hashes before
installing into the new output directory. Java and Node run on the remote build host.

After verified retrieval, move any old `generated/project` aside and rename
`generated/reordered` to `generated/project`. From the example directory run
`python -m verify_project`, `python -m mypy` and `python -m run 3`. The edit should
observe B → A → C and return `3`. Update the application test's expected result and
step order for your edited example. The conformance tests use their fixed generated
fixtures and fixed test functions.

If dispatch fails, check `gh auth status`, Actions access and caller registration.
Correct compiler diagnostics in the named source files, commit and push the changes,
then submit to a new output directory. The runner rejects missing, stale or mismatched
generation before importing the generated application. This source integrity check assumes
you trust the local workspace and its Python caches.

## Debug a handler

```sh
python -m pdb -m run 3
```

At the debugger prompt, enter `b business.py:16`, then `c`. Enter `p value` in
`finish` to inspect the input, then `c` to finish. The original handler receives
`8`. The reordered handler receives `7`.

## Compare policy behavior

The tests use the unchanged [51 shared policy scenarios](../../research/native-local-projection/runtime-policy/v1/scenarios.json)
and [five native edge scenarios](../../research/native-local-projection/runtime-policy/edge-scenarios.json).
They invoke three compiler generations through the installed wheel. The baseline gives
A two retries and B none, the project gives B one retry, and the changed-order fixture
runs B → A → C.

The adapter verifies handler calls, logical types, attempts, context snapshots and
terminal observations. An attached generated awaitable returns the accepted
`CANCELLED` record after started handler work drains. The original research subscriber
suppresses cancellation delivery. The three legacy ownership rows check that the
runtime factory rejects the option before execution. The older unknown-failure edge
expects `UNKNOWN_FAILURE`. The adapter checks the current profile's
`UNKNOWN_FAILURE_CODE` and translates that label for the older comparison. The shared
case files and original research probes retain their expectations.
