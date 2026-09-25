# Repository instructions

This repository owns learning examples and architectural proofs for The Pipeline Framework. Examples are
executable compatibility surfaces, not framework implementations and not libraries for other repositories to
depend upon.

Keep every example dependent on released TPF compiler, runtime, contract, connector, Block, and Expansion
artifacts. Do not restore source dependencies on the framework monorepo. Application-specific support modules
may live beside the example that owns them, but must not masquerade as public ecosystem artifacts.

Real applications, reference implementations, ecosystem connectors, packaged Blocks, Expansion distributions,
and runtime-owned Spring smoke tests do not belong here.

## Cross-repository system tests

Owner-local verification is the first gate. `.github/tpf-system-tests.json` owns the stable example and Restaurant
HA suite commands. `TPF Candidate Build` and the trusted publisher create an immutable, source-only candidate
manifest; `tpf/system-tests` records compatibility evidence on that exact source SHA.

For a coordinated change, wait for `TPF Candidate Publish` to succeed for the current head SHA of every
participating pull request. Then run `TPF System Tests — Compatibility Set` in
`The-Pipeline-Framework/pipelineframework` with one stable set ID and 2–10 pull-request URLs. Any new commit
invalidates the previous set: wait for its new candidate publisher and dispatch again. Do not substitute snapshots,
branch heads, source checkouts or a composite Maven reactor. See the canonical
[cross-repository system-test runbook](https://github.com/The-Pipeline-Framework/pipelineframework/blob/main/docs/evolve/cross-repository-system-tests.md).

Repository setup requires repository-scoped dispatch credentials. If the workflow exposes them as
`SYSTEM_TEST_APP_ID` and `SYSTEM_TEST_APP_PRIVATE_KEY`, they must belong to a dispatch-only App installed solely on
`pipelineframework`, never the coordinator App. This source-only publisher does not need Maven package authority;
fork publication additionally requires the
`safe-to-system-test` label. Never expose dispatch or status credentials to owner-suite jobs.

Always use an isolated Maven local repository:

```sh
./mvnw <goals> -Dmaven.repo.local="$PWD/.m2/repository"
```

Do not introduce Maven profiles. These examples are not published; `central-publishing` is therefore not needed.
There must be one canonical Maven reactor and lifecycle.

Before changing an example, preserve the compiler/runtime behavior it proves and keep its focused integration
test green. Do not use one example as a shared library for an unrelated example unless the dependency is itself
the architectural behavior under test.
