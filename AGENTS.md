# Repository instructions

This repository owns learning examples and architectural proofs for The Pipeline Framework. Examples are
executable compatibility surfaces, not framework implementations and not libraries for other repositories to
depend upon.

Keep every example dependent on released TPF compiler, runtime, contract, connector, Block, and Expansion
artifacts. Do not restore source dependencies on the framework monorepo. Application-specific support modules
may live beside the example that owns them, but must not masquerade as public ecosystem artifacts.

Real applications, reference implementations, ecosystem connectors, packaged Blocks, Expansion distributions,
and runtime-owned Spring smoke tests do not belong here.

Always use an isolated Maven local repository:

```sh
./mvnw <goals> -Dmaven.repo.local="$PWD/.m2/repository"
```

Do not introduce Maven profiles. These examples are not published; `central-publishing` is therefore not needed.
There must be one canonical Maven reactor and lifecycle.

Before changing an example, preserve the compiler/runtime behavior it proves and keep its focused integration
test green. Do not use one example as a shared library for an unrelated example unless the dependency is itself
the architectural behavior under test.
