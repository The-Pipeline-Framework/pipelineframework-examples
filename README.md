# The Pipeline Framework Examples

This repository contains learning examples and architectural proofs for
[The Pipeline Framework](https://pipelineframework.org/). They are executable compatibility surfaces for
released compiler, runtime, connector, Block, and Expansion artifacts; they do not publish framework artifacts
of their own.

## Catalogue

- `callable-loop-proof` — an imported Block that owns an ordinary callable loop
- `graphql-block-proof` — a GraphQL Block plus application-owned connector bindings
- `local-command-proof` — replay-safe local Command execution
- `openapi-capability-proof` — imported OpenAPI capabilities and callback jobs
- `rag-composition-proof` — RAG composition across embedding, vector-store, and LLM connectors
- `restaurant-approval` — a larger multi-module approval pipeline reference
- `stdio-object-demo` — object admission and publication over a CLI boundary

The GraphQL proof keeps its application-specific connectors under
`graphql-block-proof/connectors`; they are part of the proof, not ecosystem connector releases.

Real applications and long-lived reference implementations intentionally live elsewhere:

- `csv-payments` and `rag-turnkey` are independently owned applications.
- Checkout/TPFGo, Search, and QuickBooks Collections Briefing are reference implementations.
- Spring smoke tests belong to the standalone runtime repository that owns Spring behavior.

## Build

The examples consume released `26.9.4-SNAPSHOT` TPF artifacts from Maven Central's snapshot repository.

```sh
./mvnw clean verify -U -Dgpg.skip -Dmaven.repo.local="$PWD/.m2/repository"
```

Override the `pipelineframework.*.version` properties only when deliberately testing another compatible released
artifact set.
