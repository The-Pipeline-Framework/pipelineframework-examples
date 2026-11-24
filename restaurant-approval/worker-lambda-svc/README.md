# Restaurant Lambda REST worker

This module packages the existing Restaurant services as a whole-bundle Lambda
HTTP worker for `aws-durable-java21-rest-worker-v1`. It exposes the existing native
registered REST worker protocol; it does not install an application Coordinator,
application web hosting, a database, a broker or a token service.

The single canonical `config/pipeline.yaml` is v3. Its REST default preserves the
existing generated monolith API. This worker's explicit compiler arguments select
`LOCAL` transport and `COMPUTE` platform for its inner whole-bundle services. Its
emitted schema3 contract is `LOCAL`/`COMPUTE`, module `monolith-svc`, layout
`MONOLITH`, with `localTransitionExecution=true`. The registered, signed REST
worker envelope is supplied by `RestTransitionWorkerResource` and the Lambda HTTP
bridge, rather than a generated `REST`/`FUNCTION` handler. The schema2 Release has
one `AWS_DURABLE_REST_WORKER` entry at
`io.quarkus.amazon.lambda.http.LambdaHttpHandler`, whose package path is
`lib/io.quarkus.quarkus-amazon-lambda-http-3.39.2.jar`.

`AwsDurableHostedProfile.requireRestWorkerRelease` in the pinned Runtime model
validates canonical local execution, the whole-pipeline artifact and that exact
HTTP bridge. Release production executes this gate. This is distinct from the
`aws-durable-java21-in-process-v1` full-host profile, which excludes Await.
Package validation alone does not qualify a serverless Command/Await journey;
this application's retained Await still needs installed integration evidence.

 Canonical values use Java
records and the decision union uses named accepted/declined wrappers. Existing
order fields, UUID/time generation and final decision rules are preserved. The
existing Next.js request/decision JSON and monolith journey stay supported by the
Restaurant-owned compatibility adapter; the worker does not package that adapter
or the UI. Business code and validators remain application-owned.

From the repository root, with JDK21:

```sh
./mvnw -f restaurant-approval/pom.xml -pl monolith-svc,worker-lambda-svc -am \
  -Dtest=RestaurantApprovalAwaitMonolithTest,RestaurantHostedWorkerContractTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Dquarkus.container-image.build=false \
  -Dmaven.repo.local="$PWD/.m2/repository" clean test
./mvnw -f restaurant-approval/pom.xml -pl worker-lambda-svc -am \
  -DskipTests -Dquarkus.container-image.build=false \
  -Dmaven.repo.local="$PWD/.m2/repository" package
```

`target/function.zip` includes the compiler-produced canonical catalog and exact
worker closure. The current compiler can retain duplicate transport sources on
incremental REST rebuilds; the owner verification command deliberately cleans
generated outputs. Never repair or commit generated Java to hide that limitation.

Release production is explicitly enabled at `verify` with
`-Dtpf.release.skip=false -Drestaurant.release.version=<immutable-version>`.
Its producer configuration selects the unchanged worker profile, one Lambda HTTP
entry, and the complete ZIP. Local file URIs are preparation artifacts; they are
not a published/promotable Release.

The optional-execution-canary reader closure is qualified with immutable published
Runtime candidate `26.11.3-pr.130.19945e1766d5` and Contracts candidate
`26.11.3-pr.68.299b6d3f27ae`. Compatibility runs override
`pipelineframework.runtime.version`, `restaurant.worker.host.version`,
`restaurant.worker.model.version`, `restaurant.worker.resolver.version` and
`restaurant.release.plugin.version` with that Runtime candidate, and
`restaurant.contracts.version` with the Contracts candidate. The compiler is
independently pinned to `26.11.3-20261116.153108-2` for this qualification.
The plugin's isolated reader graph is pinned explicitly too; application dependency
management alone does not select the producer's reader.

This flow supplies no static terminal execution canary before the external decision.
The physical ZIP/entry closure and no-canary Release production pass with those
published candidates. Native activation, a successful execution and installed
integration remain separate gates. Older schema2 readers that require a canary
cannot consume this optional-canary contract.

The operator must configure the original immutable Release, exact worker signing-key
reference/version and Coordinator Await-authority binding. The package holds no
Coordinator stores or signing authority for human completion tokens. Generic
scoped human interaction reads/completion require their separately qualified
published Runtime interfaces. External signed callbacks retain their own
original-token contract.

The UI remains application-owned and separately hosted. The first development
journey may use a qualified attended loopback UI and an existing narrowly scoped
server-only credential, reporting its real service actor. It must enforce local
Host/origin/CSRF boundaries and must not expose a privileged public proxy or reuse
control-plane cookies on an application origin. Public UI hosting and
application authentication are separate gates.
