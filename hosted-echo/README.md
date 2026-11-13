# Hosted Echo with Await

Echo accepts a typed value, records the connector's acknowledgement, and
waits for its typed completion. The native Command/Await contract uses a
signed resume token. The example keeps the original `com.example.echo`
pipeline identity and uses the existing private AWS Durable FUNCTION layout.

## Build and verify two local Releases

Run this from the examples repository root with JDK 25. Both variants build
from this source. The second build changes the projector result so retention
checks can tell which package processed a completion.

```sh
set -e
example="$PWD/hosted-echo"
repository="$PWD/.m2/repository"
out=$(mktemp -d "${TMPDIR:-/tmp}/hosted-echo.XXXXXX")
./mvnw -B -pl hosted-echo/provider -am install -Dmaven.repo.local="$repository"

for variant in A B; do
  mkdir "$out/$variant"
  cp -R "$example/app/pom.xml" "$example/app/pipeline.yaml" "$example/app/src" "$out/$variant/"
done
projector=src/main/java/com/example/echo/application/EchoProjector.java
sed 's/release-A:/release-B:/' "$out/A/$projector" > "$out/B/$projector"
./mvnw -B -f "$out/A/pom.xml" clean package dependency:build-classpath \
  -Dmaven.repo.local="$repository" -Dmdep.outputFile="$out/classpath"
./mvnw -B -f "$out/B/pom.xml" clean package -Dmaven.repo.local="$repository"
java -cp "$(cat "$out/classpath")" "$example/VerifyPair.java" "$out"
```

The verifier writes the two original Release Descriptors and resolves the
DURABLE and ACTION ZIPs using TPF 26.11.2. It verifies all ten entry bindings,
requires compiled connector metadata and exact authority for
`store → fixture.echo:1`, and loads each completion projector from its own
package. Given a matching acknowledgement and completion, the results are
`release-A:hello` and `release-B:hello`. A mismatched completion is rejected.
The Command effect key stays stable on retry and differs across executions,
including when the A and B canaries echo the same value.

## Existing Hosted environment

Use this example as a subsequent release only in an existing Echo
environment qualified for the private FUNCTION layout, the Command effect
binding and the approved connector. The IN_PROCESS smoke layout in
TPF 26.11.2 supports only internal steps without Await.

The local descriptors use file artifact URIs. The synthetic provider
acknowledges the Command, uses a reserved example callback address, and
rejects incoming HTTP callback authentication. A live run needs an approved
provider receiver and completion sender that retain the original signed token
and deliver the typed completion through the stack's mapped Await queue.
Publish the verified artifacts through the environment's approved release
repository when that route is available. This example performs no cloud
provisioning.
