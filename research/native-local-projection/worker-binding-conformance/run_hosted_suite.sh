#!/usr/bin/env bash
set -euo pipefail

proof_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$proof_dir/../../.." && pwd)
packages=${TPF_NATIVE_HOST_PACKAGES:?Set TPF_NATIVE_HOST_PACKAGES to the verified native candidate directory}
resolved_set=${TPF_RESOLVED_SET:?Set TPF_RESOLVED_SET to the immutable Maven compatibility set}
python=${TPF_PYTHON:-python3.14}
read -r -a maven_args <<< "${MAVEN_ARGS:?MAVEN_ARGS must point to the isolated candidate Maven repository}"

work=$(mktemp -d)
dynamo=
cleanup() {
  if [[ -n "$dynamo" ]]; then docker rm -f "$dynamo" >/dev/null; fi
  rm -rf "$work"
}
trap cleanup EXIT

for component in compiler runtime; do
  version=$("$python" - "$resolved_set" "$component" <<'PY'
import json
from pathlib import Path
import sys

version = json.loads(Path(sys.argv[1]).read_bytes())["components"][sys.argv[2]]["mavenVersion"]
if not version or version.endswith("-SNAPSHOT"):
    raise SystemExit("Expected an immutable Maven candidate version")
print(version)
PY
  )
  "$repo/mvnw" -B "${maven_args[@]}" -f "$proof_dir/hosted-$component.pom.xml" \
    "-Dpipelineframework.$component.version=$version" \
    org.apache.maven.plugins:maven-dependency-plugin:3.9.0:build-classpath \
    "-Dmdep.outputFile=$work/$component.classpath" -DincludeScope=runtime
done

"$python" - "$resolved_set" "$packages" "$work" <<'PY'
from hashlib import file_digest, sha256
import json
import os
from pathlib import Path
import sys

resolved_path, packages, work = map(Path, sys.argv[1:])
resolved = json.loads(resolved_path.read_bytes())
native = json.loads((packages / "native-packages.json").read_bytes())
runtime = resolved["components"]["runtime"]
manifest_path = resolved_path.parent / "manifests/runtime/compatibility-candidate-manifest.json"
manifest_bytes = manifest_path.read_bytes()
if "sha256:" + sha256(manifest_bytes).hexdigest() != runtime["manifestDigest"]:
    raise SystemExit("Runtime Maven manifest did not match the selected compatibility set")
runtime_manifest = json.loads(manifest_bytes)
if (runtime_manifest["sourceSha"] != runtime["sha"] or native["sourceSha"] != runtime["sha"]
        or runtime_manifest["testedSha"] != native["testedSha"]):
    raise SystemExit("Native packages and Maven runtime were not built from the same tested tree")

def pin(path):
    path = path.resolve(strict=True)
    with path.open("rb") as artifact:
        return {"path": str(path), "sha256": file_digest(artifact, "sha256").hexdigest()}

pins = {}
for field, suffix in (("nodePackage", ".tgz"), ("pythonWheel", ".whl")):
    entries = list(packages.glob(f"*{suffix}"))
    if len(entries) != 1:
        raise SystemExit(f"Expected one native archive with suffix {suffix}")
    pins[field] = pin(entries[0])
    if pins[field]["sha256"] != native[field]:
        raise SystemExit(f"Native candidate digest mismatch for {field}")
for field, name in (("compilerClasspath", "compiler"), ("javaClasspath", "runtime")):
    classpath = (work / f"{name}.classpath").read_text().strip().split(os.pathsep)
    pins[field] = [pin(Path(jar)) for jar in classpath]
    expected = resolved["components"][name]["mavenVersion"]
    artifact = "pipelineframework-compiler" if name == "compiler" else "pipelineframework"
    if not any(Path(jar["path"]).name == f"{artifact}-{expected}.jar" for jar in pins[field]):
        raise SystemExit(f"Packaged classpath does not contain {artifact}:{expected}")
runtime_files = {
    file["name"]: file["sha256"]
    for artifact in runtime_manifest["mavenArtifacts"]
    for file in artifact["files"] if file["name"].endswith(".jar")
}
installed = {Path(jar["path"]).name: jar["sha256"] for jar in pins["javaClasspath"]}
for artifact in ("pipelineframework", "pipelineframework-release-producer", "pipelineframework-release-resolver"):
    name = f"{artifact}-{runtime['mavenVersion']}.jar"
    if name not in runtime_files or name not in installed:
        raise SystemExit(f"Runtime candidate classpath is missing {name}")
for name in installed.keys() & runtime_files.keys():
    if installed[name] != runtime_files[name]:
        raise SystemExit(f"Installed runtime artifact digest differs from the candidate manifest: {name}")
(work / "pins.json").write_text(json.dumps(pins, indent=2))
print(json.dumps({"artifactPins": pins, "nativeSource": native,
                  "compilerSource": resolved["components"]["compiler"]["sha"]}), flush=True)
PY

dynamo_image=amazon/dynamodb-local@sha256:d89f8fcc6b1a39cb35976c248ed42a28c66ae00dc043099210f5571e42648ab4
dynamo=$(docker run --rm -d -p 127.0.0.1::8000 "$dynamo_image" \
  -jar DynamoDBLocal.jar -dbPath /tmp -sharedDb)
endpoint="http://$(docker port "$dynamo" 8000/tcp)"
docker inspect --format 'DynamoDB Local image {{.Image}}' "$dynamo"
for ((attempt=0; attempt<30; attempt++)); do
  if curl --silent --max-time 2 --output /dev/null "$endpoint"; then break; fi
  sleep 1
done
"$python" -B "$proof_dir/run_hosted_worker.py" "$work/pins.json" "$endpoint"
