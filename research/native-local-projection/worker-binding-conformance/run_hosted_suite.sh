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
  if [[ "$component" == runtime ]]; then runtime_version=$version; fi
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
def selected_manifest(component):
    selection = resolved["components"][component]
    manifest_path = resolved_path.parent / "manifests" / component / "compatibility-candidate-manifest.json"
    manifest_bytes = manifest_path.read_bytes()
    if "sha256:" + sha256(manifest_bytes).hexdigest() != selection["manifestDigest"]:
        raise SystemExit(f"{component} Maven manifest did not match the selected compatibility set")
    manifest = json.loads(manifest_bytes)
    if manifest["sourceSha"] != selection["sha"]:
        raise SystemExit(f"{component} Maven manifest source did not match the selected compatibility set")
    return manifest

# The bootstrap verifies retained baseline JARs before archiving the Maven repository.
# Compatibility manifests in the shard inputs cover components rebuilt for this set.
compiler_rebuilt = any(candidate["component"] == "compiler" for candidate in resolved["candidates"])
compiler_manifest = selected_manifest("compiler") if compiler_rebuilt else None
runtime_manifest = selected_manifest("runtime")
if (native["sourceSha"] != runtime_manifest["sourceSha"]
        or native["testedSha"] != runtime_manifest["testedSha"]):
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
for field, component, manifest, required in (
    ("compilerClasspath", "compiler", compiler_manifest, ("pipelineframework-compiler",)),
    ("javaClasspath", "runtime", runtime_manifest,
     ("pipelineframework", "pipelineframework-release-producer", "pipelineframework-release-resolver")),
):
    classpath = (work / f"{component}.classpath").read_text().strip().split(os.pathsep)
    pins[field] = [pin(Path(jar)) for jar in classpath]
    manifest_files = {
        file["name"]: file["sha256"]
        for artifact in manifest["mavenArtifacts"]
        for file in artifact["files"] if file["name"].endswith(".jar")
    } if manifest is not None else {}
    expected = resolved["components"][component]["mavenVersion"]
    installed_names = {Path(jar["path"]).name for jar in pins[field]}
    for artifact in required:
        name = f"{artifact}-{expected}.jar"
        if name not in installed_names:
            raise SystemExit(f"{component} classpath is missing {name}")
        if manifest is not None and name not in manifest_files:
            raise SystemExit(f"{component} candidate manifest is missing {name}")
    for jar in pins[field]:
        name = Path(jar["path"]).name
        if name in manifest_files and jar["sha256"] != manifest_files[name]:
            raise SystemExit(f"Installed {component} artifact digest differs from the candidate manifest: {name}")
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
if [[ "${TPF_PACKAGED_INGRESS:-0}" == 1 ]]; then
  "$python" -B "$proof_dir/run_packaged_ingress.py" "$work/pins.json" "$endpoint" \
    "$runtime_version" "$repo/mvnw" "${maven_args[@]}"
else
  "$python" -B "$proof_dir/run_hosted_worker.py" "$work/pins.json" "$endpoint"
fi
