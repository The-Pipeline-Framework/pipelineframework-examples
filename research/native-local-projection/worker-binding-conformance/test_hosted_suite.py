import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class HostedSuiteProvenanceTest(unittest.TestCase):
    def setUp(self):
        fixture = tempfile.TemporaryDirectory()
        self.addCleanup(fixture.cleanup)
        self.root = Path(fixture.name)
        source = Path(__file__).resolve()
        proof = self.root / "research/native-local-projection/worker-binding-conformance"
        proof.mkdir(parents=True)
        self.runner = proof / "run_hosted_suite.sh"
        shutil.copyfile(source.with_name(self.runner.name), self.runner)

        maven = self.root / "mvnw"
        maven.write_text("""#!/bin/bash
for argument do
  case "$argument" in -Dmdep.outputFile=*) output="${argument#*=}";; esac
done
cp "$FIXTURE_ROOT/${output##*/}" "$output"
""")
        maven.chmod(0o755)
        docker = self.root / "docker"
        docker.write_text("#!/bin/bash\ntouch \"$FIXTURE_ROOT/docker-called\"\nexit 90\n")
        docker.chmod(0o755)

        self.components = {}
        self.compiler_jar = self.root / "pipelineframework-compiler-candidate.jar"
        self.compiler_manifest = self.manifest("compiler")
        self.add_component("compiler", ("pipelineframework-compiler",))
        self.add_component("runtime", ("pipelineframework", "pipelineframework-release-producer",
                                       "pipelineframework-release-resolver"))

        native = {"sourceSha": "runtime-source", "testedSha": "runtime-tested"}
        for extension, key in (("tgz", "nodePackage"), ("whl", "pythonWheel")):
            archive = self.root / f"fixture.{extension}"
            archive.write_text(extension)
            native[key] = self.digest(archive)
        (self.root / "native-packages.json").write_text(json.dumps(native))

    @staticmethod
    def digest(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def manifest(self, component):
        return self.root / "manifests" / component / "compatibility-candidate-manifest.json"

    def add_component(self, component, artifacts):
        jars = [self.root / f"{artifact}-candidate.jar" for artifact in artifacts]
        for jar in jars:
            jar.write_text(jar.name)
        (self.root / f"{component}.classpath").write_text(os.pathsep.join(map(str, jars)))
        manifest = self.manifest(component)
        manifest.parent.mkdir(parents=True)
        manifest.write_text(json.dumps({
            "sourceSha": f"{component}-source",
            "testedSha": f"{component}-tested",
            "mavenArtifacts": [
                {"files": [{"name": jar.name, "sha256": self.digest(jar)}]} for jar in jars
            ],
        }))
        self.components[component] = {
            "sha": f"{component}-source",
            "mavenVersion": "candidate",
            "manifestDigest": "sha256:" + self.digest(manifest),
        }

    def invoke(self, *, rebuilt_compiler=True, rejection=None):
        candidates = ["compiler", "runtime"] if rebuilt_compiler else ["runtime"]
        resolved = self.root / "resolved-set.json"
        resolved.write_text(json.dumps({
            "components": self.components,
            "candidates": [{"component": component} for component in candidates],
        }))
        result = subprocess.run(["bash", self.runner], env=os.environ | {
            "FIXTURE_ROOT": str(self.root),
            "MAVEN_ARGS": f"-Dmaven.repo.local={self.root}",
            "PATH": f"{self.root}{os.pathsep}{os.environ['PATH']}",
            "TPF_NATIVE_HOST_PACKAGES": str(self.root),
            "TPF_PYTHON": sys.executable,
            "TPF_RESOLVED_SET": str(resolved),
        }, capture_output=True, text=True)
        if rejection is None:
            self.assertEqual(90, result.returncode, result.stderr)
            self.assertTrue((self.root / "docker-called").is_file())
            self.assertEqual("compiler-source", json.loads(result.stdout)["compilerSource"])
        else:
            self.assertEqual(1, result.returncode, result.stderr)
            self.assertIn(rejection, result.stderr)
            self.assertFalse((self.root / "docker-called").exists())

    def test_selected_compiler_candidate_reaches_docker(self):
        self.invoke()

    def test_retained_baseline_without_compatibility_manifest_reaches_docker(self):
        self.compiler_manifest.unlink()
        self.invoke(rebuilt_compiler=False)

    def test_tampered_selected_compiler_jar_stops_before_docker(self):
        self.compiler_jar.write_text("tampered")
        self.invoke(rejection="Installed compiler artifact digest differs")

    def test_altered_selected_compiler_manifest_stops_before_docker(self):
        self.compiler_manifest.write_text("{}")
        self.invoke(rejection="compiler Maven manifest did not match")

    def test_selected_compiler_manifest_wrong_source_stops_before_docker(self):
        manifest = json.loads(self.compiler_manifest.read_bytes())
        manifest["sourceSha"] = "wrong"
        self.compiler_manifest.write_text(json.dumps(manifest))
        self.components["compiler"]["manifestDigest"] = "sha256:" + self.digest(self.compiler_manifest)
        self.invoke(rejection="compiler Maven manifest source did not match")

    def test_missing_selected_compiler_manifest_stops_before_docker(self):
        self.compiler_manifest.unlink()
        self.invoke(rejection="FileNotFoundError")
