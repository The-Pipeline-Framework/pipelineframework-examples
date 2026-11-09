"""Focused guard tests: optimized Python must not turn mismatches into certificates."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent


class EvidenceTools(unittest.TestCase):
    def run_tool(self, script, *arguments):
        return subprocess.run([sys.executable, '-B', '-O', str(script), *map(str, arguments)],
                              text=True, capture_output=True, timeout=30)

    def require_success(self, result, marker):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(marker, result.stdout)

    def require_mismatch(self, result, message, success_marker):
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(message, result.stderr)
        self.assertNotIn(success_marker, result.stdout)

    def test_optimized_sync_rejects_artifact_and_provenance_drift(self):
        with tempfile.TemporaryDirectory(prefix='projection-sync-') as temporary:
            root = Path(temporary)
            shutil.copyfile(ROOT / 'sync_fixture.py', root / 'sync_fixture.py')
            shutil.copyfile(ROOT / 'scenarios.json', root / 'scenarios.json')
            compiler = root / 'compiler'
            source = compiler / 'conformance/polyglot-projection-v0'
            fixtures = source / 'fixtures'
            fixtures.mkdir(parents=True)
            outputs = source / 'generated'
            shutil.copytree(ROOT / 'generated', outputs)
            # Synthetic producer bytes isolate this checker's comparisons; no compiler
            # execution or generation qualification is claimed by this tool regression.
            for name in ['Probe.java', 'run.py', 'dependencies.pom.xml']:
                (source / name).write_text(f'test source: {name}\n')
            fingerprint = hashlib.sha256()
            for path in sorted([*source.glob('*.java'), source / 'run.py', source / 'dependencies.pom.xml'], key=lambda p: p.name):
                fingerprint.update(path.name.encode() + b'\0' + path.read_bytes() + b'\0')
            (fixtures / 'pipeline.yaml').write_text('synthetic DSL bytes\n')
            (fixtures / 'native-bindings.json').write_text('{}\n')
            shutil.copyfile(root / 'scenarios.json', fixtures / 'scenarios.json')
            projection = json.loads((outputs / 'projection.json').read_text())
            projection['provenance'].update(
                generatorTreeSha256=fingerprint.hexdigest(),
                dslSha256=hashlib.sha256((fixtures / 'pipeline.yaml').read_bytes()).hexdigest(),
                bindingsSha256=hashlib.sha256((fixtures / 'native-bindings.json').read_bytes()).hexdigest())
            (outputs / 'projection.json').write_text(json.dumps(projection))
            # Only Git metadata is substituted. All source, fixture, output and provenance
            # comparisons execute the real tool under -O, including its normal CLI parser.
            launcher = (
                'import runpy, subprocess, sys\n'
                'def git_metadata(command, **kwargs):\n'
                '    if command[-1] == "HEAD": return "test-revision\\n"\n'
                '    if command[-2:] != ["status", "--porcelain"]: raise RuntimeError("status must cover checkout")\n'
                '    return ""\n'
                'subprocess.check_output = git_metadata\n'
                'runpy.run_path(sys.argv.pop(1), run_name="__main__")\n')

            def sync(check=False):
                return subprocess.run([sys.executable, '-B', '-O', '-c', launcher,
                                       str(root / 'sync_fixture.py'), '--compiler-repo', str(compiler),
                                       *(['--check'] if check else [])],
                                      text=True, capture_output=True, timeout=30)

            self.require_success(sync(), 'FIXTURE_COPIED_FROM_COMPILER')
            self.require_success(sync(True), 'FIXTURE_MATCHES_COMPILER')
            artifact = root / 'generated/projection.json'
            original = artifact.read_bytes()
            artifact.write_bytes(original + b' ')
            self.require_mismatch(sync(True), 'compiler output drift: projection.json', 'FIXTURE_MATCHES_COMPILER')
            artifact.write_bytes(original)
            provenance_file = root / 'PROVENANCE.json'
            provenance = json.loads(provenance_file.read_text())
            provenance['sourceRevision'] = 'different-revision'
            provenance_file.write_text(json.dumps(provenance))
            self.require_mismatch(sync(True), 'source provenance changed', 'FIXTURE_MATCHES_COMPILER')

    def test_optimized_reference_comparison_rejects_wrong_expected_result(self):
        with tempfile.TemporaryDirectory(prefix='projection-reference-') as temporary:
            root = Path(temporary)
            for name in ['check_journey.py', 'PROVENANCE.json', 'scenarios.json', 'business.ts',
                         'business.py', 'journey.mjs', 'journey.py', 'local-shell.mjs', 'local_shell.py']:
                shutil.copyfile(ROOT / name, root / name)
            generated = root / 'generated'
            shutil.copytree(ROOT / 'generated', generated)
            self.require_success(self.run_tool(root / 'check_journey.py'), 'JAVA_REFERENCE_MATCH:python:7-cases:2-contexts')
            reference_file = generated / 'java-reference.json'
            reference = json.loads(reference_file.read_text())
            reference['cases'][0]['result']['value'] = 99
            reference_file.write_text(json.dumps(reference))
            reference_hash = hashlib.sha256(reference_file.read_bytes()).hexdigest()
            (generated / 'java-reference.sha256').write_text(reference_hash + '  java-reference.json\n')
            # Update integrity pins deliberately so the test reaches semantic report comparison.
            provenance_file = root / 'PROVENANCE.json'
            provenance = json.loads(provenance_file.read_text())
            for name in ['java-reference.json', 'java-reference.sha256']:
                provenance['artifactSha256'][name] = hashlib.sha256((generated / name).read_bytes()).hexdigest()
            provenance_file.write_text(json.dumps(provenance))
            self.require_mismatch(self.run_tool(root / 'check_journey.py'),
                                  'node disagrees with pinned Java reference', 'JAVA_REFERENCE_MATCH')


if __name__ == '__main__':
    unittest.main()
