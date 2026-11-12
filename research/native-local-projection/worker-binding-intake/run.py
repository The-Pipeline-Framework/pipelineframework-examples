"""Exercise the worker carriers and schema-1 release closure with released Java artifacts."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
POLICY = HERE.parent / 'runtime-policy'
sys.path.insert(0, str(POLICY))
from check_runtime_policy import REPO, classpath, digest, require


def released_classpath(work):
    policy = json.loads((POLICY / 'PROVENANCE.json').read_bytes())
    expected = [*policy['releasedArtifacts'],
                {'group': 'org.pipelineframework', 'artifact': 'pipelineframework-runtime-protocol',
                 'version': '26.11.1', 'sha256': '38026a66d07d17d4a9ec218cda8a26dc2944313519172be79b9c900b3e1e996e'},
                {'group': 'org.pipelineframework', 'artifact': 'pipelineframework-runtime-serialization',
                 'version': '26.11.1', 'sha256': '876a75e8e87caa30970783657da6435258fe1c3eca30a68a0b0c3a2d2cc34558'}]
    return classpath(work, expected)


def checked_fixture(generated, inventory, extension):
    for filename in ('projection.json', f'application.{extension}', f'models.{extension}'):
        require(digest(generated / filename) == inventory[filename], f'Generated fixture drift: {generated / filename}')
    return generated


def main():
    node = REPO / 'experimental/native-node-local'
    node_baseline = node / 'fixtures/baseline'
    node_inventory = json.loads((node_baseline / 'generation-receipt.json').read_bytes())['files']
    python = REPO / 'experimental/native-python-local'
    python_inventory = json.loads((python / 'fixtures/provenance.json').read_bytes())['fixtures']['baseline']['files']
    fixtures = (
        ('node', 'ts', checked_fixture(node_baseline, node_inventory, 'ts'), node / 'src/business.ts'),
        ('python', 'py', checked_fixture(python / 'fixtures/baseline/generated', python_inventory, 'py'),
         python / 'business.py'),
    )

    with tempfile.TemporaryDirectory(prefix='tpf-worker-binding-intake-') as directory:
        work = Path(directory)
        jars = released_classpath(work)
        classes = work / 'classes'
        classes.mkdir()
        compiled = subprocess.run(['javac', '--release', '21', '-proc:none', '-cp', jars, '-d', str(classes),
                                   str(HERE / 'WorkerBindingIntakeProbe.java')],
                                  cwd=REPO, capture_output=True, text=True, timeout=30)
        require(compiled.returncode == 0, f'Java probe compilation failed\n{compiled.stdout}{compiled.stderr}')
        for language, extension, generated, business in fixtures:
            fixture_work = work / language
            fixture_work.mkdir()
            result = subprocess.run([
                'java', '-cp', os.pathsep.join((str(classes), jars)), 'WorkerBindingIntakeProbe',
                language, extension, str(generated), str(business), str(fixture_work),
            ], cwd=REPO, capture_output=True, text=True, timeout=30)
            marker = f'WORKER_BINDING_INTAKE_PASS:{language}'
            require(result.returncode == 0 and result.stdout.splitlines()[-1:] == [marker],
                    f'No completed {language} probe report\n{result.stdout}{result.stderr}')
            print(result.stdout, end='', flush=True)


if __name__ == '__main__':
    main()
