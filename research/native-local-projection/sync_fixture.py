"""Copy/check exact compiler outputs. Run the compiler's producer first; this never generates a projection."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent
FILES = ['projection.json', 'models.ts', 'models.py', 'SHA256SUMS',
         'java-reference.json', 'java-reference.sha256']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--compiler-repo', type=Path, required=True)
    parser.add_argument('--check', action='store_true', help='report drift without copying files')
    args = parser.parse_args()
    compiler = args.compiler_repo.resolve()
    source = compiler / 'conformance/polyglot-projection-v0'
    outputs = source / 'generated'
    source_paths = sorted([*source.glob('*.java'), source / 'run.py', source / 'dependencies.pom.xml'],
                          key=lambda p: p.name)
    fingerprint = hashlib.sha256()
    for path in source_paths:
        fingerprint.update(path.name.encode() + b'\0' + path.read_bytes() + b'\0')
    projection = json.loads((outputs / 'projection.json').read_bytes())
    assert fingerprint.hexdigest() == projection['provenance']['generatorTreeSha256'], 'producer sources changed; regenerate first'
    fixtures = {'dslSha256': 'pipeline.yaml', 'bindingsSha256': 'native-bindings.json'}
    for digest_key, name in fixtures.items():
        assert hashlib.sha256((source / 'fixtures' / name).read_bytes()).hexdigest() == projection['provenance'][digest_key]
    assert (source / 'fixtures/scenarios.json').read_bytes() == (ROOT / 'scenarios.json').read_bytes()
    revision = subprocess.check_output(['git', '-C', str(compiler), 'rev-parse', 'HEAD'], text=True).strip()
    status = subprocess.check_output(['git', '-C', str(compiler), 'status', '--porcelain', '--',
                                      'conformance/polyglot-projection-v0'], text=True).strip()
    provenance = {
        'sourceRepository': 'https://github.com/The-Pipeline-Framework/pipelineframework-compiler',
        'sourceRevision': revision,
        'sourceModifiedAtCapture': bool(status),
        'sourceDirectory': 'conformance/polyglot-projection-v0',
        'generatorTreeSha256': fingerprint.hexdigest(),
        'sourceFileSha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_paths},
        'artifactSha256': {name: hashlib.sha256((outputs / name).read_bytes()).hexdigest() for name in FILES},
    }
    destination = ROOT / 'generated'
    destination.mkdir(exist_ok=True)
    for name in FILES:
        if args.check:
            assert (destination / name).read_bytes() == (outputs / name).read_bytes(), f'compiler output drift: {name}'
        else:
            shutil.copyfile(outputs / name, destination / name)
    provenance_file = ROOT / 'PROVENANCE.json'
    if args.check:
        assert json.loads(provenance_file.read_text()) == provenance, 'source provenance changed; recapture exact producer outputs'
    else:
        provenance_file.write_text(json.dumps(provenance, indent=2) + '\n')
    print('FIXTURE_MATCHES_COMPILER' if args.check else 'FIXTURE_COPIED_FROM_COMPILER')


if __name__ == '__main__':
    main()
