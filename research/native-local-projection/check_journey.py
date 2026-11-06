"""Contributor integration check: pinned Java reference equals actual Node/Python reports.

The subprocesses here launch tests; neither native execution shell launches a subprocess.
The Java report is explicit research evidence, not an on-demand JVM dependency.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent


def main():
    generated = ROOT / 'generated'
    provenance = json.loads((ROOT / 'PROVENANCE.json').read_text())
    for name, digest in provenance['artifactSha256'].items():
        assert hashlib.sha256((generated / name).read_bytes()).hexdigest() == digest, f'provenance drift: {name}'
    reference_bytes = (generated / 'java-reference.json').read_bytes()
    reference_digest = (generated / 'java-reference.sha256').read_text().split()[0]
    assert hashlib.sha256(reference_bytes).hexdigest() == reference_digest
    reference = json.loads(reference_bytes)
    assert reference.pop('language') == 'java'
    for language, command in [('node', ['node', str(ROOT / 'journey.mjs')]),
                               ('python', [sys.executable, '-B', str(ROOT / 'journey.py')])]:
        result = subprocess.run(command, cwd=ROOT, check=True, capture_output=True,
                                text=True, timeout=30)
        actual = json.loads(result.stdout)
        assert actual.pop('language') == language
        assert actual == reference, f'{language} disagrees with pinned Java reference'
        print(f'JAVA_REFERENCE_MATCH:{language}:{len(actual["cases"])}-cases:{len(actual["contexts"])}-contexts')


if __name__ == '__main__':
    main()
