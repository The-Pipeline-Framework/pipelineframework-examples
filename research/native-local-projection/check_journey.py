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


def require(condition, message):
    if not condition:
        raise ValueError(message)


def main():
    generated = ROOT / 'generated'
    provenance = json.loads((ROOT / 'PROVENANCE.json').read_text())
    for name, digest in provenance['artifactSha256'].items():
        require(hashlib.sha256((generated / name).read_bytes()).hexdigest() == digest,
                f'provenance drift: {name}')
    reference_bytes = (generated / 'java-reference.json').read_bytes()
    reference_digest = (generated / 'java-reference.sha256').read_text().split()[0]
    require(hashlib.sha256(reference_bytes).hexdigest() == reference_digest,
            'Java reference digest mismatch')
    reference = json.loads(reference_bytes)
    require(reference.pop('language', None) == 'java', 'expected Java reference language')
    for language, command in [('node', ['node', str(ROOT / 'journey.mjs')]),
                               ('python', [sys.executable, '-B', str(ROOT / 'journey.py')])]:
        result = subprocess.run(command, cwd=ROOT, check=True, capture_output=True,
                                text=True, timeout=30)
        actual = json.loads(result.stdout)
        require(actual.pop('language', None) == language, f'expected {language} report language')
        require(actual == reference, f'{language} disagrees with pinned Java reference')
        print(f'JAVA_REFERENCE_MATCH:{language}:{len(actual["cases"])}-cases:{len(actual["contexts"])}-contexts')


if __name__ == '__main__':
    main()
