"""Check fixed transition transcripts in each language, then challenge their oracles."""
import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
FIXTURE = HERE.parent
GENERATED = FIXTURE / 'runtime-policy/v1/generated'
sys.path.insert(0, str(FIXTURE / 'runtime-policy'))
from check_runtime_policy import REPO, classpath

RELEASED_ARTIFACTS = [
    {'group': 'org.pipelineframework', 'artifact': 'pipelineframework-runtime-protocol', 'version': '26.11.1',
     'sha256': '38026a66d07d17d4a9ec218cda8a26dc2944313519172be79b9c900b3e1e996e'},
    {'group': 'org.pipelineframework', 'artifact': 'pipelineframework-runtime-serialization', 'version': '26.11.1',
     'sha256': '876a75e8e87caa30970783657da6435258fe1c3eca30a68a0b0c3a2d2cc34558'},
]
CASES = set("""
verified-worker tampered expired future bad-timestamp missing-signature invalid-envelope
wrong-tenant wrong-release wrong-contract wrong-pipeline wrong-range legacy-payload wrong-type
authority-field no-release invalid-payload unknown-profile missing-artifact-pin wrong-artifact-pin
unverified-protocol bare-json-outer outer-inner-encoding substituted-projection nonce-authentication-order
nonce-consumed-before-envelope-parse unknown-outcome-and-authorized-redrive normal-settlement
incompatible-worker-result blank-execution blank-trace non-ascii-context targeted-replay
wrong-claimed-execution one-redrive-only stale-redrive-claim
""".split())


def command_for_java(work, corpus):
    jars = classpath(work, RELEASED_ARTIFACTS)
    classes = work / 'classes'
    classes.mkdir()
    subprocess.run(['javac', '--release', '21', '-proc:none', '-cp', jars, '-d', str(classes),
                    str(HERE / 'TransitionAdmissionProbe.java')], cwd=REPO, check=True, timeout=30)
    return ['java', '-cp', os.pathsep.join((str(classes), jars)), 'TransitionAdmissionProbe',
            str(corpus), str(GENERATED)]


def cases_by_id(document):
    cases = {row['id']: row for row in document['cases']}
    if len(cases) != len(document['cases']) or cases.keys() != CASES:
        raise ValueError('missing, duplicate or unexpected admission transcript')
    return cases


def corruptions(document):
    changed = deepcopy(document)
    changed['frames']['original']['body'] += ' '
    yield 'changed-signed-bytes', 'verified-worker', changed

    changed = deepcopy(document)
    cases_by_id(changed)['unknown-profile']['expected']['payloadReads'] = 1
    yield 'unexpected-decoder-call', 'unknown-profile', changed

    changed = deepcopy(document)
    case = cases_by_id(changed)['unknown-outcome-and-authorized-redrive']
    independent = next(event for event in case['events']
                       if event.get('authority') == 'independent-observer'
                       and event.get('disposition') == 'ORIGINAL_ATTEMPT_INACTIVE')
    independent['authority'] = 'worker-advertisement'
    yield 'remove-disposition-authority', case['id'], changed

    changed = deepcopy(document)
    case = cases_by_id(changed)['unknown-outcome-and-authorized-redrive']
    stale = next(event for event in case['events'] if event['op'] == 'complete')
    stale['frame'] = 'redrive'
    yield 'settle-current-instead-of-stale', case['id'], changed


def expect_pass(command, language):
    result = subprocess.run(command, cwd=FIXTURE, capture_output=True, text=True, timeout=30)
    marker = f'TRANSITION_ADMISSION_PASS:{language}:{len(CASES)}'
    if result.returncode != 0 or result.stdout.strip() != marker:
        raise ValueError(f'{language} failed its admission corpus\n{result.stdout}\n{result.stderr}')
    print(marker, flush=True)


def expect_mismatch(command, language, name, case_id):
    result = subprocess.run(command, cwd=FIXTURE, capture_output=True, text=True, timeout=30)
    marker = f'TRANSITION_ADMISSION_MISMATCH:{case_id}'
    if result.returncode != 1 or not result.stderr.splitlines() or result.stderr.splitlines()[0] != marker:
        raise ValueError(f'{language} failed to reject {name} at {case_id}\n{result.stdout}\n{result.stderr}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--native-only', action='store_true', help='skip the released Java comparison')
    options = parser.parse_args()
    corpus = HERE / 'vectors.json'
    document = json.loads(corpus.read_bytes())
    cases_by_id(document)
    with tempfile.TemporaryDirectory(prefix='tpf-transition-admission-') as directory:
        work = Path(directory)
        commands = {
            'node': lambda path: ['node', str(HERE / 'probe.mjs'), str(path)],
            'python': lambda path: [sys.executable, '-B', str(HERE / 'probe.py'), str(path)],
        }
        if not options.native_only:
            java = command_for_java(work, corpus)
            commands['java'] = lambda path: [*java[:-2], str(path), java[-1]]
        for language, command in commands.items():
            expect_pass(command(corpus), language)
        checks = 0
        for name, case_id, changed in corruptions(document):
            candidate = work / (name + '.json')
            candidate.write_text(json.dumps(changed), encoding='utf-8')
            for language, command in commands.items():
                expect_mismatch(command(candidate), language, name, case_id)
                checks += 1
        print(f'TRANSITION_ADMISSION_CORRUPTION_PASS:{checks}')


if __name__ == '__main__':
    main()
