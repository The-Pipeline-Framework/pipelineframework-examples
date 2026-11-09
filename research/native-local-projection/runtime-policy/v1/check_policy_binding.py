"""Run the live binding against fixed expectations, with optional JVM-free native verification."""
import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from check_runtime_policy import FIXTURE, REPO, TARGET, classpath, digest, read_json, require

PROFILE = 'tpf-polyglot-unary-policy/1'
ARTIFACTS = {'baseline', 'changed-order', 'downstream-retry'}
ARTIFACT_FILES = {'SHA256SUMS', 'models.py', 'models.ts', 'projection.json'}
CASES = set("""
success changed-order b-budget-zero b-budget-one exhaust-preserves-failure
business-rejection raw-host-exception wire-unclassified unknown-code malformed-failure
missing-failure-field lying-eligibility spoofed-owner spoofed-step unclassified-unsanitised
raw-null implicit-return raw-record empty missing-value nonnull-null invalid-record out-of-range-record
legacy-retry legacy-recovery null-ownership null-input
unsupported-profile unsupported-owner unsupported-capability invalid-budget unsupported-type
cancel-prestart cancel-handoff handoff-released cancel-pending-retry pending-retry-released
cancel-inflight-success cancel-inflight-failure cancel-after-b-start
complete-before-cancel failure-before-cancel invalid-before-cancel
context-missing context-absent context-blank context-type context-authority context-non-ascii
overlap-left overlap-right
""".split())


def checked_artifacts():
    provenance = read_json(HERE / 'PROVENANCE.json')
    require(set(provenance['artifacts']) == ARTIFACTS, 'missing or unexpected compiler fixtures')
    require({path.name for path in (HERE / 'generated').iterdir()} == ARTIFACTS,
            'generated fixture inventory drift')
    projections, digests = {}, {}
    for name, source in provenance['artifacts'].items():
        folder = HERE / 'generated' / name
        require(set(source['sha256']) == ARTIFACT_FILES
                and {path.name for path in folder.iterdir()} == ARTIFACT_FILES, f'{name} inventory drift')
        for filename, expected in source['sha256'].items():
            require(digest(folder / filename) == expected, f'{name}/{filename} provenance drift')
        sums = dict(line.split('  ')[::-1] for line in (folder / 'SHA256SUMS').read_text().splitlines())
        require(sums == {key: value for key, value in source['sha256'].items() if key != 'SHA256SUMS'},
                f'{name} compiler and example digest inventories disagree')
        for model in ('models.ts', 'models.py'):
            legacy = FIXTURE / 'generated' / model
            # The producer emits the same registry fields in projection order.
            matches = (sorted((folder / model).read_bytes().splitlines()) == sorted(legacy.read_bytes().splitlines())
                       if name == 'changed-order' else sums[model] == digest(legacy))
            require(matches, f'{name}/{model} no longer matches the authored business model')
        projection = read_json(folder / 'projection.json')
        pinned = projection['provenance']
        require(projection['profile'] == PROFILE
                and pinned['generatorTreeSha256'] == provenance['generatorTreeSha256']
                and pinned['dslSha256'] == provenance['inputFileSha256'][source['dsl']]
                and pinned['bindingsSha256'] == provenance['inputFileSha256'][source['bindings']],
                f'{name} compiler source pin mismatch')
        digests[name] = sums['projection.json']
        projections[name] = projection

    baseline = projections['baseline']
    reordered = deepcopy(baseline)
    reordered['steps'][0], reordered['steps'][1] = reordered['steps'][1], reordered['steps'][0]
    reordered['provenance']['dslSha256'] = projections['changed-order']['provenance']['dslSha256']
    require(projections['changed-order'] == reordered
            and reordered['provenance']['dslSha256'] != baseline['provenance']['dslSha256'],
            'reordered artifact did not change only its DSL order and provenance')
    retry = deepcopy(baseline)
    require(retry['steps'][1]['id'] == 'B' and retry['steps'][1]['retryLimit'] == 0,
            'expected zero baseline B retry budget')
    retry['steps'][1]['retryLimit'] = 1
    retry['provenance']['bindingsSha256'] = projections['downstream-retry']['provenance']['bindingsSha256']
    require(projections['downstream-retry'] == retry
            and retry['provenance']['bindingsSha256'] != baseline['provenance']['bindingsSha256'],
            'alternate artifact did not change only its B budget and provenance')
    return digests


def expectations():
    corpus = read_json(HERE / 'scenarios.json')
    require(corpus['profile'] == PROFILE and corpus['schemaVersion'] == 1, 'invalid live case corpus')
    ids = [row['id'] for row in corpus['cases']]
    require(len(ids) == len(CASES) and set(ids) == CASES, 'missing, duplicate or unexpected live cases')
    result = {}
    for row in corpus['cases']:
        terminal = row['expected']['terminal']
        context = row.get('context', {'executionId': row['id'], 'traceId': f"trace-{row['id']}"})
        result[row['id']] = {
            'id': row['id'],
            'calls': [{'step': step, 'input': value, **context, 'mutationBlocked': True}
                      for step, value in row['expected']['calls']],
            'terminal': terminal,
            'observations': 1,
            'delivery': [] if terminal['kind'] == 'CANCELLED' else [terminal['kind']],
        }
    return result


def check_report(report, language, digests, expected):
    require(report['language'] == language and report['profile'] == PROFILE, f'{language} reported the wrong profile')
    require(report['artifactSha256'] == digests, f'{language} did not execute the pinned compiler projections')
    rows = report['cases']
    require([row['id'] for row in rows] == list(expected), f'{language} live case inventory differs')
    for row in rows:
        wanted = expected[row['id']]
        # JSON booleans are not counts or int32 values, even though Python equates True and 1.
        require(json.dumps(row, sort_keys=True) == json.dumps(wanted, sort_keys=True),
                f"{language}/{row['id']} mismatch\nexpected {wanted}\nobserved {row}")
    print(f'POLICY_BINDING_PASS:{language}:{len(rows)} live cases', flush=True)


def run_native(command):
    completed = subprocess.run(command, cwd=FIXTURE, check=True, stdout=subprocess.PIPE, text=True, timeout=30)
    return json.loads(completed.stdout)


def run_java():
    TARGET.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix='policy-v1-', dir=TARGET))
    released = read_json(HERE.parent / 'PROVENANCE.json')['releasedArtifacts']
    jars = classpath(work, released)
    classes = work / 'classes'
    classes.mkdir()
    subprocess.run(['javac', '--release', '21', '-proc:none', '-cp', jars, '-d', str(classes),
                    str(HERE / 'PolicyBinding.java'), str(HERE / 'ReleasedPolicyProbe.java')],
                   cwd=REPO, check=True, timeout=30)
    report = work / 'java-runtime-report.json'
    subprocess.run(['java', '-cp', os.pathsep.join([str(classes), jars]), 'org.pipelineframework.ReleasedPolicyProbe',
                    str(HERE / 'generated'), str(HERE / 'scenarios.json'), str(report)],
                   cwd=REPO, check=True, timeout=45)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--native-only', action='store_true', help='run only Node and Python without a local JVM')
    args = parser.parse_args()
    digests, expected = checked_artifacts(), expectations()
    for language, command in [
        ('node', ['node', str(HERE / 'native.mjs')]),
        ('python', [sys.executable, '-B', str(HERE / 'native.py')]),
    ]:
        check_report(run_native(command), language, digests, expected)
    if not args.native_only:
        report = run_java()
        check_report(read_json(report), 'java-released-runtime', digests, expected)
        print(f'Released Java observations: {report}')


if __name__ == '__main__':
    main()
