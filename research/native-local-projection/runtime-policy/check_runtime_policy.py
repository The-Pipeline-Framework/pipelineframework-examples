"""Compare the released Java unary execution seam with the current native probes."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
FIXTURE = HERE.parent
REPO = HERE.parents[2]
TARGET = REPO / 'target/native-local-runtime-policy'
LOCAL_REPOSITORY = REPO / '.m2/repository'
BASELINE = FIXTURE / 'generated/projection.json'
SCENARIOS = FIXTURE / 'scenarios.json'
ALTERNATE = HERE / 'downstream-retry-projection.json'
EDGES = HERE / 'edge-scenarios.json'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_json(path):
    return json.loads(path.read_bytes())


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def checked_fixtures():
    provenance = read_json(HERE / 'PROVENANCE.json')
    expected_baseline = provenance['baselineProjectionSha256']
    require(digest(BASELINE) == expected_baseline, 'baseline projection drift')
    require(read_json(FIXTURE / 'PROVENANCE.json')['artifactSha256']['projection.json'] == expected_baseline,
            'baseline projection inventories disagree')
    require(digest(SCENARIOS) == provenance['baselineScenariosSha256'], 'baseline scenario drift')
    require(digest(EDGES) == provenance['edgeScenariosSha256'], 'runtime policy edge scenario drift')
    edge_ids = [row['id'] for row in read_json(EDGES)['cases']]
    require(len(edge_ids) == 5 and set(edge_ids) == {
        'zero-B-retry', 'one-B-retry', 'unknown-A', 'recovery-A', 'null-C',
    }, 'missing or duplicate runtime policy edge scenarios')
    coordinates = [(row['group'], row['artifact'], row['version']) for row in provenance['releasedArtifacts']]
    require(len(coordinates) == 3 and set(coordinates) == {
        ('org.pipelineframework', 'pipelineframework', '26.11.1'),
        ('org.pipelineframework', 'pipelineframework-runtime-core', '26.11.1'),
        ('io.smallrye.reactive', 'mutiny', '3.3.0'),
    }, 'unexpected released runtime artifact inventory')
    retry = provenance['downstreamRetry']
    require(digest(ALTERNATE) == retry['projectionSha256'], 'B retry projection drift')
    require(digest(HERE / 'downstream-retry-bindings.json') == retry['bindingsSha256'], 'B retry bindings drift')

    # The alternate is a genuine compiler artifact with exactly one changed policy.
    baseline = read_json(BASELINE)
    alternate = read_json(ALTERNATE)
    require(alternate['provenance']['generatorTreeSha256'] == retry['generatorTreeSha256'],
            'unexpected B retry generator')
    require(alternate['provenance']['bindingsSha256'] == retry['bindingsSha256'], 'projection binding mismatch')
    require(alternate['steps'][1]['id'] == 'B' and alternate['steps'][1]['retryLimit'] == 1
            and baseline['steps'][1]['id'] == 'B' and baseline['steps'][1]['retryLimit'] == 0, 'unexpected B retry budget')
    alternate['steps'][1]['retryLimit'] = 0
    alternate['provenance']['bindingsSha256'] = baseline['provenance']['bindingsSha256']
    require(alternate == baseline, 'B retry projection differs beyond its B retry binding')
    return provenance


def classpath(work, expected):
    output = work / 'classpath.txt'
    subprocess.run([
        str(REPO / 'mvnw'), '-f', str(HERE / 'dependencies.pom.xml'), '--batch-mode', '--no-transfer-progress',
        'org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath',
        f'-Dmaven.repo.local={LOCAL_REPOSITORY}', f'-Dmdep.outputFile={output}',
    ], cwd=REPO, check=True, timeout=240)
    paths = [Path(entry) for entry in output.read_text().strip().split(os.pathsep)]
    require(all(path.resolve().is_relative_to(LOCAL_REPOSITORY.resolve()) for path in paths),
            'classpath entry outside worktree Maven repository')
    require(all(not path.parent.name.endswith('-SNAPSHOT') for path in paths), 'snapshot dependency on classpath')
    for row in expected:
        artifact = row['artifact']
        version = row['version']
        jar_name = f'{artifact}-{version}.jar'
        expected_path = LOCAL_REPOSITORY.joinpath(*row['group'].split('.'), artifact, version, jar_name)
        require([path for path in paths if path.name == jar_name] == [expected_path],
                f'expected exactly one released {artifact} on the classpath')
        require(digest(expected_path) == row['sha256'], f'released {artifact} digest mismatch')
    return os.pathsep.join(map(str, paths))


def run_java(work, provenance):
    jars = classpath(work, provenance['releasedArtifacts'])
    classes = work / 'classes'
    classes.mkdir()
    subprocess.run(['javac', '--release', '21', '-proc:none', '-cp', jars, '-d', str(classes),
                    str(HERE / 'ReleasedRuntimeProbe.java')], cwd=REPO, check=True, timeout=30)
    report = work / 'java-runtime-report.json'
    subprocess.run(['java', '-cp', os.pathsep.join([str(classes), jars]), 'org.pipelineframework.ReleasedRuntimeProbe',
                    str(BASELINE), str(SCENARIOS), str(ALTERNATE), str(EDGES), str(report)],
                   cwd=REPO, check=True, timeout=30)
    return report


def checked_report(actual, expected, label):
    require(actual == expected, f'{label} mismatch\nexpected {expected}\nobserved {actual}')


def native_report(command):
    completed = subprocess.run(command, cwd=FIXTURE, check=True, stdout=subprocess.PIPE, text=True, timeout=30)
    return json.loads(completed.stdout)


def compare(report, provenance):
    scenarios = read_json(SCENARIOS)
    cases = [{'id': row['id'], **row['expected']} for row in scenarios['cases']]
    contexts = [{
        'executionId': row['executionId'], 'traceId': row['traceId'], 'result': row['expected'],
        'observations': [{'step': step, 'executionId': row['executionId'], 'traceId': row['traceId']} for step in 'ABC'],
    } for row in scenarios['contexts']]
    require(len(cases) == 7 and len(contexts) == 2, 'baseline comparison corpus size changed')
    shared = {
        'profile': 'tpf-polyglot-probe/0',
        'projectionSha256': provenance['baselineProjectionSha256'],
        'scenariosSha256': provenance['baselineScenariosSha256'],
        'cases': cases,
        'contexts': contexts,
    }
    signals = {
        'pre-cancelled': 'ADMISSION_SKIPPED_SUBSCRIPTION',
        'cancel-after-A': 'SUBSCRIBER_CANCELLED_WHILE_A_VALUE_HELD',
        'uncancelled-A-barrier': 'TWO_SUBSCRIPTIONS_COMPLETED_AFTER_RELEASE',
        'subscriber-context': {'directMutiny': True, 'insideReleasedSteps': [False, False, False]},
    }
    edges = read_json(EDGES)['cases']
    checked_report(report, {
        'language': 'java-released-runtime', **shared, 'signals': signals,
        'downstreamProjectionSha256': provenance['downstreamRetry']['projectionSha256'],
        'edges': {row['id']: row['java'] for row in edges},
    }, 'released Java')
    for language, journey, edge_probe in [
        ('node', ['node', 'journey.mjs'], ['node', str(HERE / 'native-edges.mjs')]),
        ('python', [sys.executable, '-B', 'journey.py'], [sys.executable, '-B', str(HERE / 'native_edges.py')]),
    ]:
        checked_report(native_report(journey), {'language': language, **shared}, f'{language} baseline')
        checked_report(native_report(edge_probe), {row['id']: row['native'] for row in edges}, f'{language} edges')
        print(f'RELEASED_RUNTIME_COMPARISON_PASS:{language}:7 cases, 2 overlap runs, {len(edges)} policy edges')


def main():
    provenance = checked_fixtures()
    TARGET.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix='run-', dir=TARGET))
    report = run_java(work, provenance)
    compare(read_json(report), provenance)
    print(f'Released Java observations: {report}')


if __name__ == '__main__':
    main()
