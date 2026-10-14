"""Archive/template controls only; no AWS execution evidence."""
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).parents[1]
SPEC = importlib.util.spec_from_file_location('transition_fixture', ROOT / 'scripts/transition-fault-fixture.py')
FIXTURE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIXTURE)
DEPLOYMENT = Path(os.environ.get('TPF_AWS_DURABLE_DEPLOYMENT_JAR',
    str(ROOT / 'production-tests/target/conformance-dependencies/deployment.jar')))

def production_template():
    with zipfile.ZipFile(DEPLOYMENT) as archive:
        return archive.read('aws-durable-queue-async.yaml')

class TransitionFaultFixtureTest(unittest.TestCase):
    def test_derivation_keeps_production_worker_and_alarm_contracts(self):
        source = production_template()
        derived = FIXTURE.derived_template(source)
        self.assertIn(b'CompletionObservationOutstandingAlarm:', derived)
        self.assertIn(b'TPF_CONFORMANCE_TRANSITION_FAULT_TABLE: !Ref ConformanceTransitionFaultTable', derived)
        self.assertIn(b'Roles: [!Ref WorkerRole]', derived)
        self.assertIn(b'Resource: !GetAtt ConformanceTransitionFaultTable.Arn', derived)
        self.assertIn(b'"transitionVersionArn":"${TransitionWorkerVersion}"', derived)
        self.assertIn(b'QUARKUS_LAMBDA_HANDLER: !Ref WorkHandler', derived)
        self.assertNotIn(b'conformance-transition-delivery', source)
        with self.assertRaises(ValueError): FIXTURE.derived_template(source.replace(b'Outputs:', b'OutputsMissing:'))

    def test_archive_requires_discovered_wrapper_and_actual_production_delegate(self):
        names = [FIXTURE.WRAPPER + '.class', FIXTURE.WRAPPER + '_Bean.class', FIXTURE.DELEGATE.replace('.', '/') + '.class', FIXTURE.DELEGATE.replace('.', '/') + '_Bean.class']
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / 'function.zip'
            source = production_template()
            derived = FIXTURE.derived_template(source)
            for missing in [None] + names:
                with self.subTest(missing=missing):
                    with zipfile.ZipFile(archive, 'w') as output:
                        for name in names:
                            if name != missing: output.writestr(name, b'fixture-class')
                    if missing:
                        with self.assertRaises(ValueError): FIXTURE.verify(archive, source, derived)
                    else:
                        binding = FIXTURE.verify(archive, source, derived)
                        self.assertTrue(binding['fixtureOnly'])
                        self.assertEqual(FIXTURE.DELEGATE, binding['delegate'])
                        with self.assertRaises(ValueError): FIXTURE.verify(archive, source, derived + b'\n# uncontrolled mutation')
