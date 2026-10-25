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
PREP_SPEC = importlib.util.spec_from_file_location('combined_prepare', ROOT / 'scripts/prepare-production-bundle.py')
PREPARE = importlib.util.module_from_spec(PREP_SPEC)
PREP_SPEC.loader.exec_module(PREPARE)
DEPLOYMENT = Path(os.environ.get('TPF_AWS_DURABLE_DEPLOYMENT_JAR',
    str(ROOT / 'production-tests/target/conformance-dependencies/deployment.jar')))

def production_template():
    with zipfile.ZipFile(DEPLOYMENT) as archive:
        return archive.read('aws-durable-queue-async.yaml')

class TransitionFaultFixtureTest(unittest.TestCase):
    def test_combined_work_and_transition_wiring_retains_original_provenance(self):
        import hashlib
        source = production_template()
        combined = PREPARE.combined_delivery_template(source)
        self.assertIn(b'Default: conformance-work-delivery', combined)
        self.assertIn(b'Default: conformance-transition-delivery', combined)
        self.assertIn(b'ConformanceWorkFaultAccess:', combined)
        self.assertIn(b'ConformanceTransitionFaultAccess:', combined)
        digest = hashlib.sha256(source).hexdigest().encode()
        self.assertEqual(2, combined.count(b'"productionTemplateSha256":"' + digest + b'"'))
        for preserved in (b'CompletionObservationOutstandingAlarm:', b'TransitionResponseQueue:', b'LoggingConfig:'):
            self.assertIn(preserved, combined)
        self.assertNotIn(b'conformance-work-delivery', source)

    def test_derivation_keeps_production_worker_and_alarm_contracts(self):
        source = production_template()
        derived = FIXTURE.derived_template(source)
        self.assertIn(b'CompletionObservationOutstandingAlarm:', derived)
        self.assertIn(b'TPF_CONFORMANCE_TRANSITION_FAULT_TABLE: !Ref ConformanceTransitionFaultTable', derived)
        self.assertIn(b'Roles: [!Ref WorkerRole]', derived)
        self.assertIn(b'Resource: !GetAtt ConformanceTransitionFaultTable.Arn', derived)
        self.assertIn(b'"transitionVersionArn":"${TransitionWorkerVersion}"', derived)
        self.assertRegex(derived, rb"QUARKUS_LAMBDA_HANDLER: !Ref (?:WorkHandler|'WorkHandler')(?=[,\r\n}])")
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


class LiteralTransitionFaultFixtureTest(unittest.TestCase):
    LEGACY = (b"Parameters:\n"
              b"  TransitionWorkerHandler: {Type: String, Default: aws-durable-transition-worker}\n"
              b"Resources:\n"
              b"  TransitionWorkerFunction:\n"
              b"    Properties:\n"
              b"      Environment:\n"
              b"        Variables: {<<: *ActionEnvironment, QUARKUS_LAMBDA_HANDLER: !Ref TransitionWorkerHandler}\n"
              b"  WorkFunction:\n"
              b"    Properties: {Handler: unchanged}\n"
              b"Outputs:\n")
    LITERAL = LEGACY.replace(
        b"        Variables: {<<: *ActionEnvironment, QUARKUS_LAMBDA_HANDLER: !Ref TransitionWorkerHandler}\n",
        b"        Variables:\n          QUARKUS_LAMBDA_HANDLER: !Ref 'TransitionWorkerHandler'\n")

    def test_literal_adds_only_the_transition_environment_entry(self):
        derived = FIXTURE.derived_template(self.LITERAL)
        self.assertIn(b"          QUARKUS_LAMBDA_HANDLER: !Ref 'TransitionWorkerHandler'\n"
                      b"          TPF_CONFORMANCE_TRANSITION_FAULT_TABLE: !Ref ConformanceTransitionFaultTable\n", derived)
        self.assertIn(b"  WorkFunction:\n    Properties: {Handler: unchanged}\n", derived)
        self.assertNotIn(b"<<:", derived)
        self.assertEqual(1, derived.count(b'TPF_CONFORMANCE_TRANSITION_FAULT_TABLE:'))

    def test_unquoted_literal_handler_is_the_same_reference(self):
        source = self.LITERAL.replace(b"!Ref 'TransitionWorkerHandler'", b"!Ref TransitionWorkerHandler")
        self.assertIn(b'TPF_CONFORMANCE_TRANSITION_FAULT_TABLE:', FIXTURE.derived_template(source))

    def test_missing_or_duplicate_transition_resource_is_denied(self):
        for source in [self.LITERAL.replace(b'TransitionWorkerFunction:', b'OtherFunction:'),
                       self.LITERAL.replace(b'Outputs:', b'  TransitionWorkerFunction:\n    Type: AWS::Lambda::Function\nOutputs:')]:
            with self.subTest(source=source), self.assertRaises(ValueError): FIXTURE.derived_template(source)

    def test_missing_duplicate_or_mixed_handler_is_denied(self):
        handler = b"          QUARKUS_LAMBDA_HANDLER: !Ref 'TransitionWorkerHandler'\n"
        for source in [self.LITERAL.replace(handler, b''), self.LITERAL.replace(handler, handler * 2),
                       self.LITERAL.replace(handler, handler + b'        Variables: {<<: *ActionEnvironment, QUARKUS_LAMBDA_HANDLER: !Ref TransitionWorkerHandler}\n')]:
            with self.subTest(source=source), self.assertRaises(ValueError): FIXTURE.derived_template(source)

    def test_wrong_resource_handler_and_preexisting_fault_entry_are_denied(self):
        for source in [self.LITERAL.replace(b"!Ref 'TransitionWorkerHandler'", b'!Ref WorkHandler'),
                       self.LITERAL.replace(b'  WorkFunction:', b'          TPF_CONFORMANCE_TRANSITION_FAULT_TABLE: wrong\n  WorkFunction:')]:
            with self.subTest(source=source), self.assertRaises(ValueError): FIXTURE.derived_template(source)

    def test_handler_outside_variables_or_alternate_duplicate_is_denied(self):
        handler = b"          QUARKUS_LAMBDA_HANDLER: !Ref 'TransitionWorkerHandler'\n"
        for source in [self.LITERAL.replace(handler, b"          OTHER: unchanged\n      Metadata:\n        Other:\n" + handler),
                       self.LITERAL.replace(handler, handler + b"          QUARKUS_LAMBDA_HANDLER: !Ref WorkHandler\n"),
                       self.LITERAL.replace(b"  WorkFunction:", b"      Metadata:\n        Other:\n" + handler + b"  WorkFunction:")]:
            with self.subTest(source=source), self.assertRaises(ValueError): FIXTURE.derived_template(source)

    def test_double_quoted_literal_handler_is_the_same_reference(self):
        source = self.LITERAL.replace(b"!Ref 'TransitionWorkerHandler'", b'!Ref "TransitionWorkerHandler"')
        self.assertIn(b'TPF_CONFORMANCE_TRANSITION_FAULT_TABLE:', FIXTURE.derived_template(source))

    def test_inline_duplicate_transition_resource_is_denied(self):
        for source in [self.LEGACY, self.LITERAL]:
            for key in [b'TransitionWorkerFunction', b"'TransitionWorkerFunction'", b'"TransitionWorkerFunction"']:
                invalid = source.replace(b'Outputs:', b'  ' + key + b': {Type: AWS::Lambda::Function}\nOutputs:')
                with self.subTest(source=invalid), self.assertRaises(ValueError): FIXTURE.derived_template(invalid)

    def test_environment_requires_unique_properties_mapping(self):
        for source in [self.LEGACY, self.LITERAL]:
            for invalid in [source.replace(b'    Properties:\n      Environment:', b'    Metadata:\n      Environment:'),
                            source.replace(b'    Properties:\n      Environment:', b'    Properties: {}\n    Properties:\n      Environment:'),
                            source.replace(b'    Properties:\n      Environment:', b"    'Properties': {}\n    Properties:\n      Environment:")]:
                with self.subTest(source=invalid), self.assertRaises(ValueError): FIXTURE.derived_template(invalid)

    def test_inline_or_quoted_environment_and_variables_duplicates_are_denied(self):
        for source in [self.LEGACY, self.LITERAL]:
            for indent, key in [(b'      ', b'Environment'), (b'        ', b'Variables')]:
                for spelling in [key, b"'" + key + b"'", b'"' + key + b'"']:
                    invalid = source.replace(indent + key + b':', indent + spelling + b': {}\n' + indent + key + b':')
                    with self.subTest(source=invalid), self.assertRaises(ValueError): FIXTURE.derived_template(invalid)
