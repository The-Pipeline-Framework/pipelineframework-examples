"""Validate the candidate result shape using the existing Draft 2020-12 tooling."""
import json
from pathlib import Path
import unittest

from jsonschema import Draft202012Validator


ADMISSION = Path(__file__).resolve().parents[1] / 'native-local-projection' / 'transition-admission'
PROFILE = 'tpf-polyglot-failed-result/1'


class PortableResultSchemaTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        schema = json.loads((ADMISSION / 'failed-result.schema.json').read_bytes())
        Draft202012Validator.check_schema(schema)
        cls.validator = Draft202012Validator(schema)
        cls.cases = json.loads((ADMISSION / 'vectors.json').read_bytes())['cases']

    def test_settled_failure_examples_conform(self):
        codes = set()
        for case in self.cases:
            for settlement in case['expected']['settlements']:
                if 'failure' in settlement:
                    with self.subTest(case=case['id']):
                        failure = settlement['failure']
                        self.validator.validate({
                            'resultProfile': PROFILE, 'outcome': 'FAILED', 'failure': failure,
                        })
                        codes.add(failure['code'])
        self.assertEqual(codes, {'TRANSIENT_EXHAUSTED', 'BUSINESS_REJECTION', 'UNCLASSIFIED'})

    def test_injected_result_shapes(self):
        expectations = set()
        for case in self.cases:
            for event in case['events']:
                if 'candidateSchemaValid' in event:
                    with self.subTest(case=case['id'], response=event['response']):
                        valid = event['candidateSchemaValid']
                        self.assertEqual(valid, self.validator.is_valid(event['response']))
                        expectations.add(valid)
        self.assertEqual(expectations, {True, False})
