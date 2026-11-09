"""The comparison must reject plausible but incorrect live reports."""
from copy import deepcopy
import unittest

from check_policy_binding import PROFILE, check_report, expectations


class ReportCheckTest(unittest.TestCase):
    def test_distinct_live_observations_are_required(self):
        expected = expectations()
        language = 'test-consumer'
        report = {'language': language, 'profile': PROFILE, 'artifactSha256': {},
                  'cases': list(expected.values())}

        for case_id, field, observed in [
            ('success', 'observations', True),
            ('cancel-handoff', 'delivery', ['COMPLETED']),
        ]:
            with self.subTest(case=case_id):
                corrupted = deepcopy(report)
                row = next(item for item in corrupted['cases'] if item['id'] == case_id)
                row[field] = observed
                with self.assertRaisesRegex(ValueError, f'{language}/{case_id} mismatch'):
                    check_report(corrupted, language, {}, expected)

        corrupted = deepcopy(report)
        row = next(item for item in corrupted['cases'] if item['id'] == 'b-budget-one')
        row['calls'][2]['input'] = 3
        with self.assertRaisesRegex(ValueError, f'{language}/b-budget-one mismatch'):
            check_report(corrupted, language, {}, expected)


if __name__ == '__main__':
    unittest.main()
