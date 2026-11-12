from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

from run_probes import run_with_deadline


class ProbeRunnerTest(unittest.TestCase):
    def test_deadline_terminates_a_stuck_child(self):
        with TemporaryDirectory() as directory:
            marker = Path(directory) / 'started'
            command = [
                sys.executable, '-c',
                'from pathlib import Path; import sys, threading; '
                'Path(sys.argv[1]).touch(); threading.Event().wait()',
                str(marker),
            ]
            diagnostic = StringIO()
            with redirect_stderr(diagnostic):
                status = run_with_deadline(command, timeout=2)
            self.assertTrue(marker.is_file(), 'child did not reach the blocking wait')
            self.assertEqual(status, 124)
            self.assertIn('2-second deadline', diagnostic.getvalue())
