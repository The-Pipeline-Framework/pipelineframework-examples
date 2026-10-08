"""Exercise suite teardown without building images or requiring Docker."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


RUNNER = Path(__file__).with_name("run-system-test-suite.sh")


class RestaurantSuiteCleanupTest(unittest.TestCase):
    def run_fixture(self, failing_proof="", docker_failure=""):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "scripts").mkdir()
            shutil.copyfile(RUNNER, root / "scripts/run-system-test-suite.sh")
            container = root / "restaurant-approval/self-host/container"
            container.mkdir(parents=True)
            for proof in ["demo", "incident", "recovery"]:
                (container / f"run-container-ha-{proof}.sh").write_text(
                    '#!/usr/bin/env bash\n'
                    f'echo "{proof}:$1" >> "$CAPTURE"\n'
                    f'if [[ "$FAIL_PROOF" == "{proof}:$1" ]]; then exit 7; fi\n'
                    'exit 0\n'
                )
            binaries = root / "bin"
            binaries.mkdir()
            docker = binaries / "docker"
            docker.write_text(
                '#!/usr/bin/env bash\n'
                'test "$TPF_REPO_ROOT" = "$EXPECTED_ROOT" || exit 9\n'
                'echo "$*" >> "$CAPTURE"\n'
                'if [[ "$*" == *" logs "* ]]; then\n'
                '  echo "retained diagnostic tail"\n'
                '  [[ "$DOCKER_FAILURE" != logs ]] || exit 8\n'
                'fi\n'
                'if [[ "$*" == *" down "* && "$DOCKER_FAILURE" == down ]]; then exit 8; fi\n'
                'exit 0\n'
            )
            docker.chmod(0o755)
            capture = root / "capture.txt"
            environment = dict(os.environ, PATH=f"{binaries}{os.pathsep}{os.environ['PATH']}",
                               CAPTURE=str(capture), FAIL_PROOF=failing_proof,
                               DOCKER_FAILURE=docker_failure, EXPECTED_ROOT=str(root))
            environment.pop("TPF_REPO_ROOT", None)
            result = subprocess.run(["bash", str(root / "scripts/run-system-test-suite.sh"),
                                     "restaurant-ha"], env=environment, capture_output=True, text=True)
            return result, capture.read_text().splitlines(), str(container / "compose.yaml")

    def test_success_cleans_only_own_project_without_failure_logs(self):
        result, calls, compose = self.run_fixture()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls[:4], ["demo:--prepare-images", "demo:--ci", "incident:--ci", "recovery:--ci"])
        self.assertEqual(calls[4:], [f"compose -f {compose} down -v --remove-orphans"])

    def test_failure_captures_logs_before_cleanup_and_preserves_exit(self):
        for proof in ["demo:--prepare-images", "demo:--ci", "incident:--ci", "recovery:--ci"]:
            with self.subTest(proof=proof):
                result, calls, compose = self.run_fixture(proof)
                self.assertEqual(result.returncode, 7, result.stderr)
                self.assertEqual(calls[-2:], [f"compose -f {compose} logs --no-color --tail 100",
                                             f"compose -f {compose} down -v --remove-orphans"])
                self.assertIn("retained diagnostic tail", result.stdout)

    def test_failed_log_collection_still_cleans_up(self):
        result, calls, _ = self.run_fixture("demo:--ci", "logs")
        self.assertEqual(result.returncode, 7)
        self.assertTrue(calls[-1].endswith("down -v --remove-orphans"))

    def test_cleanup_failure_does_not_mask_original_failure(self):
        result, _, _ = self.run_fixture("incident:--ci", "down")
        self.assertEqual(result.returncode, 7)
        self.assertIn("cleanup failed", result.stderr)

    def test_cleanup_failure_blocks_otherwise_green_suite(self):
        result, _, _ = self.run_fixture(docker_failure="down")
        self.assertEqual(result.returncode, 1)
        self.assertIn("cleanup failed", result.stderr)


if __name__ == "__main__":
    unittest.main()
