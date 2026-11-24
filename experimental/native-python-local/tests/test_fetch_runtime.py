from hashlib import sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from fetch_runtime import REPOSITORY, WHEEL, WORKFLOW, fetch_runtime


class RuntimeDownloadTests(unittest.TestCase):
    def test_installs_only_a_verified_runtime_candidate(self):
        wheel = b"mock candidate wheel"
        digest = sha256(wheel).hexdigest()
        head = "a" * 40
        pin = {"run": "42", "head": head, "wheelSha256": digest}

        for failure in (None, "run", "source", "wheel"):
            with self.subTest(failure=failure), TemporaryDirectory() as temporary:
                generated = Path(temporary) / "generated"
                downloaded = []

                def gh(args):
                    if args == ["--version"]:
                        return "gh version 2.100.0"
                    if args[0] == "api":
                        return json.dumps({
                            "id": 42, "head_sha": "b" * 40 if failure == "run" else head,
                            "path": WORKFLOW, "head_repository": {"full_name": REPOSITORY},
                            "conclusion": "success",
                        })
                    self.assertEqual(args[:8], [
                        "run", "download", "42", "--repo", f"github.com/{REPOSITORY}",
                        "--name", f"python-local-runtime-{head}", "--dir",
                    ])
                    staging = Path(args[-1])
                    downloaded.append(staging)
                    (staging / WHEEL).write_bytes(b"tampered" if failure == "wheel" else wheel)
                    (staging / "source-sha.txt").write_text(("b" * 40 if failure == "source" else head) + "\n")
                    (staging / "SHA256SUMS").write_text(f"{digest}  ./{WHEEL}\n")
                    return ""

                if failure:
                    with self.assertRaisesRegex(ValueError, {
                        "run": "workflow and source commit", "source": "different source commit",
                        "wheel": "pinned SHA-256",
                    }[failure]):
                        fetch_runtime(pin, generated, gh)
                    self.assertFalse((generated / "runtime").exists())
                    self.assertFalse(any(staging.exists() for staging in downloaded))
                else:
                    self.assertEqual(fetch_runtime(pin, generated, gh).read_bytes(), wheel)
