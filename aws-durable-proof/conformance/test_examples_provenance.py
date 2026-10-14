"""Relocated harness identity must not substitute for production runtime provenance."""
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).parents[1]
SPEC = importlib.util.spec_from_file_location("bundle_prepare", ROOT / "scripts/prepare-production-bundle.py")
PREPARE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREPARE)
GATE = PREPARE.GATE


class ExamplesProvenanceTest(unittest.TestCase):
    def test_promotion_requires_exact_examples_producer_revision(self):
        expected = "e" * 40
        GATE.verify_examples_revision({"examplesRevision": expected}, expected)
        for report in ({}, {"examplesRevision": "main"}, {"examplesRevision": "f" * 40}):
            with self.subTest(report=report), self.assertRaises(GATE.GateFailure):
                GATE.verify_examples_revision(report, expected)
        with self.assertRaises(GATE.GateFailure):
            GATE.verify_examples_revision({"examplesRevision": "main"}, "main")

    def test_preparation_pins_external_runtime_and_fails_closed_on_cleanliness(self):
        runtime, examples = "a" * 40, "e" * 40
        with tempfile.TemporaryDirectory() as directory:
            args = ["prepare", "--action-archive", "action", "--coordinator-archive", "coordinator",
                    "--deployment-archive", "deployment", "--operation-http-archive", "http",
                    "--item-archive", "item", "--operation-http-url", "https://example.invalid/",
                    "--output", directory, "--runtime-sha", runtime,
                    "--compiler-sha", "c" * 40, "--bom-sha", "b" * 40]
            for attested, dirty in [(False, False), (True, False), (True, True)]:
                with self.subTest(attested=attested, dirty=dirty):
                    invocation = args + (["--runtime-source-tree-clean"] if attested else [])
                    with patch.object(sys, "argv", invocation), \
                         patch.object(PREPARE.subprocess, "check_output", side_effect=[examples, " M fixture" if dirty else ""]), \
                         patch.object(PREPARE, "prepare", return_value={}) as build:
                        PREPARE.main()
                    self.assertEqual(runtime, build.call_args.args[6]["runtime"])
                    self.assertEqual(attested and not dirty, build.call_args.args[7])
                    import json
                    manifest = json.loads((Path(directory) / "build-manifest.json").read_text())
                    self.assertEqual(examples, manifest["examplesRevision"])
