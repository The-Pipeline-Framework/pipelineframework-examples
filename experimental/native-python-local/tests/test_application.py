import asyncio
from dataclasses import replace
from importlib import metadata
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory
import unittest

import tpf_experimental_python_local
from tpf_experimental_python_local import PythonLocalDiagnostic, create_python_local_runtime

from verify_project import PROJECT, ROOT, verify_project


class ApplicationTests(unittest.TestCase):
    def test_runtime_is_the_installed_distribution(self):
        installed = metadata.distribution("tpf-experimental-python-local")
        installed_module = installed.locate_file("tpf_experimental_python_local/__init__.py")
        self.assertEqual(Path(tpf_experimental_python_local.__file__).resolve(), installed_module.resolve())

    def test_authored_functions_through_generated_api(self):
        verify_project()
        from business import handlers
        from generated.project.application import Completed, InvocationContext, create_application
        from generated.project.models import InputValue, OutputValue

        observations = []
        runtime = create_python_local_runtime(on_observation=observations.append)
        application = create_application(runtime, handlers)
        result = asyncio.run(application.run(InputValue(3), InvocationContext("test", "trace-test")))
        self.assertIsInstance(result, Completed)
        self.assertEqual(result.outcome.value, OutputValue(5))
        self.assertEqual(
            [event["stepId"] for event in observations if event["kind"] == "ATTEMPT_STARTED"],
            ["A", "B", "C"],
        )
        self.assertEqual([event["record"] for event in observations if event["kind"] == "TERMINAL"], [result])

    def test_wrong_nominal_and_invalid_int32_inputs_do_not_invoke_a_handler(self):
        verify_project()
        from business import handlers
        from generated.project.application import InvocationContext, Rejected, create_application
        from generated.project.models import FirstValue, InputValue

        observations = []
        application = create_application(create_python_local_runtime(on_observation=observations.append), handlers)
        context = InvocationContext("test", "trace-test")
        for invalid in (FirstValue(3), InputValue(True), InputValue(3.0), InputValue(1 << 31), InputValue(-(1 << 31) - 1)):
            with self.subTest(input=invalid):
                terminal = asyncio.run(application.run(invalid, context))
                self.assertEqual(terminal, Rejected("INVALID_VALUE"))
        self.assertEqual([event["kind"] for event in observations], ["TERMINAL"] * 5)

    def test_loader_rejects_tampered_digest_and_missing_handler(self):
        verify_project()
        from business import handlers
        from generated.project.application import create_application

        runtime = create_python_local_runtime()

        class TamperedProjection:
            def load(self, binding):
                return runtime.load(replace(binding, projection_bytes=binding.projection_bytes + b" "))

        with self.assertRaises(PythonLocalDiagnostic) as mismatch:
            create_application(TamperedProjection(), handlers)
        self.assertEqual(mismatch.exception.code, "ARTIFACT_DIGEST_MISMATCH")
        with self.assertRaises(PythonLocalDiagnostic) as missing:
            create_application(runtime, {"normalize": handlers["normalize"], "finish": handlers["finish"]})
        self.assertEqual(missing.exception.code, "MISSING_HANDLER")

    def test_rejects_stale_project_and_changed_generated_file(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / "generated" / "project"
            copytree(PROJECT, directory)
            for name in ("pipeline.yaml", "native-bindings.json"):
                (root / name).write_bytes((ROOT / name).read_bytes())
            verify_project(directory, root)
            source = root / "pipeline.yaml"
            source.write_bytes(source.read_bytes() + b"\n# changed\n")
            with self.assertRaisesRegex(ValueError, r"current pipeline.yaml"):
                verify_project(directory, root)
            generated = directory / "models.py"
            generated.write_bytes(generated.read_bytes() + b"\n# changed\n")
            with self.assertRaisesRegex(ValueError, r"digest mismatch for models.py"):
                verify_project(directory, ROOT)


    def test_rejects_extra_packages_and_symlinked_generated_sources(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / "generated" / "project"
            copytree(PROJECT, directory)
            for name in ("pipeline.yaml", "native-bindings.json"):
                (root / name).write_bytes((ROOT / name).read_bytes())
            for unexpected in (
                directory / "__init__.py",
                directory / "application.pyc",
                directory.parent / "__init__.py",
                root / "generated.py",
            ):
                with self.subTest(unexpected=unexpected):
                    unexpected.touch()
                    with self.assertRaisesRegex(ValueError, r"Unexpected"):
                        verify_project(directory, root)
                    unexpected.unlink()
            source = directory / "models.py"
            source.unlink()
            source.symlink_to(PROJECT / source.name)
            with self.assertRaisesRegex(ValueError, r"Unexpected files"):
                verify_project(directory, root)


if __name__ == "__main__":
    unittest.main()
