"""Local configuration-boundary tests; fake URLs do not assert endpoint availability."""
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest
import zipfile
from unittest.mock import Mock

DIRECTORY = Path(__file__).parents[1] / "scripts"


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, DIRECTORY / filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


GATE = module("binding_gate", "verify-production-conformance.py")
RUNNER = module("binding_runner", "run-production-conformance.py")
PREPARE = module("binding_prepare", "prepare-production-bundle.py")


class CompiledOperationBindingTest(unittest.TestCase):
    URL = "https://operation-host.example.invalid/"

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.action = self.root / "function.zip"
        self.item = self.root / "item-function.zip"
        self.write_archive(self.action, "action")
        self.write_archive(self.item, "itemAction")

    def properties(self, role, url=None):
        return "\n".join("quarkus.rest-client." + key + ".url=" + (url or self.URL)
                         for key in GATE.OPERATION_CLIENTS[role]) + "\n"

    def write_archive(self, path, role, content=None, extra=()):
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("application.properties", self.properties(role) if content is None else content)
            for name, value in extra:
                archive.writestr(name, value)

    def check(self, url=None):
        return GATE.verify_operation_http_binding(self.action, self.item, url or self.URL)

    def test_exact_filtered_scalar_and_item_urls_are_attested(self):
        result = self.check()
        self.assertEqual(self.URL, result["operationHttpUrl"])
        self.assertEqual(2, len(result["compiledBindings"]["action"]))
        self.assertEqual(1, len(result["compiledBindings"]["itemAction"]))

    def test_generated_ordinal_90_defaults_do_not_replace_compiled_application_urls(self):
        self.write_archive(self.action, "action", extra=(("META-INF/pipeline/orchestrator-clients.properties",
                            self.properties("action", "https://localhost:8444")),))
        self.assertEqual(self.URL, self.check()["operationHttpUrl"])
        for extra in ("config_ordinal=80", "quarkus.config.locations=https://other.example.invalid/config.properties"):
            self.write_archive(self.action, "action", self.properties("action") + extra + "\n")
            with self.subTest(extra=extra), self.assertRaises(GATE.GateFailure):
                self.check()

    def test_placeholder_absent_or_mismatched_candidate_urls_fail(self):
        for role, path in (("action", self.action), ("itemAction", self.item)):
            for content in (self.properties(role, "UNCONFIGURED"), "",
                            self.properties(role, self.URL.rstrip("/")), self.properties(role, "${operation.url}"),
                            self.properties(role, self.URL + " ")):
                with self.subTest(role=role, content=content):
                    self.write_archive(path, role, content)
                    with self.assertRaises(GATE.GateFailure):
                        self.check()
            self.write_archive(path, role)
        for expected in ("UNCONFIGURED", "", "${operation.url}", "https://user:secret@example.invalid/",
                         "https://example.invalid/?route=x", "https://example.invalid/#route", "https://example.invalid:invalid/",
                         self.URL + " ", self.URL + r"\path"):
            with self.subTest(expected=expected), self.assertRaises(GATE.GateFailure):
                GATE.verify_operation_http_binding(self.action, self.item, expected)

    def test_duplicate_profile_alias_or_shadow_sources_cannot_hide_an_override(self):
        key = "quarkus.rest-client.process-first-approval.url"
        variants = (
            key + "=" + self.URL,
            "%prod." + key + "=" + self.URL,
            'quarkus.rest-client."process-first-approval".url=' + self.URL,
            r"quarkus.rest-client.process\u002dfirst-approval.url=" + self.URL,
            "quarkus.rest-client.process-first-approval.uri=" + self.URL,
            "org.pipelineframework.awsconformance.pipeline.pipeline.ProcessFirstApprovalRestClient/mp-rest/url=" + self.URL,
        )
        for extra in variants:
            with self.subTest(extra=extra):
                self.write_archive(self.action, "action", self.properties("action") + extra + "\n")
                with self.assertRaises(GATE.GateFailure):
                    self.check()
        self.write_archive(self.action, "action", extra=(("application.yaml", "quarkus: {}"),))
        with self.assertRaises(GATE.GateFailure):
            self.check()
        nested = io.BytesIO()
        with zipfile.ZipFile(nested, "w") as jar:
            jar.writestr("META-INF/microprofile-config.properties", key + "=" + self.URL)
        self.write_archive(self.action, "action", extra=(("lib/shadow.jar", nested.getvalue()),))
        with self.assertRaises(GATE.GateFailure):
            self.check()

    def test_runtime_url_override_must_match_the_compiled_literal(self):
        clients = GATE.OPERATION_CLIENTS["action"]
        GATE.verify_runtime_operation_http_binding({"QUARKUS_REST_CLIENT_PROCESS_FIRST_APPROVAL_URL": self.URL}, clients, self.URL)
        for key in ("QUARKUS_REST_CLIENT_PROCESS_FIRST_APPROVAL_URL",
                    "QUARKUS_REST_CLIENT__PROCESS_FIRST_APPROVAL__URI"):
            with self.subTest(key=key), self.assertRaises(GATE.GateFailure):
                GATE.verify_runtime_operation_http_binding({key: "https://different.example.invalid/"}, clients, self.URL)
        for environment in ({"QUARKUS_CONFIG_LOCATIONS": "https://other.example.invalid/config.properties"},
                            {"JAVA_TOOL_OPTIONS": "-Dquarkus.rest-client.process-first-approval.url=https://other.example.invalid/"}):
            with self.subTest(environment=environment), self.assertRaises(GATE.GateFailure):
                GATE.verify_runtime_operation_http_binding(environment, clients, self.URL)

    def test_producer_rejects_archive_binding_before_any_aws_call(self):
        self.write_archive(self.action, "action", self.properties("action", "UNCONFIGURED"))
        (self.root / "pipeline-contract.json").write_text('{"pipelineId":"generated"}')
        boundary = Mock()
        producer = RUNNER.ProductionScenarios({"operationHttpUrl": self.URL}, self.root, 1, boundary)
        with self.assertRaises(ValueError):
            producer.preflight()
        boundary.call.assert_not_called()
        boundary.invoke.assert_not_called()
        boundary.capture.assert_not_called()

    def test_preparation_rejects_binding_before_creating_a_candidate_bundle(self):
        self.write_archive(self.item, "itemAction", self.properties("itemAction", "UNCONFIGURED"))
        output = self.root / "candidate-output"
        with self.assertRaises(ValueError):
            PREPARE.prepare(self.action, self.root / "not-read-coordinator.jar", self.root / "not-read-deployment.jar",
                            self.root / "not-read-operation.zip", self.item, output,
                            {"runtime": "a" * 40, "compiler": "b" * 40, "bom": "c" * 40}, True, self.URL)
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
