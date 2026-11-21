import copy
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


BOOTSTRAP = Path(__file__).parent / "container" / "bootstrap-localstack.sh"
INDEX_KEYS = {
    "await-interaction-by-unit": ("query_unit_key", "query_unit_sort"),
    "await-interaction-pending-by-tenant": ("query_pending_tenant_key", "query_pending_deadline_sort"),
    "await-interaction-pending-by-assignee": ("query_pending_assignee_key", "query_pending_deadline_sort"),
    "await-interaction-pending-by-group": ("query_pending_group_key", "query_pending_deadline_sort"),
    "await-interaction-pending-by-step": ("query_pending_step_key", "query_pending_deadline_sort"),
    "await-interaction-pending-by-deadline": ("query_deadline_key", "query_deadline_sort"),
    "await-interaction-continuation-work": ("query_continuation_key", "query_continuation_due_epoch_ms"),
    "await-interaction-by-execution": ("query_execution_key", "query_execution_sort"),
}


def retained_schema():
    names = {"tenant_id", "interaction_id"}
    indexes = []
    for name, (partition, sort) in INDEX_KEYS.items():
        names.update((partition, sort))
        indexes.append({"IndexName": name, "IndexStatus": "ACTIVE", "Projection": {"ProjectionType": "ALL"},
                        "KeySchema": [{"AttributeName": partition, "KeyType": "HASH"},
                                      {"AttributeName": sort, "KeyType": "RANGE"}]})
    return {"Table": {"TableStatus": "ACTIVE", "GlobalSecondaryIndexes": indexes,
                      "KeySchema": [{"AttributeName": "tenant_id", "KeyType": "HASH"},
                                    {"AttributeName": "interaction_id", "KeyType": "RANGE"}],
                      "AttributeDefinitions": [{"AttributeName": name, "AttributeType": "N" if name == "query_continuation_due_epoch_ms" else "S"}
                                               for name in sorted(names)]}}


class BootstrapRetainedTableTest(unittest.TestCase):
    def run_bootstrap(self, schema, fresh=False):
        # Exercise the actual shell entry point; substitute only the external Docker/AWS CLI boundary.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "schema.json").write_text(json.dumps(schema))
            docker = root / "docker"
            docker.write_text('''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
with open(os.environ["BOOTSTRAP_TEST_CALLS"], "a") as log:
    log.write(json.dumps(args) + "\\n")
if "awslocal" in args:
    aws = args[args.index("awslocal") + 1:]
    if aws[:2] == ["dynamodb", "describe-table"]:
        if os.environ["BOOTSTRAP_TEST_FRESH"] == "true":
            sys.exit(1)
        if aws[aws.index("--table-name") + 1] == "tpf_await_interaction":
            print(Path(os.environ["BOOTSTRAP_TEST_SCHEMA"]).read_text())
        else:
            print('{"Table": {}}')
sys.exit(0)
''')
            docker.chmod(0o755)
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"],
                       BOOTSTRAP_TEST_SCHEMA=str(root / "schema.json"),
                       BOOTSTRAP_TEST_CALLS=str(root / "calls.jsonl"),
                       BOOTSTRAP_TEST_FRESH=str(fresh).lower())
            result = subprocess.run(["bash", str(BOOTSTRAP)], env=env, text=True, capture_output=True)
            calls = [json.loads(line) for line in (root / "calls.jsonl").read_text().splitlines()]
            return result, calls

    def assert_no_reset_or_table_mutation(self, calls):
        self.assertFalse(any("down" in call for call in calls), calls)
        for call in calls:
            if "tpf_await_interaction" in call:
                self.assertFalse(any(operation in call for operation in ("create-table", "update-table", "delete-table")), call)

    def test_valid_retained_table_is_reused_without_mutation(self):
        result, calls = self.run_bootstrap(retained_schema())
        self.assertEqual(0, result.returncode, result.stderr)
        self.assert_no_reset_or_table_mutation(calls)

    def test_each_missing_new_index_fails_with_explicit_manual_local_reset_guidance(self):
        for missing in ("await-interaction-continuation-work", "await-interaction-by-execution"):
            with self.subTest(missing=missing):
                schema = retained_schema()
                schema["Table"]["GlobalSecondaryIndexes"] = [index for index in schema["Table"]["GlobalSecondaryIndexes"] if index["IndexName"] != missing]
                result, calls = self.run_bootstrap(schema)
                self.assertNotEqual(0, result.returncode)
                self.assertIn(missing, result.stderr)
                self.assertIn("down -v", result.stderr)
                self.assertIn("data may be discarded", result.stderr)
                self.assert_no_reset_or_table_mutation(calls)

    def test_incompatible_retained_schema_fails_closed(self):
        table = retained_schema()
        variants = []
        wrong_type = copy.deepcopy(table)
        next(item for item in wrong_type["Table"]["AttributeDefinitions"] if item["AttributeName"] == "query_continuation_due_epoch_ms")["AttributeType"] = "S"
        variants.append(wrong_type)
        wrong_keys = copy.deepcopy(table)
        wrong_keys["Table"]["GlobalSecondaryIndexes"][-1]["KeySchema"][0]["AttributeName"] = "tenant_id"
        variants.append(wrong_keys)
        wrong_projection = copy.deepcopy(table)
        wrong_projection["Table"]["GlobalSecondaryIndexes"][-1]["Projection"]["ProjectionType"] = "KEYS_ONLY"
        variants.append(wrong_projection)
        for schema in variants:
            with self.subTest(schema=schema):
                result, calls = self.run_bootstrap(schema)
                self.assertNotEqual(0, result.returncode)
                self.assertIn("schema is incompatible", result.stderr)
                self.assert_no_reset_or_table_mutation(calls)

    def test_fresh_creation_retains_both_new_indexes(self):
        result, calls = self.run_bootstrap({}, fresh=True)
        self.assertEqual(0, result.returncode, result.stderr)
        create = next(call for call in calls if "create-table" in call and "tpf_await_interaction" in call)
        for name in ("await-interaction-continuation-work", "await-interaction-by-execution"):
            self.assertTrue(any("IndexName=" + name + "," in argument for argument in create), create)
        self.assertFalse(any("down" in call or "delete-table" in call or "update-table" in call for call in calls))


if __name__ == "__main__":
    unittest.main()
