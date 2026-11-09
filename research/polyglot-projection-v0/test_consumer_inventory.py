"""Keep the live consumers tied to the producer and inputs in this checkout."""
import hashlib
import json
import unittest

import run


class ConsumerInventoryTest(unittest.TestCase):
    def test_live_consumer_uses_local_producer_bytes(self):
        """Relocation must preserve provenance and all three generated packages."""
        consumer = run.ROOT / "research/native-local-projection/runtime-policy/v1"
        provenance = json.loads((consumer / "PROVENANCE.json").read_text())
        self.assertEqual("https://github.com/The-Pipeline-Framework/pipelineframework-examples",
                         provenance["sourceRepository"])
        self.assertEqual(run.HERE, run.ROOT / provenance["sourceDirectory"])
        snapshot = run.source_snapshot(run.HERE)
        self.assertEqual(provenance["generatorTreeSha256"], run.snapshot_digest(snapshot))
        self.assertEqual(provenance["sourceFileSha256"],
                         {name: hashlib.sha256(data).hexdigest() for name, data in snapshot})
        for name, expected in provenance["inputFileSha256"].items():
            self.assertEqual(expected, hashlib.sha256((run.HERE / name).read_bytes()).hexdigest(), name)
        for name, artifact in provenance["artifacts"].items():
            producer = run.HERE / "generated-policy" / name
            target = consumer / "generated" / name
            self.assertEqual(set(artifact["sha256"]), {p.name for p in producer.iterdir()})
            self.assertEqual(set(artifact["sha256"]), {p.name for p in target.iterdir()})
            for filename, expected in artifact["sha256"].items():
                data = (producer / filename).read_bytes()
                self.assertEqual(expected, hashlib.sha256(data).hexdigest(), f"{name}/{filename}")
                self.assertEqual(data, (target / filename).read_bytes(), f"{name}/{filename}")
