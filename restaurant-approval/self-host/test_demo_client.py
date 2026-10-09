import importlib.util
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


spec = importlib.util.spec_from_file_location("restaurant_demo_client", Path(__file__).with_name("demo-client.py"))
demo_client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demo_client)


def encoded(value, payload_type):
    return {
        "payloadTypeId": payload_type,
        "payloadEncoding": demo_client.PAYLOAD_ENCODING,
        "payload": json.dumps(value),
    }


class ResultPayloadTest(unittest.TestCase):
    def test_decodes_hosted_transition_map_result(self):
        terminal = {
            "outcome": encoded("APPROVED", "java.lang.String"),
            "restaurantStatus": encoded("ACCEPTED", "java.lang.String"),
            "resolvedAt": encoded(1791535931.833713, "java.lang.Double"),
        }
        response = {"resultPayload": encoded({"items": terminal}, "java.util.Map")}
        args = SimpleNamespace(base_url="http://localhost:8081", tenant_id="restaurant-demo", token="token")

        with patch.object(demo_client, "request", return_value=response), patch.object(
            demo_client, "auth", return_value="token"
        ):
            result = demo_client.result_payload(args, "execution-1")

        self.assertEqual("APPROVED", result["outcome"])
        self.assertEqual("ACCEPTED", result["restaurantStatus"])
        self.assertEqual(1791535931.833713, result["resolvedAt"])

    def test_decodes_direct_domain_object_result(self):
        payload = {"outcome": "DECLINED", "restaurantStatus": "DECLINED"}
        envelope = encoded(payload, "org.pipelineframework.restaurantapproval.common.domain.TerminalOrderState")

        self.assertEqual(payload, demo_client.decode_payload(envelope))

    def test_rejects_unexpected_payload_encoding(self):
        envelope = encoded("APPROVED", "java.lang.String")
        envelope["payloadEncoding"] = "application/json"

        with self.assertRaisesRegex(ValueError, "Unsupported result payload encoding"):
            demo_client.decode_payload(envelope)


if __name__ == "__main__":
    unittest.main()
