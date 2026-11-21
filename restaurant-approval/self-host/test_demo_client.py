import base64
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

    def test_decodes_v3_canonical_terminal_result(self):
        terminal = {"outcome": "APPROVED", "restaurantStatus": "ACCEPTED"}
        canonical = {"canonicalTypeId": "TerminalOrderState", "typeExpressionFingerprint": "retained-type",
            "catalogFingerprint": "retained-catalog", "encoding": "application/tpf-canonical+json",
            "encodingVersion": 1, "payload": base64.b64encode(json.dumps(terminal).encode()).decode()}
        envelope = encoded(canonical, "org.pipelineframework.orchestrator.TypedDurablePayload")
        self.assertEqual(terminal, demo_client.decode_payload(envelope))
        canonical["canonicalTypeId"] = "ForeignType"
        with self.assertRaisesRegex(ValueError, "Unexpected Restaurant terminal canonical type"):
            demo_client.decode_payload(encoded(canonical, "org.pipelineframework.orchestrator.TypedDurablePayload"))

    def test_submission_uses_the_v3_domain_input_instead_of_the_legacy_dto(self):
        args = SimpleNamespace(base_url="http://localhost:8081", tenant_id="restaurant-demo", pipeline_id="restaurant", control_plane_token="test")
        with patch.object(demo_client, "request", return_value={"executionId": "execution"}) as request:
            demo_client.submit_order(args, "Ada", "Cafe", quiet=True)
        payload = request.call_args.kwargs["body"]["inputPayload"]
        self.assertEqual("org.pipelineframework.restaurantapproval.common.domain.PlaceRestaurantOrderRequest", payload["payloadTypeId"])
        self.assertEqual("Ada", json.loads(payload["payload"])["customerName"])

    def test_completion_supplies_the_canonical_rest_union_discriminator(self):
        args = SimpleNamespace(base_url="http://localhost:8081", tenant_id="restaurant-demo", control_plane_token="token")
        interaction = {"interactionId": "interaction", "requestPayload": {"orderId": "12345678-1234-4234-8234-123456789012"}}
        with patch.object(demo_client, "request", return_value={"interactionId": "interaction"}) as request:
            demo_client.complete(args, interaction, "declined")
        self.assertEqual(demo_client.DECISION_TYPE, request.call_args.kwargs["body"]["responsePayload"]["payloadTypeId"])
        decision = demo_client.decode_payload(request.call_args.kwargs["body"]["responsePayload"])
        self.assertEqual("declined", decision["discriminator"])
        self.assertEqual(interaction["requestPayload"]["orderId"], decision["declined"]["orderId"])

    def test_rejects_unexpected_payload_encoding(self):
        envelope = encoded("APPROVED", "java.lang.String")
        envelope["payloadEncoding"] = "application/json"

        with self.assertRaisesRegex(ValueError, "Unsupported result payload encoding"):
            demo_client.decode_payload(envelope)


if __name__ == "__main__":
    unittest.main()
