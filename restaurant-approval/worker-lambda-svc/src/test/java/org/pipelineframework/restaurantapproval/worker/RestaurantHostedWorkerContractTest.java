package org.pipelineframework.restaurantapproval.worker;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.restaurantapproval.common.domain.*;
import org.pipelineframework.restaurantapproval.validate_order_request.service.ProcessValidateOrderRequestService;
import org.pipelineframework.restaurantapproval.create_pending_approval.service.ProcessCreatePendingApprovalService;
import org.pipelineframework.restaurantapproval.finalize_restaurant_decision.service.ProcessFinalizeRestaurantDecisionService;

/** Real compiled canonical codecs and unchanged authored service semantics; no Coordinator substitute. */
class RestaurantHostedWorkerContractTest {
    @ParameterizedTest @ValueSource(booleans = {true, false})
    void canonicalOrderDecisionAndTerminalValuesPreserveAcceptedAndDeclinedSemantics(boolean accepted) throws Exception {
        PipelineContractDescriptor contract;
        try (var input = getClass().getResourceAsStream("/META-INF/pipeline/pipeline-contract.json")) {
            assertNotNull(input);
            contract = PipelineJson.mapper().readValue(input, PipelineContractDescriptor.class);
        }
        assertEquals(3, contract.steps().size());
        assertTrue(contract.capabilities().localTransitionExecution());
        assertTrue(contract.canonicalTypes().containsKey("RestaurantDecision"));
        var request = new PlaceRestaurantOrderRequest(UUID.fromString("f2347002-f1be-444b-a399-d8d493c58682"),
                "Ada", "Cafe TPF", "Soup", new BigDecimal("12.50"), "EUR");
        var inputCodec = codec(contract, "PlaceRestaurantOrderRequest");
        var decoded = (PlaceRestaurantOrderRequest) inputCodec.decode(inputCodec.encode(request));
        assertEquals(request.requestId(), decoded.requestId());
        assertEquals(request.totalAmount(), decoded.totalAmount());
        var validated = new ProcessValidateOrderRequestService().process(decoded).await().atMost(Duration.ofSeconds(2));
        var pending = new ProcessCreatePendingApprovalService().process(validated).await().atMost(Duration.ofSeconds(2));
        assertEquals(request.requestId(), pending.requestId());
        var pendingCodec = codec(contract, "PendingRestaurantApproval");
        var retained = (PendingRestaurantApproval) pendingCodec.decode(pendingCodec.encode(pending));
        assertEquals(pending.orderId(), retained.orderId());
        RestaurantDecision decision = accepted
                ? new RestaurantDecision.Accepted(new RestaurantOrderAccepted(retained.orderId(), Instant.parse("2026-11-20T10:00:00Z"), "Approved"))
                : new RestaurantDecision.Declined(new RestaurantOrderDeclined(retained.orderId(), Instant.parse("2026-11-20T10:00:00Z"), "Declined", "Kitchen busy"));
        var legacy = PipelineJson.mapper().readTree(PipelineJson.mapper().writeValueAsBytes(decision));
        assertEquals(accepted ? "accepted" : "declined", legacy.path("type").asText());
        assertTrue(legacy.has(accepted ? "accepted" : "declined"));
        assertEquals(decision, PipelineJson.mapper().treeToValue(legacy, RestaurantDecision.class));
        var decisionCodec = codec(contract, "RestaurantDecision");
        var wire = decisionCodec.encode(decision);
        // Pin the public canonical JSON shape consumed by independent server-side clients.
        var envelope = PipelineJson.mapper().readTree(wire.payload());
        assertEquals("org.pipelineframework.orchestrator.TypedDurablePayload", wire.payloadTypeId());
        assertEquals("application/tpf-transition+json", wire.payloadEncoding());
        assertEquals("RestaurantDecision", envelope.path("canonicalTypeId").asText());
        var canonicalDecision = PipelineJson.mapper().readTree(java.util.Base64.getDecoder().decode(envelope.path("payload").asText()));
        assertEquals(accepted ? "accepted" : "declined", canonicalDecision.path("discriminator").asText());
        assertEquals(retained.orderId().toString(), canonicalDecision.path("value").path("orderId").asText());
        assertEquals(accepted ? "Accepted" : "Declined", canonicalDecision.path("case").asText());
        var restored = (RestaurantDecision) decisionCodec.decode(wire);
        assertEquals(decision, restored);
        var terminal = new ProcessFinalizeRestaurantDecisionService().process(restored).await().atMost(Duration.ofSeconds(2));
        var terminalCodec = codec(contract, "TerminalOrderState");
        var result = (TerminalOrderState) terminalCodec.decode(terminalCodec.encode(terminal));
        assertEquals(retained.orderId(), result.orderId());
        assertEquals(accepted ? "APPROVED" : "DECLINED", result.outcome());
        assertEquals(accepted ? "ACCEPTED" : "DECLINED", result.restaurantStatus());
    }
    private static CompiledCanonicalTransitionPayloadCodec codec(PipelineContractDescriptor contract, String type) {
        return new CompiledCanonicalTransitionPayloadCodec(new JsonTransitionPayloadCodec(), contract, type, type);
    }
}
