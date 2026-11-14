package org.pipelineframework.examples.openapiproof;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.UUID;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.pipelineframework.blocks.openapi.mapping.OpenApiMappingState;
import org.pipelineframework.blocks.openapi.mapping.OpenApiRepresentationMappingProposal;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.examples.openapiproof.connector.MappingProposalRecorder;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.invocation.PipelineInvocationSteps;
import org.pipelineframework.step.StepOneToOne;

@QuarkusTest
class OpenApiMappingCompositionIT {
    @Inject @Any Instance<StepOneToOne<OpenApiMappingState, OpenApiRepresentationMappingProposal>> pipelines;
    @Inject PipelineInvocationRuntime runtime;
    @Inject MappingProposalRecorder proposals;

    @Test
    void recursiveRetryReturnsItsProposalThroughTheOriginalCaller() throws Exception {
        var state = OpenApiMappingState.start("evidence.lookup:response", "RESPONSE",
            "{\"type\":\"object\",\"properties\":{\"evidence\":{\"type\":\"string\"}},\"required\":[\"evidence\"]}",
            "{\"type\":\"object\",\"properties\":{\"payload\":{\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"}},\"required\":[\"value\"]}},\"required\":[\"payload\"]}",
            3);
        var caller = pipelines.stream()
            .filter(step -> ((PipelineInvocationSteps.LinkedInvocation) step).linkedDefinition().definitionId().equals("mapping-caller"))
            .findFirst().orElseThrow(() -> new AssertionError("generated mapping-caller not found"));
        proposals.reset();
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("mapping-proof", UUID.randomUUID().toString(), 0));
        try {
            var result = runtime.invokeStepUni(() -> caller.applyOneToOne(state))
                .await().atMost(Duration.ofSeconds(10));
            assertTrue(result.valid());
            assertEquals(2, result.turns());
            assertEquals(state.operationIdentity(), result.operationIdentity());
            assertEquals(state.sourceSchemaFingerprint(), result.sourceSchemaFingerprint());
            assertEquals(state.targetSchemaFingerprint(), result.targetSchemaFingerprint());
            assertEquals("payload.value", PipelineJson.mapper().readTree(result.mappingOptionsJson())
                .path("fields").path("evidence").asText());
            assertEquals(2, proposals.inputs().size());
            assertTrue(proposals.inputs().get(0).diagnostics().isEmpty());
            assertTrue(proposals.inputs().get(1).diagnostics().toString().contains("does not exist"));
        } finally {
            PipelineExecutionContextHolder.clear();
        }
    }
}
