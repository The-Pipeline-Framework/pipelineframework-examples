package org.pipelineframework.awsconformance.items;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.file.Path;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitStreamOneToOneStep;
import org.pipelineframework.aws.durable.AwsDurableInputDecoder;
import org.pipelineframework.awsconformance.items.common.dto.ApprovalDto;

class GeneratedItemFixtureArchitectureTest {
    @Test
    void compilerRetainsStreamingOperationAndProducesPerItemCompletion() throws Exception {
        var contract = new ObjectMapper().readTree(Path.of("target/classes/META-INF/pipeline/pipeline-contract.json").toFile());
        assertThat(contract.path("pipelineId").asText()).isEqualTo("org.pipelineframework.awsconformance.items");
        assertThat(contract.path("steps").size()).isEqualTo(1);
        var step = contract.path("steps").get(0);
        assertThat(step.path("cardinality").asText()).isEqualTo("ONE_TO_MANY");
        assertThat(step.path("deferredCompletion").path("transportType").asText()).isEqualTo("interaction-api");
        assertThat(AwaitStreamOneToOneStep.class.isAssignableFrom(Class.forName(step.path("runtimeClass").asText()))).isTrue();
    }

    @Test
    void generatedDecoderAndOperationClientUseTheSamePrivateRepresentation() throws Exception {
        var decoderType = Class.forName("org.pipelineframework.awsconformance.items.orchestrator.service.AwsDurablePipelineInputDecoder");
        var decoder = (AwsDurableInputDecoder) decoderType.getConstructor().newInstance();
        var mapper = decoderType.getDeclaredField("mapper");
        mapper.setAccessible(true);
        mapper.set(decoder, new ObjectMapper());
        assertThat(decoder.decode("{\"request\":\"101\"}")).isEqualTo(new ApprovalDto("101"));
        var client = Class.forName("org.pipelineframework.awsconformance.items.pipeline.pipeline.ProcessItemApprovalsRestClientStep");
        assertThat(client.getMethod("applyOneToMany", ApprovalDto.class)).isNotNull();
    }
}
