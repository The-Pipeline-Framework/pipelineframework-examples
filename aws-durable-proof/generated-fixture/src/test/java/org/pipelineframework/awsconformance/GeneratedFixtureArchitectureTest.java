package org.pipelineframework.awsconformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.AwsDurableInputDecoder;
import org.pipelineframework.awsconformance.common.dto.ApprovalDto;

class GeneratedFixtureArchitectureTest {
    @Test
    void compilerProducesBothAwaitBoundariesAndTheirMetadata() throws Exception {
        var contract = new ObjectMapper().readTree(Path.of("target/classes/META-INF/pipeline/pipeline-contract.json").toFile());
        assertThat(contract.path("pipelineId").asText()).isEqualTo("org.pipelineframework.awsconformance");
        assertThat(contract.path("platform").asText()).isEqualTo("FUNCTION");
        assertThat(contract.path("steps").size()).isEqualTo(2);
        for (var step : contract.path("steps")) {
            assertThat(step.path("deferredCompletion").path("transportType").asText()).isEqualTo("interaction-api");
            String runtimeClass = step.path("runtimeClass").asText();
            assertThat(runtimeClass).startsWith("org.pipelineframework.awsconformance.").endsWith("DeferredCompletionStep");
            assertThat(Class.forName(runtimeClass)).isNotNull();
        }
        assertThat(Files.exists(Path.of("src/main/resources/META-INF/pipeline")))
            .as("contract and order metadata must come from compilation").isFalse();
    }

    @Test
    void generatedInputDecoderUsesTheActualRestRepresentation() throws Exception {
        var decoderType = Class.forName("org.pipelineframework.awsconformance.orchestrator.service.AwsDurablePipelineInputDecoder");
        assertThat(AwsDurableInputDecoder.class.isAssignableFrom(decoderType)).isTrue();
        var decoder = (AwsDurableInputDecoder) decoderType.getConstructor().newInstance();
        var mapper = decoderType.getDeclaredField("mapper");
        mapper.setAccessible(true);
        mapper.set(decoder, new ObjectMapper());
        assertThat(decoder.decode("{\"request\":\"approval-1\"}"))
            .isEqualTo(new ApprovalDto("approval-1"));
        try (var authored = Files.walk(Path.of("src/main/java"))) {
            assertThat(authored.noneMatch(path -> path.getFileName().toString().equals("AwsDurablePipelineInputDecoder.java")))
                .as("the production decoder seam must not be authored by the fixture").isTrue();
        }
    }
}
