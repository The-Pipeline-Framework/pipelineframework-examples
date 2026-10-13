package org.pipelineframework.awsconformance.bootstrap;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptorValidator;
import static org.assertj.core.api.Assertions.assertThat;

class ReleaseFixtureInstallerTest {
    @Test
    void registryFixtureDescriptorIsValidatedAgainstActualCompilerProducedContract() throws Exception {
        var contract = PipelineJson.mapper().readValue(Path.of(
            "../generated-fixture/target/classes/META-INF/pipeline/pipeline-contract.json").toFile(), PipelineContractDescriptor.class);
        var descriptor = new ReleaseFixtureInstaller().descriptor(contract, "conformance-release-1",
            Path.of("../generated-fixture/target/function.zip").toAbsolutePath().normalize().toUri().toASCIIString(), "sha256:" + "a".repeat(64));
        new PipelineReleaseDescriptorValidator().validate(descriptor, contract);
        assertThat(descriptor.contractVersion()).isEqualTo(contract.contractVersion());
        assertThat(descriptor.artifacts().getFirst().stepIds()).containsExactly("First Approval", "Second Approval");
    }
}
