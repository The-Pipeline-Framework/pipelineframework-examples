package org.pipelineframework.examples.shared.coordinator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig.RegisteredRestTargetConfig;
import org.pipelineframework.orchestrator.RegisteredRestWorkerTargetSource;

/** The proof substitutes the provider's versioned object with an atomically replaced local file. */
@ApplicationScoped
class ProofFileWorkerTargetSource implements RegisteredRestWorkerTargetSource {
    @ConfigProperty(name = "tpf.proof.worker-target-file")
    Optional<String> targetFile;

    @Override
    public boolean enabled() {
        return targetFile.isPresent();
    }

    @Override
    public Uni<Map<String, RegisteredRestTargetConfig>> snapshot() {
        return Uni.createFrom().item(() -> {
            var properties = new Properties();
            try (var reader = Files.newBufferedReader(Path.of(targetFile.orElseThrow()))) {
                properties.load(reader);
            } catch (IOException failure) {
                throw new IllegalStateException("Proof route file unavailable", failure);
            }
            var config = new SmallRyeConfigBuilder().withMapping(PipelineOrchestratorConfig.class)
                .withValidateUnknown(false).withDefaultValue("pipeline.orchestrator.mode", "QUEUE_ASYNC");
            properties.forEach((key, value) -> config.withDefaultValue(key.toString(), value.toString()));
            return Map.copyOf(config.build().getConfigMapping(PipelineOrchestratorConfig.class).worker().targets());
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    @Override
    public Uni<Void> prepareCredential(String reference) {
        return Uni.createFrom().voidItem(); // Test workers use pinned local process credentials.
    }
}
