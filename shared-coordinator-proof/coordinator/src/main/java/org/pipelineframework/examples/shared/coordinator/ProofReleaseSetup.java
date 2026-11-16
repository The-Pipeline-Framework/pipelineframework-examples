package org.pipelineframework.examples.shared.coordinator;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.time.Duration;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.release.*;

/** Fixture-only deployment observations; execution still enters through the production control plane. */
@ApplicationScoped
public class ProofReleaseSetup {
    @Inject PipelineReleaseRegistrar registrar;
    @Inject PipelineReleaseRegistry registry;

    void seed(@Observes StartupEvent started) throws Exception {
        String manifest = System.getProperty("tpf.proof.release-setup");
        if (manifest == null) return;
        var root = PipelineJson.mapper().readTree(Path.of(manifest).toFile());
        for (var item : root.path("releases")) {
            var binding = PipelineJson.mapper().treeToValue(item.path("binding"), PipelineReleaseEntryBinding.class);
            var verified = registrar.validate(binding.tenantId(), binding.pipelineId(),
                item.path("descriptorPath").asText(), item.path("registeredAtEpochMs").asLong());
            binding.requireRelease(verified);
            registry.register(verified).await().atMost(Duration.ofSeconds(20));
            registry.registerEntryBinding(binding).await().atMost(Duration.ofSeconds(20));
        }
        for (var item : root.path("activations")) {
            String tenant = item.path("tenantId").asText();
            String pipeline = item.path("pipelineId").asText();
            String release = item.path("releaseVersion").asText();
            var expected = registry.currentActivationObservation(tenant, pipeline).await().atMost(Duration.ofSeconds(20));
            String actual = expected.identifiedEvent().map(CurrentActivationEvent::releaseVersion).orElse("");
            if (!actual.equals(item.path("expectedReleaseVersion").asText())) {
                throw new IllegalStateException("Fixture activation precondition differs from retained head");
            }
            var record = registry.get(tenant, pipeline, release).await().atMost(Duration.ofSeconds(20)).orElseThrow();
            registry.activateOnceIfCurrent(new ActivationOperationCommand(item.path("operationKey").asText(),
                record, item.path("activatedAtEpochMs").asLong()), expected).await().atMost(Duration.ofSeconds(20));
        }
    }
}
