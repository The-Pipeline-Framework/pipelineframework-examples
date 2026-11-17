package org.pipelineframework.research;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

import io.quarkus.runtime.Startup;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.deployment.release.CredentialResolver;
import org.pipelineframework.deployment.release.ResolverProfile;
import org.pipelineframework.orchestrator.NativeWorkerReleaseBinding;
import org.pipelineframework.orchestrator.NativeWorkerReleaseBinding.Target;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

/** Supplies trusted deployment inputs to the real coordinator. This fixture owns no execution policy. */
@ApplicationScoped
public class PreparedBinding {
    @Produces
    @Startup
    @Singleton
    NativeWorkerReleaseBinding binding(@ConfigProperty(name = "proof.launch") Path launch) throws IOException {
        var json = PipelineJson.mapper();
        var input = json.readTree(Files.readAllBytes(launch));
        var target = json.treeToValue(input.required("target"), Target.class);
        try (var resource = getClass().getClassLoader().getResourceAsStream(PipelineContractDescriptor.RESOURCE_PATH)) {
            if (resource == null || !json.readValue(resource, PipelineContractDescriptor.class).equals(target.coordinatorContract())) {
                throw new IllegalStateException("Packaged Contract differs from the prepared coordinator Contract");
            }
        }
        // The Release verifier requires fresh staging, including on a restart over retained state.
        return NativeWorkerReleaseBinding.prepare(target, Path.of(input.required("descriptor").asText()),
            Files.createTempDirectory(launch.getParent(), "packaged-binding-"),
            ResolverProfile.localOnly(), CredentialResolver.NONE);
    }
}
