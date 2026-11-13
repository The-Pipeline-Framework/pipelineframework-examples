package org.pipelineframework.examples.shared.coordinator;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineControlPlane;

/** Package-absence evidence from the real CDI application; not an execution acceptance gate. */
@ApplicationScoped
public class CoordinatorBootstrapEvidence {
    @Inject PipelineControlPlane controlPlane;

    void record(@Observes StartupEvent started) throws Exception {
        String destination = System.getProperty("tpf.proof.bootstrap-evidence");
        if (destination == null) return;
        var loader = Thread.currentThread().getContextClassLoader();
        var forbidden = Files.readAllLines(Path.of(System.getProperty("tpf.proof.forbidden-classes")));
        for (String name : forbidden) {
            try {
                Class.forName(name, false, loader);
                throw new IllegalStateException("Application class leaked into Coordinator: " + name);
            } catch (ClassNotFoundException expected) {
                // This is the required production packaging boundary.
            }
        }
        var resources = Files.readAllLines(Path.of(System.getProperty("tpf.proof.forbidden-resources")));
        for (String resource : resources) {
            if (loader.getResource(resource) != null) {
                throw new IllegalStateException("Application resource leaked into Coordinator: " + resource);
            }
        }
        for (String resource : List.of("pipeline.yaml", "META-INF/pipeline/pipeline-contract.json",
            "META-INF/pipeline/order.json", "fixture-a1.marker", "fixture-a2.marker", "fixture-b.marker")) {
            if (loader.getResource(resource) != null) throw new IllegalStateException("Application resource leaked: " + resource);
        }
        var evidence = new LinkedHashMap<String, Object>();
        evidence.put("phase", "CDI_BOOTSTRAP_ONLY");
        evidence.put("controlPlaneBean", controlPlane.getClass().getName());
        evidence.put("absentClasses", forbidden);
        evidence.put("absentResources", resources);
        evidence.put("classLoader", loader.getClass().getName());
        PipelineJson.mapper().writeValue(Path.of(destination).toFile(), evidence);
    }
}
