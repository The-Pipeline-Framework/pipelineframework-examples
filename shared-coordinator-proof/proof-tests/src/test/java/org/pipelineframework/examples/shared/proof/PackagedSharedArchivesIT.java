package org.pipelineframework.examples.shared.proof;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.time.Duration;
import java.util.Set;
import java.util.jar.JarInputStream;
import java.util.zip.ZipFile;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PackagedSharedArchivesIT {
    private static final Set<String> APPLICATION_PACKAGES = Set.of(
        "org/pipelineframework/examples/shared/expense/", "org/pipelineframework/examples/shared/parcel/");

    @Test
    void productionSharedArchivesContainNoFixtureClassesOrCompiledPipeline() throws Exception {
        Path root = Path.of("../../aws-durable-proof");
        for (String module : List.of("shared-action", "shared-await-lifecycle")) {
            Path artifact = root.resolve(module).resolve("target/function.zip");
            try (var zip = new ZipFile(artifact.toFile())) {
                assertNotNull(zip.getEntry("io/quarkus/runner/ApplicationImpl.class"), artifact.toString());
                var entries = zip.stream().filter(entry -> !entry.isDirectory()).toList();
                assertTrue(entries.stream().anyMatch(entry -> entry.getName().startsWith("lib/")));
                for (var entry : entries) {
                    rejectApplicationContent(entry.getName(), artifact);
                    if (!entry.getName().endsWith(".jar")) continue;
                    try (var jar = new JarInputStream(zip.getInputStream(entry))) {
                        for (var member = jar.getNextJarEntry(); member != null; member = jar.getNextJarEntry()) {
                            if (!member.isDirectory()) rejectApplicationContent(member.getName(), artifact);
                        }
                    }
                }
            }
        }
    }

    @Test
    void dedicatedLifecycleZipServesWorkerCallbacksAndHidesAdministration() throws Exception {
        Path module = Path.of("../../aws-durable-proof/shared-await-lifecycle").toAbsolutePath().normalize();
        var settings = Map.of(
            "pipeline.orchestrator.mode", "QUEUE_ASYNC",
            "pipeline.orchestrator.idempotency-policy", "CLIENT_KEY_REQUIRED",
            "pipeline.orchestrator.state-provider", "memory",
            "pipeline.orchestrator.dispatcher-provider", "event",
            "pipeline.orchestrator.dlq-provider", "log",
            "pipeline.orchestrator.control-plane.enabled", "true",
            "pipeline.orchestrator.control-plane.require-remote-worker", "true",
            "pipeline.orchestrator.control-plane.payload-materialization", "SERIALIZED",
            "pipeline.orchestrator.worker.targeting-mode", "registered-rest",
            "quarkus.otel.sdk.disabled", "true");
        String route = "/pipeline/coordinator/await/registrations";
        String admin = "/tpf/control-plane/tenants/proof/executions";
        try (var function = PackagedBootstrapIT.start(module, settings, Path.of("target/shared-lifecycle.log"));
             var client = HttpClient.newHttpClient()) {
            PackagedBootstrapIT.awaitStatus(function, admin, null, 404);
            var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + function.port() + route))
                .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}")).build();
            assertEquals(400, client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode());
        }
    }

    private static void rejectApplicationContent(String name, Path artifact) {
        assertFalse(APPLICATION_PACKAGES.stream().anyMatch(name::startsWith), () -> artifact + " contains " + name);
        assertFalse(name.equals("META-INF/pipeline/pipeline-contract.json"), () -> artifact + " contains " + name);
    }
}
