package org.pipelineframework.examples.shared.proof;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.RestTransitionWorkerProtocol;
import org.pipelineframework.orchestrator.TransitionWorkerSignature;
import static org.junit.jupiter.api.Assertions.*;

/** Real production CDI/bootstrap evidence, deliberately separate from execution acceptance. */
class PackagedBootstrapIT {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();

    @Test void productionPackagesBootWithApplicationFreeCoordinator() throws Exception {
        Path root = Path.of("..").toAbsolutePath().normalize();
        Path evidence = Path.of("target/bootstrap-evidence").toAbsolutePath();
        Files.createDirectories(evidence);
        var classes = new TreeSet<String>();
        var resources = new TreeSet<String>();
        var workers = new LinkedHashMap<String, RunningPackage>();
        var contracts = new LinkedHashMap<String, JsonNode>();
        var inventory = new LinkedHashMap<String, Object>();
        try {
            for (String app : List.of("a1", "a2", "b")) {
                Path module = root.resolve("application-" + app);
                Path artifact = artifact(module);
                try (var jar = new JarFile(artifact.toFile())) {
                    jar.stream().filter(e -> e.getName().endsWith(".class"))
                        .map(e -> e.getName().replace('/', '.').replaceFirst("\\.class$", ""))
                        .forEach(classes::add);
                    jar.stream().filter(e -> !e.isDirectory() && e.getName().startsWith("META-INF/pipeline/"))
                        .map(e -> e.getName()).forEach(resources::add);
                    try (var input = jar.getInputStream(jar.getJarEntry("META-INF/pipeline/pipeline-contract.json"))) {
                        contracts.put(app, JSON.readTree(input));
                    }
                }
                if (app.startsWith("a")) {
                    JsonNode descriptor = contracts.get(app).path("steps").get(0)
                        .path("deferredCompletion").path("runtimeDescriptor");
                    assertEquals(1, descriptor.path("schemaVersion").asInt());
                    assertEquals("Pending", descriptor.path("canonicalInputTypeId").asText());
                    assertEquals("Approved", descriptor.path("canonicalOutputTypeId").asText());
                    assertEquals("ApprovalReply", descriptor.path("completionPayloadTypeId").asText());
                    assertFalse(descriptor.has("transportConfig"), "Worker transport settings must stay out of the compiled contract");
                    assertTrue(contracts.get(app).path("steps").get(0).path("deferredCompletion")
                        .path("transportConfigFingerprint").asText().matches("[0-9a-f]{64}"),
                        "The compiled contract must retain the transport configuration fingerprint");
                }
                inventory.put(app, inventory(module));
                var worker = start(module, Map.of("tpf.proof.worker.secret", "local-proof-worker-" + app,
                    "pipeline.orchestrator.release-version", "release-" + app,
                    "pipeline.orchestrator.worker.artifact-id", "worker-" + app,
                    "pipeline.orchestrator.worker.artifact-digest", "sha256:" + digest(artifact)), evidence.resolve(app + ".log"));
                workers.put(app, worker);
                // An unsigned request must reach the production worker's authentication guard.
                awaitStatus(worker, RestTransitionWorkerProtocol.CAPABILITIES_PATH, null, 401);
                verifyCapability(worker, app, contracts.get(app), digest(artifact), evidence);
            }
            for (String name : classes) assertThrows(ClassNotFoundException.class, () -> Class.forName(name));
            Path forbiddenClasses = evidence.resolve("forbidden-classes.txt");
            Path forbiddenResources = evidence.resolve("forbidden-resources.txt");
            Files.write(forbiddenClasses, classes);
            Files.write(forbiddenResources, resources);
            var properties = new LinkedHashMap<String, String>();
            properties.put("tpf.proof.admin.token", "local-proof-admin");
            properties.put("tpf.proof.bootstrap-evidence", evidence.resolve("coordinator.json").toString());
            properties.put("tpf.proof.forbidden-classes", forbiddenClasses.toString());
            properties.put("tpf.proof.forbidden-resources", forbiddenResources.toString());
            for (var entry : workers.entrySet()) {
                String app = entry.getKey();
                JsonNode contract = contracts.get(app);
                String prefix = "pipeline.orchestrator.worker.targets." + app + ".";
                properties.put("tpf.proof.worker." + app + ".secret", "local-proof-worker-" + app);
                var target = Map.of("tenant-id", app.equals("b") ? "tenant-b" : "tenant-a",
                    "pipeline-id", contract.path("pipelineId").asText(),
                    "contract-version", contract.path("contractVersion").asText(),
                    "release-version", "release-" + app, "worker-id", "worker-" + app,
                    "endpoint", "http://127.0.0.1:" + entry.getValue().port(),
                    "shared-secret-ref", "sys:tpf.proof.worker." + app + ".secret", "artifact-id", "worker-" + app,
                    "artifact-digest", "sha256:" + digest(artifact(root.resolve("application-" + app))));
                target.forEach((key, value) -> properties.put(prefix + key, value));
            }
            Path coordinator = root.resolve("coordinator");
            inventory.put("coordinator", inventory(coordinator));
            try (var process = start(coordinator, properties, evidence.resolve("coordinator.log"))) {
                awaitStatus(process, "/tpf/control-plane/tenants/tenant-a/interactions/pending", "local-proof-admin", 200);
                JsonNode report = JSON.readTree(evidence.resolve("coordinator.json").toFile());
                assertEquals("CDI_BOOTSTRAP_ONLY", report.path("phase").asText());
                assertEquals(classes.size(), report.path("absentClasses").size());
                assertEquals(resources.size(), report.path("absentResources").size());
            }
            inventory.put("phase", "CDI_BOOTSTRAP_ONLY");
            JSON.writerWithDefaultPrettyPrinter().writeValue(evidence.resolve("packaged-artifacts.json").toFile(), inventory);
        } finally {
            for (var worker : workers.values()) worker.close();
            inventory.putIfAbsent("phase", "CDI_BOOTSTRAP_ATTEMPT");
            JSON.writerWithDefaultPrettyPrinter().writeValue(evidence.resolve("packaged-artifacts.json").toFile(), inventory);
        }
    }

    private static void verifyCapability(RunningPackage worker, String app, JsonNode contract,
                                         String artifactDigest, Path evidence) throws Exception {
        String timestamp = java.time.Instant.now().toString();
        String nonce = java.util.UUID.randomUUID().toString();
        String path = RestTransitionWorkerProtocol.CAPABILITIES_PATH;
        String signature = TransitionWorkerSignature.sign("local-proof-worker-" + app,
            RestTransitionWorkerProtocol.CAPABILITIES_METHOD, path, timestamp, nonce, new byte[0]);
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + worker.port() + path))
            .timeout(Duration.ofSeconds(5)).GET()
            .header(TransitionWorkerSignature.TIMESTAMP_HEADER, timestamp)
            .header(TransitionWorkerSignature.NONCE_HEADER, nonce)
            .header(TransitionWorkerSignature.SIGNATURE_HEADER, signature).build();
        var response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), response.body());
        JsonNode capability = JSON.readTree(response.body());
        assertEquals(contract.path("pipelineId"), capability.path("pipelineId"));
        assertEquals(contract.path("contractVersion"), capability.path("contractVersion"));
        assertEquals("release-" + app, capability.path("releaseVersion").asText());
        assertEquals("worker-" + app, capability.path("artifactId").asText());
        assertEquals("sha256:" + artifactDigest, capability.path("artifactDigest").asText());
        JSON.writerWithDefaultPrettyPrinter().writeValue(evidence.resolve(app + "-capability.json").toFile(), capability);
    }

    static Path artifact(Path module) throws Exception {
        try (var files = Files.list(module.resolve("target/quarkus-app/app"))) {
            var artifacts = files.filter(p -> p.getFileName().toString().endsWith(".jar")).toList();
            assertEquals(1, artifacts.size(), "Exactly one application JAR must be packaged: " + module);
            return artifacts.get(0);
        }
    }

    static List<Map<String, String>> inventory(Path module) throws Exception {
        var result = new ArrayList<Map<String, String>>();
        try (var files = Files.walk(module.resolve("target/quarkus-app"))) {
            for (Path file : files.filter(p -> p.toString().endsWith(".jar")).sorted().toList()) {
                result.add(Map.of("path", file.toString(), "sha256", digest(file)));
            }
        }
        assertFalse(result.isEmpty());
        return result;
    }
    static String digest(Path file) throws Exception {
        return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
    }
    static RunningPackage start(Path module, Map<String, String> properties, Path log) throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        return start(module, properties, log, port);
    }
    static RunningPackage start(Path module, Map<String, String> properties, Path log, int port) throws Exception {
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin/java").toString());
        command.add("-Dquarkus.http.port=" + port);
        properties.forEach((key, value) -> command.add("-D" + key + "=" + value));
        command.addAll(List.of("-jar", module.resolve("target/quarkus-app/quarkus-run.jar").toString()));
        var builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().keySet().removeIf(key -> key.startsWith("AWS_") || key.startsWith("PIPELINE_")
            || key.equals("JAVA_TOOL_OPTIONS") || key.equals("JDK_JAVA_OPTIONS"));
        return new RunningPackage(builder.start(), port, log);
    }
    static void awaitStatus(RunningPackage process, String path, String bearer, int status) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(40);
        while (System.nanoTime() < end && process.process().isAlive()) {
            try {
                var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + process.port() + path))
                    .timeout(Duration.ofSeconds(2)).GET();
                if (bearer != null) request.header("Authorization", "Bearer " + bearer);
                var response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
                assertEquals(status, response.statusCode(), response.body());
                return;
            } catch (java.io.IOException transientStartup) { Thread.sleep(100); }
        }
        fail("Production package did not become ready: " + Files.readString(process.log()));
    }
    record RunningPackage(Process process, int port, Path log) implements AutoCloseable {
        public void close() throws InterruptedException {
            process.destroy();
            if (!process.waitFor(5, TimeUnit.SECONDS)) { process.destroyForcibly(); process.waitFor(5, TimeUnit.SECONDS); }
        }
    }
}
