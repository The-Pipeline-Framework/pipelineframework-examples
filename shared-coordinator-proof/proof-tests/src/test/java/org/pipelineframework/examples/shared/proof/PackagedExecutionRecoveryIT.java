package org.pipelineframework.examples.shared.proof;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.orchestrator.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.pipelineframework.examples.shared.proof.PackagedBootstrapIT.*;

/** Complete real-process proof; never substitutes a fixture engine or silently skips persistence. */
class PackagedExecutionRecoveryIT {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String ADMIN = "local-proof-admin";
    private static final String SHARED_KEY = "same-client-key";
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private int coordinatorPort;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unrelatedReleasesAwaitAndRecoverThroughOneApplicationFreeCoordinator(boolean refreshable) throws Exception {
        Path root = Path.of("..").toAbsolutePath().normalize();
        Path evidence = Path.of("target/execution-evidence" + (refreshable ? "-refreshable" : "")).toAbsolutePath();
        Files.createDirectories(evidence);
        Files.deleteIfExists(evidence.resolve("execution-proof.json"));
        try (var socket = new ServerSocket(0)) { coordinatorPort = socket.getLocalPort(); }
        var workers = new LinkedHashMap<String, RunningPackage>();
        var workerProperties = new LinkedHashMap<String, Map<String, String>>();
        var contracts = new LinkedHashMap<String, JsonNode>();
        var descriptors = new LinkedHashMap<String, Map<String, Object>>();
        var archives = new LinkedHashMap<String, Path>();
        var properties = new LinkedHashMap<String, String>();
        var routes = new LinkedHashMap<String, String>();
        Path routeFile = evidence.resolve("routing.properties");
        var inventory = new LinkedHashMap<String, Object>();
        var forbiddenClasses = new TreeSet<String>(); var forbiddenResources = new TreeSet<String>();
        RunningPackage coordinator = null;
        try (var persistence = new PersistentProofStore()) {
            properties.putAll(persistence.properties());
            properties.put("tpf.proof.admin.token", ADMIN);
            properties.put("pipeline.orchestrator.admin.enabled", "true");
            properties.put("pipeline.orchestrator.admin.admin-token-ref", "sys:tpf.proof.admin.token");
            properties.put("pipeline.orchestrator.worker.lifecycle.stale-after", "PT30M");
            for (String app : List.of("a1", "a2", "b")) {
                Path module = root.resolve("application-" + app); Path jarPath = artifact(module);
                JsonNode contract; byte[] compiledContractBytes;
                try (var jar = new JarFile(jarPath.toFile())) {
                    try (var input = jar.getInputStream(jar.getJarEntry("META-INF/pipeline/pipeline-contract.json"))) {
                        compiledContractBytes = input.readAllBytes();
                    }
                    contract = JSON.readTree(compiledContractBytes);
                    JsonNode order = JSON.readTree(jar.getInputStream(jar.getJarEntry("META-INF/pipeline/order.json")));
                    assertEquals(1, order.path("boundarySchemaVersion").asInt());
                    assertEquals(app.equals("b") ? 4 : 3, order.path("order").size());
                    assertEquals(order.path("order").size(), order.path("stepBoundaries").size());
                    for (int index = 0; index < order.path("order").size(); index++) {
                        assertEquals(order.path("order").get(index).asText(), order.path("stepBoundaries").get(index).path("stepClass").asText());
                    }
                    if (!app.equals("b")) {
                        String[][] edges = {{"Request", "Pending"}, {"Pending", "Approved"}, {"Approved", "Receipt"}};
                        for (int index = 0; index < edges.length; index++) {
                            assertEquals(edges[index][0], order.path("stepBoundaries").get(index).path("inputTypeId").asText());
                            assertEquals(edges[index][1], order.path("stepBoundaries").get(index).path("outputTypeId").asText());
                        }
                    }
                    jar.stream().filter(e -> e.getName().endsWith(".class")).map(e -> e.getName().replace('/', '.').replaceFirst("\\.class$", "")).forEach(forbiddenClasses::add);
                    jar.stream().filter(e -> !e.isDirectory() && e.getName().startsWith("META-INF/pipeline/")).map(e -> e.getName()).forEach(forbiddenResources::add);
                }
                contracts.put(app, contract); inventory.put(app, inventory(module));
                Path archive = module.resolve("target/function.zip");
                archives.put(app, archive);
                inventory.put(app + "Archive", Map.of("path", archive.toString(), "sha256", digest(archive), "closureInventoryKey", app,
                    "compiledContractSha256", HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(compiledContractBytes))));
                String checksum = "sha256:" + digest(archive);
                var workerConfig = Map.of("tpf.proof.worker.secret", secret(app),
                    "pipeline.orchestrator.release-version", "release-" + app,
                    "pipeline.orchestrator.worker.artifact-id", "worker-" + app,
                    "pipeline.orchestrator.worker.artifact-digest", checksum,
                    "pipeline.orchestrator.worker.await-authority.base-url", origin(),
                    "pipeline.orchestrator.worker.await-authority.worker-id", "worker-" + app,
                    "pipeline.orchestrator.worker.await-authority.entry-id", "entry-" + app,
                    "pipeline.orchestrator.worker.allow-loopback-http", "true");
                // No persistence, admin, or token-signing credentials are passed to workers.
                workerProperties.put(app, workerConfig);
                var worker = start(module, workerConfig, evidence.resolve(app + ".log")); workers.put(app, worker);
                awaitStatus(worker, RestTransitionWorkerProtocol.CAPABILITIES_PATH, null, 401);
                String endpoint = "http://127.0.0.1:" + worker.port();
                String tenant = tenant(app); String pipeline = contract.path("pipelineId").asText();
                String contractVersion = contract.path("contractVersion").asText();
                String prefix = "pipeline.orchestrator.worker.targets." + app + ".";
                var target = Map.of("tenant-id", tenant, "pipeline-id", pipeline, "contract-version", contractVersion,
                    "release-version", "release-" + app, "worker-id", "worker-" + app, "endpoint", endpoint,
                    "shared-secret-ref", "sys:tpf.proof.worker." + app + ".secret", "artifact-id", "worker-" + app,
                    "artifact-digest", checksum, "entry-id", "entry-" + app);
                target.forEach((key, value) -> routes.put(prefix + key, value));
                properties.put("tpf.proof.worker." + app + ".secret", secret(app));
                Path descriptor = module.resolve("target/pipeline-release.json");
                var binding = new LinkedHashMap<String, Object>();
                binding.put("tenantId", tenant); binding.put("pipelineId", pipeline); binding.put("contractVersion", contractVersion);
                binding.put("releaseVersion", "release-" + app); binding.put("entryId", "entry-" + app);
                binding.put("artifactId", "worker-" + app); binding.put("artifactDigest", checksum);
                binding.put("provider", "local-process"); binding.put("resourceId", endpoint); binding.put("codeDigest", checksum);
                binding.put("deploymentOperationId", "deployment-" + app);
                binding.put("dispatchBinding", Map.of("protocol", "rest", "endpoint", endpoint,
                    "credentialRef", "sys:tpf.proof.worker." + app + ".secret", "workerId", "worker-" + app));
                descriptors.put(app, Map.of("descriptorPath", descriptor.toString(), "registeredAtEpochMs", 1_700_000_000_000L, "binding", binding));
            }
            if (refreshable) {
                properties.put("tpf.proof.worker-target-file", routeFile.toString());
                publishRoutes(routeFile, routes, Set.of("a1", "b"));
            } else {
                properties.putAll(routes);
            }
            Path classList = evidence.resolve("forbidden-classes.txt"), resourceList = evidence.resolve("forbidden-resources.txt");
            Files.write(classList, forbiddenClasses); Files.write(resourceList, forbiddenResources);
            properties.put("tpf.proof.forbidden-classes", classList.toString()); properties.put("tpf.proof.forbidden-resources", resourceList.toString());
            properties.put("tpf.proof.bootstrap-evidence", evidence.resolve("coordinator-before.json").toString());
            Path setup = evidence.resolve("setup-before.json");
            JSON.writeValue(setup.toFile(), Map.of("releases",
                refreshable ? List.of(descriptors.get("a1"), descriptors.get("b")) : descriptors.values(), "activations", List.of(
                activation("a1", contracts.get("a1"), ""), activation("b", contracts.get("b"), ""))));
            properties.put("tpf.proof.release-setup", setup.toString());
            coordinator = start(root.resolve("coordinator"), properties, evidence.resolve("coordinator-before.log"), coordinatorPort);
            awaitStatus(coordinator, pendingPath("tenant-a"), ADMIN, 200);
            for (String app : (refreshable ? List.of("a1", "b") : workers.keySet())) {
                registerWorker(app, workers.get(app), contracts.get(app), archives.get(app));
            }
            if (refreshable) {
                assertEquals(409, workerRegistration("a2", workers.get("a2"), contracts.get("a2"), archives.get("a2")).statusCode());
            }
            assertEquals(401, request("GET", pendingPath("tenant-a"), null, secret("a1")).statusCode());
            assertEquals(401, request("GET", "/tpf/admin/tenants/tenant-a/pipelines/" + contracts.get("a1").path("pipelineId").asText()
                + "/workers", null, secret("a1")).statusCode());
            String a1 = submit("a1", contracts.get("a1"), Map.of("requestId", "expense-1", "amount", 10));
            String b = submit("b", contracts.get("b"), Map.of("parcelId", "parcel-1", "weight", 4));
            awaitState("tenant-a", a1, "WAITING_EXTERNAL"); awaitState("tenant-b", b, "SUCCEEDED");
            JsonNode interactionA1 = pending("tenant-a", a1);
            if (refreshable) {
                publishRoutes(routeFile, routes, Set.of("a1", "a2", "b"));
                registerWorker("a2", workers.get("a2"), contracts.get("a2"), archives.get("a2"));
                assertEquals(interactionA1.path("interactionId"), pending("tenant-a", a1).path("interactionId"));
            }
            int retainedA1Port = workers.get("a1").port();
            workers.get("a1").close();
            awaitState("tenant-a", a1, "WAITING_EXTERNAL");
            assertEquals(interactionA1.path("interactionId"), pending("tenant-a", a1).path("interactionId"));
            var metadata = request("GET", "/tpf/admin/tenants/tenant-a/pipelines/" + contracts.get("a1").path("pipelineId").asText()
                + "/releases/release-a1", null, ADMIN);
            assertEquals(200, metadata.statusCode(), metadata.body());
            assertEquals("release-a1", JSON.readTree(metadata.body()).path("releaseVersion").asText());

            assertTrue(pendingList("tenant-b").isEmpty(), "Tenant B must not observe A1 interactions");
            assertEquals(404, request("GET", executionPath("tenant-b", a1), null, ADMIN).statusCode());
            // A forged payload with legitimate catalog/type fingerprints must not execute business code.
            var malformed = submitBody(contracts.get("b"), Map.of("parcelId", "bad", "weight", "not-an-integer"), "malformed");
            var malformedResponse = request("POST", executionsPath("tenant-b"), malformed, ADMIN);
            if (malformedResponse.statusCode() == 200) {
                String id = JSON.readTree(malformedResponse.body()).path("executionId").asText();
                awaitState("tenant-b", id, "FAILED");
                JsonNode failed = JSON.readTree(request("GET", executionPath("tenant-b", id), null, ADMIN).body());
                String reason = failed.path("errorMessage").asText().toLowerCase(java.util.Locale.ROOT);
                assertTrue(reason.contains("canonical") || reason.contains("weight") || reason.contains("integer"), failed.toString());
            } else assertEquals(400, malformedResponse.statusCode(), malformedResponse.body());
            coordinator.close(); coordinator = null;
            Path after = evidence.resolve("setup-after.json");
            JSON.writeValue(after.toFile(), Map.of("releases", refreshable ? List.of(descriptors.get("a2")) : List.of(),
                "activations", List.of(activation("a2", contracts.get("a2"), "release-a1"))));
            properties.put("tpf.proof.release-setup", after.toString());
            properties.put("tpf.proof.bootstrap-evidence", evidence.resolve("coordinator-after.json").toString());
            coordinator = start(root.resolve("coordinator"), properties, evidence.resolve("coordinator-after.log"), coordinatorPort);
            awaitStatus(coordinator, pendingPath("tenant-a"), ADMIN, 200);
            assertEquals(interactionA1.path("interactionId"), pending("tenant-a", a1).path("interactionId"));
            workers.put("a1", start(root.resolve("application-a1"), workerProperties.get("a1"), evidence.resolve("a1-restarted.log"), retainedA1Port));
            awaitStatus(workers.get("a1"), RestTransitionWorkerProtocol.CAPABILITIES_PATH, null, 401);
            for (String app : workers.keySet()) registerWorker(app, workers.get(app), contracts.get(app), archives.get(app));
            complete(interactionA1, contracts.get("a1")); awaitState("tenant-a", a1, "SUCCEEDED");
            String a2 = submit("a2", contracts.get("a2"), Map.of("requestId", "expense-2", "amount", 20));
            assertNotEquals(a1, a2, "The same client key must be scoped to immutable Release identity");
            awaitState("tenant-a", a2, "WAITING_EXTERNAL");
            if (refreshable) {
                coordinator.close();
                coordinator = restartWithActivation(root, properties, evidence, "rollback",
                    activation("a1", contracts.get("a1"), "release-a2", "rollback-to-a1"));
                String rolledBack = submit("a1", contracts.get("a1"),
                    Map.of("requestId", "expense-rollback", "amount", 30), "rollback-admission");
                awaitState("tenant-a", rolledBack, "WAITING_EXTERNAL");
                complete(pending("tenant-a", rolledBack), contracts.get("a1"));
                awaitState("tenant-a", rolledBack, "SUCCEEDED");
                assertEquals("A1", result("tenant-a", rolledBack).path("revision").asText());
            }
            complete(pending("tenant-a", a2), contracts.get("a2"));
            awaitState("tenant-a", a2, "SUCCEEDED");
            JsonNode resultA1 = result("tenant-a", a1), resultA2 = result("tenant-a", a2), resultB = result("tenant-b", b);
            assertEquals(JSON.valueToTree(Map.of("requestId", "expense-1:projected-A1", "outcome", "approved", "revision", "A1")), resultA1);
            assertEquals(JSON.valueToTree(Map.of("requestId", "expense-2:projected-A2", "outcome", "approved", "revision", "A2")), resultA2);
            assertEquals(JSON.valueToTree(Map.of("parcelId", "parcel-1", "receipt", "B:POST-12")), resultB);
            for (var proof : Map.of("a1", resultA1, "a2", resultA2, "b", resultB).entrySet()) {
                Path release = root.resolve("application-" + proof.getKey()).resolve("target/pipeline-release.json");
                var expected = JSON.readTree(release.toFile()).path("deployment").path("check")
                    .path("expectedResult").path("payload").asText();
                assertEquals(JSON.readTree(expected), proof.getValue(), proof.getKey());
            }
            if (refreshable) {
                coordinator.close();
                coordinator = restartWithActivation(root, properties, evidence, "restore",
                    activation("a2", contracts.get("a2"), "release-a1", "restore-to-a2"));
                publishRoutes(routeFile, routes, Set.of("a2", "b"));
                assertEquals(409, workerRegistration("a1", workers.get("a1"), contracts.get("a1"), archives.get("a1")).statusCode());
                assertEquals(resultA1, result("tenant-a", a1));
                var next = request("POST", executionsPath("tenant-b"),
                    submitBody(contracts.get("b"), Map.of("parcelId", "parcel-2", "weight", 4), "post-retirement"), ADMIN);
                assertEquals(200, next.statusCode(), next.body());
                awaitState("tenant-b", JSON.readTree(next.body()).path("executionId").asText(), "SUCCEEDED");
                String afterRemoval = submit("a2", contracts.get("a2"),
                    Map.of("requestId", "expense-after-drain", "amount", 40), "post-retirement");
                awaitState("tenant-a", afterRemoval, "WAITING_EXTERNAL");
                complete(pending("tenant-a", afterRemoval), contracts.get("a2"));
                awaitState("tenant-a", afterRemoval, "SUCCEEDED");
                assertEquals("A2", result("tenant-a", afterRemoval).path("revision").asText());
            }
            inventory.put("coordinator", inventory(root.resolve("coordinator")));
            inventory.put("phase", "PACKAGED_EXECUTION_RECOVERY_SCENARIO_PASSED");
            inventory.put("executions", Map.of("a1", a1, "a2", a2, "b", b));
            inventory.put("results", Map.of("a1", resultA1, "a2", resultA2, "b", resultB));
            JSON.writerWithDefaultPrettyPrinter().writeValue(evidence.resolve("execution-proof.json").toFile(), inventory);
        } finally {
            if (coordinator != null) coordinator.close();
            for (var worker : workers.values()) worker.close();
            http.close();
        }
    }

    private RunningPackage restartWithActivation(Path root, Map<String, String> properties, Path evidence, String phase,
                                                 Map<String, Object> activation) throws Exception {
        Path setup = evidence.resolve("setup-" + phase + ".json");
        JSON.writeValue(setup.toFile(), Map.of("releases", List.of(), "activations", List.of(activation)));
        properties.put("tpf.proof.release-setup", setup.toString());
        properties.put("tpf.proof.bootstrap-evidence", evidence.resolve("coordinator-" + phase + ".json").toString());
        var running = start(root.resolve("coordinator"), properties, evidence.resolve("coordinator-" + phase + ".log"), coordinatorPort);
        try {
            awaitStatus(running, pendingPath("tenant-a"), ADMIN, 200);
            return running;
        } catch (Throwable failure) {
            running.close();
            throw failure;
        }
    }

    private static Map<String, Object> activation(String app, JsonNode contract, String expected) {
        return activation(app, contract, expected, "activate-" + app);
    }

    private static Map<String, Object> activation(String app, JsonNode contract, String expected, String operationKey) {
        return Map.of("tenantId", tenant(app), "pipelineId", contract.path("pipelineId").asText(), "releaseVersion", "release-" + app,
            "expectedReleaseVersion", expected, "operationKey", operationKey, "activatedAtEpochMs", System.currentTimeMillis());
    }
    private static void publishRoutes(Path file, Map<String, String> routes, Set<String> included) throws Exception {
        var data = new Properties();
        routes.forEach((key, value) -> {
            if (included.stream().anyMatch(app -> key.startsWith("pipeline.orchestrator.worker.targets." + app + "."))) {
                data.setProperty(key, value);
            }
        });
        Path candidate = Files.createTempFile(file.getParent(), "routes-", ".properties");
        try (var writer = Files.newBufferedWriter(candidate)) {
            data.store(writer, null);
        }
        Files.move(candidate, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private void registerWorker(String app, RunningPackage worker, JsonNode contract, Path archive) throws Exception {
        var response = workerRegistration(app, worker, contract, archive);
        assertEquals(200, response.statusCode(), response.body());
    }

    private HttpResponse<String> workerRegistration(String app, RunningPackage worker, JsonNode contract, Path archive)
        throws Exception {
        String path = "/tpf/admin/tenants/" + tenant(app) + "/pipelines/" + contract.path("pipelineId").asText() + "/workers/register";
        return request("POST", path, Map.of("contractVersion", contract.path("contractVersion").asText(),
            "releaseVersion", "release-" + app, "workerId", "worker-" + app, "protocol", "rest",
            "endpoint", "http://127.0.0.1:" + worker.port(), "artifactId", "worker-" + app,
            "artifactDigest", "sha256:" + digest(archive)), ADMIN);
    }
    private String submit(String app, JsonNode contract, Map<String, Object> input) throws Exception {
        return submit(app, contract, input, SHARED_KEY);
    }

    private String submit(String app, JsonNode contract, Map<String, Object> input, String clientKey) throws Exception {
        var response = request("POST", executionsPath(tenant(app)), submitBody(contract, input, clientKey), ADMIN);
        assertEquals(200, response.statusCode(), response.body());
        String id = JSON.readTree(response.body()).path("executionId").asText(); assertFalse(id.isBlank(), response.body()); return id;
    }
    private static Map<String, Object> submitBody(JsonNode contract, Map<String, Object> input, String key) throws Exception {
        return Map.of("pipelineId", contract.path("pipelineId").asText(), "inputShape", "UNI",
            "inputPayload", canonical(contract, "Request", input), "idempotencyKey", key, "outputStreaming", false);
    }
    private static SerializedTransitionPayload canonical(JsonNode contract, String type, Map<String, Object> value) throws Exception {
        var typed = new TypedDurablePayload(type, contract.path("canonicalTypes").path(type).path("definitionFingerprint").asText(),
            contract.path("canonicalCatalogFingerprint").asText(), JsonDurablePayloadCodec.ENCODING, 1, JSON.writeValueAsBytes(value));
        return new JsonTransitionPayloadCodec().encode(typed);
    }
    private void complete(JsonNode interaction, JsonNode contract) throws Exception {
        var response = request("POST", "/tpf/control-plane/tenants/tenant-a/interactions/complete",
            Map.of("interactionId", interaction.path("interactionId").asText(), "idempotencyKey", "complete-" + interaction.path("interactionId").asText(),
                "actor", "proof", "responsePayload", canonical(contract, "ApprovalReply", Map.of("approved", true))), ADMIN);
        assertEquals(200, response.statusCode(), response.body());
    }
    private JsonNode pendingList(String tenant) throws Exception {
        var response = request("GET", pendingPath(tenant), null, ADMIN); assertEquals(200, response.statusCode(), response.body());
        return JSON.readTree(response.body());
    }
    private JsonNode pending(String tenant, String execution) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        do {
            for (JsonNode item : pendingList(tenant)) if (execution.equals(item.path("executionId").asText())) return item;
            Thread.sleep(100);
        } while (System.nanoTime() < end);
        throw new AssertionError("No retained Await interaction for " + execution);
    }
    private JsonNode result(String tenant, String execution) throws Exception {
        var response = request("GET", executionPath(tenant, execution) + "/result", null, ADMIN);
        assertEquals(200, response.statusCode(), response.body());
        JsonNode frame = JSON.readTree(response.body()).path("resultPayload");
        Object value = new JsonTransitionPayloadCodec().decode(new SerializedTransitionPayload(frame.path("payloadTypeId").asText(),
            frame.path("payloadEncoding").asText(), frame.path("payload").asText()));
        var typed = TypedDurablePayload.fromDurableValue(value).orElseThrow();
        return JSON.readTree(typed.payload());
    }
    private void awaitState(String tenant, String execution, String expected) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(45); String last = "";
        do {
            var response = request("GET", executionPath(tenant, execution), null, ADMIN);
            assertEquals(200, response.statusCode(), response.body()); last = response.body();
            String status = JSON.readTree(last).path("status").asText();
            if (expected.equals(status)) return;
            assertFalse(Set.of("FAILED", "CANCELLED", "EXPIRED").contains(status), last);
            Thread.sleep(100);
        } while (System.nanoTime() < end);
        throw new AssertionError("Expected " + expected + ": " + last);
    }
    private HttpResponse<String> request(String method, String path, Object body, String token) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(origin() + path)).timeout(Duration.ofSeconds(20))
            .header("Authorization", "Bearer " + token).header("Content-Type", "application/json");
        builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body)));
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }
    private String origin() { return "http://127.0.0.1:" + coordinatorPort; }
    private static String tenant(String app) { return app.equals("b") ? "tenant-b" : "tenant-a"; }
    private static String secret(String app) { return "local-proof-worker-" + app; }
    private static String executionsPath(String tenant) { return "/tpf/control-plane/tenants/" + tenant + "/executions"; }
    private static String executionPath(String tenant, String execution) { return executionsPath(tenant) + "/" + execution; }
    private static String pendingPath(String tenant) { return "/tpf/control-plane/tenants/" + tenant + "/interactions/pending"; }
}
