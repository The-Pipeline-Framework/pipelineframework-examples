/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.proof;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.PolyglotProofHarness;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.examples.springhost.SpringBillingHostApplication;
import org.pipelineframework.orchestrator.DynamoProofStoreHelper;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.type.CanonicalTypeCatalogue;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * End-to-end ADR-0094 proof running a real Spring Boot 4.1.2 {@link SpringBillingHostApplication}
 * (built on {@code pipelineframework-spring-host-starter} with zero Quarkus/Vert.x/Mutiny) against:
 * <ol>
 *   <li>TPF's local crash-safe disk stores ({@code tpf dev} model) and production
 *       {@code QueueAsyncCoordinator} + LocalStack DynamoDB ({@code DynamoExecutionStateStore})
 *       covering {@code ONE_TO_ONE}, {@code ONE_TO_MANY}, full-duplex {@code MANY_TO_MANY},
 *       {@code MANY_TO_ONE}, durable {@code WAITING_EXTERNAL} Await, exact 29-digit {@code decimal},
 *       odd {@code int64 > 2^53}, {@code BUSINESS_REJECTION}, and {@code TRANSIENT} retry.</li>
 *   <li>The real native {@code tpf dev} executable (when available on platform/CI), proving
 *       streaming {@code 1:1 -> 1:N -> N:N -> N:1} and {@code SIGKILL}-resilient durable Await
 *       against the live Spring Boot host match LocalStack DynamoDB byte-for-byte.</li>
 * </ol>
 */
class SpringBootForeignHostEquivalenceIT {

    private static final String SHARED_SECRET = "spring-boot-foreign-host-eq-secret-012345";
    private static final String EXACT_DECIMAL = "12345678901234567890.123456789";
    private static final long ODD_INT64 = 9007199254740993L;
    private static final ObjectMapper EXACT_JSON = CanonicalTypeCatalogue.exactJsonMapper();

    private static final String STREAM_OK_PAYLOAD = """
        {"orderId":"ord-eq-1","sequenceNumber":%d,"unitAmount":%s,"lineCount":3,"failFirstAttempt":false}
        """.formatted(ODD_INT64, EXACT_DECIMAL);
    private static final String STREAM_REJECT_PAYLOAD = """
        {"orderId":"ord-eq-neg","sequenceNumber":10,"unitAmount":-5.00,"lineCount":1,"failFirstAttempt":false}
        """;
    private static final String STREAM_TRANSIENT_PAYLOAD = """
        {"orderId":"ord-eq-retry","sequenceNumber":20,"unitAmount":10.00,"lineCount":2,"failFirstAttempt":true}
        """;
    private static final String AWAIT_PAYLOAD = """
        {"paymentId":"pay-spring-1","sequenceNumber":%d,"amount":%s,"failFirstAttempt":false}
        """.formatted(ODD_INT64, EXACT_DECIMAL);
    private static final String AWAIT_COMPLETION = """
        {"paymentId":"pay-spring-1","sequenceNumber":%d,"settledAmount":%s}
        """.formatted(ODD_INT64 + 1L, EXACT_DECIMAL);

    @Test
    void springBootForeignHostMatchesBetweenDiskDynamoDbAndNativeTpfDev(@TempDir Path temp) throws Exception {
        Path streamYaml = extractResource("spring-streaming-pipeline.yaml", temp);
        Path awaitYaml = extractResource("spring-await-connector-pipeline.yaml", temp);

        try (DynamoProofStoreHelper dynamoHelper = new DynamoProofStoreHelper()) {
            // 1. Run 1:1 -> 1:N -> N:N duplex -> N:1 on disk store vs LocalStack DynamoDB
            StreamingSnapshot diskStream = runStreaming(streamYaml, temp.resolve("disk-stream"), null, "disk-spring");
            StreamingSnapshot dynamoStream = runStreaming(
                streamYaml, temp.resolve("dynamo-stream"), dynamoHelper::createExecutionStateStore, "dynamo-spring");

            assertStreamingSnapshot("disk", diskStream);
            assertStreamingSnapshot("dynamo", dynamoStream);
            assertEquals(diskStream.okJson(), dynamoStream.okJson(),
                "Spring Boot 1:1->1:N->N:N->N:1 canonical JSON must match between disk and DynamoDB");
            assertEquals(diskStream.retryJson(), dynamoStream.retryJson());
            assertEquals(diskStream.rejectedStatus(), dynamoStream.rejectedStatus());
            assertEquals(diskStream.rejectedStepIndex(), dynamoStream.rejectedStepIndex());
            assertEquals(diskStream.rejectedAttempt(), dynamoStream.rejectedAttempt());

            // 2. Run durable Await suspension + resume on disk store vs LocalStack DynamoDB
            String diskAwaitJson = runAwait(awaitYaml, temp.resolve("disk-await"), null, "disk-await");
            String dynamoAwaitJson = runAwait(
                awaitYaml, temp.resolve("dynamo-await"), dynamoHelper::createExecutionStateStore, "dynamo-await");
            assertEquals(diskAwaitJson, dynamoAwaitJson,
                "Spring Boot durable Await canonical JSON must match between disk and DynamoDB");
            assertTrue(dynamoAwaitJson.contains(EXACT_DECIMAL));
            assertTrue(dynamoAwaitJson.contains(Long.toString(ODD_INT64 + 1L)));

            // 3. Run real native `tpf dev` binary against the live Spring Boot app (when available)
            Optional<Path> nativeBinaryOpt = NativeTpfSigkillEquivalenceIT.resolveNativeTpfBinary();
            boolean isCi = "true".equalsIgnoreCase(System.getenv("CI"));
            if (nativeBinaryOpt.isEmpty()) {
                if (isCi && NativeTpfSigkillEquivalenceIT.detectPlatform().isPresent()) {
                    throw new AssertionError("Native tpf binary failed to download/resolve in CI");
                }
                return;
            }
            Path nativeBinary = nativeBinaryOpt.orElseThrow();
            Path emptyPathDir = temp.resolve("empty-path");
            Path isolatedHome = temp.resolve("home");
            Files.createDirectories(emptyPathDir);
            Files.createDirectories(isolatedHome);

            try (ConfigurableApplicationContext springCtx = startSpringHost(SHARED_SECRET)) {
                URI springUri = hostUri(springCtx);

                // 3a. Streaming 1:1 -> 1:N -> N:N duplex -> N:1 via native `tpf dev`
                Path nativeStreamDir = temp.resolve("native-stream-state");
                try (NativeTpfSigkillEquivalenceIT.NativeDevProcess devStream =
                        NativeTpfSigkillEquivalenceIT.startNativeDev(
                            nativeBinary, isolatedHome, emptyPathDir, streamYaml, springUri,
                            nativeStreamDir, SHARED_SECRET)) {
                    JsonNode run = NativeTpfSigkillEquivalenceIT.httpPostJson(
                        devStream.baseUri().resolve("/runs"),
                        "{\"tenantId\":\"native-spring\",\"executionKey\":\"stream-1\",\"payload\":"
                            + STREAM_OK_PAYLOAD + "}");
                    assertEquals("SUCCEEDED", run.path("status").asText(), "Native tpf dev streaming failed: " + run);
                    String nativeStreamJson = EXACT_JSON.writeValueAsString(run.path("result"));
                    assertEquals(EXACT_JSON.readTree(dynamoStream.okJson()), EXACT_JSON.readTree(nativeStreamJson),
                        "Native tpf dev + Spring Boot streaming result must match LocalStack DynamoDB");
                }

                // 3b. Durable Await surviving SIGKILL via native `tpf dev`
                Path nativeAwaitDir = temp.resolve("native-await-state");
                NativeTpfSigkillEquivalenceIT.NativeDevProcess dev1 =
                    NativeTpfSigkillEquivalenceIT.startNativeDev(
                        nativeBinary, isolatedHome, emptyPathDir, awaitYaml, springUri,
                        nativeAwaitDir, SHARED_SECRET);
                String execId;
                String interactionId;
                try {
                    JsonNode run = NativeTpfSigkillEquivalenceIT.httpPostJson(
                        dev1.baseUri().resolve("/runs"),
                        "{\"tenantId\":\"native-spring\",\"executionKey\":\"await-1\",\"payload\":"
                            + AWAIT_PAYLOAD + "}");
                    assertEquals("WAITING_EXTERNAL", run.path("status").asText(), "Expected WAITING_EXTERNAL: " + run);
                    execId = run.path("executionId").asText();
                    interactionId = run.path("interactionId").asText();
                    assertFalse(interactionId.isBlank());
                    NativeTpfSigkillEquivalenceIT.sigkill(dev1);
                } finally {
                    dev1.close();
                }

                try (NativeTpfSigkillEquivalenceIT.NativeDevProcess dev2 =
                        NativeTpfSigkillEquivalenceIT.startNativeDev(
                            nativeBinary, isolatedHome, emptyPathDir, awaitYaml, springUri,
                            nativeAwaitDir, SHARED_SECRET)) {
                    JsonNode completed = NativeTpfSigkillEquivalenceIT.httpPostJson(
                        dev2.baseUri().resolve("/awaits/complete"),
                        "{\"tenantId\":\"native-spring\",\"interactionId\":\"" + interactionId
                            + "\",\"completionId\":\"comp-spring-await-1\",\"payload\":" + AWAIT_COMPLETION + "}");
                    assertEquals("SUCCEEDED", completed.path("status").asText(),
                        "Resumed native tpf dev Await failed: " + completed);
                    assertFalse(completed.path("duplicate").asBoolean());
                    String nativeAwaitJson = EXACT_JSON.writeValueAsString(completed.path("result"));
                    assertEquals(EXACT_JSON.readTree(dynamoAwaitJson), EXACT_JSON.readTree(nativeAwaitJson),
                        "SIGKILL-recovered native tpf dev + Spring Boot Await result must match DynamoDB");
                }
            }
        }
    }

    private static StreamingSnapshot runStreaming(
        Path yaml,
        Path dataDir,
        java.util.function.Function<org.pipelineframework.orchestrator.PipelineOrchestratorConfig,
            org.pipelineframework.orchestrator.ExecutionStateStore> storeOverride,
        String tenant
    ) throws Exception {
        try (ConfigurableApplicationContext springCtx = startSpringHost(SHARED_SECRET);
             PolyglotProofHarness harness = new PolyglotProofHarness(
                 yaml, hostUri(springCtx), hostUri(springCtx), SHARED_SECRET,
                 dataDir, 2, Map.of(), storeOverride)) {

            ExecutionRecord<Object, Object> ok = harness.submitAndDrain(tenant, "ok-1", STREAM_OK_PAYLOAD);
            assertEquals(ExecutionStatus.SUCCEEDED, ok.status(), ok.errorMessage());
            String okJson = harness.getResultCanonicalString(tenant, ok.executionId());

            ExecutionRecord<Object, Object> rejected = harness.submitAndDrain(tenant, "rej-1", STREAM_REJECT_PAYLOAD);

            ExecutionRecord<Object, Object> retryBefore = harness.submitAndDrain(tenant, "ret-1", STREAM_TRANSIENT_PAYLOAD);
            Thread.sleep(25);
            harness.sweepAndDrain(System.currentTimeMillis() + 60_000L);
            ExecutionRecord<Object, Object> retryAfter = harness.getExecution(tenant, retryBefore.executionId());
            assertEquals(ExecutionStatus.SUCCEEDED, retryAfter.status(), retryAfter.errorMessage());
            String retryJson = harness.getResultCanonicalString(tenant, retryAfter.executionId());

            return new StreamingSnapshot(
                ok.status(), okJson,
                rejected.status(), rejected.failedStepIndex(), rejected.attempt(),
                retryBefore.status(), retryAfter.status(), retryJson);
        }
    }

    private static String runAwait(
        Path yaml,
        Path dataDir,
        java.util.function.Function<org.pipelineframework.orchestrator.PipelineOrchestratorConfig,
            org.pipelineframework.orchestrator.ExecutionStateStore> storeOverride,
        String tenant
    ) throws Exception {
        try (ConfigurableApplicationContext springCtx = startSpringHost(SHARED_SECRET);
             PolyglotProofHarness harness = new PolyglotProofHarness(
                 yaml, hostUri(springCtx), hostUri(springCtx), SHARED_SECRET,
                 dataDir, 2, Map.of(), storeOverride)) {

            ExecutionRecord<Object, Object> waiting = harness.submitAndDrain(tenant, "await-1", AWAIT_PAYLOAD);
            assertEquals(ExecutionStatus.WAITING_EXTERNAL, waiting.status(), waiting.errorMessage());
            AwaitInteractionRecord interaction = harness.awaitInteraction(tenant, waiting.awaitUnitId());
            ExecutionRecord<Object, Object> done = harness.completeAwaitAndDrain(tenant, interaction, AWAIT_COMPLETION);
            assertEquals(ExecutionStatus.SUCCEEDED, done.status(), done.errorMessage());
            return harness.getResultCanonicalString(tenant, done.executionId());
        }
    }

    private static void assertStreamingSnapshot(String label, StreamingSnapshot snap) throws Exception {
        assertEquals(ExecutionStatus.SUCCEEDED, snap.okStatus(), label + " OK status");
        JsonNode okNode = EXACT_JSON.readTree(snap.okJson());
        assertEquals("ord-eq-1", okNode.path("orderId").asText());
        assertEquals(ODD_INT64 + 3L, okNode.path("sequenceNumber").asLong());
        assertEquals(3, okNode.path("totalLines").asInt());
        BigDecimal expectedTotal = new BigDecimal(EXACT_DECIMAL)
            .add(new BigDecimal("0.000000001"))
            .multiply(BigDecimal.valueOf(3));
        assertEquals(0, expectedTotal.compareTo(okNode.path("grandTotal").decimalValue()));

        assertEquals(ExecutionStatus.FAILED, snap.rejectedStatus(), label + " rejection must fail terminally");
        assertEquals(0, snap.rejectedStepIndex(), label + " rejection step index");
        assertEquals(0, snap.rejectedAttempt(), label + " BUSINESS_REJECTION must not retry");
        assertEquals(ExecutionStatus.WAIT_RETRY, snap.retryBeforeStatus(), label + " transient must pause in WAIT_RETRY");
        assertEquals(ExecutionStatus.SUCCEEDED, snap.retryAfterStatus(), label + " transient must succeed on sweep");
    }

    private static ConfigurableApplicationContext startSpringHost(String secret) {
        return new SpringApplicationBuilder(SpringBillingHostApplication.class)
            .web(WebApplicationType.REACTIVE)
            .properties(
                "server.port=0",
                "server.address=127.0.0.1",
                "tpf.host.shared-secret=" + secret)
            .run();
    }

    private static URI hostUri(ConfigurableApplicationContext ctx) {
        int port = Integer.parseInt(ctx.getEnvironment().getRequiredProperty("local.server.port"));
        return URI.create("http://127.0.0.1:" + port);
    }

    private static Path extractResource(String resourceName, Path temp) throws Exception {
        Path dest = temp.resolve(resourceName);
        try (var in = SpringBootForeignHostEquivalenceIT.class.getResourceAsStream("/" + resourceName)) {
            if (in == null) {
                throw new IllegalStateException("Missing classpath resource: " + resourceName);
            }
            Files.write(dest, in.readAllBytes());
        }
        return dest;
    }

    private record StreamingSnapshot(
        ExecutionStatus okStatus,
        String okJson,
        ExecutionStatus rejectedStatus,
        int rejectedStepIndex,
        int rejectedAttempt,
        ExecutionStatus retryBeforeStatus,
        ExecutionStatus retryAfterStatus,
        String retryJson
    ) {
    }
}
