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

package org.pipelineframework.examples.springhost;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.connector.CommandDispatchIdentity;
import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.CommandOperation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.CommandReconciliationInvocation;
import org.pipelineframework.connector.CommandReconciliationResult;
import org.pipelineframework.connector.CommandRecoveryBinding;
import org.pipelineframework.connector.CommandRecoveryTarget;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorInvocationTarget;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.connector.polyglot.PolyglotConnectorConnection;
import org.pipelineframework.connector.polyglot.PolyglotConnectorProvider;
import org.pipelineframework.connector.polyglot.PolyglotConnectorSpec;
import org.pipelineframework.orchestrator.CanonicalFunctionHostManyToManyStep;
import org.pipelineframework.orchestrator.CanonicalFunctionHostManyToOneStep;
import org.pipelineframework.orchestrator.CanonicalFunctionHostOneToManyStep;
import org.pipelineframework.orchestrator.CanonicalFunctionHostStep;
import org.pipelineframework.orchestrator.JsonDurablePayloadCodec;
import org.pipelineframework.orchestrator.TransitionWorkerFailureException;
import org.pipelineframework.orchestrator.TypedDurablePayload;
import org.pipelineframework.processor.util.PipelineContractMetadataGenerator;
import org.pipelineframework.step.NonRetryableException;
import org.pipelineframework.type.CanonicalTypeCatalogue;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * End-to-end component proof running a real Spring Boot 4.1.2 {@link SpringBillingHostApplication}
 * behind TPF's standard {@link CanonicalFunctionHostStep}, {@link CanonicalFunctionHostOneToManyStep},
 * {@link CanonicalFunctionHostManyToManyStep}, {@link CanonicalFunctionHostManyToOneStep}, and
 * {@link PolyglotConnectorProvider} (Query, Command, and Reconciliation).
 */
class SpringForeignHostProofTest {

    static final String SHARED_SECRET = "spring-proof-shared-hmac-secret-0123456789";
    static final ObjectMapper EXACT_JSON = CanonicalTypeCatalogue.exactJsonMapper();

    @Test
    void realSpringBootHostExecutesAllFourCardinalitiesAndTier2Connectors(@TempDir Path temp) throws Exception {
        ExecutorService connectorExecutor = Executors.newCachedThreadPool();
        try (ConfigurableApplicationContext springCtx = startSpringHost(SHARED_SECRET)) {
            URI baseUri = hostUri(springCtx);

            // 1. Compile canonical v3 contract from YAML (zero Spring-specific compiler renderers!)
            CompiledContract streamContract = compileContract("spring-streaming-pipeline.yaml", temp);
            CanonicalTypeCatalogue streamCatalogue = streamContract.catalogue();
            String catalogFp = streamContract.catalogFingerprint();
            String orderFp = streamContract.definitionFingerprints().get("BillingOrder");
            String lineFp = streamContract.definitionFingerprints().get("OrderLine");
            String summaryFp = streamContract.definitionFingerprints().get("OrderSummary");

            CanonicalFunctionHostStep validate = new CanonicalFunctionHostStep(
                new CanonicalFunctionHostStep.Spec(
                    "validate-order", 0, "BillingOrder", orderFp, "BillingOrder", orderFp,
                    catalogFp, streamCatalogue, baseUri, SHARED_SECRET, Duration.ofSeconds(10),
                    Optional.of("cmd-validate-order")),
                true);
            CanonicalFunctionHostOneToManyStep expand = new CanonicalFunctionHostOneToManyStep(
                new CanonicalFunctionHostOneToManyStep.Spec(
                    "expand-lines", 1, "BillingOrder", orderFp, "OrderLine", lineFp,
                    catalogFp, streamCatalogue, baseUri, SHARED_SECRET, Duration.ofSeconds(10),
                    Duration.ofSeconds(10), Optional.of("cmd-expand-lines")),
                true);
            CanonicalFunctionHostManyToManyStep enrich = new CanonicalFunctionHostManyToManyStep(
                new CanonicalFunctionHostManyToManyStep.Spec(
                    "enrich-lines", 2, "OrderLine", lineFp, "OrderLine", lineFp,
                    catalogFp, streamCatalogue, baseUri, SHARED_SECRET, Duration.ofSeconds(10),
                    Duration.ofSeconds(10), Optional.of("cmd-enrich-lines")),
                true);
            CanonicalFunctionHostManyToOneStep summarise = new CanonicalFunctionHostManyToOneStep(
                new CanonicalFunctionHostManyToOneStep.Spec(
                    "summarise-order", 3, "OrderLine", lineFp, "OrderSummary", summaryFp,
                    catalogFp, streamCatalogue, baseUri, SHARED_SECRET, Duration.ofSeconds(10),
                    Optional.of("cmd-summarise-order")),
                true);

            // Exact 29-digit BigDecimal + odd int64 > 2^53 flowing through 1:1 -> 1:N -> N:N -> N:1
            String hugeAmount = "12345678901234567890.123456789";
            long oddSeq = 9007199254740993L;
            TypedDurablePayload input = durablePayload(
                "BillingOrder", orderFp, catalogFp,
                "{\"orderId\":\"ord-1\",\"sequenceNumber\":" + oddSeq + ",\"unitAmount\":" + hugeAmount
                    + ",\"lineCount\":3,\"failFirstAttempt\":false}");

            TypedDurablePayload validated = validate.applyOneToOne(input).await().atMost(Duration.ofSeconds(10));
            Multi<TypedDurablePayload> expanded = expand.applyOneToMany(validated);
            Multi<TypedDurablePayload> enriched = enrich.applyTransform(expanded);
            TypedDurablePayload summary = summarise.applyReduce(enriched).await().atMost(Duration.ofSeconds(10));

            JsonNode summaryJson = EXACT_JSON.readTree(summary.payload());
            assertEquals("ord-1", summaryJson.path("orderId").asText());
            assertEquals(oddSeq + 3L, summaryJson.path("sequenceNumber").asLong());
            assertEquals(3, summaryJson.path("totalLines").asInt());
            BigDecimal expectedTotal = new BigDecimal(hugeAmount)
                .add(new BigDecimal("0.000000001"))
                .multiply(BigDecimal.valueOf(3));
            assertEquals(0, expectedTotal.compareTo(summaryJson.path("grandTotal").decimalValue()));

            // Portable BUSINESS_REJECTION is non-retryable
            TypedDurablePayload negative = durablePayload(
                "BillingOrder", orderFp, catalogFp,
                "{\"orderId\":\"ord-neg\",\"sequenceNumber\":1,\"unitAmount\":-1.00,\"lineCount\":1,\"failFirstAttempt\":false}");
            TransitionWorkerFailureException rejection = assertThrows(
                TransitionWorkerFailureException.class,
                () -> validate.applyOneToOne(negative).await().atMost(Duration.ofSeconds(10)));
            assertInstanceOf(NonRetryableException.class, rejection.getCause());
            assertTrue(rejection.getMessage().contains("INVALID_ORDER"));

            // Portable TRANSIENT is retryable and succeeds on retry
            TypedDurablePayload retryable = durablePayload(
                "BillingOrder", orderFp, catalogFp,
                "{\"orderId\":\"ord-retry\",\"sequenceNumber\":2,\"unitAmount\":10.00,\"lineCount\":1,\"failFirstAttempt\":true}");
            TransitionWorkerFailureException transientErr = assertThrows(
                TransitionWorkerFailureException.class,
                () -> validate.applyOneToOne(retryable).await().atMost(Duration.ofSeconds(10)));
            assertTrue(validate.shouldRetry(transientErr));
            assertTrue(transientErr.getMessage().contains("LEDGER_BUSY"));
            TypedDurablePayload retriedOk = validate.applyOneToOne(retryable).await().atMost(Duration.ofSeconds(10));
            assertEquals("ord-retry", EXACT_JSON.readTree(retriedOk.payload()).path("orderId").asText());

            // 2. Tier-2 Custom Query, Command, and Command Reconciliation Connectors
            CompiledContract connContract = compileContract("spring-await-connector-pipeline.yaml", temp);
            PolyglotConnectorSpec connectorSpec = new PolyglotConnectorSpec(
                connContract.catalogue(),
                List.of(new PolyglotConnectorSpec.QueryOperationSpec("billing.lookup", 1, "AccountLookup", "AccountProfile")),
                List.of(new PolyglotConnectorSpec.CommandOperationSpec(
                    "billing.charge", 1, "ChargeCommand", "ChargeReceipt", true, Optional.of("billing.receipt"))));
            PolyglotConnectorProvider provider = new PolyglotConnectorProvider(connectorSpec);

            ConnectionResolver resolver = new ConnectionResolver() {
                @Override
                public <T extends org.pipelineframework.connector.ResolvedConnection> java.util.concurrent.CompletionStage<T> resolve(
                    org.pipelineframework.connector.ConnectionResolutionRequest<T> request
                ) {
                    PolyglotConnectorConnection conn = new PolyglotConnectorConnection(
                        baseUri, SHARED_SECRET, Map.of("BILLING_API_KEY", "authorized-billing-key"), true);
                    return CompletableFuture.completedFuture(request.connectionType().cast(conn));
                }
            };
            ConnectorRuntimeContext rtCtx = ConnectorRuntimeContext.of(
                "spring-proof", connectorExecutor, Clock.systemUTC(), Optional.of(resolver));
            provider.start(rtCtx, new PolyglotConnectorProvider.ProviderConfig(
                "spring-billing-conn", Optional.of("billing:spring"), Optional.of(5_000),
                Optional.of(true), Optional.of("billing.receipt"))).toCompletableFuture().join();

            @SuppressWarnings("unchecked")
            QueryOperation<JsonNode, PolyglotConnectorProvider.OperationConfig, JsonNode> queryOp =
                (QueryOperation<JsonNode, PolyglotConnectorProvider.OperationConfig, JsonNode>) provider.operations().stream()
                    .filter(op -> op instanceof QueryOperation && "billing.lookup".equals(op.id()))
                    .findFirst().orElseThrow();
            ConnectorOperationIdentity cmdOpId = new ConnectorOperationIdentity(
                PolyglotConnectorProvider.PROVIDER_ID, "billing.charge", ConnectorOperationKind.COMMAND, 1);
            ConnectorBindingName billingBinding = ConnectorBindingName.of("billing-host");
            ConnectorExecutionContext execCtx = new ConnectorExecutionContext(
                Optional.of("spring-tenant"), Optional.of("exec-1"), Optional.of("spring-proof"),
                Optional.of("1"), Optional.of("release-1"), Optional.of("billing.charge"),
                Optional.of(new ConnectorInvocationTarget(billingBinding, cmdOpId)),
                Optional.empty(), Optional.empty(), Optional.empty());
            QueryOutcome<JsonNode> found = queryOp.query(new QueryInvocation<>(
                EXACT_JSON.readTree("{\"accountId\":\"acct-99\"}"),
                new PolyglotConnectorProvider.OperationConfig(Optional.of("eu-central")),
                JsonNode.class,
                execCtx,
                Optional.empty())).toCompletableFuture().join();
            assertInstanceOf(QueryOutcome.Found.class, found);
            assertEquals("eu-central", ((QueryOutcome.Found<JsonNode>) found).output().path("region").asText());

            @SuppressWarnings("unchecked")
            CommandOperation<JsonNode, PolyglotConnectorProvider.OperationConfig, JsonNode> cmdOp =
                (CommandOperation<JsonNode, PolyglotConnectorProvider.OperationConfig, JsonNode>) provider.operations().stream()
                    .filter(op -> op instanceof CommandOperation && "billing.charge".equals(op.id()))
                    .findFirst().orElseThrow();

            // Ambiguous dispatch -> reconcile confirms the applied ledger entry!
            CommandDispatchIdentity ambIdentity = new CommandDispatchIdentity("cmd-amb", "idem-amb-1", "att-1");
            CommandInvocation<JsonNode, PolyglotConnectorProvider.OperationConfig> ambInvocation = new CommandInvocation<>(
                EXACT_JSON.readTree("{\"accountId\":\"acct-99\",\"amount\":42.50,\"simulateAmbiguous\":true}"),
                new PolyglotConnectorProvider.OperationConfig(Optional.empty()),
                JsonNode.class,
                execCtx,
                Optional.of(ambIdentity));
            CommandOutcome<JsonNode> ambOutcome = cmdOp.dispatch(ambInvocation).toCompletableFuture().join();
            assertInstanceOf(CommandOutcome.Ambiguous.class, ambOutcome);

            CommandRecoveryTarget target = cmdOp.recoveryTarget(ambInvocation).orElseThrow();
            CommandRecoveryBinding binding = new CommandRecoveryBinding(
                "spring-tenant", "cmd-amb", "idem-amb-1", "att-1", "exec-1", "spring-proof", "1", "release-1",
                "billing.charge",
                cmdOpId,
                1,
                billingBinding,
                JsonNode.class.getName(),
                JsonNode.class.getName(),
                "a".repeat(64),
                target.configuration(),
                target);
            CommandReconciliationResult<JsonNode> reconciled = cmdOp.reconcile(
                new CommandReconciliationInvocation<>(ambInvocation, binding)).toCompletableFuture().join();
            assertInstanceOf(CommandReconciliationResult.ConfirmedSucceeded.class, reconciled);
            CommandReconciliationResult.ConfirmedSucceeded<JsonNode> confirmed =
                (CommandReconciliationResult.ConfirmedSucceeded<JsonNode>) reconciled;
            assertEquals("rcpt-reconciled-cmd-amb", confirmed.receipt().value());
            assertEquals("idem-amb-1", confirmed.outcome().output().path("idempotencyKey").asText());
        } finally {
            connectorExecutor.shutdownNow();
        }
    }

    static ConfigurableApplicationContext startSpringHost(String sharedSecret) {
        return new SpringApplicationBuilder(SpringBillingHostApplication.class)
            .web(WebApplicationType.REACTIVE)
            .properties(
                "server.port=0",
                "server.address=127.0.0.1",
                "tpf.host.shared-secret=" + sharedSecret)
            .run();
    }

    static URI hostUri(ConfigurableApplicationContext ctx) {
        int port = Integer.parseInt(ctx.getEnvironment().getRequiredProperty("local.server.port"));
        return URI.create("http://127.0.0.1:" + port);
    }

    private record CompiledContract(
        String catalogFingerprint,
        CanonicalTypeCatalogue catalogue,
        Map<String, String> definitionFingerprints
    ) {
    }

    static CompiledContract compileContract(String yamlResource, Path temp) throws Exception {
        Path yamlPath = temp.resolve(yamlResource);
        try (var in = SpringForeignHostProofTest.class.getResourceAsStream("/" + yamlResource)) {
            if (in == null) {
                throw new IllegalStateException("Missing test YAML resource: " + yamlResource);
            }
            Files.write(yamlPath, in.readAllBytes());
        }
        PipelineTemplateConfig template = new PipelineTemplateConfigLoader().load(yamlPath);
        PipelineYamlConfig yamlConfig = new PipelineYamlConfigLoader().load(yamlPath);
        String contractJson = new PipelineContractMetadataGenerator().renderPolyglotContract(template, yamlConfig);
        JsonNode contractTree = EXACT_JSON.readTree(contractJson);
        String catalogFp = contractTree.path("canonicalCatalogFingerprint").asText();
        Map<String, Map<String, Object>> canonicalTypesMap = EXACT_JSON.convertValue(
            contractTree.path("canonicalTypes"), new TypeReference<>() { });
        CanonicalTypeCatalogue catalogue = CanonicalTypeCatalogue.fromCanonicalTypes(canonicalTypesMap);
        Map<String, String> fps = new LinkedHashMap<>();
        contractTree.path("canonicalTypes").fields().forEachRemaining(entry ->
            fps.put(entry.getKey(), entry.getValue().path("definitionFingerprint").asText()));
        return new CompiledContract(catalogFp, catalogue, Map.copyOf(fps));
    }

    static TypedDurablePayload durablePayload(String typeId, String defFp, String catalogFp, String json) {
        return new TypedDurablePayload(
            typeId, defFp, catalogFp,
            JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION,
            json.getBytes(StandardCharsets.UTF_8));
    }
}
