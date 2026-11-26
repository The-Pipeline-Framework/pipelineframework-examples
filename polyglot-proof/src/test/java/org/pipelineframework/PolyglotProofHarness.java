package org.pipelineframework;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitSuspendedException;
import org.pipelineframework.awaitable.AwaitUnitCreateCommand;
import org.pipelineframework.awaitable.AwaitUnitOccurrence;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.command.CommandStepSupport;
import org.pipelineframework.command.InMemoryCommandEffectStore;
import org.pipelineframework.command.NativeCommandSelector;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConnectorBinding;
import org.pipelineframework.config.pipeline.PipelineYamlOperationSelection;
import org.pipelineframework.config.pipeline.PipelineYamlStep;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderLease;
import org.pipelineframework.connector.ConnectorRegistry;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.ResolvedConnection;
import org.pipelineframework.connector.polyglot.PolyglotConnectorConnection;
import org.pipelineframework.connector.polyglot.PolyglotConnectorProvider;
import org.pipelineframework.connector.polyglot.PolyglotConnectorSpec;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.objectpublish.ObjectPublishCompletionService;
import org.pipelineframework.orchestrator.CanonicalFunctionHostManyToOneStep;
import org.pipelineframework.orchestrator.CanonicalFunctionHostOneToManyStep;
import org.pipelineframework.orchestrator.CanonicalFunctionHostStep;
import org.pipelineframework.orchestrator.CoordinationHost;
import org.pipelineframework.orchestrator.DeadLetterEnvelope;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.DynamoProofStoreHelper;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionResultShapeResolver;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.JsonDurablePayloadCodec;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.LocalControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerExecutionMode;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.TypedDurablePayload;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.InMemoryControlPlaneJournal;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.processor.util.PipelineContractMetadataGenerator;
import org.pipelineframework.proof.DiskProofStores;
import org.pipelineframework.query.InMemoryQueryCaptureStore;
import org.pipelineframework.query.NativeQuerySelector;
import org.pipelineframework.query.QueryStepDescriptor;
import org.pipelineframework.query.QueryStepSupport;
import org.pipelineframework.step.ConfigFactory;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.telemetry.NoOpProofTelemetry;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * Reusable standalone coordinator harness wiring TPF's actual {@link QueueAsyncCoordinator},
 * {@link PipelineExecutionService}, {@link CanonicalFunctionHostStep} ({@code 1:1}/{@code 1:N}/{@code N:1}),
 * {@link PolyglotConnectorProvider}, and pluggable {@link ExecutionStateStore} (crash-safe local disk
 * or production {@code DynamoExecutionStateStore}).
 */
public final class PolyglotProofHarness implements AutoCloseable {

    public static final ObjectMapper JSON = PipelineJson.mapper().copy()
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final Duration TIMEOUT = Duration.ofSeconds(15);
    private static final Duration STEP_TIMEOUT = Duration.ofSeconds(10);

    private final PipelineTemplateConfig template;
    private final PipelineContractDescriptor contract;
    private final String catalogFingerprint;
    private final Map<String, String> definitionFingerprints;
    private final CanonicalTypeCatalogue catalogue;
    private final DiskProofStores diskStores;
    private final ExecutionStateStore stateStore;
    private final QueueAsyncCoordinator coordinator;
    private final PipelineExecutionService executionService;
    private final ClassLoader orderClassLoader;
    private final Queue<ExecutionWorkItem> workQueue = new ConcurrentLinkedQueue<>();
    private final Queue<DeadLetterEnvelope> deadLetters = new ConcurrentLinkedQueue<>();
    private final Map<Integer, String> awaitOutputTypes = new LinkedHashMap<>();
    private final ExecutorService connectorExecutor;
    private final Optional<ConnectorBindingRegistry> bindingRegistry;
    private final Optional<ConnectorRuntimeContext> connectorContext;
    private final Optional<CommandStepSupport> commandSupport;
    private final Optional<QueryStepSupport> querySupport;

    public PolyglotProofHarness(
        Path pipelineYaml,
        URI functionHostUri,
        URI connectorHostUri,
        String sharedSecret,
        Path dataDir,
        int maxRetries,
        Map<String, String> connectorSecrets,
        Function<PipelineOrchestratorConfig, ExecutionStateStore> stateStoreOverride
    ) throws IOException {
        JsonTransitionPayloadCodec payloadCodec = new JsonTransitionPayloadCodec();
        this.diskStores = new DiskProofStores(dataDir, payloadCodec);
        this.template = new PipelineTemplateConfigLoader().load(pipelineYaml);
        PipelineYamlConfig yamlConfig = new PipelineYamlConfigLoader().load(pipelineYaml);
        String contractJson = new PipelineContractMetadataGenerator().renderPolyglotContract(template, yamlConfig);
        this.contract = JSON.readValue(contractJson, PipelineContractDescriptor.class);

        JsonNode contractTree = JSON.readTree(contractJson);
        this.catalogFingerprint = contractTree.path("canonicalCatalogFingerprint").asText();
        Map<String, Map<String, Object>> canonicalTypesMap = JSON.convertValue(
            contractTree.path("canonicalTypes"), new TypeReference<>() { });
        this.catalogue = CanonicalTypeCatalogue.fromCanonicalTypes(canonicalTypesMap);
        Map<String, String> fps = new LinkedHashMap<>();
        contractTree.path("canonicalTypes").fields().forEachRemaining(entry ->
            fps.put(entry.getKey(), entry.getValue().path("definitionFingerprint").asText()));
        this.definitionFingerprints = Map.copyOf(fps);

        PipelineOrchestratorConfig config = orchestratorConfig(contract.pipelineId(), maxRetries);
        this.stateStore = stateStoreOverride != null
            ? Objects.requireNonNull(stateStoreOverride.apply(config), "stateStoreOverride")
            : diskStores.executionStore;

        this.connectorExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "polyglot-proof-connector");
            t.setDaemon(true);
            return t;
        });

        ConnectorHolder connHolder = buildConnectorSupport(
            yamlConfig, connectorHostUri, sharedSecret, connectorSecrets, config);
        this.bindingRegistry = connHolder.bindingRegistry();
        this.connectorContext = connHolder.runtimeContext();
        this.commandSupport = connHolder.commandSupport();
        this.querySupport = connHolder.querySupport();

        List<Object> steps = new ArrayList<>();
        List<Map<String, Object>> edges = new ArrayList<>();
        buildStepsAndEdges(yamlConfig, functionHostUri, sharedSecret, connHolder, steps, edges);

        Files.createDirectories(dataDir);
        Path orderFile = dataDir.resolve("meta-order.json");
        Files.write(orderFile, JSON.writeValueAsBytes(Map.of(
            "order", edges.stream().map(e -> e.get("stepClass")).toList(),
            "boundarySchemaVersion", 1,
            "stepBoundaries", edges,
            "definitionBoundaries", List.of())));
        var orderUrl = orderFile.toUri().toURL();
        this.orderClassLoader = new ClassLoader(PolyglotProofHarness.class.getClassLoader()) {
            @Override
            public Enumeration<java.net.URL> getResources(String name) throws IOException {
                return "META-INF/pipeline/order.json".equals(name)
                    ? Collections.enumeration(List.of(orderUrl))
                    : super.getResources(name);
            }
        };

        DurableAwaitCoordinator awaitCoordinator = new DurableAwaitCoordinator(
            diskStores.interactionStore, diskStores.unitStore);

        WorkDispatcher dispatcher = new WorkDispatcher() {
            @Override public Uni<Void> enqueueNow(ExecutionWorkItem item) {
                workQueue.add(item);
                return Uni.createFrom().voidItem();
            }
            @Override public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
                return Uni.createFrom().voidItem();
            }
            @Override public String providerName() { return "proof-queue"; }
            @Override public Optional<String> startupValidationError() { return Optional.empty(); }
        };
        DeadLetterPublisher dlq = new DeadLetterPublisher() {
            @Override public Uni<Void> publish(DeadLetterEnvelope envelope) {
                deadLetters.add(envelope);
                return Uni.createFrom().voidItem();
            }
            @Override public Optional<String> startupValidationError() { return Optional.empty(); }
        };

        SegmentBoundaryLedger ledger = new SegmentBoundaryLedger(new InMemoryControlPlaneJournal());
        ExecutionFailureHandler failureHandler = new ExecutionFailureHandler();
        failureHandler.orchestratorConfig = config;
        failureHandler.segmentBoundaryLedger = ledger;

        PipelineReleaseIdentityResolver identityResolver = new PipelineReleaseIdentityResolver() {
            @Override public Optional<PipelineContractDescriptor> compiledContract() {
                return Optional.of(contract);
            }
        };

        this.coordinator = new QueueAsyncCoordinator();
        coordinator.orchestratorConfig = config;
        coordinator.executionStateStore = stateStore;
        coordinator.workDispatcher = dispatcher;
        coordinator.deadLetterPublisher = dlq;
        coordinator.awaitCoordinator = awaitCoordinator;
        coordinator.checkpointPublicationService = new CheckpointPublicationService() {
            @Override public boolean enabled() { return false; }
            @Override public Uni<Void> publishIfConfigured(ExecutionRecord<Object, Object> r, Object payload) {
                return Uni.createFrom().voidItem();
            }
        };
        coordinator.objectPublishCompletionService = new ObjectPublishCompletionService() {
            @Override public boolean enabled() { return false; }
        };
        coordinator.executionInputPolicy = new ExecutionInputPolicy();
        coordinator.executionInputPolicy.orchestratorConfig = config;
        coordinator.executionResultShapeResolver = new ExecutionResultShapeResolver() {
            @Override public ExecutionResultShape resolve() { return ExecutionResultShape.SINGLE; }
        };
        coordinator.executionFailureHandler = failureHandler;
        coordinator.segmentBoundaryLedger = ledger;
        coordinator.controlPlaneAdmissionPolicy = new LocalControlPlaneAdmissionPolicy();
        coordinator.releaseIdentityResolver = identityResolver;
        coordinator.transitionPayloadCodec = payloadCodec;
        coordinator.transitionWorkerExecutor = new TransitionWorkerExecutor(config, new PipelineInvocationRuntime());

        PipelineConfig pipelineConfig = new PipelineConfig();
        pipelineConfig.defaults().retryLimit(0);
        NoOpProofTelemetry telemetry = new NoOpProofTelemetry();

        PipelineRunner runner = new PipelineRunner();
        runner.configFactory = new ConfigFactory();
        runner.pipelineConfig = pipelineConfig;
        runner.runTelemetry = telemetry;
        runner.stepTelemetry = telemetry;
        runner.stepOrderer = new PipelineStepOrderer() {
            @Override List<Object> orderSteps(List<Object> inputSteps) { return inputSteps; }
        };
        runner.parallelismPolicyResolver = new PipelineParallelismPolicyResolver() {
            @Override ParallelismPolicy resolveParallelismPolicy(PipelineConfig c) { return ParallelismPolicy.SEQUENTIAL; }
            @Override int resolveMaxConcurrency(PipelineConfig c) { return 1; }
        };
        runner.cacheSupportFactory = new PipelineCacheSupportFactory(null, null, "none") {
            @Override PipelineRunner.CacheReadSupport buildCacheReadSupport() { return null; }
        };
        runner.stepExecutor = new PipelineStepExecutor();

        LocalPipelineControlPlane localControlPlane = new LocalPipelineControlPlane() {
            @Override public void initializeQueueMode() { }
        };
        localControlPlane.queueAsyncCoordinator = coordinator;

        this.executionService = new PipelineExecutionService();
        executionService.orchestratorConfig = config;
        executionService.pipelineConfig = pipelineConfig;
        executionService.pipelineRunner = runner;
        executionService.healthCheckService = new HealthCheckService() {
            @Override public boolean checkHealthOfDependentServices(List<Object> s) { return true; }
        };
        executionService.controlPlane = localControlPlane;
        executionService.configFactory = new ConfigFactory();
        executionService.pipelineStepResolver = new PipelineStepResolver() {
            @Override List<Object> loadPipelineSteps() { return List.copyOf(steps); }
        };
        executionService.pipelineStepConfig = stepConfig();
        executionService.stepOrderer = runner.stepOrderer;
        executionService.awaitCoordinator = awaitCoordinator;
        executionService.executionHooks = new ExecutionHooks();
        executionService.executionInputPolicy = coordinator.executionInputPolicy;
        executionService.transitionPayloadCodec = payloadCodec;
        executionService.releaseIdentityResolver = identityResolver;
        localControlPlane.pipelineExecutionService = executionService;
        executionService.runStartupHealthChecks();
    }

    public ExecutionRecord<Object, Object> submitAndDrain(String tenantId, String executionKey, String payloadJson) {
        String inputType = template.steps().getFirst().inputTypeName();
        TypedDurablePayload input = new TypedDurablePayload(
            inputType,
            definitionFingerprints.get(inputType),
            catalogFingerprint,
            JsonDurablePayloadCodec.ENCODING,
            JsonDurablePayloadCodec.ENCODING_VERSION,
            payloadJson.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        var accepted = coordinator.executePipelineAsync(input, tenantId, executionKey, false).await().atMost(TIMEOUT);
        drainQueue();
        return getExecution(tenantId, accepted.executionId());
    }

    public ExecutionRecord<Object, Object> getExecution(String tenantId, String executionId) {
        return stateStore.getExecution(tenantId, executionId).await().atMost(TIMEOUT).orElseThrow();
    }

    public JsonNode getResultJson(String tenantId, String executionId) throws IOException {
        Object raw = coordinator.getExecutionResultPayload(tenantId, executionId).await().atMost(TIMEOUT);
        Object decoded = raw instanceof SerializedTransitionPayload s
            ? coordinator.transitionPayloadCodec.decode(s)
            : raw;
        TypedDurablePayload typed = (TypedDurablePayload) decoded;
        return JSON.readTree(typed.payload());
    }

    public String getResultCanonicalString(String tenantId, String executionId) {
        Object raw = coordinator.getExecutionResultPayload(tenantId, executionId).await().atMost(TIMEOUT);
        Object decoded = raw instanceof SerializedTransitionPayload s
            ? coordinator.transitionPayloadCodec.decode(s)
            : raw;
        TypedDurablePayload typed = (TypedDurablePayload) decoded;
        return new String(typed.payload(), java.nio.charset.StandardCharsets.UTF_8);
    }

    public AwaitInteractionRecord awaitInteraction(String tenantId, String awaitUnitId) {
        return diskStores.interactionStore.findByUnit(tenantId, awaitUnitId).await().atMost(TIMEOUT).getFirst();
    }

    public ExecutionRecord<Object, Object> completeAwaitAndDrain(
        String tenantId, AwaitInteractionRecord interaction, String responsePayloadJson) {
        String outputType = awaitOutputTypes.getOrDefault(interaction.stepIndex(), interaction.outputType());
        TypedDurablePayload response = new TypedDurablePayload(
            outputType,
            definitionFingerprints.get(outputType),
            catalogFingerprint,
            JsonDurablePayloadCodec.ENCODING,
            JsonDurablePayloadCodec.ENCODING_VERSION,
            responsePayloadJson.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        coordinator.completeAwait(new AwaitCompletionCommand(
            tenantId, interaction.interactionId(), interaction.correlationId(),
            "comp-" + System.nanoTime(), response, "proof-approver", System.currentTimeMillis()))
            .await().atMost(TIMEOUT);
        drainQueue();
        return getExecution(tenantId, interaction.executionId());
    }

    public void sweepAndDrain(long nowEpochMs) {
        coordinator.sweepOnce(nowEpochMs).await().atMost(TIMEOUT);
        drainQueue();
    }

    public int deadLetterCount() {
        return deadLetters.size();
    }

    public int dispatchIntentCount() {
        return diskStores.dispatchIntentStore.size();
    }

    public Optional<CommandStepSupport> commandSupport() {
        return commandSupport;
    }

    public Optional<QueryStepSupport> querySupport() {
        return querySupport;
    }

    public void drainQueue() {
        int safety = 0;
        while (!workQueue.isEmpty() && safety++ < 50) {
            ExecutionWorkItem item = workQueue.poll();
            if (item == null) break;
            PipelineTransitionWorker tcclWorker = new PipelineTransitionWorker() {
                @Override
                public Uni<TransitionResultEnvelope> executeTransition(TransitionCommandEnvelope command) {
                    ClassLoader previous = Thread.currentThread().getContextClassLoader();
                    Thread.currentThread().setContextClassLoader(orderClassLoader);
                    try {
                        return executionService.executeTransition(command);
                    } finally {
                        Thread.currentThread().setContextClassLoader(previous);
                    }
                }
                @Override public String providerName() { return executionService.providerName(); }
            };
            ClassLoader previous = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(orderClassLoader);
            try {
                coordinator.processExecutionWorkItem(item, tcclWorker).await().atMost(TIMEOUT);
            } finally {
                Thread.currentThread().setContextClassLoader(previous);
            }
        }
    }

    @Override
    public void close() {
        if (bindingRegistry.isPresent() && connectorContext.isPresent()) {
            try {
                bindingRegistry.orElseThrow().stop(connectorContext.orElseThrow())
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }
        connectorExecutor.shutdownNow();
    }

    private ConnectorHolder buildConnectorSupport(
        PipelineYamlConfig yamlConfig,
        URI connectorHostUri,
        String sharedSecret,
        Map<String, String> connectorSecrets,
        PipelineOrchestratorConfig orchestratorConfig
    ) {
        Map<String, PipelineYamlStep> yamlByToken = indexYamlSteps(yamlConfig);
        List<PolyglotConnectorSpec.QueryOperationSpec> queries = new ArrayList<>();
        List<PolyglotConnectorSpec.CommandOperationSpec> commands = new ArrayList<>();
        Map<String, ConnectorBindingName> bindings = new LinkedHashMap<>();

        for (PipelineTemplateStep step : template.steps()) {
            PipelineYamlStep ys = yamlByToken.get(normalizeToken(step.name()));
            if (ys == null || ys.operationSelection().isEmpty()) continue;
            PipelineYamlOperationSelection sel = ys.operationSelection().orElseThrow();
            PipelineYamlConnectorBinding binding = yamlConfig.connectors().get(sel.using());
            if (binding == null || !"polyglot.connector".equalsIgnoreCase(binding.provider())) continue;
            bindings.putIfAbsent(sel.using(), ConnectorBindingName.of(sel.using()));
            String kind = ys.kind().trim().toLowerCase(Locale.ROOT);
            int major = sel.operationVersion();
            if ("query".equals(kind)) {
                queries.add(new PolyglotConnectorSpec.QueryOperationSpec(
                    sel.operation(), major, step.inputTypeName(), step.outputTypeName()));
            } else if ("command".equals(kind)) {
                Map<String, Object> cfg = binding.config() == null ? Map.of() : binding.config();
                boolean reconcile = Boolean.parseBoolean(String.valueOf(cfg.getOrDefault("reconciliationSupported", false)));
                Optional<String> refKind = Optional.ofNullable(cfg.get("receiptReferenceKind"))
                    .map(Object::toString).filter(v -> !v.isBlank());
                commands.add(new PolyglotConnectorSpec.CommandOperationSpec(
                    sel.operation(), major, step.inputTypeName(), step.outputTypeName(), reconcile, refKind));
            }
        }
        if (queries.isEmpty() && commands.isEmpty()) {
            return new ConnectorHolder(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }

        PolyglotConnectorProvider provider = new PolyglotConnectorProvider(
            new PolyglotConnectorSpec(catalogue, queries, commands));
        ConnectionResolver resolver = new ConnectionResolver() {
            @Override
            public <T extends ResolvedConnection> CompletionStage<T> resolve(ConnectionResolutionRequest<T> request) {
                if (request.connectionType().equals(PolyglotConnectorConnection.class)) {
                    PolyglotConnectorConnection conn = new PolyglotConnectorConnection(
                        connectorHostUri, sharedSecret, connectorSecrets, true);
                    return CompletableFuture.completedStage(request.connectionType().cast(conn));
                }
                return CompletableFuture.failedStage(new IllegalArgumentException("Unknown connection type"));
            }
        };
        ConnectorRuntimeContext runtimeContext = ConnectorRuntimeContext.of(
            "polyglot-proof", connectorExecutor, Clock.systemUTC(), Optional.of(resolver));
        List<ConnectorBindingDefinition> bindingDefs = new ArrayList<>();
        for (ConnectorBindingName name : bindings.values()) {
            bindingDefs.add(new ConnectorBindingDefinition(
                name, PolyglotConnectorProvider.PROVIDER_ID, 1,
                new ConnectorConfigurationDocument(Map.of("connection", "polyglot-connector-host"))));
        }
        ConnectorBindingRegistry registry = ConnectorBindingRegistry.fromProviders(
            bindingDefs, List.of(provider), p -> ConnectorProviderLease.of(p, () -> { }));
        registry.start(runtimeContext).toCompletableFuture().join();

        CommandStepSupport cmdSupport = new CommandStepSupport(
            new ConnectorRegistry(List.of()), registry, runtimeContext,
            List.of(new InMemoryCommandEffectStore()), orchestratorConfig);
        QueryStepSupport qSupport = new QueryStepSupport(
            List.of(), List.of(new InMemoryQueryCaptureStore()), registry, runtimeContext);
        return new ConnectorHolder(Optional.of(qSupport), Optional.of(cmdSupport),
            Optional.of(registry), Optional.of(runtimeContext));
    }

    private void buildStepsAndEdges(
        PipelineYamlConfig yamlConfig,
        URI functionHostUri,
        String sharedSecret,
        ConnectorHolder connHolder,
        List<Object> steps,
        List<Map<String, Object>> edges
    ) {
        Map<String, PipelineYamlStep> yamlByToken = indexYamlSteps(yamlConfig);
        for (int i = 0; i < template.steps().size(); i++) {
            PipelineTemplateStep step = template.steps().get(i);
            PipelineYamlStep ys = yamlByToken.get(normalizeToken(step.name()));
            String kind = ys == null ? "internal" : ys.kind().trim().toLowerCase(Locale.ROOT);
            String stepId = toKebabCase(step.name());
            String inputType = step.inputTypeName();
            String outputType = step.outputTypeName();
            String inputFp = definitionFingerprints.get(inputType);
            String outputFp = definitionFingerprints.get(outputType);
            boolean isAwait = "await".equals(kind) || "awaitable".equals(kind) || stepId.startsWith("await-");

            Object instance;
            if (isAwait) {
                awaitOutputTypes.put(i, outputType);
                instance = new DurableAwaitStep(diskStores.unitStore, diskStores.interactionStore, i, stepId, outputType);
            } else if (("query".equals(kind) || "command".equals(kind))
                && ys != null && ys.operationSelection().isPresent()) {
                PipelineYamlOperationSelection sel = ys.operationSelection().orElseThrow();
                PipelineYamlConnectorBinding binding = yamlConfig.connectors().get(sel.using());
                int major = sel.operationVersion();
                if ("query".equals(kind)) {
                    QueryStepDescriptor desc = QueryStepDescriptor.nativeQuery(
                        stepId, JsonNode.class.getName(), JsonNode.class.getName(), "ONE_TO_ONE",
                        new NativeQuerySelector(
                            ConnectorBindingName.of(sel.using()),
                            new ConnectorOperationIdentity(PolyglotConnectorProvider.PROVIDER_ID, sel.operation(),
                                ConnectorOperationKind.QUERY, major), 1),
                        Map.of());
                    instance = new ProofQueryStep(connHolder.querySupport().orElseThrow(), desc,
                        outputType, outputFp, catalogFingerprint);
                } else {
                    Map<String, Object> cfg = binding.config() == null ? Map.of() : binding.config();
                    boolean reconcile = Boolean.parseBoolean(String.valueOf(cfg.getOrDefault("reconciliationSupported", false)));
                    CommandDescriptor desc = CommandDescriptor.nativeCommand(
                        stepId,
                        new NativeCommandSelector(
                            Optional.of(ConnectorBindingName.of(sel.using())),
                            new ConnectorOperationIdentity(PolyglotConnectorProvider.PROVIDER_ID, sel.operation(),
                                ConnectorOperationKind.COMMAND, major),
                            1,
                            new CommandPolicy(true, true, reconcile, Optional.empty(),
                                Optional.of(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED), false)),
                        JsonNode.class.getName(), JsonNode.class.getName(),
                        stepId + "-id", CommandDuplicatePolicy.RETURN_RECORDED, Map.of());
                    instance = new ProofCommandStep(connHolder.commandSupport().orElseThrow(), desc,
                        stepId, outputType, outputFp, catalogFingerprint);
                }
            } else {
                String card = normalizeCardinality(step.cardinality());
                Optional<String> cmdId = Optional.of("cmd-" + stepId);
                instance = switch (card) {
                    case "ONE_TO_ONE" -> new CanonicalFunctionHostStep(
                        new CanonicalFunctionHostStep.Spec(
                            stepId, i, inputType, inputFp, outputType, outputFp,
                            catalogFingerprint, catalogue, functionHostUri, sharedSecret,
                            STEP_TIMEOUT, cmdId).withDispatchIntentStore(diskStores.dispatchIntentStore),
                        true);
                    case "ONE_TO_MANY" -> new CanonicalFunctionHostOneToManyStep(
                        new CanonicalFunctionHostOneToManyStep.Spec(
                            stepId, i, inputType, inputFp, outputType, outputFp,
                            catalogFingerprint, catalogue, functionHostUri, sharedSecret,
                            STEP_TIMEOUT, STEP_TIMEOUT, cmdId, diskStores.dispatchIntentStore,
                            4 * 1024 * 1024),
                        true);
                    case "MANY_TO_ONE" -> new CanonicalFunctionHostManyToOneStep(
                        new CanonicalFunctionHostManyToOneStep.Spec(
                            stepId, i, inputType, inputFp, outputType, outputFp,
                            catalogFingerprint, catalogue, functionHostUri, sharedSecret,
                            STEP_TIMEOUT, cmdId, diskStores.dispatchIntentStore,
                            4 * 1024 * 1024),
                        true);
                    default -> throw new IllegalArgumentException("Unsupported cardinality: " + card);
                };
            }
            steps.add(instance);
            edges.add(Map.of(
                "stepClass", instance.getClass().getName(),
                "inputTypeId", inputType,
                "outputTypeId", outputType));
        }
    }

    private record ConnectorHolder(
        Optional<QueryStepSupport> querySupport,
        Optional<CommandStepSupport> commandSupport,
        Optional<ConnectorBindingRegistry> bindingRegistry,
        Optional<ConnectorRuntimeContext> runtimeContext
    ) { }

    private static final class ProofQueryStep extends ConfigurableStep
        implements StepOneToOne<TypedDurablePayload, TypedDurablePayload> {
        private final QueryStepSupport querySupport;
        private final QueryStepDescriptor descriptor;
        private final String outputType;
        private final String outputFp;
        private final String catalogFp;

        ProofQueryStep(QueryStepSupport querySupport, QueryStepDescriptor descriptor,
            String outputType, String outputFp, String catalogFp) {
            this.querySupport = querySupport;
            this.descriptor = descriptor;
            this.outputType = outputType;
            this.outputFp = outputFp;
            this.catalogFp = catalogFp;
        }

        @Override
        public Uni<TypedDurablePayload> applyOneToOne(TypedDurablePayload input) {
            try {
                JsonNode node = JSON.readTree(input.payload());
                return querySupport.queryOneToOne(descriptor, node, JsonNode.class).map(out -> {
                    try {
                        return new TypedDurablePayload(outputType, outputFp, catalogFp,
                            JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION,
                            JSON.writeValueAsBytes(out));
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                });
            } catch (IOException e) {
                return Uni.createFrom().failure(e);
            }
        }
    }

    private static final class ProofCommandStep extends ConfigurableStep
        implements StepOneToOne<TypedDurablePayload, TypedDurablePayload> {
        private final CommandStepSupport commandSupport;
        private final CommandDescriptor descriptor;
        private final String stepId;
        private final String outputType;
        private final String outputFp;
        private final String catalogFp;

        ProofCommandStep(CommandStepSupport commandSupport, CommandDescriptor descriptor,
            String stepId, String outputType, String outputFp, String catalogFp) {
            this.commandSupport = commandSupport;
            this.descriptor = descriptor;
            this.stepId = stepId;
            this.outputType = outputType;
            this.outputFp = outputFp;
            this.catalogFp = catalogFp;
        }

        @Override
        public Uni<TypedDurablePayload> applyOneToOne(TypedDurablePayload input) {
            try {
                JsonNode node = JSON.readTree(input.payload());
                String commandId = stepId + "-" + java.util.HexFormat.of().formatHex(
                    java.security.MessageDigest.getInstance("SHA-256").digest(input.payload())).substring(0, 16);
                return commandSupport.<JsonNode, JsonNode>execute(descriptor, (d, i) -> commandId, node).map(out -> {
                    try {
                        return new TypedDurablePayload(outputType, outputFp, catalogFp,
                            JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION,
                            JSON.writeValueAsBytes(out));
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                });
            } catch (Exception e) {
                return Uni.createFrom().failure(e);
            }
        }
    }

    private static final class DurableAwaitStep extends ConfigurableStep
        implements StepOneToOne<TypedDurablePayload, TypedDurablePayload> {
        private final AwaitUnitStore unitStore;
        private final AwaitInteractionStore interactionStore;
        private final int stepIndex;
        private final String stepId;
        private final String outputType;

        DurableAwaitStep(AwaitUnitStore unitStore, AwaitInteractionStore interactionStore,
            int stepIndex, String stepId, String outputType) {
            this.unitStore = unitStore;
            this.interactionStore = interactionStore;
            this.stepIndex = stepIndex;
            this.stepId = stepId;
            this.outputType = outputType;
        }

        @Override
        public Uni<TypedDurablePayload> applyOneToOne(TypedDurablePayload input) {
            var ctx = AwaitExecutionContextHolder.get();
            if (ctx == null) return Uni.createFrom().failure(new IllegalStateException("Missing AwaitExecutionContext"));
            long now = System.currentTimeMillis();
            long deadlineMs = now + Duration.ofMinutes(10).toMillis();
            long ttlEpochS = (now / 1000L) + 3600L;
            String unitId = new AwaitUnitOccurrence(
                ctx.tenantId(), ctx.executionId(), ctx.identityScope(), stepId, stepIndex,
                ctx.awaitOccurrenceCause().orElseThrow()).unitId();
            String correlationId = "corr-" + ctx.executionId() + "-step" + stepIndex;
            return unitStore.createOrGet(new AwaitUnitCreateCommand(
                    ctx.tenantId(), unitId, ctx.executionId(), stepId, stepIndex, "ONE_TO_ONE", now, ttlEpochS))
                .chain(() -> interactionStore.createOrGet(new AwaitCreateCommand(
                    ctx.tenantId(), ctx.executionId(), stepId, stepIndex, outputType, outputType,
                    "causation-" + ctx.executionId(), "idem-" + ctx.executionId(), correlationId,
                    input, "reviewer", "approvals", "callback", unitId, null,
                    Map.of("transport", "callback"), now, deadlineMs, ttlEpochS, Optional.empty())))
                .chain(created -> unitStore.attachPrimaryInteraction(
                        ctx.tenantId(), unitId, created.record().interactionId(), now + 1L)
                    .chain(() -> unitStore.markDispatchComplete(ctx.tenantId(), unitId, 1, now + 2L))
                    .chain(() -> Uni.createFrom().failure(new AwaitSuspendedException(
                        ctx.tenantId(), ctx.executionId(), unitId, stepIndex))));
        }
    }

    private static final class DurableAwaitCoordinator extends AwaitCoordinator {
        private final AwaitInteractionStore interactions;
        private final AwaitUnitStore units;

        DurableAwaitCoordinator(AwaitInteractionStore interactions, AwaitUnitStore units) {
            this.interactions = interactions;
            this.units = units;
        }

        @Override public Uni<Void> importSuspension(TransitionAwaitSuspension s) { return Uni.createFrom().voidItem(); }
        @Override public Uni<TransitionAwaitSuspension> suspensionSnapshot(AwaitSuspendedException s) {
            return Uni.createFrom().item(new TransitionAwaitSuspension(s.tenantId(), s.executionId(), s.unitId(), s.stepIndex()));
        }
        @Override public Uni<org.pipelineframework.awaitable.AwaitCompletionResult> complete(AwaitCompletionCommand cmd) {
            return interactions.complete(cmd);
        }
        @Override public Uni<AwaitUnitRecord> recordCompletion(AwaitInteractionRecord record, long nowEpochMs) {
            return units.markCompleted(record.tenantId(), record.unitId(), nowEpochMs)
                .map(u -> u.orElseThrow(() -> new IllegalStateException("Await unit OCC conflict")));
        }
        @Override public Uni<AwaitUnitRecord> getUnit(String tenantId, String unitId) {
            return units.get(tenantId, unitId).map(u -> u.orElseThrow());
        }
        @Override public Uni<Object> loadResumePayload(String tenantId, String unitId) {
            return getUnit(tenantId, unitId)
                .chain(u -> interactions.get(tenantId, u.primaryInteractionId()))
                .map(r -> r.orElseThrow().responsePayload());
        }
        @Override public Uni<Object> loadResumeInput(String tenantId, String unitId) {
            return loadResumePayload(tenantId, unitId);
        }
        @Override public Uni<List<AwaitInteractionRecord>> findDueTimeoutConsequences(long now, int limit) {
            return interactions.findDueTimeoutConsequences(now, limit);
        }
        @Override public Uni<List<AwaitInteractionRecord>> findTimedOut(long now, int limit) {
            return interactions.findTimedOut(now, limit);
        }
    }

    private static Map<String, PipelineYamlStep> indexYamlSteps(PipelineYamlConfig yamlConfig) {
        Map<String, PipelineYamlStep> byToken = new LinkedHashMap<>();
        for (PipelineYamlStep s : yamlConfig.steps()) {
            if (s != null && s.name() != null) byToken.put(normalizeToken(s.name()), s);
        }
        return byToken;
    }

    private static String normalizeToken(String raw) {
        return raw.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
    }

    private static String toKebabCase(String raw) {
        String spaced = raw.replaceAll("([a-z0-9])([A-Z])", "$1-$2")
            .replaceAll("[^a-zA-Z0-9]+", "-").toLowerCase(Locale.ROOT);
        String trimmed = spaced.replaceAll("^-+|-+$", "");
        return trimmed.isEmpty() ? "step" : trimmed;
    }

    private static String normalizeCardinality(String raw) {
        if (raw == null || raw.isBlank()) return "ONE_TO_ONE";
        return switch (raw.trim().toUpperCase(Locale.ROOT)) {
            case "ONE_TO_ONE", "UNARY" -> "ONE_TO_ONE";
            case "ONE_TO_MANY", "EXPANSION" -> "ONE_TO_MANY";
            case "MANY_TO_ONE", "REDUCTION" -> "MANY_TO_ONE";
            default -> raw.trim().toUpperCase(Locale.ROOT);
        };
    }

    private static PipelineOrchestratorConfig orchestratorConfig(String pipelineId, int maxRetries) {
        PipelineOrchestratorConfig.DynamoConfig dynamo = DynamoProofStoreHelper.dynamoConfig();
        return new PipelineOrchestratorConfig() {
            @Override public OrchestratorMode mode() { return OrchestratorMode.QUEUE_ASYNC; }
            @Override public CoordinationHost coordinationHost() { return CoordinationHost.NATIVE; }
            @Override public boolean processLoopsDisabled() { return false; }
            @Override public String defaultTenant() { return "default"; }
            @Override public String pipelineId() { return pipelineId; }
            @Override public String releaseVersion() { return "proof-1"; }
            @Override public int executionTtlDays() { return 1; }
            @Override public long leaseMs() { return Duration.ofMinutes(2).toMillis(); }
            @Override public int maxRetries() { return maxRetries; }
            @Override public Optional<Duration> maxCircuitDeferral() { return Optional.empty(); }
            @Override public Duration retryDelay() { return Duration.ofMillis(10); }
            @Override public double retryMultiplier() { return 1.0d; }
            @Override public Duration sweepInterval() { return Duration.ofSeconds(30); }
            @Override public int sweepLimit() { return 50; }
            @Override public OrchestratorIdempotencyPolicy idempotencyPolicy() { return OrchestratorIdempotencyPolicy.CLIENT_KEY_REQUIRED; }
            @Override public String stateProvider() { return ""; }
            @Override public String dispatcherProvider() { return ""; }
            @Override public String dlqProvider() { return ""; }
            @Override public Optional<String> queueUrl() { return Optional.empty(); }
            @Override public Optional<String> dlqUrl() { return Optional.empty(); }
            @Override public Optional<String> resumeTokenSecret() { return Optional.empty(); }
            @Override public DynamoConfig dynamo() { return dynamo; }
            @Override public SqsConfig sqs() { return null; }
            @Override public TenancyConfig tenancy() { return null; }
            @Override public QuotaConfig quotas() { return null; }
            @Override public ControlPlaneConfig controlPlane() { return null; }
            @Override public AdminConfig admin() { return null; }
            @Override public ReleasesConfig releases() { return null; }
            @Override public WorkerConfig worker() { return WORKER_CONFIG; }
            @Override public boolean strictStartup() { return false; }
        };
    }

    private static final PipelineOrchestratorConfig.WorkerConfig WORKER_CONFIG =
        new PipelineOrchestratorConfig.WorkerConfig() {
            @Override public PipelineOrchestratorConfig.WorkerAwaitAuthorityConfig awaitAuthority() { return null; }
            @Override public String targetingMode() { return "local"; }
            @Override public Map<String, PipelineOrchestratorConfig.RegisteredRestTargetConfig> targets() { return Map.of(); }
            @Override public boolean allowLoopbackHttp() { return true; }
            @Override public TransitionWorkerExecutionMode executionMode() { return TransitionWorkerExecutionMode.SAME_THREAD; }
            @Override public int maxInFlight() { return 16; }
            @Override public Duration saturatedDelay() { return Duration.ofMillis(50); }
            @Override public List<String> allowedPayloadPrefixes() { return List.of(); }
            @Override public Optional<String> artifactId() { return Optional.empty(); }
            @Override public Optional<String> artifactDigest() { return Optional.empty(); }
            @Override public PipelineOrchestratorConfig.RestWorkerConfig rest() { return null; }
            @Override public PipelineOrchestratorConfig.GrpcWorkerConfig grpc() { return null; }
            @Override public PipelineOrchestratorConfig.SqsWorkerConfig sqs() { return null; }
            @Override public PipelineOrchestratorConfig.WorkerLifecycleConfig lifecycle() { return null; }
        };

    private static PipelineStepConfig stepConfig() {
        return new PipelineStepConfig() {
            @Override public StepConfig defaults() { return null; }
            @Override public ParallelismPolicy parallelism() { return ParallelismPolicy.SEQUENTIAL; }
            @Override public Integer maxConcurrency() { return 1; }
            @Override public Integer maxRecursiveDepth() { return 64; }
            @Override public AwaitAdmissionConfig awaitAdmission() { return null; }
            @Override public CallbackConfig callback() { return () -> true; }
            @Override public HealthConfig health() {
                return new HealthConfig() {
                    @Override public Boolean enabled() { return false; }
                    @Override public Duration startupTimeout() { return Duration.ofSeconds(5); }
                    @Override public String restPath() { return "/q/health"; }
                };
            }
            @Override public CacheConfig cache() { return null; }
            @Override public TelemetryConfig telemetry() { return null; }
            @Override public KillSwitchConfig killSwitch() { return null; }
            @Override public Map<String, StepConfig> step() { return Map.of(); }
            @Override public Map<String, ModuleConfig> module() { return Map.of(); }
            @Override public ClientConfig client() { return null; }
        };
    }
}
