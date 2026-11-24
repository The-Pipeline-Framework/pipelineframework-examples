package org.pipelineframework;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.inject.Instance;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.deployment.release.CredentialResolver;
import org.pipelineframework.deployment.release.ResolverProfile;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.objectpublish.ObjectPublishCompletionService;
import org.pipelineframework.orchestrator.ControlPlaneSecretResolver;
import org.pipelineframework.orchestrator.DeadLetterEnvelope;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.DynamoExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionAdmissionReceipt;
import org.pipelineframework.orchestrator.ExecutionDurablePayloadResolver;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionResultShapeResolver;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.HostedExecutionAdmissionResource;
import org.pipelineframework.orchestrator.HostedExecutionAdmissionResource.Request;
import org.pipelineframework.orchestrator.HostedPipelineControlPlaneResource;
import org.pipelineframework.orchestrator.JsonDurablePayloadCodec;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.NativeWorkerReleaseBinding;
import org.pipelineframework.orchestrator.NativeWorkerReleaseBinding.Target;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.RestPipelineTransitionWorker;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.dto.HostedExecutionResultResponse;
import org.pipelineframework.orchestrator.release.InMemoryPipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistrar;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;

/** Drives the installed runtime's hosted resource and QUEUE_ASYNC delivery using fixture dependency wiring. */
final class HostedWorkerCoordinatorProbe {
    private static final ObjectMapper JSON = PipelineJson.mapper();
    private static final Duration TIMEOUT = Duration.ofSeconds(15);
    private static final String AUTH = "Bearer fixture-admin-token";
    private static final String COMPLETION_INPUT = "{ \n\"value\" : 3 }";

    private final Target target;
    private final NativeWorkerReleaseBinding binding;
    private final PipelineOrchestratorConfig config;
    private final PipelineReleaseRegistry registry = new InMemoryPipelineReleaseRegistry();
    private final List<ExecutionWorkItem> deliveries = new ArrayList<>();
    private final List<DeadLetterEnvelope> deadLetters = new ArrayList<>();
    private final DynamoDbClient dynamo;
    private final DynamoExecutionStateStore store;
    private final LocalPipelineControlPlane control;
    private final HostedPipelineControlPlaneResource hosted;
    private final HostedExecutionAdmissionResource admission;
    private final RestPipelineTransitionWorker worker = new RestPipelineTransitionWorker();
    private final String inputType;
    private final String outputType;
    private final String prefix;
    private Optional<ExecutionAdmissionReceipt> completionReceipt = Optional.empty();

    public static void main(String[] args) throws Exception {
        Path launch = Path.of(args[0]);
        URI dynamoEndpoint = URI.create(args[1]);
        require("http".equals(dynamoEndpoint.getScheme()) && ("127.0.0.1".equals(dynamoEndpoint.getHost())
            || "localhost".equals(dynamoEndpoint.getHost())), "Dynamo endpoint must be a local HTTP service");
        try (var dynamo = DynamoDbClient.builder().endpointOverride(dynamoEndpoint).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .build()) {
            var probe = new HostedWorkerCoordinatorProbe(launch, dynamo);
            try {
                String mode = args.length == 3 ? args[2] : "dispatch";
                var result = switch (mode) {
                    case "dispatch" -> probe.dispatchAndReplay();
                    case "unavailable" -> probe.rejectUnavailable();
                    default -> throw new IllegalArgumentException("Unknown proof mode");
                };
                System.out.println(JSON.writeValueAsString(result));
            } finally {
                probe.deleteTables();
            }
        } finally {
            Infrastructure.getDefaultWorkerPool().shutdownNow();
            if (Infrastructure.getDefaultExecutor() instanceof ExecutorService executor) {
                executor.shutdownNow();
            }
        }
    }

    private HostedWorkerCoordinatorProbe(Path launch, DynamoDbClient dynamo) throws Exception {
        JsonNode input = JSON.readTree(Files.readAllBytes(launch));
        this.dynamo = dynamo;
        target = JSON.treeToValue(input.required("target"), Target.class);
        Path descriptor = Path.of(input.required("descriptor").asText());
        binding = NativeWorkerReleaseBinding.prepare(target, descriptor,
            Files.createTempDirectory(launch.getParent(), "coordinator-binding-"), ResolverProfile.localOnly(),
            CredentialResolver.NONE);
        inputType = input.required("inputType").asText();
        outputType = input.required("outputType").asText();
        prefix = "native_proof_" + UUID.randomUUID().toString().replace("-", "");
        config = new SmallRyeConfigBuilder().withMapping(PipelineOrchestratorConfig.class)
            .withDefaultValues(Map.of(
                "pipeline.orchestrator.mode", "QUEUE_ASYNC",
                "pipeline.orchestrator.state-provider", "dynamo",
                "pipeline.orchestrator.idempotency-policy", "CLIENT_KEY_REQUIRED",
                "pipeline.orchestrator.control-plane.enabled", "true",
                "pipeline.orchestrator.control-plane.admin-token", "fixture-admin-token",
                "pipeline.orchestrator.worker.rest.request-timeout", "PT5S",
                "pipeline.orchestrator.dynamo.execution-table", prefix + "_execution",
                "pipeline.orchestrator.dynamo.execution-key-table", prefix + "_key",
                "pipeline.orchestrator.dynamo.execution-payload-table", prefix + "_payload"))
            .build().getConfigMapping(PipelineOrchestratorConfig.class);

        var registrar = new PipelineReleaseRegistrar();
        var release = registrar.validate(target.tenantId(), target.pipelineId(), descriptor.toString(),
            System.currentTimeMillis());
        registry.register(release).await().atMost(TIMEOUT);
        store = store();
        createTable("_execution", "tenant_id", "execution_id");
        createTable("_key", "tenant_execution_key");
        createTable("_payload", "payload_id", "payload_part");

        var codec = new JsonTransitionPayloadCodec();
        ControlPlaneSecretResolver secrets = ref -> {
            require(target.sharedSecretRef().equals(ref), "Unexpected secret reference");
            return input.required("secret").asText();
        };
        inject(worker, "orchestratorConfig", config);
        inject(worker, "secretResolver", secrets);
        inject(worker, "invocationRuntime", new PipelineInvocationRuntime());

        var queue = new QueueAsyncCoordinator();
        queue.orchestratorConfig = config;
        queue.executionStateStores = instance(store);
        queue.workDispatchers = instance(dispatcher());
        queue.deadLetterPublishers = instance(deadLetterPublisher());
        queue.executionInputPolicy = new ExecutionInputPolicy();
        queue.executionInputPolicy.orchestratorConfig = config;
        queue.executionResultShapeResolver = new ExecutionResultShapeResolver();
        queue.transitionPayloadCodec = codec;
        queue.releaseRegistry = registry;
        queue.segmentBoundaryLedger = new SegmentBoundaryLedger();
        queue.executionFailureHandler = new ExecutionFailureHandler();
        queue.executionFailureHandler.orchestratorConfig = config;
        queue.executionFailureHandler.segmentBoundaryLedger = queue.segmentBoundaryLedger;
        queue.awaitCoordinator = new AwaitCoordinator();
        queue.transitionWorkerExecutor = new TransitionWorkerExecutor(config);
        queue.checkpointPublicationService = new CheckpointPublicationService();
        queue.objectPublishCompletionService = new ObjectPublishCompletionService();
        inject(queue, "nativeWorkerBinding", instance(binding));
        inject(queue, "pairedRestWorker", worker);

        control = new LocalPipelineControlPlane();
        control.queueAsyncCoordinator = queue;
        hosted = new HostedPipelineControlPlaneResource();
        inject(hosted, "orchestratorConfig", config);
        inject(hosted, "controlPlane", control);
        inject(hosted, "releaseRegistry", registry);
        inject(hosted, "releaseRegistrar", registrar);
        inject(hosted, "payloadCodec", codec);
        inject(hosted, "secretResolver", secrets);
        inject(hosted, "nativeWorkerBinding", instance(binding));
        admission = new HostedExecutionAdmissionResource();
        inject(admission, "hosted", hosted);
        inject(admission, "nativeControlPlane", control);
        inject(admission, "pairedRestWorker", worker);
    }

    private ObjectNode dispatchAndReplay() throws Exception {
        ObjectNode report = exercise();
        System.out.println("DISPATCH_COMPLETE");
        System.out.flush();
        try (var input = new BufferedReader(new InputStreamReader(System.in))) {
            require("OFFLINE".equals(input.readLine()), "Worker shutdown acknowledgement was missing");
        }
        require(admit("fresh-offline", "{\"value\":3}").getStatus() == 503, "Stopped worker admitted new input");
        var replay = admit("completed", COMPLETION_INPUT);
        require(replay.getStatus() == 200 && completionReceipt.orElseThrow().equals(replay.getEntity())
            && deliveries.isEmpty(), "Offline replay lost the original receipt or re-enqueued");
        var lookup = admission.get(target.tenantId(), target.pipelineId(), "completed", AUTH)
            .await().atMost(TIMEOUT);
        require(lookup.getStatus() == 200 && completionReceipt.orElseThrow().equals(lookup.getEntity()),
            "Offline inquiry lost the original receipt");
        report.put("offlineReplay", replay.getStatus());
        return report;
    }

    private ObjectNode exercise() throws Exception {
        ObjectNode report = JSON.createObjectNode();
        ObjectNode invocations = report.putObject("invocations");
        ObjectNode terminal = report.putObject("terminal");
        PipelineTransitionWorker unexpected = command -> {
            throw new AssertionError("Native admission reached the legacy Java worker");
        };
        require(admit("invalid-int32", "{\"value\":2147483648}").getStatus() == 400, "Admitted an invalid int32");
        require(deliveries.isEmpty(), "Invalid input dispatched work");
        for (Case test : List.of(new Case("completed", 3, Optional.empty()),
                new Case("transient", 10, Optional.of("TRANSIENT_EXHAUSTED")),
                new Case("business", 11, Optional.of("BUSINESS_REJECTION")),
                new Case("unclassified", 12, Optional.of("UNCLASSIFIED")))) {
            String payload = test.code().isEmpty() ? COMPLETION_INPUT : "{\"value\":" + test.value() + "}";
            var response = admit(test.key(), payload);
            require(response.getStatus() == 200, "Hosted admission returned " + response.getStatus());
            var receipt = (ExecutionAdmissionReceipt) response.getEntity();
            var delivery = new ExecutionWorkItem(target.tenantId(), receipt.executionId());
            require(deliveries.equals(List.of(delivery)), "Admission did not enqueue exactly one work item");
            deliveries.clear();
            var original = store.getExecution(delivery.tenantId(), delivery.executionId())
                .await().atMost(TIMEOUT).orElseThrow();
            var input = NativeWorkerReleaseBinding.AdmittedInput.fromSnapshot(original.inputPayload()).orElseThrow();
            require(input.descriptorDigest().equals(target.descriptorDigest())
                && input.payload().equals(new SerializedTransitionPayload(inputType, target.profiles().payloadEncoding(), payload)),
                "Changed typed input after storage");
            control.processExecutionWorkItem(delivery, unexpected).await().atMost(TIMEOUT);
            control.processExecutionWorkItem(delivery, unexpected).await().atMost(TIMEOUT);
            var observed = store().getExecution(delivery.tenantId(), delivery.executionId())
                .await().atMost(TIMEOUT).orElseThrow();
            require(observed.attempt() == original.attempt() && deliveries.isEmpty(), "Coordinator retried execution");
            ObjectNode result = terminal.putObject(test.key());
            result.put("status", observed.status().name()).put("version", observed.version())
                .put("attempt", observed.attempt()).put("transitionKey", observed.lastTransitionKey());
            if (test.code().isPresent()) {
                String code = test.code().orElseThrow();
                require(observed.status() == ExecutionStatus.FAILED && code.equals(observed.errorCode())
                    && observed.failedStepIndex() == 1, "Unexpected terminal portable failure: " + observed);
                var dlq = deadLetters.removeFirst();
                require(code.equals(dlq.errorCode()) && !dlq.retryable(), "Unexpected dead letter: " + dlq);
                String diagnostic = switch (code) {
                    case "TRANSIENT_EXHAUSTED" -> "Local retry budget exhausted";
                    case "BUSINESS_REJECTION" -> "Business function rejected input";
                    case "UNCLASSIFIED" -> "Unclassified business function failure";
                    default -> throw new AssertionError("Unexpected failure code " + code);
                };
                require(diagnostic.equals(observed.errorMessage()) && diagnostic.equals(dlq.errorMessage()),
                    "Changed portable diagnostic or leaked native exception");
                result.put("failure", code).put("diagnostic", diagnostic);
            } else {
                completionReceipt = Optional.of(receipt);
                require(observed.status() == ExecutionStatus.SUCCEEDED, "Native completion did not settle: " + observed);
                var expected = new SerializedTransitionPayload(outputType, target.profiles().payloadEncoding(),
                    "{\"value\":5}");
                require(List.of(expected).equals(observed.resultPayload()), "Changed typed result after storage");
                var exposed = hosted.getExecutionResult(delivery.tenantId(), delivery.executionId(), AUTH)
                    .await().atMost(TIMEOUT);
                require(exposed.getStatus() == 200
                    && expected.equals(((HostedExecutionResultResponse) exposed.getEntity()).resultPayload()),
                    "Changed typed hosted result");
                result.set("typedPayload", JSON.valueToTree(expected));
            }
            require(deadLetters.isEmpty(), "Duplicate dead letter");
            var replay = admit(test.key(), payload);
            require(replay.getStatus() == 200 && receipt.equals(replay.getEntity()) && deliveries.isEmpty(),
                "Original admission replay changed the receipt or re-enqueued work");
            require(admit(test.key(), "{\"value\":17}").getStatus() == 409, "Different input replayed as original");
            invocations.set(test.key(), JSON.valueToTree(Map.of(
                "executionId", delivery.executionId(), "traceId", delivery.executionId())));
        }
        return report;
    }

    private ObjectNode rejectUnavailable() {
        var response = admit("unavailable", "{\"value\":3}");
        require(response.getStatus() == 503 && deliveries.isEmpty(), "Unavailable worker was admitted");
        var lookup = admission.get(target.tenantId(), target.pipelineId(), "unavailable", AUTH)
            .await().atMost(TIMEOUT);
        require(lookup.getStatus() == 404, "Unavailable worker admission left a receipt");
        return JSON.createObjectNode().put("unavailable", response.getStatus());
    }

    private jakarta.ws.rs.core.Response admit(String key, String payload) {
        var serialized = new SerializedTransitionPayload(inputType, target.profiles().payloadEncoding(), payload);
        return admission.admit(target.tenantId(), target.pipelineId(), AUTH,
            new Request(key, target.contractVersion(), target.releaseVersion(), ExecutionInputShape.UNI,
                serialized, false)).await().atMost(TIMEOUT);
    }

    private DynamoExecutionStateStore store() throws Exception {
        var durable = new ExecutionDurablePayloadResolver();
        inject(durable, "releaseRegistry", registry);
        inject(durable, "codec", new JsonDurablePayloadCodec());
        var state = new DynamoExecutionStateStore();
        inject(state, "client", dynamo);
        inject(state, "orchestratorConfig", config);
        inject(state, "transitionPayloadCodec", new JsonTransitionPayloadCodec());
        inject(state, "durablePayloadResolver", durable);
        return state;
    }

    private WorkDispatcher dispatcher() {
        return new WorkDispatcher() {
            @Override public Optional<String> startupValidationError() { return Optional.empty(); }
            @Override public Uni<Void> enqueueNow(ExecutionWorkItem item) {
                deliveries.add(item);
                return Uni.createFrom().voidItem();
            }
            @Override public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
                throw new AssertionError("Native attempt scheduled another delivery");
            }
        };
    }

    private DeadLetterPublisher deadLetterPublisher() {
        return new DeadLetterPublisher() {
            @Override public Optional<String> startupValidationError() { return Optional.empty(); }
            @Override public Uni<Void> publish(DeadLetterEnvelope envelope) {
                deadLetters.add(envelope);
                return Uni.createFrom().voidItem();
            }
        };
    }

    private void createTable(String suffix, String... keys) {
        var attributes = new ArrayList<AttributeDefinition>();
        var schema = new ArrayList<KeySchemaElement>();
        for (String key : keys) {
            attributes.add(AttributeDefinition.builder().attributeName(key).attributeType("S").build());
            schema.add(KeySchemaElement.builder().attributeName(key)
                .keyType(schema.isEmpty() ? KeyType.HASH : KeyType.RANGE).build());
        }
        dynamo.createTable(request -> request.tableName(prefix + suffix).billingMode(BillingMode.PAY_PER_REQUEST)
            .attributeDefinitions(attributes).keySchema(schema));
    }

    private void deleteTables() {
        for (String suffix : List.of("_execution", "_key", "_payload")) {
            dynamo.deleteTable(request -> request.tableName(prefix + suffix));
        }
    }

    private static void inject(Object bean, String field, Object value) throws ReflectiveOperationException {
        var member = bean.getClass().getDeclaredField(field);
        member.setAccessible(true);
        member.set(bean, value);
    }

    @SuppressWarnings("unchecked")
    private static <T> Instance<T> instance(T bean) {
        return (Instance<T>) Proxy.newProxyInstance(Instance.class.getClassLoader(), new Class<?>[] {Instance.class},
            (ignored, method, arguments) -> switch (method.getName()) {
                case "get" -> bean;
                case "isUnsatisfied", "isAmbiguous" -> false;
                case "isResolvable" -> true;
                case "iterator" -> List.of(bean).iterator();
                case "stream" -> Stream.of(bean);
                default -> throw new UnsupportedOperationException(method.getName());
            });
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private record Case(String key, int value, Optional<String> code) {}
}
