package org.pipelineframework;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.runtime.core.PipelineRunnerCore;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/** Bounded binding over the released runtime seam. Artifact admission has no business side effects. */
final class PolicyBinding {
    static final String PROFILE = "tpf-polyglot-unary-policy/1";
    private static final String OWNER = "LOCAL_SHELL";
    private static final String SANITISED = "Unclassified business function failure";
    private static final Map<String, String> POLICY = Map.of(
        "failureMapping", "structured-failure/1", "retryOwner", OWNER,
        "resultMapping", "explicit-value/1", "cancellation", "first-accepted-terminal/1",
        "context", "explicit-snapshot/1", "recovery", "REJECT", "legacyPolicy", "REJECT");

    record Step(String id, String handler, int retries) {}
    record Value(int value) {}
    record ValueResult(String kind, Value value) {
        ValueResult(Value value) { this("VALUE", value); }
    }

    /** The existing host policy is explicit input to admission, alongside projected policy. */
    record HostPolicy(StepConfig settings, Optional<Predicate<Throwable>> independentRetry) {}

    @FunctionalInterface
    interface Handler {
        Uni<JsonNode> invoke(Value input, Map<String, String> context);
    }

    static final class Rejected extends RuntimeException {
        Rejected(String code) { super(code); }
    }

    static final class StructuredFailure extends RuntimeException {
        private final JsonNode envelope;

        StructuredFailure(JsonNode envelope) {
            super("Structured business function failure");
            this.envelope = envelope;
        }
    }

    enum FailureCode { TRANSIENT, BUSINESS_REJECTION, UNCLASSIFIED }
    record Failure(FailureCode code, String message, String owner, String stepId, boolean retryable) {}

    /** Only this private, validated carrier can reach the released retry predicate. */
    private static final class ValidatedFailure extends RuntimeException {
        private final Failure failure;

        ValidatedFailure(Failure failure) {
            super(failure.code().name());
            this.failure = failure;
        }
    }

    static List<Step> admit(JsonNode root, Map<String, Handler> handlers) {
        keys(root, "UNSUPPORTED_PROJECTION", "schemaVersion", "profile", "kind", "pipeline",
            "requiredCapabilities", "types", "steps", "contextKeys", "provenance", "policy");
        require(exactInteger(root.path("schemaVersion"), 1, 1), "UNSUPPORTED_SCHEMA");
        require(PROFILE.equals(root.path("profile").textValue())
            && "compiler-projection-probe".equals(root.path("kind").textValue()), "UNSUPPORTED_PROFILE");
        JsonNode policy = root.path("policy");
        keys(policy, "UNSUPPORTED_POLICY", POLICY.keySet().toArray(String[]::new));
        POLICY.forEach((key, expected) -> require(expected.equals(policy.path(key).textValue()), "UNSUPPORTED_POLICY"));
        JsonNode pipeline = root.path("pipeline");
        keys(pipeline, "UNSUPPORTED_PIPELINE", "id", "inputType", "outputType", "transport", "platform", "execution");
        require(nonemptyText(pipeline.path("id")), "INVALID_PIPELINE_ID");
        require("LOCAL".equals(pipeline.path("transport").textValue())
            && "COMPUTE".equals(pipeline.path("platform").textValue())
            && "DIRECT".equals(pipeline.path("execution").textValue()), "UNSUPPORTED_EXECUTION");
        array(root.path("requiredCapabilities"), "UNSUPPORTED_CAPABILITY",
            "pure", "linear", "unary", "local", "retry", "cancellation", "context");
        array(root.path("contextKeys"), "UNSUPPORTED_CONTEXT", "executionId", "traceId");
        String type = pipeline.path("inputType").asText();
        require(type.equals(pipeline.path("outputType").textValue()), "TYPE_MISMATCH");
        validateTypes(root.path("types"), type);

        JsonNode steps = root.path("steps");
        require(steps.isArray() && steps.size() == 3, "UNSUPPORTED_STEP_SHAPE");
        var bindings = new ArrayList<Step>();
        var ids = new HashSet<String>();
        var registered = new HashSet<String>();
        for (JsonNode step : steps) {
            keys(step, "UNSUPPORTED_STEP", "id", "inputType", "outputType", "handler", "retryLimit");
            require(nonemptyText(step.path("id")) && ids.add(step.path("id").textValue()), "DUPLICATE_STEP");
            require(type.equals(step.path("inputType").textValue())
                && type.equals(step.path("outputType").textValue()), "TYPE_MISMATCH");
            require(nonemptyText(step.path("handler")) && handlers.containsKey(step.path("handler").textValue()),
                "MISSING_HANDLER");
            require(registered.add(step.path("handler").textValue()), "AMBIGUOUS_HANDLER");
            require(exactInteger(step.path("retryLimit"), 0, 2), "UNSUPPORTED_RETRY");
            bindings.add(new Step(step.path("id").textValue(), step.path("handler").textValue(),
                step.path("retryLimit").intValue()));
        }
        JsonNode provenance = root.path("provenance");
        keys(provenance, "INVALID_PROVENANCE", "dslSha256", "bindingsSha256", "generatorTreeSha256",
            "contractsVersion", "dslJarSha256", "semanticModelJarSha256");
        require("26.11.1".equals(provenance.path("contractsVersion").textValue()), "INVALID_PROVENANCE");
        provenance.properties().forEach(entry -> require(entry.getKey().equals("contractsVersion")
            || (entry.getValue().isTextual() && entry.getValue().textValue().matches("[a-f0-9]{64}")),
            "INVALID_PROVENANCE"));
        return List.copyOf(bindings);
    }

    private static void validateTypes(JsonNode types, String executable) {
        require(types.isArray() && !types.isEmpty(), "UNSUPPORTED_TYPE");
        var names = new HashSet<String>();
        for (JsonNode type : types) {
            keys(type, "UNSUPPORTED_TYPE", "id", "fields");
            require(nonemptyText(type.path("id")) && names.add(type.path("id").textValue()), "UNSUPPORTED_TYPE");
            JsonNode fields = type.path("fields");
            require(fields.isArray() && !fields.isEmpty(), "UNSUPPORTED_TYPE");
            var fieldNames = new HashSet<String>();
            for (JsonNode field : fields) {
                keys(field, "UNSUPPORTED_TYPE", "name", "type", "presence", "nullability");
                require(nonemptyText(field.path("name")) && fieldNames.add(field.path("name").textValue())
                    && "int32".equals(field.path("type").textValue())
                    && Set.of("REQUIRED", "OPTIONAL").contains(field.path("presence").asText())
                    && Set.of("NON_NULL", "NULLABLE").contains(field.path("nullability").asText()), "UNSUPPORTED_TYPE");
            }
            if (executable.equals(type.path("id").textValue())) {
                JsonNode field = fields.path(0);
                require(fields.size() == 1 && "value".equals(field.path("name").textValue())
                    && "REQUIRED".equals(field.path("presence").textValue())
                    && "NON_NULL".equals(field.path("nullability").textValue()), "UNSUPPORTED_TYPE");
            }
        }
        require(names.contains(executable), "TYPE_MISMATCH");
    }

    private static void array(JsonNode node, String diagnostic, String... values) {
        require(node.isArray() && node.size() == values.length, diagnostic);
        for (int index = 0; index < values.length; index++) {
            require(values[index].equals(node.path(index).textValue()), diagnostic);
        }
    }

    private static void keys(JsonNode node, String diagnostic, String... names) {
        require(node != null && node.isObject() && node.size() == names.length, diagnostic);
        for (String name : names) require(node.has(name), diagnostic);
    }

    private static void require(boolean condition, String diagnostic) {
        if (!condition) throw new Rejected(diagnostic);
    }

    private static boolean nonemptyText(JsonNode node) {
        return node.isTextual() && !node.textValue().isEmpty();
    }

    private static boolean exactInteger(JsonNode node, int min, int max) {
        if (!node.isNumber()) return false;
        BigDecimal value = node.decimalValue();
        return value.stripTrailingZeros().scale() <= 0
            && value.compareTo(BigDecimal.valueOf(min)) >= 0 && value.compareTo(BigDecimal.valueOf(max)) <= 0;
    }

    private static Map<String, String> snapshot(JsonNode caller) {
        keys(caller, "INVALID_CONTEXT", "executionId", "traceId");
        for (JsonNode value : caller) {
            require(value.isTextual() && value.textValue().chars().anyMatch(ch ->
                ch != ' ' && ch != '\t' && ch != '\r' && ch != '\n'), "INVALID_CONTEXT");
        }
        return Map.of("executionId", caller.path("executionId").textValue(), "traceId", caller.path("traceId").textValue());
    }

    private static Value value(JsonNode raw) {
        keys(raw, "INVALID_VALUE", "value");
        JsonNode number = raw.path("value");
        require(number.isIntegralNumber() && number.canConvertToInt(), "INVALID_VALUE");
        return new Value(number.intValue());
    }

    private static Value result(JsonNode raw) {
        if (raw != null && raw.isObject() && raw.size() == 1 && "EMPTY".equals(raw.path("kind").textValue())) {
            throw new Rejected("UNSUPPORTED_EMPTY");
        }
        keys(raw, "INVALID_VALUE", "kind", "value");
        require("VALUE".equals(raw.path("kind").textValue()), "INVALID_VALUE");
        return value(raw.path("value"));
    }

    private static Throwable bindFailure(Throwable error, String stepId) {
        if (!(error instanceof StructuredFailure structured)) {
            return new ValidatedFailure(new Failure(FailureCode.UNCLASSIFIED, SANITISED, OWNER, stepId, false));
        }
        JsonNode raw = structured.envelope;
        keys(raw, "MALFORMED_FAILURE", "code", "message", "owner", "stepId", "retryable");
        require(raw.path("code").isTextual() && raw.path("message").isTextual()
            && raw.path("owner").isTextual() && raw.path("stepId").isTextual()
            && raw.path("retryable").isBoolean(), "MALFORMED_FAILURE");
        FailureCode code;
        try {
            code = FailureCode.valueOf(raw.path("code").textValue());
        } catch (IllegalArgumentException unknown) {
            throw new Rejected("UNKNOWN_FAILURE_CODE");
        }
        String message = raw.path("message").textValue();
        boolean retryable = raw.path("retryable").booleanValue();
        require(OWNER.equals(raw.path("owner").textValue()) && stepId.equals(raw.path("stepId").textValue())
            && retryable == (code == FailureCode.TRANSIENT)
            && (code != FailureCode.UNCLASSIFIED || SANITISED.equals(message)), "FAILURE_BINDING_MISMATCH");
        return new ValidatedFailure(new Failure(code, message, OWNER, stepId, retryable));
    }

    private static Map<String, Object> failed(Throwable failure) {
        if (failure instanceof ValidatedFailure structured) {
            return Map.of("kind", "FAILED", "failure", structured.failure);
        }
        if (failure instanceof Rejected rejected) return Map.of("kind", "REJECTED", "diagnostic", rejected.getMessage());
        throw new IllegalStateException("Failure escaped the policy boundary", failure);
    }

    private static final class CancelledDispatch extends CancellationException {}

    static final class Invocation {
        final CompletableFuture<Map<String, Object>> observed = new CompletableFuture<>();
        final CompletableFuture<String> delivered = new CompletableFuture<>();
        final CompletableFuture<Boolean> hostCancelled = new CompletableFuture<>();
        private final List<String> delivery = new ArrayList<>();
        private Optional<Cancellable> subscriber = Optional.empty();
        private boolean cancelled;
        private int observations;

        synchronized int observations() { return observations; }
        synchronized List<String> delivery() { return List.copyOf(delivery); }

        private synchronized void commit(Map<String, Object> outcome) {
            if (!observed.isDone()) {
                observations++;
                observed.complete(outcome);
            }
        }

        private synchronized void deliver(String kind) {
            if (observed.getNow(Map.of()).getOrDefault("kind", "").equals(kind)) {
                delivery.add(kind);
                delivered.complete(kind);
            }
        }

        private synchronized void commitFailure(Throwable error) {
            if (!observed.isDone()) commit(failed(error));
        }

        private synchronized void deliverFailure(Throwable error) {
            if ("CANCELLED".equals(observed.getNow(Map.of()).get("kind"))) return;
            try {
                deliver((String) failed(error).get("kind"));
            } catch (RuntimeException escaped) {
                observed.completeExceptionally(escaped);
                delivered.completeExceptionally(escaped);
            }
        }

        void cancel() {
            Optional<Cancellable> target;
            synchronized (this) {
                cancelled = true;
                commit(Map.of("kind", "CANCELLED"));
                target = subscriber;
            }
            target.ifPresent(Cancellable::cancel);
        }

        void reject(Rejected error, Context ambient) {
            subscribe(Uni.createFrom().failure(error), ambient);
        }

        void start(List<Step> bindings, Map<String, Handler> handlers, JsonNode input, JsonNode caller, HostPolicy host,
                   boolean preCancelled, Context ambient, BiFunction<Step, Value, Uni<Value>> afterReturn) {
            // Configured recovery or a second classifier would change settlement before delivery.
            try {
                require(!host.settings().recoverOnFailure() && host.independentRetry().isEmpty(),
                    "POLICY_OWNERSHIP_CONFLICT");
                Value initial = value(input);
                Map<String, String> context = snapshot(caller);
                if (preCancelled) {
                    cancel();
                    return;
                }
                var steps = bindings.stream().map(step ->
                    new BoundStep(step, handlers.get(step.handler()), context, this)).toList();
                Object chain = new PipelineRunnerCore().runSync(Uni.createFrom().item(initial), steps, 0, steps.size(),
                    (step, current, index) -> invoke(step, current, afterReturn));
                @SuppressWarnings("unchecked") var output = (Uni<Value>) chain;
                subscribe(output, ambient);
            } catch (Rejected error) {
                reject(error, ambient);
            }
        }

        private static Uni<Value> invoke(BoundStep step, Object input,
                                         BiFunction<Step, Value, Uni<Value>> afterReturn) {
            // Gate only a value already accepted by the released executor, before its next dispatch.
            @SuppressWarnings("unchecked")
            Uni<Value> next = (Uni<Value>) PipelineStepExecutor.applyOneToOneUnchecked(step, input);
            return next.onItem().transformToUni(value -> afterReturn.apply(step.binding, value));
        }

        private void subscribe(Uni<Value> output, Context ambient) {
            Cancellable handle = output.onCancellation().invoke(() -> hostCancelled.complete(true))
                .onItem().invoke(value -> commit(Map.of("kind", "COMPLETED", "outcome", new ValueResult(value))))
                .onFailure().invoke(this::commitFailure)
                .subscribe().with(ambient, value -> deliver("COMPLETED"), this::deliverFailure);
            boolean cancelNow;
            synchronized (this) {
                subscriber = Optional.of(handle);
                cancelNow = cancelled;
            }
            // Cancellation may arrive while subscribe() is synchronously invoking the first handler.
            if (cancelNow) handle.cancel();
        }

        private synchronized Uni<JsonNode> dispatch(Handler handler, Value input, Map<String, String> context) {
            if (cancelled) return Uni.createFrom().failure(new CancelledDispatch());
            try {
                return Optional.ofNullable(handler.invoke(input, context)).orElseGet(() -> Uni.createFrom().nullItem());
            } catch (RuntimeException error) {
                return Uni.createFrom().failure(error);
            }
        }
    }

    private static final class BoundStep extends ConfigurableStep implements StepOneToOne<Value, Value> {
        private final Step binding;
        private final Handler handler;
        private final Map<String, String> context;
        private final Invocation invocation;

        BoundStep(Step binding, Handler handler, Map<String, String> context, Invocation invocation) {
            this.binding = binding;
            this.handler = handler;
            this.context = context;
            this.invocation = invocation;
            initialiseWithConfig(new StepConfig().retryLimit(binding.retries())
                .retryWait(Duration.ofMillis(1)).maxBackoff(Duration.ofMillis(2)).recoverOnFailure(false).jitter(false));
        }

        @Override
        public boolean shouldRetry(Throwable error) {
            return error instanceof ValidatedFailure validated && validated.failure.code() == FailureCode.TRANSIENT;
        }

        @Override
        public Uni<Value> applyOneToOne(Value input) {
            // Map the result before StepOneToOne's Uni-to-Multi retry path could collapse null.
            return invocation.dispatch(handler, input, context)
                .onFailure().transform(error -> {
                    if (error instanceof CancelledDispatch) return error;
                    try {
                        return bindFailure(error, binding.id());
                    } catch (Rejected rejected) {
                        return rejected;
                    }
                })
                .onItem().transform(PolicyBinding::result);
        }
    }
}
