package org.pipelineframework;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.mutiny.subscription.UniEmitter;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.runtime.core.PipelineRunnerCore;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.NonRetryableException;
import org.pipelineframework.step.StepOneToOne;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

/**
 * Contributor-only use of the released unary executor. Package access reaches the same
 * per-item handoff as PipelineRunner with no cache, Await, or configured telemetry.
 */
final class ReleasedRuntimeProbe {
    private static final Map<String, IntUnaryOperator> FUNCTIONS = Map.of(
        "a", value -> Math.addExact(value, 1),
        "b", value -> Math.multiplyExact(value, 2),
        "c", value -> Math.subtractExact(value, 3));

    private record Value(int value) {}
    private record Binding(String id, String handler, int retryLimit) {}
    private record FixtureContext(String executionId, String traceId) {
        Context asSubscriberContext() {
            return Context.of("executionId", executionId, "traceId", traceId);
        }
    }
    private record Observation(String step, String executionId, String traceId) {}
    private record Fault(String step, String code, int times) {}
    private record Pending(UniEmitter<? super Value> emitter, Value value) {}

    public static void main(String[] args) throws Exception {
        require(args.length == 5, "expected baseline projection, scenarios, B retry projection, edges and report paths");
        try {
            new ReleasedRuntimeProbe().report(
                Path.of(args[0]), Path.of(args[1]), Path.of(args[2]), Path.of(args[3]), Path.of(args[4]));
        } finally {
            Infrastructure.getDefaultWorkerPool().shutdownNow();
            if (Infrastructure.getDefaultExecutor() instanceof ExecutorService executor) executor.shutdownNow();
        }
    }

    private void report(Path projectionPath, Path scenariosPath, Path alternatePath, Path edgesPath, Path output)
        throws Exception {
        var json = new ObjectMapper();
        byte[] projectionBytes = Files.readAllBytes(projectionPath);
        byte[] scenarioBytes = Files.readAllBytes(scenariosPath);
        byte[] alternateBytes = Files.readAllBytes(alternatePath);
        List<Binding> baseline = bindings(json.readTree(projectionBytes));
        List<Binding> alternate = bindings(json.readTree(alternateBytes));
        JsonNode scenarios = json.readTree(scenarioBytes);
        require(scenarios.path("schemaVersion").asInt() == 1, "unsupported scenarios schema");

        var cases = new ArrayList<Map<String, Object>>();
        var signals = new LinkedHashMap<String, Object>();
        for (JsonNode row : scenarios.path("cases")) {
            String id = row.path("id").textValue();
            Optional<Fault> fault = Optional.ofNullable(row.get("fault")).map(ReleasedRuntimeProbe::fault);
            var context = new FixtureContext(id, "trace-" + id);
            Map<String, Object> summary;
            if (row.path("cancelBefore").asBoolean()) {
                // An already-aborted caller never subscribes. There is no cancellation event to map.
                summary = new Run(baseline, fault, false, context, Optional.empty()).summary("CANCELLED");
                signals.put(id, "ADMISSION_SKIPPED_SUBSCRIPTION");
            } else if (row.has("cancelAfter")) {
                require(row.path("cancelAfter").asText().equals("A"), "only the A handoff has a barrier");
                var gate = new ReturnGate();
                var held = new Run(baseline, fault, false, context, Optional.of(gate));
                Cancellable subscription = held.subscribe(value(row.path("input")), 3);
                require(gate.pending().value().equals(new Value(4)), "A produced its typed value before cancellation");
                subscription.cancel();
                require(held.cancelled.get(5, TimeUnit.SECONDS), "downstream cancellation signal missing");
                require(gate.terminated.get(5, TimeUnit.SECONDS), "cancellation did not reach A's producer");
                gate.release();
                require(!held.result.isDone(), "cancelled subscription delivered a terminal result");
                summary = held.summary("CANCELLED");
                signals.put(id, "SUBSCRIBER_CANCELLED_WHILE_A_VALUE_HELD");
            } else {
                var run = new Run(baseline, fault, false, context, Optional.empty());
                run.subscribe(value(row.path("input")), 3);
                summary = run.result.get(5, TimeUnit.SECONDS);
            }
            var identified = new LinkedHashMap<>(summary);
            identified.put("id", id);
            cases.add(identified);
        }

        // Both A producers are subscribed and held before either can advance. Release on the
        // same thread in reverse order. Observe the separately bound fixture context in each step.
        var contextRows = new ArrayList<JsonNode>();
        var contextRuns = new ArrayList<Run>();
        var gates = new ArrayList<ReturnGate>();
        for (JsonNode row : scenarios.path("contexts")) {
            var gate = new ReturnGate();
            var context = new FixtureContext(row.path("executionId").asText(), row.path("traceId").asText());
            var run = new Run(baseline, Optional.empty(), false, context, Optional.of(gate));
            run.subscribe(value(row.path("input")), 3);
            require(!run.result.isDone(), "context run completed before release");
            gate.pending();
            contextRows.add(row);
            contextRuns.add(run);
            gates.add(gate);
        }
        require(gates.size() == 2, "expected exactly two overlapping context runs");
        for (ReturnGate gate : gates.reversed()) gate.release();
        var contexts = new ArrayList<Map<String, Object>>();
        for (int index = 0; index < contextRuns.size(); index++) {
            var row = contextRows.get(index);
            var run = contextRuns.get(index);
            var summary = run.result.get(5, TimeUnit.SECONDS);
            require(summary.get("outcome").equals("COMPLETED"), "uncancelled A gate did not complete");
            contexts.add(Map.of(
                "executionId", row.path("executionId").asText(), "traceId", row.path("traceId").asText(),
                "result", summary.get("result"), "observations", List.copyOf(run.observations)));
        }
        signals.put("uncancelled-A-barrier", "TWO_SUBSCRIPTIONS_COMPLETED_AFTER_RELEASE");

        var edges = new LinkedHashMap<String, Object>();
        for (JsonNode row : json.readTree(edgesPath.toFile()).path("cases")) {
            var selected = switch (row.path("projection").asText()) {
                case "baseline" -> baseline;
                case "downstream-retry" -> alternate;
                default -> throw new IllegalArgumentException("unknown edge projection " + row.path("projection"));
            };
            var fault = fault(row.path("fault"));
            var result = row.path("recoverOnFailure").asBoolean()
                ? Map.of("range", edge(selected, fault, true, 1), "full", edge(selected, fault, true, 3))
                : edge(selected, fault, false, 3);
            edges.put(row.path("id").asText(), result);
        }
        signals.put("subscriber-context", subscriberContext(baseline));
        json.writerWithDefaultPrettyPrinter().writeValue(output.toFile(), Map.of(
            "language", "java-released-runtime", "profile", "tpf-polyglot-probe/0",
            "projectionSha256", sha256(projectionBytes), "scenariosSha256", sha256(scenarioBytes),
            "downstreamProjectionSha256", sha256(alternateBytes), "cases", cases, "contexts", contexts,
            "signals", signals, "edges", edges));
    }

    private static Map<String, Object> edge(List<Binding> bindings, Fault fault, boolean recover, int stop) throws Exception {
        var run = new Run(bindings, Optional.of(fault), recover, new FixtureContext("edge", "trace-edge"), Optional.empty());
        run.subscribe(new Value(3), stop);
        var report = new LinkedHashMap<>(run.result.get(5, TimeUnit.SECONDS));
        run.failure.ifPresent(failure -> report.put("javaFailureClass", failure.getClass().getName()));
        return report;
    }

    private static Fault fault(JsonNode raw) {
        return new Fault(raw.path("step").textValue(), raw.path("code").textValue(), raw.path("times").intValue());
    }

    private static Map<String, Object> subscriberContext(List<Binding> baseline) throws Exception {
        var keys = new FixtureContext("subscriber", "trace-subscriber");
        var direct = new CompletableFuture<Boolean>();
        Uni.createFrom().context(context -> Uni.createFrom().item(hasSubscriberKeys(context)))
            .subscribe().with(keys.asSubscriberContext(), direct::complete, direct::completeExceptionally);
        var run = new Run(baseline, Optional.empty(), false, keys, Optional.empty());
        run.subscribe(new Value(3), 3);
        require(run.result.get(5, TimeUnit.SECONDS).get("outcome").equals("COMPLETED"), "context diagnostic did not finish");
        return Map.of("directMutiny", direct.get(5, TimeUnit.SECONDS),
            "insideReleasedSteps", List.copyOf(run.subscriberKeys));
    }

    private static boolean hasSubscriberKeys(Context context) {
        return context.contains((Object) "executionId") && context.contains((Object) "traceId");
    }

    private static List<Binding> bindings(JsonNode projection) {
        JsonNode pipeline = projection.path("pipeline");
        require(projection.path("schemaVersion").asInt() == 1
            && projection.path("profile").asText().equals("tpf-polyglot-probe/0")
            && pipeline.path("transport").asText().equals("LOCAL")
            && pipeline.path("platform").asText().equals("COMPUTE")
            && pipeline.path("execution").asText().equals("DIRECT"), "unsupported projection");
        var steps = new ArrayList<Binding>();
        for (JsonNode step : projection.path("steps")) {
            var retry = step.path("retryLimit");
            String id = step.path("id").asText();
            String handler = step.path("handler").asText();
            require(retry.isInt() && retry.intValue() >= 0 && retry.intValue() <= 2
                && id.equals(handler.toUpperCase(java.util.Locale.ROOT)) && FUNCTIONS.containsKey(handler)
                && step.path("inputType").asText().equals("ProbeValue")
                && step.path("outputType").asText().equals("ProbeValue"), "unsupported step binding");
            steps.add(new Binding(id, handler, retry.intValue()));
        }
        require(steps.stream().map(Binding::id).toList().equals(List.of("A", "B", "C")), "unsupported step order");
        return List.copyOf(steps);
    }

    private static Value value(JsonNode input) {
        var number = input.path("value");
        require(input.isObject() && input.size() == 1 && number.isIntegralNumber() && number.canConvertToInt(),
            "expected a canonical int32 value");
        return new Value(number.intValue());
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    private static final class ReturnGate {
        private final CompletableFuture<Pending> ready = new CompletableFuture<>();
        private final CompletableFuture<Boolean> terminated = new CompletableFuture<>();

        Uni<Value> hold(Value value) {
            return Uni.createFrom().emitter(emitter -> {
                emitter.onTermination(() -> terminated.complete(true));
                ready.complete(new Pending(emitter, value));
            });
        }

        Pending pending() throws Exception {
            return ready.get(5, TimeUnit.SECONDS);
        }

        void release() throws Exception {
            Pending pending = pending();
            pending.emitter().complete(pending.value());
        }
    }

    private static final class Run {
        private final List<ProbeStep> steps;
        private final List<Observation> observations = new ArrayList<>();
        private final List<Boolean> subscriberKeys = new ArrayList<>();
        private final CompletableFuture<Map<String, Object>> result = new CompletableFuture<>();
        private final CompletableFuture<Boolean> cancelled = new CompletableFuture<>();
        private final FixtureContext context;
        private Optional<Throwable> failure = Optional.empty();

        Run(List<Binding> bindings, Optional<Fault> fault, boolean recover, FixtureContext context,
            Optional<ReturnGate> gate) {
            this.context = context;
            steps = bindings.stream().map(binding -> new ProbeStep(binding,
                fault.filter(candidate -> candidate.step().equals(binding.id())),
                recover && binding.id().equals("A"), gate.filter(ignored -> binding.id().equals("A")),
                context, observations, subscriberKeys)).toList();
        }

        @SuppressWarnings("unchecked")
        Cancellable subscribe(Value input, int stop) {
            Object chain = new PipelineRunnerCore().runSync(Uni.createFrom().item(input), steps, 0, stop,
                (step, current, index) -> PipelineStepExecutor.applyOneToOneUnchecked(step, current));
            return ((Uni<Value>) chain).onCancellation().invoke(() -> cancelled.complete(true))
                .subscribe().with(context.asSubscriberContext(), output -> {
                    var summary = summary(output == null ? "COMPLETED_NULL" : "COMPLETED");
                    if (output != null) summary.put("result", output);
                    result.complete(summary);
                }, error -> {
                    failure = Optional.of(error);
                    var summary = summary("FAILED");
                    if (error instanceof TransientFault) summary.put("failureCode", "TRANSIENT");
                    if (error instanceof NonRetryableException) summary.put("failureCode", "BUSINESS_REJECTION");
                    result.complete(summary);
                });
        }

        Map<String, Object> summary(String outcome) {
            var attempts = new LinkedHashMap<String, Integer>();
            steps.forEach(step -> attempts.put(step.binding.id(), step.attempts.get()));
            var summary = new LinkedHashMap<String, Object>();
            summary.put("outcome", outcome);
            summary.put("attempts", attempts);
            return summary;
        }
    }

    private static final class ProbeStep extends ConfigurableStep implements StepOneToOne<Value, Value> {
        private final Binding binding;
        private final Optional<Fault> fault;
        private final Optional<ReturnGate> gate;
        private final FixtureContext context;
        private final List<Observation> observations;
        private final List<Boolean> subscriberKeys;
        private final AtomicInteger attempts = new AtomicInteger();

        ProbeStep(Binding binding, Optional<Fault> fault, boolean recover, Optional<ReturnGate> gate,
                  FixtureContext context, List<Observation> observations, List<Boolean> subscriberKeys) {
            this.binding = binding;
            this.fault = fault;
            this.gate = gate;
            this.context = context;
            this.observations = observations;
            this.subscriberKeys = subscriberKeys;
            initialiseWithConfig(new StepConfig().retryLimit(binding.retryLimit())
                .retryWait(Duration.ofMillis(1)).maxBackoff(Duration.ofMillis(2)).recoverOnFailure(recover).jitter(false));
        }

        @Override
        public Uni<Value> applyOneToOne(Value input) {
            int attempt = attempts.incrementAndGet();
            return Uni.createFrom().context(subscriberContext -> {
                subscriberKeys.add(hasSubscriberKeys(subscriberContext));
                observations.add(new Observation(binding.id(), context.executionId(), context.traceId()));
                Optional<Fault> active = fault.filter(candidate -> attempt <= candidate.times());
                if (active.isPresent()) {
                    return switch (active.orElseThrow().code()) {
                        case "TRANSIENT" -> Uni.createFrom().failure(new TransientFault());
                        case "BUSINESS_REJECTION" -> Uni.createFrom().failure(new NonRetryableException("fixture rejection"));
                        case "UNKNOWN" -> Uni.createFrom().failure(new IllegalStateException("unknown fixture failure"));
                        case "NULL_RESULT" -> Uni.createFrom().nullItem();
                        default -> throw new IllegalArgumentException("unsupported fixture fault " + active.orElseThrow());
                    };
                }
                var output = new Value(FUNCTIONS.get(binding.handler()).applyAsInt(input.value()));
                return gate.map(value -> value.hold(output)).orElseGet(() -> Uni.createFrom().item(output));
            });
        }
    }

    private static final class TransientFault extends RuntimeException {
        TransientFault() { super("fixture transient failure"); }
    }
}
