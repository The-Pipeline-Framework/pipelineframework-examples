package org.pipelineframework;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.NonRetryableException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongUnaryOperator;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.pipelineframework.PolicyBinding.*;

/** Live cases over Mutiny subscriptions, the released retry scheduler and the profiled binding. */
final class ReleasedPolicyProbe {
    private static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final Map<String, LongUnaryOperator> BUSINESS = Map.of(
        "a", value -> value + 1, "b", value -> value * 2, "c", value -> value - 3);
    private static final List<String> ARTIFACTS = List.of("baseline", "changed-order", "downstream-retry");
    private static final ConfigurableStep LEGACY = new ConfigurableStep() {};
    private static final Context AMBIENT = Context.of("executionId", "ambient", "traceId", "ambient", "credential", "ambient");

    private record Artifact(JsonNode projection, String digest) {}
    private record Call(String step, int input, String executionId, String traceId, boolean mutationBlocked) {}

    public static void main(String[] args) throws Exception {
        require(args.length == 3, "expected artifact directory, scenarios and output");
        var scheduler = new RetryScheduler();
        Infrastructure.setDefaultExecutor(scheduler);
        try {
            new ReleasedPolicyProbe().run(Path.of(args[0]), Path.of(args[1]), Path.of(args[2]), scheduler);
        } finally {
            scheduler.shutdownNow();
        }
    }

    private void run(Path fixtures, Path cases, Path output, RetryScheduler scheduler) throws Exception {
        var inventory = new LinkedHashMap<String, Artifact>();
        for (String name : ARTIFACTS) {
            Path directory = fixtures.resolve(name);
            byte[] raw = Files.readAllBytes(directory.resolve("projection.json"));
            String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(raw));
            require(Files.readAllLines(directory.resolve("SHA256SUMS")).contains(digest + "  projection.json"),
                "projection digest mismatch " + name);
            inventory.put(name, new Artifact(JSON.readTree(raw), digest));
        }
        var corpus = JSON.readTree(cases.toFile());
        require(PROFILE.equals(corpus.path("profile").textValue()) && corpus.path("schemaVersion").asInt() == 1,
            "unsupported case corpus");
        var reports = new LinkedHashMap<String, Object>();
        var overlap = new ArrayList<Case>();
        // The application's subscriber has plausible ambient values. The binding admits only explicit caller values.
        var ambientPresent = new CompletableFuture<Boolean>();
        Uni.createFrom().context(context -> Uni.createFrom().item("ambient".equals(context.get((Object) "traceId"))))
            .subscribe().with(AMBIENT, ambientPresent::complete, ambientPresent::completeExceptionally);
        require(await(ambientPresent), "ambient context control did not see the actual subscriber context");

        for (JsonNode row : corpus.path("cases")) {
            var test = new Case(row);
            Artifact artifact = inventory.get(row.path("projection").asText("baseline"));
            test.start(artifact.projection(), scheduler);
            test.waitUntilHeld();
            if (row.path("overlap").asBoolean()) {
                overlap.add(test);
                continue;
            }
            test.settle();
            reports.put(test.id(), test.report());
        }
        require(overlap.size() == 2, "expected two overlapping live invocations");
        overlap.forEach(Case::mutateCaller);
        for (Case test : overlap.reversed()) {
            test.settle();
            reports.put(test.id(), test.report());
        }

        var ordered = new ArrayList<>();
        corpus.path("cases").forEach(row -> ordered.add(reports.get(row.path("id").textValue())));
        var digests = new LinkedHashMap<String, String>();
        inventory.forEach((name, artifact) -> digests.put(name, artifact.digest()));
        var baseline = inventory.get("baseline").projection();
        checkCancellationDuringRegistration(baseline, "a", false);
        checkCancellationDuringRegistration(baseline, "a", true);
        checkCancellationDuringRegistration(baseline, "c", true);
        JSON.writeValue(output.toFile(), Map.of("language", "java-released-runtime", "profile", PROFILE,
            "artifactSha256", digests, "cases", ordered));
    }

    /** While subscribe() is still on the stack, a late signal can race pending host cancellation. */
    private static void checkCancellationDuringRegistration(JsonNode projection, String heldStep, boolean lateBeforeRegistration) {
        var invocation = new Invocation();
        var published = new CompletableFuture<Runnable>();
        var allowRegistration = new CompletableFuture<Boolean>();
        var registered = new CompletableFuture<Boolean>();
        var downstream = new AtomicInteger();
        Handler next = (input, context) -> {
            downstream.incrementAndGet();
            JsonNode output = JSON.valueToTree(new ValueResult(input));
            return Uni.createFrom().item(output);
        };
        Handler hold = (input, context) -> Uni.createFrom().emitter(emitter -> {
            published.complete(() -> emitter.complete(JSON.valueToTree(new ValueResult(new Value(4)))));
            await(allowRegistration);
        });
        var handlers = new LinkedHashMap<>(Map.of("a", next, "b", next, "c", next));
        handlers.put(heldStep, hold);
        Thread.ofVirtual().start(() -> {
            try {
                invocation.start(admit(projection, handlers), handlers, JSON.createObjectNode().put("value", 3),
                    JSON.createObjectNode().put("executionId", "registration").put("traceId", "registration-trace"),
                    new HostPolicy(new StepConfig().recoverOnFailure(false), Optional.empty()), false, AMBIENT,
                    (step, value) -> Uni.createFrom().item(value));
                registered.complete(true);
            } catch (RuntimeException failure) {
                registered.completeExceptionally(failure);
            }
        });
        Runnable releaseBusiness = await(published);
        invocation.cancel();
        require(!registered.isDone(), "subscriber was registered before the cancellation barrier");
        if (lateBeforeRegistration) releaseBusiness.run();
        allowRegistration.complete(true);
        await(registered);
        if (!lateBeforeRegistration) {
            require(await(invocation.hostCancelled), "pending cancellation did not reach the registered subscriber");
            releaseBusiness.run();
        }
        require(await(invocation.observed).equals(Map.of("kind", "CANCELLED")) && invocation.observations() == 1
            && invocation.delivery().isEmpty() && downstream.get() == (heldStep.equals("a") ? 0 : 2),
            "cancellation before registration allowed downstream work or delivery");
    }

    private static final class Case {
        private final JsonNode row;
        private final JsonNode caller;
        private final Invocation invocation = new Invocation();
        private final List<Call> calls = new ArrayList<>();
        private final AsyncGate held = new AsyncGate();
        private final RetryGate retry = new RetryGate();

        Case(JsonNode row) {
            this.row = row;
            caller = row.has("context") ? row.path("context").deepCopy() : JSON.createObjectNode()
                .put("executionId", id()).put("traceId", "trace-" + id());
        }

        String id() { return row.path("id").textValue(); }

        private boolean atGate(String phase, String step) {
            JsonNode gate = row.path("gate");
            return phase.equals(gate.path("phase").textValue()) && step.equals(gate.path("step").textValue());
        }

        private boolean retryHeld() { return "before-retry".equals(row.path("gate").path("phase").textValue()); }

        void start(JsonNode projection, RetryScheduler scheduler) {
            var handlers = new LinkedHashMap<String, Handler>();
            BUSINESS.forEach((key, business) -> handlers.put(key, bind(key.toUpperCase(java.util.Locale.ROOT), business)));
            List<Step> steps;
            try {
                steps = admit(mutated(projection, row.path("artifactMutation")), handlers);
            } catch (Rejected error) {
                invocation.reject(error, AMBIENT);
                return;
            }
            var ownership = row.path("ownership");
            if (!ownership.isMissingNode() && (!ownership.isObject() || ownership.size() != 2
                    || !ownership.path("legacyRetry").isBoolean() || !ownership.path("recovery").isBoolean())) {
                invocation.reject(new Rejected("INVALID_POLICY"), AMBIENT);
                return;
            }
            var settings = new StepConfig().recoverOnFailure(ownership.path("recovery").asBoolean());
            Optional<Predicate<Throwable>> independentRetry = ownership.path("legacyRetry").asBoolean()
                ? Optional.of(LEGACY::shouldRetry) : Optional.empty();
            if (retryHeld()) scheduler.arm(retry);
            invocation.start(steps, Map.copyOf(handlers),
                row.has("input") ? row.path("input") : JSON.createObjectNode().put("value", 3), caller,
                new HostPolicy(settings, independentRetry), "prestart".equals(row.path("cancel").textValue()), AMBIENT,
                (step, value) -> atGate("after-return", step.id())
                    ? held.hold(() -> value) : Uni.createFrom().item(value));
        }

        private Handler bind(String step, LongUnaryOperator business) {
            var attempts = new AtomicInteger();
            return (input, snapshot) -> {
                int attempt = attempts.getAndIncrement();
                boolean mutationBlocked;
                try {
                    snapshot.put("traceId", "handler mutation");
                    mutationBlocked = false;
                } catch (UnsupportedOperationException expected) {
                    mutationBlocked = true;
                }
                calls.add(new Call(step, input.value(), snapshot.get("executionId"), snapshot.get("traceId"), mutationBlocked));
                JsonNode fault = row.path("faults").path(step).path(attempt);
                if ("throw".equals(fault.path("kind").textValue())) {
                    // A legacy Java marker does not classify this raw host exception as business rejection.
                    throw new NonRetryableException("private provider detail");
                }
                Supplier<Optional<JsonNode>> produce = () -> switch (fault.path("kind").asText()) {
                    case "failure" -> throw new StructuredFailure(fault.path("envelope"));
                    case "implicit" -> Optional.empty();
                    case "return" -> fault.path("outcome").isNull() ? Optional.empty() : Optional.of(fault.path("outcome"));
                    default -> Optional.of(JSON.createObjectNode().put("kind", "VALUE")
                        .set("value", JSON.createObjectNode().put("value", business.applyAsLong(input.value()))));
                };
                // A real emitter holds the asynchronous handler's signal. Its producer still settles after release.
                if (attempt == 0 && atGate("in-flight", step)) {
                    return held.hold(produce).onItem().transformToUni(Case::fromOptional);
                }
                return Uni.createFrom().item(produce).onItem().transformToUni(Case::fromOptional);
            };
        }

        private static Uni<JsonNode> fromOptional(Optional<JsonNode> result) {
            return result.map(value -> Uni.createFrom().item(value)).orElseGet(() -> Uni.createFrom().nullItem());
        }

        void waitUntilHeld() {
            if (retryHeld()) await(retry.entered);
            else if (row.has("gate")) await(held.pending);
        }

        void mutateCaller() {
            ((ObjectNode) caller).put("executionId", "caller mutation").put("traceId", "caller mutation");
        }

        void settle() {
            if ("during".equals(row.path("cancel").textValue())) {
                cancelTwice();
                require(await(invocation.hostCancelled), "cancellation did not reach the actual subscription");
                require(await(invocation.observed).equals(Map.of("kind", "CANCELLED"))
                    && invocation.delivery().isEmpty(), "cancelled execution delivered while held");
            }
            if (retryHeld()) {
                retry.release.complete(true);
                await(retry.drained);
            } else if (row.has("gate")) {
                await(held.pending).run();
            }
        }

        private void cancelTwice() {
            invocation.cancel();
            invocation.cancel();
        }

        Map<String, Object> report() {
            var terminal = await(invocation.observed);
            if (!terminal.get("kind").equals("CANCELLED")) await(invocation.delivered);
            if ("after".equals(row.path("cancel").textValue())) cancelTwice();
            return Map.of("id", id(), "calls", List.copyOf(calls), "terminal", terminal,
                "observations", invocation.observations(), "delivery", invocation.delivery());
        }
    }

    private static JsonNode mutated(JsonNode original, JsonNode mutation) {
        if (mutation.isMissingNode()) return original;
        JsonNode altered = original.deepCopy();
        JsonNode current = altered;
        JsonNode path = mutation.path("path");
        for (int index = 0; index < path.size() - 1; index++) {
            JsonNode key = path.path(index);
            current = key.isIntegralNumber() ? current.path(key.intValue()) : current.path(key.textValue());
        }
        JsonNode last = path.path(path.size() - 1);
        if (last.isIntegralNumber()) ((ArrayNode) current).set(last.intValue(), mutation.path("value"));
        else ((ObjectNode) current).set(last.textValue(), mutation.path("value"));
        return altered;
    }

    private static final class AsyncGate {
        private final CompletableFuture<Runnable> pending = new CompletableFuture<>();

        <T> Uni<T> hold(Supplier<T> produce) {
            return Uni.createFrom().emitter(emitter -> pending.complete(() -> {
                T result;
                try {
                    result = produce.get();
                } catch (RuntimeException failure) {
                    emitter.fail(failure);
                    return;
                }
                emitter.complete(result);
            }));
        }
    }

    private static final class RetryGate {
        private final CompletableFuture<Boolean> entered = new CompletableFuture<>();
        private final CompletableFuture<Boolean> release = new CompletableFuture<>();
        private final CompletableFuture<Boolean> drained = new CompletableFuture<>();
    }

    /** Hold the actual Mutiny scheduled task before dispatch, never a second retry loop. */
    private static final class RetryScheduler extends ScheduledThreadPoolExecutor {
        private final AtomicReference<RetryGate> armed = new AtomicReference<>();
        private final ThreadLocal<RetryGate> active = new ThreadLocal<>();

        RetryScheduler() { super(1); }

        void arm(RetryGate gate) { require(armed.compareAndSet(null, gate), "retry gate already armed"); }

        @Override
        protected void beforeExecute(Thread worker, Runnable task) {
            super.beforeExecute(worker, task);
            Optional.ofNullable(armed.getAndSet(null)).ifPresent(gate -> {
                active.set(gate);
                gate.entered.complete(true);
                await(gate.release);
            });
        }

        @Override
        protected void afterExecute(Runnable task, Throwable failure) {
            Optional.ofNullable(active.get()).ifPresent(gate -> gate.drained.complete(true));
            active.remove();
            super.afterExecute(task, failure);
        }
    }

    private static <T> T await(CompletableFuture<T> future) {
        try {
            return future.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted live case", interrupted);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException failure) {
            throw new IllegalStateException("live case did not reach its awaited boundary", failure);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
