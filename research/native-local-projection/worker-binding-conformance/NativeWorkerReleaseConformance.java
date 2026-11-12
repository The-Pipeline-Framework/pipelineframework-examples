import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.deployment.release.CredentialResolver;
import org.pipelineframework.deployment.release.ReleaseLoader;
import org.pipelineframework.deployment.release.ReleaseVerificationException;
import org.pipelineframework.deployment.release.ResolverProfile;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.NativeWorkerReleaseBinding;
import org.pipelineframework.orchestrator.NativeWorkerReleaseBinding.Language;
import org.pipelineframework.orchestrator.NativeWorkerReleaseBinding.Profiles;
import org.pipelineframework.orchestrator.NativeWorkerReleaseBinding.Target;
import org.pipelineframework.orchestrator.PolyglotInt32PayloadBinding;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.release.producer.DefaultPipelineReleaseProducer;
import org.pipelineframework.release.producer.ReleaseArtifactInput;
import org.pipelineframework.release.producer.ReleaseProductionRequest;

/** Builds schema-1 Releases and emits trusted launch inputs only after preparation and command pinning. */
final class NativeWorkerReleaseConformance {
    private static final ObjectMapper JSON = PipelineJson.mapper();
    private static final String WORKER = "native-worker";
    private static final String TRUTH = "compiler-truth";
    private static final String TENANT = "tenant-7";
    private static final String OUTER = "application/tpf-transition-envelope+json";
    private static final String PAYLOAD = "tpf-polyglot-int32-json/1";
    private static final String EXECUTION = "tpf-polyglot-unary-policy/1";
    private static final String RESULT = "tpf-polyglot-failed-result/1";
    private static final Profiles PROFILES = new Profiles(OUTER, PAYLOAD, EXECUTION, RESULT);

    private final DefaultPipelineReleaseProducer producer = new DefaultPipelineReleaseProducer();
    private final Path bundle;
    private final Path carrier;
    private final Path metadata;
    private final Path work;
    private final Language language;
    private final PipelineContractDescriptor contract;
    private final List<String> types;
    private final ObjectNode rejections = JSON.createObjectNode();

    private NativeWorkerReleaseConformance(String[] args) throws IOException {
        language = Language.valueOf(args[0].toUpperCase(Locale.ROOT));
        Path generated = Path.of(args[1]);
        bundle = Path.of(args[2]);
        work = Files.createDirectory(Path.of(args[4]));
        carrier = generated.resolve("compiled-truth.zip");
        metadata = Files.createDirectory(work.resolve("metadata"));
        Files.copy(generated.resolve("pipeline-contract.json"), metadata.resolve("pipeline-contract.json"));
        Files.copy(generated.resolve("projection.json"), metadata.resolve("native-execution-projection.json"));
        contract = producer.loadContract(metadata.resolve("pipeline-contract.json"));
        var steps = contract.steps();
        types = List.of(steps.getFirst().inputTypeId(), steps.get(0).outputTypeId(),
            steps.get(1).outputTypeId(), steps.getLast().outputTypeId());
    }

    public static void main(String[] args) throws Exception {
        var proof = new NativeWorkerReleaseConformance(args);
        System.out.println(JSON.writeValueAsString(proof.exercise(Path.of(args[3]))));
    }

    private ObjectNode exercise(Path alternateEntry) throws Exception {
        Release verified = produce("v1", "verified");
        var binding = prepare(verified, "verified");
        var command = commandFor(verified);
        var attempt = binding.pin(command);
        checkCommandRejections(binding, command, attempt);

        Release replacement = produce("v2", "replacement");
        rejectRelease("descriptor-replacement", ReleaseVerificationException.Category.DESCRIPTOR,
            "Release descriptor digest mismatch", () -> prepare(verified.target(), replacement.descriptor(), "old-descriptor-pin"));
        var replacementBinding = prepare(replacement, "replacement");
        replacementBinding.pin(commandFor(replacement));
        reject("old-command-on-replacement", "Release binding", () -> replacementBinding.pin(command));
        attempt.requireCommand(command);
        require(attempt.target().equals(verified.target()), "Replacement changed the retained target");

        Path business = bundle.resolve(language == Language.NODE ? "app/business.js" : "app/business.py");
        Path kernel = bundle.resolve(language == Language.NODE
            ? "node_modules/@tpf/experimental-node-local/index.mjs" : "tpf_experimental_python_local/__init__.py");
        checkChangedRunnable(verified, business, "business-bytes");
        checkChangedRunnable(verified, kernel, "kernel-bytes");
        try (var ignored = new ReplaceFile(carrier, appended(carrier))) {
            rejectRelease("carrier-bytes", ReleaseVerificationException.Category.RESOLUTION,
                "Release artifact digest mismatch", () -> prepare(verified, "carrier-bytes"));
        }
        Path preparedKernel = binding.verifyPreparedTree().resolve(bundle.relativize(kernel));
        try (var ignored = new ReplaceFile(preparedKernel, appended(preparedKernel))) {
            try {
                binding.verifyPreparedTree();
                throw new AssertionError("Expected changed prepared kernel to be rejected");
            } catch (IOException rejected) {
                require(rejected.getMessage().contains("Prepared native worker tree changed"),
                    "Unexpected prepared tree rejection: " + rejected);
                rejections.put("prepared-kernel-bytes", rejected.getMessage());
            }
        }

        Path entry = bundle.resolve(language == Language.NODE ? "app/application.js"
            : "app/generated/project/application.py");
        Release mismatch;
        try (var ignored = new ReplaceFile(entry, Files.readAllBytes(alternateEntry))) {
            mismatch = produce("v3", "projection-mismatch");
        }
        var mismatchBinding = prepare(mismatch, "projection-mismatch");
        var mismatchCommand = commandFor(mismatch);
        var mismatchAttempt = mismatchBinding.pin(mismatchCommand);
        require(attempt.projectionDigest().equals(mismatchAttempt.projectionDigest()), "Expected original verified truth");

        ObjectNode evidence = JSON.createObjectNode();
        ObjectNode prepared = evidence.putObject("prepared");
        prepared.set("verified", launchInput(binding, attempt, command, false));
        prepared.set("projection-mismatch", launchInput(mismatchBinding, mismatchAttempt, mismatchCommand, true));
        ObjectNode receipts = evidence.putObject("receipts");
        receipts.set("verified", receipt(attempt));
        receipts.set("replacement", receipt(replacementBinding.pin(commandFor(replacement))));
        receipts.set("projection-mismatch", receipt(mismatchAttempt));
        evidence.set("rejections", rejections);
        return evidence;
    }

    private void checkCommandRejections(NativeWorkerReleaseBinding binding, TransitionCommandEnvelope command,
            NativeWorkerReleaseBinding.Attempt attempt) throws IOException {
        for (String field : List.of("tenantId", "pipelineId", "contractVersion", "releaseVersion")) {
            reject("command-" + field, "Release binding", () -> binding.pin(changed(command, field, "another")));
        }
        for (String field : List.of("currentStepIndex", "stopBeforeStepIndex")) {
            reject("command-" + field, "whole-bundle SINGLE range", () -> binding.pin(changed(command, field, 1)));
        }
        reject("command-payloadEncoding", "pinned native input binding",
            () -> binding.pin(changed(command, "payloadEncoding", OUTER)));
        reject("command-payloadTypeId", "pinned native input binding",
            () -> binding.pin(changed(command, "payloadTypeId", types.get(1))));
        reject("command-attempt", "retained native attempt", () -> attempt.requireCommand(changed(command, "attempt", 2)));
        reject("command-executionVersion", "retained native attempt",
            () -> attempt.requireCommand(changed(command, "executionVersion", 8)));
        reject("command-payload", "retained native attempt",
            () -> attempt.requireCommand(changed(command, "payload", "{\"value\":4}")));
        reject("profile-outer", "profile selection", () -> new Profiles(PAYLOAD, PAYLOAD, EXECUTION, RESULT));
        reject("profile-payload", "profile selection", () -> new Profiles(OUTER, OUTER, EXECUTION, RESULT));
        reject("profile-execution", "profile selection", () -> new Profiles(OUTER, PAYLOAD, "other-policy/1", RESULT));
        reject("profile-result", "profile selection", () -> new Profiles(OUTER, PAYLOAD, EXECUTION, "other-result/1"));
    }

    private void checkChangedRunnable(Release baseline, Path member, String test) throws IOException {
        Release changed;
        try (var ignored = new ReplaceFile(member, appended(member))) {
            changed = produce("changed-" + test, test);
        }
        require(!baseline.target().runnableArtifactDigest().equals(changed.target().runnableArtifactDigest()),
            "Changing an executing file did not change the runnable digest");
        try (var ignored = new ReplaceFile(baseline.runnable(), Files.readAllBytes(changed.runnable()))) {
            rejectRelease(test, ReleaseVerificationException.Category.RESOLUTION, "Release artifact digest mismatch",
                () -> prepare(baseline, test));
        }
    }

    private Release produce(String version, String name) throws IOException {
        Path output = Files.createDirectory(work.resolve("produce-" + name));
        String releaseVersion = language.name().toLowerCase(Locale.ROOT) + "-" + version;
        var artifacts = producer.materialize(List.of(
            new ReleaseArtifactInput(WORKER, "application-archive", bundle, bundle.toUri().toString(),
                DefaultPipelineReleaseProducer.defaultStepIds(contract),
                DefaultPipelineReleaseProducer.defaultCapabilities(contract)),
            new ReleaseArtifactInput(TRUTH, "compiled-truth", carrier, carrier.toUri().toString(), List.of(), List.of())),
            TRUTH, metadata, output);
        var release = producer.produce(
            new ReleaseProductionRequest(contract, releaseVersion, TRUTH, metadata, artifacts, true));
        require(release.schemaVersion() == 1, "Expected the existing schema-1 Release");
        Path descriptor = output.resolve("pipeline-release.json");
        producer.write(descriptor, release);
        var runnable = release.artifacts().stream().filter(artifact -> WORKER.equals(artifact.artifactId()))
            .findFirst().orElseThrow();
        String digest = new ReleaseLoader().load(descriptor).descriptorDigest();
        var target = new Target(TENANT, "worker-7", URI.create("https://worker.example"), "example-secret-ref",
            language, releaseVersion, digest, runnable.artifactId(), runnable.digest(), contract, PROFILES);
        return new Release(descriptor, Path.of(URI.create(runnable.uri())), target);
    }

    private TransitionCommandEnvelope commandFor(Release release) {
        var payload = new PolyglotInt32PayloadBinding(types.toArray(String[]::new)).encode(types.getFirst(), 3);
        return new TransitionCommandEnvelope(TENANT, "execution-7", contract.pipelineId(), contract.contractVersion(),
            release.target().releaseVersion(), 0, -1, 1, ExecutionResultShape.SINGLE, 7, "transition-7", "trace-7",
            payload.payloadTypeId(), payload.payloadEncoding(), payload.payload());
    }

    private NativeWorkerReleaseBinding prepare(Release release, String test) throws IOException {
        return prepare(release.target(), release.descriptor(), test);
    }

    private NativeWorkerReleaseBinding prepare(Target target, Path descriptor, String test) throws IOException {
        return NativeWorkerReleaseBinding.prepare(target, descriptor, work.resolve("prepare-" + test),
            ResolverProfile.localOnly(), CredentialResolver.NONE);
    }

    private ObjectNode launchInput(NativeWorkerReleaseBinding binding, NativeWorkerReleaseBinding.Attempt attempt,
            TransitionCommandEnvelope command, boolean checkOrdinaryLoad) throws IOException {
        attempt.requireCommand(command);
        var launch = JSON.createObjectNode().put("tree", binding.verifyPreparedTree().toString())
            .put("projection", Base64.getEncoder().encodeToString(binding.projectionBytes()))
            .put("inputType", attempt.expectedInputTypeId()).put("outputType", attempt.expectedOutputTypeId())
            .put("checkOrdinaryLoad", checkOrdinaryLoad);
        launch.set("inputPayload", JSON.valueToTree(attempt.command().serializedPayload()));
        launch.set("typeIds", JSON.valueToTree(types));
        launch.set("context", JSON.valueToTree(Map.of("executionId", attempt.command().executionId(),
            "traceId", attempt.command().traceId())));
        return launch;
    }

    private static ObjectNode receipt(NativeWorkerReleaseBinding.Attempt attempt) {
        var receipt = JSON.createObjectNode().put("releaseVersion", attempt.target().releaseVersion())
            .put("tenantId", attempt.target().tenantId())
            .put("workerId", attempt.target().workerId())
            .put("contractVersion", attempt.target().contractVersion())
            .put("descriptorDigest", attempt.target().descriptorDigest())
            .put("runnableArtifactId", attempt.target().runnableArtifactId())
            .put("runnableDigest", attempt.target().runnableArtifactDigest())
            .put("compiledTruthArtifactId", attempt.compiledTruth().artifactId())
            .put("compiledTruthDigest", attempt.compiledTruth().digest())
            .put("projectionDigest", attempt.projectionDigest())
            .put("attempt", attempt.command().attempt())
            .put("executionVersion", attempt.command().executionVersion())
            .put("transitionKey", attempt.command().transitionKey());
        receipt.set("profiles", JSON.valueToTree(attempt.target().profiles()));
        return receipt;
    }

    private static TransitionCommandEnvelope changed(TransitionCommandEnvelope command, String field, Object value)
            throws IOException {
        ObjectNode changed = JSON.valueToTree(command);
        changed.set(field, JSON.valueToTree(value));
        return JSON.treeToValue(changed, TransitionCommandEnvelope.class);
    }

    private void reject(String test, String message, CheckedAction action) throws IOException {
        try {
            action.run();
            throw new AssertionError("Expected rejection in " + test);
        } catch (IllegalArgumentException rejected) {
            require(rejected.getMessage().contains(message), "Unexpected rejection in " + test + ": " + rejected);
            rejections.put(test, rejected.getMessage());
        }
    }

    private void rejectRelease(String test, ReleaseVerificationException.Category category, String message,
            CheckedAction action) throws IOException {
        try {
            action.run();
            throw new AssertionError("Expected Release rejection in " + test);
        } catch (ReleaseVerificationException rejected) {
            require(rejected.category() == category && rejected.getMessage().contains(message),
                "Unexpected Release rejection in " + test + ": " + rejected);
            rejections.put(test, rejected.getMessage());
        }
    }

    private static byte[] appended(Path file) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        byte[] different = Arrays.copyOf(bytes, bytes.length + 1);
        different[bytes.length] = '\n';
        return different;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private record Release(Path descriptor, Path runnable, Target target) {}

    @FunctionalInterface
    private interface CheckedAction {
        void run() throws IOException;
    }

    private static final class ReplaceFile implements AutoCloseable {
        private final Path path;
        private final byte[] original;

        private ReplaceFile(Path path, byte[] content) throws IOException {
            this.path = path;
            this.original = Files.readAllBytes(path);
            Files.write(path, content);
        }

        @Override
        public void close() throws IOException {
            Files.write(path, original);
        }
    }
}
