import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionPayloadEncoding;
import org.pipelineframework.orchestrator.TransitionWireResult;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactUri;
import org.pipelineframework.orchestrator.release.PipelineReleaseClosureResolver;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptorValidator;
import org.pipelineframework.orchestrator.release.ResolvedPipelineRelease;
import org.pipelineframework.orchestrator.worker.PipelineWorkerCapability;

/** Exercises the released carriers and byte verifier without constructing a coordinator or worker. */
public final class WorkerBindingIntakeProbe {
    private static final ObjectMapper JSON = PipelineJson.mapper();
    private static final String PROJECTION_RESOURCE = "META-INF/pipeline/native-execution-projection.json";
    // A carrier-test value borrowed from the earlier ProbeValue transcript.
    private static final String RESEARCH_ENCODING = "tpf-polyglot-research-json/1";
    private static final List<String> RECORDS = List.of("InputValue", "FirstValue", "SecondValue", "OutputValue");
    private static final List<String> STEPS = List.of("A", "B", "C");
    private static final List<String> CAPABILITIES = List.of("local", "rest");
    private static final PipelineReleaseDescriptorValidator RELEASE_VALIDATOR = new PipelineReleaseDescriptorValidator();
    private static final PipelineReleaseClosureResolver RELEASE_RESOLVER = new PipelineReleaseClosureResolver(
        Map.of(PipelineReleaseArtifactUri.Scheme.FILE, artifact -> Files.newInputStream(Path.of(URI.create(artifact.uri())))),
        file -> JSON.readValue(file.toFile(), PipelineContractDescriptor.class));

    public static void main(String[] args) throws Exception {
        String language = args[0];
        String extension = args[1];
        Path generated = Path.of(args[2]);
        Path business = Path.of(args[3]);
        Path work = Path.of(args[4]);
        byte[] projection = Files.readAllBytes(generated.resolve("projection.json"));
        var model = JSON.readTree(projection);
        var contract = fixtureContract(language, model);
        var runnableEntries = new LinkedHashMap<String, byte[]>();
        runnableEntries.put("application." + extension, Files.readAllBytes(generated.resolve("application." + extension)));
        runnableEntries.put("models." + extension, Files.readAllBytes(generated.resolve("models." + extension)));
        runnableEntries.put(business.getFileName().toString(), Files.readAllBytes(business));
        runnableEntries.put("projection.json", projection);
        Path runnable = zip(work.resolve("native-source-fixture.zip"), runnableEntries);
        byte[] contractBytes = JSON.writeValueAsBytes(contract);
        Path truth = zip(work.resolve("truth.zip"), Map.of(
            PipelineContractDescriptor.RESOURCE_PATH, contractBytes, PROJECTION_RESOURCE, projection));

        var worker = artifact("worker", "application-archive", runnable, STEPS, CAPABILITIES);
        var carrier = artifact("truth", "compiled-truth", truth, List.of(), List.of());
        var release = release(contract, worker, carrier);
        var parsed = JSON.readValue(JSON.writeValueAsBytes(release), PipelineReleaseDescriptor.class);
        require(parsed.equals(release), "Release Descriptor round trip changed the release pins");
        var resolved = RELEASE_RESOLVER.resolve(parsed, work.resolve("resolved"));
        require(contract.equals(resolved.contract()), "The recovered Contract differs from the paired fixture Contract");
        require(Files.mismatch(resolvedArtifact(resolved, "worker"), runnable) == -1
            && Files.mismatch(resolvedArtifact(resolved, "truth"), truth) == -1, "Resolved artifacts are not byte-identical");
        require(Arrays.equals(Files.readAllBytes(resolved.compiledTruthDirectory().resolve(PROJECTION_RESOURCE)), projection),
            "Compiled Truth did not preserve the generated projection bytes");
        verifyFixtureAssociation(resolved);

        checkReleaseNegatives(work, contract, worker, carrier, runnableEntries, contractBytes, projection);
        checkEnvelopeCarriers(release, model);
        checkCapabilityCarrier(release, worker);
        System.out.println(language + " carrier round trips preserved " + String.join(", ", RECORDS)
            + " and the exact payload string. The released legacy decoder rejected the research encoding and a relabelled logical type.");
        System.out.println(language + " capability round trip preserved artifact pins and the outer envelope encoding.");
        System.out.println(language + " release verified both fixture archives and exact projection bytes. Controls covered changed layout,"
            + " unsupported Java defaults, duplicate step ownership, runnable and truth digest mismatches, and a rehashed projection substitution.");
        System.out.println("WORKER_BINDING_INTAKE_PASS:" + language);
    }

    private static PipelineContractDescriptor fixtureContract(String language, JsonNode model) throws IOException {
        var pipeline = model.path("pipeline");
        require((language + "-qualification").equals(pipeline.path("id").asText())
            && "LOCAL".equals(pipeline.path("transport").asText())
            && "COMPUTE".equals(pipeline.path("platform").asText())
            && "DIRECT".equals(pipeline.path("execution").asText())
            && "tpf-polyglot-unary-policy/1".equals(model.path("profile").asText()), "Unexpected generated baseline");
        require(model.path("types").size() == RECORDS.size() && model.path("steps").size() == STEPS.size(),
            "Unexpected number of canonical records or steps");
        var catalog = JSON.createObjectNode();
        for (JsonNode type : model.path("types")) {
            var field = type.path("fields").path(0);
            require(type.path("fields").size() == 1 && "value".equals(field.path("name").asText())
                && "int32".equals(field.path("type").asText()) && "REQUIRED".equals(field.path("presence").asText())
                && "NON_NULL".equals(field.path("nullability").asText()), "Expected one required non-null int32 field");
            catalog.set(type.path("id").asText(), type);
        }
        require(catalog.size() == RECORDS.size() && RECORDS.stream().allMatch(catalog::has), "Unexpected canonical IDs");

        // Compiler #64 emits the native projection but no coordinator Contract. These are
        // deliberately synthetic Contract contents to exercise the released Release validator.
        var fixture = JSON.createObjectNode().put("schemaVersion", 2).put("pipelineId", pipeline.path("id").asText())
            .put("contractVersion", "synthetic-intake-contract").put("contractHash", "synthetic-intake-contract")
            .put("platform", "COMPUTE").put("transport", "LOCAL").put("runtimeLayout", "PIPELINE_RUNTIME");
        fixture.set("canonicalTypes", catalog);
        fixture.set("capabilities", JSON.valueToTree(new PipelineBundleCapabilities(true, List.of("rest"))));
        var stepDescriptors = fixture.putArray("steps");
        for (int i = 0; i < STEPS.size(); i++) {
            var step = model.path("steps").path(i);
            require(STEPS.get(i).equals(step.path("id").asText()) && RECORDS.get(i).equals(step.path("inputType").asText())
                && RECORDS.get(i + 1).equals(step.path("outputType").asText()), "Unexpected canonical step chain");
            stepDescriptors.addObject().put("index", i).put("authoredName", step.path("id").asText())
                .put("kind", "SERVICE").put("cardinality", "ONE_TO_ONE")
                .put("inputTypeId", step.path("inputType").asText()).put("outputTypeId", step.path("outputType").asText());
        }
        return JSON.treeToValue(fixture, PipelineContractDescriptor.class);
    }

    private static void checkEnvelopeCarriers(PipelineReleaseDescriptor release, JsonNode model) throws IOException {
        String inputType = model.path("pipeline").path("inputType").asText();
        String outputType = model.path("pipeline").path("outputType").asText();
        require(inputType.equals(RECORDS.getFirst()) && outputType.equals(RECORDS.getLast()), "Unexpected whole-bundle boundary types");
        var legacyCodec = new JsonTransitionPayloadCodec();
        require("legacy".equals(legacyCodec.decode(legacyCodec.encode("legacy"))), "Legacy codec positive control failed");
        // Identical strings intentionally travel under distinct nominal IDs, including the two
        // intermediate IDs which must not be admitted as initial whole-bundle input.
        String exactPayload = "{ \"value\" : -2147483648 }\n";
        for (String typeId : RECORDS) {
            var payload = new SerializedTransitionPayload(typeId, RESEARCH_ENCODING, exactPayload);
            var command = new TransitionCommandEnvelope("tenant", "exec", release.pipelineId(), release.contractVersion(),
                release.releaseVersion(), 0, -1, 1, ExecutionResultShape.SINGLE, 4, "exec:0:1", "trace",
                payload.payloadTypeId(), payload.payloadEncoding(), payload.payload());
            var returned = JSON.readValue(JSON.writeValueAsBytes(command), TransitionCommandEnvelope.class);
            require(command.equals(returned) && payload.equals(returned.serializedPayload()),
                "Released request carrier changed the exact payload string or ID " + typeId);
            var resultJson = JSON.createObjectNode().put("outcome", "COMPLETED")
                .put("terminalOutputPublished", false).put("terminalInputPassthrough", false);
            resultJson.putArray("outputPayloads").add(JSON.valueToTree(payload));
            var result = JSON.readValue(JSON.writeValueAsBytes(resultJson), TransitionWireResult.class);
            var returnedResult = JSON.readValue(JSON.writeValueAsBytes(result), TransitionWireResult.class);
            require(result.equals(returnedResult) && returnedResult.outputPayloads().equals(List.of(payload)),
                "Released result carrier changed the payload or ID " + typeId);
            // The envelopes themselves do not enforce which ID is expected in the admitted range.
            if (typeId.equals(inputType)) {
                expectRejection("Unsupported transition payload encoding", () -> legacyCodec.decode(payload));
                var disguised = new SerializedTransitionPayload(typeId, JsonTransitionPayloadCodec.ENCODING, exactPayload);
                expectRejection("Failed to decode transition payload type " + typeId, () -> legacyCodec.decode(disguised));
            }
        }
    }

    private static void checkCapabilityCarrier(PipelineReleaseDescriptor release, PipelineReleaseArtifactDescriptor worker)
        throws IOException {
        var capability = new PipelineWorkerCapability(PipelineWorkerCapability.PROTOCOL_VERSION, "rest", release.pipelineId(),
            release.contractVersion(), release.releaseVersion(), worker.artifactId(), worker.digest(),
            List.of(TransitionPayloadEncoding.JSON), List.of("rest"));
        var returned = JSON.readValue(JSON.writeValueAsBytes(capability), PipelineWorkerCapability.class);
        require(capability.equals(returned)
            && returned.payloadEncodings().equals(List.of("application/tpf-transition-envelope+json")),
            "Released capability carrier changed worker pins or conflated outer and inner encodings");
    }

    private static void checkReleaseNegatives(Path work, PipelineContractDescriptor contract,
        PipelineReleaseArtifactDescriptor worker, PipelineReleaseArtifactDescriptor carrier,
        Map<String, byte[]> runnableEntries, byte[] contractBytes, byte[] projection) throws Exception {
        ObjectNode defaultContract = JSON.valueToTree(contract);
        defaultContract.set("capabilities", JSON.valueToTree(PipelineBundleCapabilities.defaults()));
        var javaDefaults = JSON.treeToValue(defaultContract, PipelineContractDescriptor.class);
        expectRejection("Release artifacts do not cover Pipeline Contract capabilities", () ->
            RELEASE_VALIDATOR.validate(release(contract, worker, carrier), javaDefaults));
        require(javaDefaults.capabilities().transitionWorkerProtocols().containsAll(List.of("grpc", "sqs")),
            "Expected ordinary Java capabilities in the negative control");

        var duplicateOwner = new PipelineReleaseArtifactDescriptor("another-worker", worker.kind(), worker.uri(),
            worker.digest(), worker.stepIds(), worker.capabilities());
        expectRejection("must be associated with exactly one deployable artifact", () ->
            RELEASE_VALIDATOR.validate(release(contract, worker, carrier, duplicateOwner), contract));

        ObjectNode differentLayout = JSON.valueToTree(contract);
        differentLayout.put("runtimeLayout", "MODULAR");
        var sameVersion = JSON.treeToValue(differentLayout, PipelineContractDescriptor.class);
        require(contract.contractVersion().equals(sameVersion.contractVersion()) && !contract.equals(sameVersion),
            "A matching Contract version must not establish full descriptor equality");

        // Same projection, different executable source bytes. Keeping the trusted digest must fail.
        var modifiedRunnableEntries = new LinkedHashMap<>(runnableEntries);
        String business = runnableEntries.keySet().stream().filter(name -> name.startsWith("business.")).findFirst().orElseThrow();
        modifiedRunnableEntries.put(business, changedBytes(runnableEntries.get(business)));
        Path modifiedRunnable = zip(work.resolve("tampered-runnable.zip"), modifiedRunnableEntries);
        expectRejection("Release artifact digest mismatch for worker", () -> RELEASE_RESOLVER.resolve(
            release(contract, relocated(worker, modifiedRunnable), carrier), work.resolve("rejected-runnable")));

        byte[] differentProjection = changedBytes(projection);
        Path modifiedTruth = zip(work.resolve("tampered-truth.zip"), Map.of(
            PipelineContractDescriptor.RESOURCE_PATH, contractBytes, PROJECTION_RESOURCE, differentProjection));
        expectRejection("Release artifact digest mismatch for truth", () -> RELEASE_RESOLVER.resolve(
            release(contract, worker, relocated(carrier, modifiedTruth)), work.resolve("rejected-truth")));

        // The substituted carrier is now self-consistent. Closure validation alone cannot
        // associate its projection with the runnable archive, even when the parsed model is equal.
        require(JSON.readTree(projection).equals(JSON.readTree(differentProjection)), "Expected a byte-only projection difference");
        var substitute = artifact(carrier.artifactId(), carrier.kind(), modifiedTruth, List.of(), List.of());
        var unrelated = RELEASE_RESOLVER.resolve(release(contract, worker, substitute), work.resolve("substituted-truth"));
        expectRejection("Runnable fixture projection differs from verified Compiled Truth", () -> verifyFixtureAssociation(unrelated));
    }

    private static void verifyFixtureAssociation(ResolvedPipelineRelease release) throws IOException {
        // This compares ZIP entries in the test. A deployment must also bind the actually loaded
        // entrypoint and dependencies to those verified bytes before reporting readiness.
        byte[] truth = Files.readAllBytes(release.compiledTruthDirectory().resolve(PROJECTION_RESOURCE));
        try (var runnable = new ZipFile(resolvedArtifact(release, "worker").toFile());
             var entry = runnable.getInputStream(runnable.getEntry("projection.json"))) {
            if (!Arrays.equals(truth, entry.readAllBytes())) {
                throw new IllegalArgumentException("Runnable fixture projection differs from verified Compiled Truth");
            }
        }
    }

    private static Path resolvedArtifact(ResolvedPipelineRelease release, String id) {
        return release.artifacts().stream().filter(artifact -> artifact.descriptor().artifactId().equals(id))
            .findFirst().orElseThrow().file();
    }

    private static PipelineReleaseDescriptor release(PipelineContractDescriptor contract,
        PipelineReleaseArtifactDescriptor worker, PipelineReleaseArtifactDescriptor carrier,
        PipelineReleaseArtifactDescriptor... additional) {
        var artifacts = new ArrayList<>(List.of(worker, carrier));
        artifacts.addAll(List.of(additional));
        return new PipelineReleaseDescriptor(PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            contract.pipelineId(), contract.contractVersion(), "synthetic-intake-release", carrier.artifactId(), artifacts);
    }

    private static PipelineReleaseArtifactDescriptor artifact(String id, String kind, Path file,
        List<String> steps, List<String> capabilities) throws Exception {
        String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
        return new PipelineReleaseArtifactDescriptor(id, kind, file.toUri().toString(), "sha256:" + digest, steps, capabilities);
    }

    private static PipelineReleaseArtifactDescriptor relocated(PipelineReleaseArtifactDescriptor artifact, Path file) {
        return new PipelineReleaseArtifactDescriptor(artifact.artifactId(), artifact.kind(), file.toUri().toString(),
            artifact.digest(), artifact.stepIds(), artifact.capabilities());
    }

    private static Path zip(Path path, Map<String, byte[]> members) throws IOException {
        try (var archive = new ZipOutputStream(Files.newOutputStream(path))) {
            for (String name : members.keySet().stream().sorted().toList()) {
                var entry = new ZipEntry(name);
                entry.setTime(0);
                archive.putNextEntry(entry);
                archive.write(members.get(name));
                archive.closeEntry();
            }
        }
        return path;
    }

    private static byte[] changedBytes(byte[] original) {
        return (new String(original, StandardCharsets.UTF_8) + "\n").getBytes(StandardCharsets.UTF_8);
    }

    @FunctionalInterface
    private interface CheckedAction {
        void run() throws IOException;
    }

    private static void expectRejection(String expected, CheckedAction action) throws IOException {
        try {
            action.run();
        } catch (IllegalArgumentException rejected) {
            if (rejected.getMessage().contains(expected)) {
                return;
            }
            throw new AssertionError("Expected rejection containing " + expected, rejected);
        }
        throw new AssertionError("Expected rejection containing " + expected);
    }

    private static void require(boolean condition, String failure) {
        if (!condition) {
            throw new AssertionError(failure);
        }
    }
}
