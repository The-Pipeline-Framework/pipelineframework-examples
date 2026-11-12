import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pipelineframework.awaitable.AwaitCompletionMetadata;
import org.pipelineframework.awaitable.CommandCompletionRejectedException;
import org.pipelineframework.aws.durable.model.AwsDurableHostedProfile;
import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.deployment.release.CredentialResolver;
import org.pipelineframework.deployment.release.ReleaseVerifier;
import org.pipelineframework.deployment.release.ResolverProfile;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDeployment;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.release.producer.DefaultPipelineReleaseProducer;
import org.pipelineframework.release.producer.ReleaseArtifactInput;
import org.pipelineframework.release.producer.ReleaseProductionRequest;

class VerifyPair {
    private static final List<String> TRIGGERS = List.of("AWS_DURABLE_COORDINATOR", "AWS_DURABLE_INGRESS",
            "AWS_DURABLE_ACTION", "SQS_WORK", "SQS_TRANSITION", "SQS_AWAIT_COMPLETION", "DYNAMO_AWAIT_WAKEUP",
            "DYNAMO_EXECUTION_WAKEUP", "SQS_DUE_WORK", "AWS_DURABLE_RECONCILIATION");
    private static final List<String> ENTRIES = List.of("coordinator", "ingress", "action", "work", "transition",
            "completion", "await-wakeup", "execution-wakeup", "due", "reconcile");
    private static final List<String> BINDINGS = List.of("command-effects", "store");
    private static final String PACKAGE = "com.example.echo.";

    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        var a = verify(root.resolve("A"), "A");
        var b = verify(root.resolve("B"), "B");
        require("com.example.echo".equals(a.pipelineId())
                && a.pipelineId().equals(b.pipelineId()), "A/B must retain the original Echo pipeline ID");
        require(a.deployment().entries().equals(b.deployment().entries()), "A/B entry layout differs");
        require(a.contractVersion().equals(b.contractVersion()), "A/B contract differs");
        require(!actionArtifact(a).digest().equals(actionArtifact(b).digest()),
                "A/B Action package digests must differ");
    }

    private static PipelineReleaseDescriptor verify(Path project, String variant) throws Exception {
        var mapper = PipelineJson.mapper();
        var target = project.resolve("target");
        var classes = target.resolve("classes");
        var contract = mapper.readValue(classes.resolve("META-INF/pipeline/pipeline-contract.json").toFile(),
                PipelineContractDescriptor.class);
        var entries = new java.util.ArrayList<PipelineReleaseDeployment.Entry>();
        for (int i = 0; i < TRIGGERS.size(); i++) {
            String trigger = TRIGGERS.get(i);
            var owner = AwsDurableHostedProfile.entryForTrigger(trigger);
            boolean driver = owner.codeGroup() == AwsDurableHostedProfile.CodeGroup.DURABLE;
            entries.add(new PipelineReleaseDeployment.Entry(ENTRIES.get(i), driver ? "driver" : "actions", contract.module(),
                    ".", owner.entryClass(), trigger, driver ? List.of() : BINDINGS,
                    driver ? "lib/modified-org.pipelineframework.pipelineframework-aws-durable-coordination-host-26.11.2.jar" : "."));
        }
        var deployment = new PipelineReleaseDeployment(AwsDurableHostedProfile.PROFILE_ID, entries, List.of(
                new PipelineReleaseDeployment.Binding("command-effects", AwsDurableHostedProfile.COMMAND_EFFECT_CAPABILITY),
                new PipelineReleaseDeployment.Binding("store", AwsDurableHostedProfile.CONNECTOR_CAPABILITY)),
                new PipelineReleaseDeployment.Check("ingress", payload("EchoInput", "hello"),
                        payload("EchoOutput", "release-" + variant + ":hello"), 120_000));
        var producer = new DefaultPipelineReleaseProducer();
        var release = producer.produce(new ReleaseProductionRequest(contract, "hosted-echo-await-" + variant.toLowerCase(java.util.Locale.ROOT),
                "actions", classes.resolve("META-INF/pipeline"),
                List.of(artifact(target, "driver", "driver.zip", List.of(), List.of()),
                        artifact(target, "actions", "function.zip", List.of("Echo"), List.of("local", "rest", "grpc", "sqs"))),
                true, deployment));
        var descriptor = Files.write(project.resolve("pipeline-release.json"), producer.serialize(release));
        Path scratch = Files.createTempDirectory(project, "verified-closure-");
        try {
            var resolved = new ReleaseVerifier().verify(descriptor, scratch,
                    ResolverProfile.localOnly(), CredentialResolver.NONE).resolved();
            Map<String, Object> metadata = mapper.readerFor(new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {})
                    .readValue(resolved.compiledTruthDirectory().resolve("META-INF/pipeline/connector-bindings.json").toFile());
            var authority = new AwsDurableHostedProfile.ConnectorAuthority("store", "fixture.echo", 1);
            AwsDurableHostedProfile.requireSupported(release, resolved.contract(), metadata, Set.of(authority));
            rejected(() -> AwsDurableHostedProfile.requireSupported(release, resolved.contract(), metadata, Set.of()));
            rejected(() -> AwsDurableHostedProfile.requireSupported(release, resolved.contract(), metadata,
                    Set.of(new AwsDurableHostedProfile.ConnectorAuthority("store", "fixture.echo", 2))));
            rejected(() -> AwsDurableHostedProfile.requireSupported(release, resolved.contract(), Map.of(), Set.of(authority)));
            var deferred = resolved.contract().steps().getFirst().deferredCompletion();
            require("CONNECTOR_CALLBACK".equals(deferred.get("mode"))
                    && "ACKNOWLEDGEMENT_REQUIRED".equals(deferred.get("commandCompletionMode"))
                    && "signedResumeToken".equals(deferred.get("correlationStrategy"))
                    && (PACKAGE + "EchoAcknowledgement").equals(deferred.get("commandAcknowledgementTypeId"))
                    && (PACKAGE + "EchoCompletion").equals(deferred.get("completionPayloadTypeId"))
                    && (PACKAGE + "EchoOutput").equals(deferred.get("finalOutputTypeId")), "Signed typed connector Await is required");
            var action = resolved.artifacts().stream()
                    .filter(artifact -> artifact.descriptor().artifactId().equals("actions")).findFirst().orElseThrow();
            verifyProjector(action.file(), (String) deferred.get("completionProjector"), "release-" + variant + ":hello");
            System.out.printf("%s %s %s, %d exact entries, verified callback authority and packaged projector%n",
                    release.releaseVersion(), release.contractVersion(), descriptor, entries.size());
        } finally {
            try (var paths = Files.walk(scratch)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(path);
                }
            }
        }
        return release;
    }

    private static PipelineReleaseArtifactDescriptor actionArtifact(PipelineReleaseDescriptor release) {
        return release.artifacts().stream().filter(artifact -> artifact.artifactId().equals("actions"))
                .findFirst().orElseThrow();
    }

    private static ReleaseArtifactInput artifact(Path target, String id, String filename, List<String> steps, List<String> capabilities) {
        var archive = target.resolve(filename);
        return new ReleaseArtifactInput(id, "lambda-zip", archive, archive.toUri().toString(), steps, capabilities);
    }

    private static Map<String, String> payload(String type, String value) {
        return Map.of("payloadTypeId", PACKAGE + type, "payloadEncoding", AwsDurableHostedProfile.TYPED_JSON_ENCODING,
                "payload", "{\"value\":\"" + value + "\"}");
    }

    private static void verifyProjector(Path archive, String name, String expected) throws Exception {
        try (var loader = new URLClassLoader(new URL[]{archive.toUri().toURL()})) {
            var projector = loader.loadClass(name);
            var input = loader.loadClass(PACKAGE + "EchoInput");
            var ack = loader.loadClass(PACKAGE + "EchoAcknowledgement");
            var completion = loader.loadClass(PACKAGE + "EchoCompletion");
            verifyCommandIdentity(loader, input);
            var operation = projector.getMethod("project", input, ack, completion, AwaitCompletionMetadata.class);
            var instance = projector.getConstructor().newInstance();
            var request = input.getConstructor(String.class).newInstance("hello");
            var receipt = ack.getConstructor(String.class).newInstance("hello");
            var typed = completion.getConstructor(String.class);
            var result = operation.invoke(instance, request, receipt, typed.newInstance("hello"), null);
            require(expected.equals(result.getClass().getMethod("value").invoke(result)), "Packaged projector result differs");
            try {
                operation.invoke(instance, request, receipt, typed.newInstance("wrong"), null);
                throw new AssertionError("Projector accepted a mismatched completion");
            } catch (InvocationTargetException rejected) {
                require(rejected.getCause() instanceof CommandCompletionRejectedException, "Unexpected projector failure");
            }
        }
    }

    private static void verifyCommandIdentity(ClassLoader loader, Class<?> input) throws Exception {
        var generator = loader.loadClass(PACKAGE + "application.StableId");
        var instance = generator.getConstructor().newInstance();
        var operation = generator.getMethod("commandId", CommandDescriptor.class, input);
        var request = input.getConstructor(String.class).newInstance("hello");
        try {
            PipelineExecutionContextHolder.set(new PipelineExecutionContext("echo", "execution-a", 0));
            var first = operation.invoke(instance, null, request);
            require(first.equals(operation.invoke(instance, null, request)), "A retry changed the Command identity");
            PipelineExecutionContextHolder.set(new PipelineExecutionContext("echo", "execution-b", 0));
            require(!first.equals(operation.invoke(instance, null, request)), "Two executions shared one Command effect");
        } finally {
            PipelineExecutionContextHolder.clear();
        }
    }

    private static void rejected(Runnable check) {
        try {
            check.run();
            throw new AssertionError("Hosted gate accepted missing or mismatched connector evidence");
        } catch (UnsupportedOperationException expected) {
            // The native gate rejected the unqualified closure.
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
