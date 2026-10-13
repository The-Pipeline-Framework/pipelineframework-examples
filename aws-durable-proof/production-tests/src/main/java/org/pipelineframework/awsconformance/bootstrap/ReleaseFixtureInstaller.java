package org.pipelineframework.awsconformance.bootstrap;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import io.smallrye.config.SmallRyeConfigBuilder;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.release.DynamoPipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseDescriptorValidator;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseStatus;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/** External fixture provisioning using the production release registry, never invoked by a deployed handler. */
public final class ReleaseFixtureInstaller {
    public record Input(String region, String releaseTable, String bucket, String kmsKeyArn, String tenant,
                        String releaseVersion, String functionArchive, String pipelineContract) { }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("One explicit release-fixture input JSON path is required");
        }
        new ReleaseFixtureInstaller().install(PipelineJson.mapper().readValue(Path.of(args[0]).toFile(), Input.class));
    }

    void install(Input input) throws Exception {
        var contract = PipelineJson.mapper().readValue(Path.of(input.pipelineContract()).toFile(), PipelineContractDescriptor.class);
        Path archive = Path.of(input.functionArchive()).toAbsolutePath().normalize();
        String digest = "sha256:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(archive)));
        String key = "tpf/releases/sha256/" + digest.substring(7) + ".zip";
        String uri = "s3://" + input.bucket() + "/" + key;
        var descriptor = descriptor(contract, input.releaseVersion(), archive.toUri().toASCIIString(), digest);
        new PipelineReleaseDescriptorValidator().validate(descriptor, contract);
        try (var s3 = S3Client.builder().region(Region.of(input.region())).httpClientBuilder(UrlConnectionHttpClient.builder()).build()) {
            s3.putObject(PutObjectRequest.builder().bucket(input.bucket()).key(key)
                .serverSideEncryption("aws:kms").ssekmsKeyId(input.kmsKeyArn())
                .metadata(Map.of("sha256", digest.substring(7), "size", Long.toString(Files.size(archive)))).build(),
                RequestBody.fromFile(archive));
        }
        var config = new SmallRyeConfigBuilder().withMapping(PipelineOrchestratorConfig.class)
            .withDefaultValue("pipeline.orchestrator.dynamo.region", input.region())
            .withDefaultValue("pipeline.orchestrator.dynamo.release-table", input.releaseTable()).build()
            .getConfigMapping(PipelineOrchestratorConfig.class);
        var registry = new DynamoPipelineReleaseRegistry(config);
        long now = System.currentTimeMillis();
        var record = new PipelineReleaseRecord(input.tenant(), contract.pipelineId(), contract.contractVersion(),
            input.releaseVersion(), PipelineReleaseStatus.REGISTERED, descriptor, "conformance-action", digest, uri,
            Files.size(archive), digest, contract, now, now, 0L);
        var registered = registry.register(record).await().atMost(Duration.ofSeconds(30));
        var activated = registry.activate(input.tenant(), contract.pipelineId(), input.releaseVersion(), now)
            .await().atMost(Duration.ofSeconds(30)).orElseThrow(() -> new IllegalStateException("Release activation was not retained"));
        if (!registered.contractVersion().equals(activated.contractVersion())) {
            throw new IllegalStateException("Release registration and activation identities differ");
        }
        System.out.println(PipelineJson.mapper().writeValueAsString(Map.of("tenant", input.tenant(),
            "pipelineId", contract.pipelineId(), "contractVersion", contract.contractVersion(),
            "releaseVersion", input.releaseVersion(), "artifactDigest", digest, "artifactUri", uri)));
    }

    PipelineReleaseDescriptor descriptor(PipelineContractDescriptor contract, String releaseVersion, String uri, String digest) {
        var artifact = new PipelineReleaseArtifactDescriptor("conformance-action", "lambda-zip", uri, digest,
            contract.steps().stream().map(step -> step.authoredName()).toList(), contract.capabilities().transitionWorkerProtocols());
        return new PipelineReleaseDescriptor(PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            contract.pipelineId(), contract.contractVersion(), releaseVersion, "conformance-action", List.of(artifact));
    }
}
