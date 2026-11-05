package org.pipelineframework.awsconformance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Exercise the distribution, without Maven's test dependencies masking missing runtime classes. */
class PackagedHostLinkageIT {
    @TempDir Path temporary;

    @Test
    void shadedDistributionConstructsItsRawHandlersAndSdkClients() throws Exception {
        var target = Path.of("target");
        List<Path> distributions;
        try (var files = Files.list(target)) {
            distributions = files.filter(path -> path.getFileName().toString().endsWith("-lambda.jar")).toList();
        }
        assertEquals(1, distributions.size(), "One actual shaded Lambda distribution required");
        String probe = PackagedHostSmokeProbe.class.getName().replace('.', '/') + ".class";
        Path copiedProbe = temporary.resolve(probe);
        Files.createDirectories(copiedProbe.getParent());
        try (var input = getClass().getClassLoader().getResourceAsStream(probe)) {
            assertNotNull(input);
            Files.copy(input, copiedProbe);
        }
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", distributions.getFirst().toAbsolutePath() + java.io.File.pathSeparator + temporary,
                PackagedHostSmokeProbe.class.getName()).redirectErrorStream(true);
        process.environment().keySet().removeIf(key -> key.startsWith("AWS_") || key.startsWith("TPF_")
                || key.equals("JAVA_TOOL_OPTIONS") || key.equals("JDK_JAVA_OPTIONS") || key.equals("_JAVA_OPTIONS"));
        process.environment().put("AWS_REGION", "us-east-2");
        process.environment().put("AWS_ACCESS_KEY_ID", "offline-fixture");
        process.environment().put("AWS_SECRET_ACCESS_KEY", "offline-fixture");
        process.environment().put("AWS_EC2_METADATA_DISABLED", "true");
        process.environment().put("TPF_AWS_DURABLE_ACTION_FUNCTION", "offline-action");
        process.environment().put("TPF_AWS_DURABLE_FUNCTION", "offline-durable");
        process.environment().put("TPF_AWS_DURABLE_QUALIFIER", "1");
        Path output = temporary.resolve("packaged-host.log");
        var child = process.redirectOutput(output.toFile()).start();
        try {
            assertTrue(child.waitFor(Duration.ofSeconds(30).toMillis(), TimeUnit.MILLISECONDS), "Offline construction timed out");
            assertEquals(0, child.exitValue(), () -> {
                try { return Files.readString(output); }
                catch (java.io.IOException unreadable) { return unreadable.toString(); }
            });
            assertTrue(Files.readString(output).contains("PACKAGED_HOST_CONSTRUCTION_PASS"));
        } finally { child.destroyForcibly(); }
    }
}

/** Only this JDK-only class is copied to the child; no JUnit, test fixture or Maven classpath is inherited. */
class PackagedHostSmokeProbe {
    public static void main(String[] arguments) throws Exception {
        int failures = 0;
        for (String service : List.of("lambda.Lambda", "dynamodb.DynamoDb", "ssm.Ssm", "secretsmanager.SecretsManager")) {
            try (var client = (AutoCloseable) Class.forName("software.amazon.awssdk.services." + service + "Client")
                    .getMethod("create").invoke(null)) {
                System.out.println("SDK_CONSTRUCTION_PASS " + service);
            } catch (Throwable failure) { failure.printStackTrace(); failures++; }
            try {
                // The coordinator explicitly selects Apache, even when the default SDK service selects another client.
                Object builder = Class.forName("software.amazon.awssdk.services." + service + "Client")
                        .getMethod("builder").invoke(null);
                Object apache = Class.forName("software.amazon.awssdk.http.apache.ApacheHttpClient")
                        .getMethod("builder").invoke(null);
                Class.forName("software.amazon.awssdk.core.client.builder.SdkSyncClientBuilder")
                        .getMethod("httpClientBuilder", Class.forName("software.amazon.awssdk.http.SdkHttpClient$Builder"))
                        .invoke(builder, apache);
                try (var client = (AutoCloseable) Class.forName("software.amazon.awssdk.utils.builder.SdkBuilder")
                        .getMethod("build").invoke(builder)) {
                    System.out.println("APACHE_SDK_CONSTRUCTION_PASS " + service);
                }
            } catch (Throwable failure) { failure.printStackTrace(); failures++; }
        }
        for (String handler : List.of("AwsDurableCoordinatorHandler", "AwsDurableIngressHandler")) {
            try {
                Class.forName("org.pipelineframework.aws.durable." + handler).getConstructor().newInstance();
                System.out.println("HANDLER_CONSTRUCTION_PASS " + handler);
            } catch (Throwable failure) { failure.printStackTrace(); failures++; }
        }
        if (failures != 0) throw new IllegalStateException("Packaged construction failures: " + failures);
        System.out.println("PACKAGED_HOST_CONSTRUCTION_PASS");
    }
}
