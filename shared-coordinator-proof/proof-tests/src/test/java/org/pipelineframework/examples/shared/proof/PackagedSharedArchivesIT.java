package org.pipelineframework.examples.shared.proof;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.jar.JarInputStream;
import java.util.zip.ZipFile;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PackagedSharedArchivesIT {
    private static final Set<String> APPLICATION_PACKAGES = Set.of(
        "org/pipelineframework/examples/shared/expense/", "org/pipelineframework/examples/shared/parcel/");

    @Test
    void productionSharedArchivesContainNoFixtureClassesOrCompiledPipeline() throws Exception {
        Path root = Path.of("../../aws-durable-proof");
        for (String module : List.of("shared-action", "shared-await-lifecycle")) {
            Path artifact = root.resolve(module).resolve("target/function.zip");
            try (var zip = new ZipFile(artifact.toFile())) {
                assertNotNull(zip.getEntry("io/quarkus/runner/ApplicationImpl.class"), artifact.toString());
                var entries = zip.stream().filter(entry -> !entry.isDirectory()).toList();
                assertTrue(entries.stream().anyMatch(entry -> entry.getName().startsWith("lib/")));
                for (var entry : entries) {
                    rejectApplicationContent(entry.getName(), artifact);
                    if (!entry.getName().endsWith(".jar")) continue;
                    try (var jar = new JarInputStream(zip.getInputStream(entry))) {
                        for (var member = jar.getNextJarEntry(); member != null; member = jar.getNextJarEntry()) {
                            if (!member.isDirectory()) rejectApplicationContent(member.getName(), artifact);
                        }
                    }
                }
            }
        }
    }

    private static void rejectApplicationContent(String name, Path artifact) {
        assertFalse(APPLICATION_PACKAGES.stream().anyMatch(name::startsWith), () -> artifact + " contains " + name);
        assertFalse(name.equals("META-INF/pipeline/pipeline-contract.json"), () -> artifact + " contains " + name);
    }
}
