package org.pipelineframework.stdio.demo;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class AuthoredExamplesGuardTest {

    private static final Pattern LEGACY_PROTO_TYPE_DECLARATION = Pattern.compile("(?m)^\\s*protoType:");
    private static final Pattern LEGACY_PIPELINE_STEP_METADATA = Pattern.compile(
        "@PipelineStep\\s*\\([^)]*(inputType\\s*=|outputType\\s*=|inboundMapper\\s*=|outboundMapper\\s*=|stepType\\s*=|backendType\\s*=)",
        Pattern.DOTALL);

    @Test
    void authoredExampleYamlAndJavaDoNotReintroduceLegacyMetadata() throws IOException {
        Path repoRoot = findRepositoryRoot();
        List<String> violations = new ArrayList<>();

        try (Stream<Path> files = Files.walk(repoRoot)) {
            files.filter(Files::isRegularFile)
                .filter(AuthoredExamplesGuardTest::isAuthoredRepositoryFile)
                .filter(path -> path.getFileName().toString().endsWith(".yaml")
                    || path.getFileName().toString().endsWith(".yml"))
                .forEach(path -> recordViolations(path, LEGACY_PROTO_TYPE_DECLARATION, violations));
        }
        try (Stream<Path> files = Files.walk(repoRoot)) {
            files.filter(Files::isRegularFile)
                .filter(AuthoredExamplesGuardTest::isAuthoredRepositoryFile)
                .filter(path -> path.toString().endsWith(".java"))
                .filter(path -> path.startsWith(repoRoot.resolve("src/main/java"))
                    || path.toString().contains("/src/main/java/"))
                .forEach(path -> recordViolations(path, LEGACY_PIPELINE_STEP_METADATA, violations));
        }

        assertTrue(violations.isEmpty(), "Authored examples contain deprecated TPF metadata:\n"
            + String.join("\n", violations));
    }

    private static Path findRepositoryRoot() {
        Path current = Path.of("").toAbsolutePath().normalize();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("AGENTS.md")) && Files.isRegularFile(current.resolve("pom.xml"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Could not locate pipelineframework-examples repository root");
    }

    private static void recordViolations(Path path, Pattern pattern, List<String> violations) {
        try {
            if (pattern.matcher(Files.readString(path)).find()) {
                violations.add(path.toString());
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to scan " + path, ex);
        }
    }

    private static boolean isAuthoredRepositoryFile(Path path) {
        String pathText = path.toString();
        return !pathText.contains("/target/") && !pathText.contains("/.m2/") && !pathText.contains("/.git/");
    }
}
