package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.processor.renderer.HttpPayloadBoundaryRenderer;

/** Keeps the HTTP application fixtures identical to current compiler output. */
class GeneratedOwnedPayloadFixtureTest {
    @TempDir
    Path work;

    @Test
    void executedHttpFixturesMatchCurrentCompilerOutput() throws Exception {
        Path yaml = Path.of(getClass().getResource("/owned-payload-http-generation.yaml").toURI());
        var config = new PipelineTemplateConfigLoader().load(yaml);
        Path generated = work.resolve("generated");
        new HttpPayloadBoundaryRenderer().render(config, generated);
        Path fixtures = Path.of(System.getProperty("basedir", "."))
            .resolve("src/test/java/org/example/pipeline");
        for (String fixture : List.of("GeneratedPayloadBoundary0.java", "GeneratedPayloadBoundary1.java")) {
            assertEquals(Files.readString(generated.resolve("org/example/pipeline").resolve(fixture)),
                Files.readString(fixtures.resolve(fixture)),
                "Executed HTTP fixture must be current compiler output: " + fixture);
        }
    }
}
