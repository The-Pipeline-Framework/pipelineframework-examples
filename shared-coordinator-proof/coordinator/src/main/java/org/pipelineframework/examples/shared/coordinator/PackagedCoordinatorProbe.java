package org.pipelineframework.examples.shared.coordinator;
import java.nio.file.*;
import java.util.*;
import org.pipelineframework.config.pipeline.PipelineJson;
/** Verifies the Coordinator production classpath without importing any application fixture. */
public final class PackagedCoordinatorProbe {
 public static void main(String[] args) throws Exception {
  var loader = PackagedCoordinatorProbe.class.getClassLoader();
  Class.forName("org.pipelineframework.QueueAsyncCoordinator", false, loader);
  var forbidden = Files.readAllLines(Path.of(args[0]));
  for (String name : forbidden) {
   try { Class.forName(name, false, loader); throw new IllegalStateException("Application class leaked: " + name); }
   catch (ClassNotFoundException expected) { }
  }
  for (String resource : Files.readAllLines(Path.of(args[2]))) {
   if (loader.getResource(resource) != null) throw new IllegalStateException("Generated application resource leaked: " + resource);
  }
  for (String resource : List.of("META-INF/pipeline/pipeline-contract.json", "pipeline.yaml",
      "fixture-a1.marker", "fixture-a2.marker", "fixture-b.marker")) {
   if (loader.getResource(resource) != null) throw new IllegalStateException("Application resource leaked: " + resource);
  }
  PipelineJson.mapper().writeValue(Path.of(args[1]).toFile(), Map.of("phase", "SCAFFOLD_ONLY",
      "absentClasses", forbidden, "classpath", System.getProperty("java.class.path")));
 }
}
