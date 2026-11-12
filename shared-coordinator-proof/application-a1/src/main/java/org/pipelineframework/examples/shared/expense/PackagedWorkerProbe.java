package org.pipelineframework.examples.shared.expense;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import org.pipelineframework.config.pipeline.PipelineJson;
/** Package isolation probe only; execution acceptance is a separate test gate. */
public final class PackagedWorkerProbe {
 public static void main(String[] args) throws Exception {
  var loader = PackagedWorkerProbe.class.getClassLoader();
  byte[] bytes;
  try (var resource = loader.getResourceAsStream("META-INF/pipeline/pipeline-contract.json")) {
   if (resource == null) throw new IllegalStateException("Missing real compiler contract");
   bytes = resource.readAllBytes();
  }
  var contract = PipelineJson.mapper().readTree(bytes);
  var report = new LinkedHashMap<String,Object>();
  report.put("revision", FixtureMarker.REVISION);
  Path artifact = Path.of(PackagedWorkerProbe.class.getProtectionDomain().getCodeSource().getLocation().toURI());
  if (!Files.isRegularFile(artifact)) throw new IllegalStateException("Worker must run from its packaged artifact");
  report.put("artifactDigest", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(artifact))));
  report.put("contractDigest", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
  report.put("contract", contract);
  report.put("classpath", System.getProperty("java.class.path"));
  report.put("phase", "SCAFFOLD_ONLY");
  PipelineJson.mapper().writeValue(Path.of(args[0]).toFile(), report);
 }
}
