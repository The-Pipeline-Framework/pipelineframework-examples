package org.pipelineframework.examples.shared.proof;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
/** This gate proves package isolation only, never execution/Await acceptance. */
class PackagedScaffoldIsolationIT {
 @Test void independentlyCompiledWorkersAreAbsentFromCoordinatorProcess() throws Exception {
  for (String name : List.of("org.pipelineframework.examples.shared.expense.FixtureMarker",
      "org.pipelineframework.examples.shared.parcel.FixtureMarker")) {
   assertThrows(ClassNotFoundException.class, () -> Class.forName(name), "Harness must not share application classes");
  }
  Path root = Path.of("..").toAbsolutePath().normalize();
  Path evidence = Path.of("target/scaffold-evidence").toAbsolutePath(); Files.createDirectories(evidence);
  var forbidden = new TreeSet<String>();
  var resources = new TreeSet<String>();
  for (String app : List.of("a1", "a2", "b")) {
   Path module = root.resolve("application-" + app);
   try (var jar = new JarFile(artifact(module).toFile())) {
    assertNotNull(jar.getEntry("META-INF/pipeline/pipeline-contract.json"));
    assertNotNull(jar.getEntry("fixture-" + app + ".marker"));
    var generated = jar.stream().filter(e -> e.getName().endsWith(".class"))
        .map(e -> e.getName().replace('/', '.').replaceFirst("\\.class$", "")).toList();
    assertTrue(generated.stream().anyMatch(name -> name.endsWith("FixtureMarker")));
    if (!app.equals("b")) assertTrue(generated.stream().anyMatch(name -> name.endsWith("DeferredCompletionStep")),
        "Real compiler-generated Await adapter must be packaged");
    forbidden.addAll(generated);
    jar.stream().filter(e -> !e.isDirectory() && e.getName().startsWith("META-INF/pipeline/"))
        .map(e -> e.getName()).forEach(resources::add);
   }
   String namespace = app.startsWith("a") ? "expense" : "parcel";
   fork(module, "org.pipelineframework.examples.shared." + namespace + ".PackagedWorkerProbe",
       evidence.resolve(app + ".json").toString());
   assertTrue(Files.readString(evidence.resolve(app + ".json")).contains(app.toUpperCase()));
  }
  var json = new com.fasterxml.jackson.databind.ObjectMapper();
  var a1 = json.readTree(evidence.resolve("a1.json").toFile()).path("contract");
  var a2 = json.readTree(evidence.resolve("a2.json").toFile()).path("contract");
  var b = json.readTree(evidence.resolve("b.json").toFile()).path("contract");
  assertEquals(a1.path("pipelineId"), a2.path("pipelineId"));
  assertNotEquals(a1.path("pipelineId"), b.path("pipelineId"));
  assertEquals(2, a1.path("steps").size());
  assertEquals(4, b.path("steps").size());
  assertFalse(a1.path("canonicalCatalogFingerprint").asText().isBlank());
  assertNotEquals(a1.path("canonicalTypes").path("Request"), b.path("canonicalTypes").path("Request"));
  assertEquals("Prepare", a1.path("steps").get(0).path("authoredName").asText());
  assertEquals("Validate", b.path("steps").get(0).path("authoredName").asText());
  assertEquals("Prepare", b.path("steps").get(1).path("authoredName").asText());
  assertEquals("Finish", a1.path("steps").get(1).path("authoredName").asText());
  assertEquals("Finish", b.path("steps").get(3).path("authoredName").asText());
  Path forbiddenResources = evidence.resolve("forbidden-resources.txt"); Files.write(forbiddenResources, resources);
  Path classes = evidence.resolve("forbidden-classes.txt"); Files.write(classes, forbidden);
  fork(root.resolve("coordinator"), "org.pipelineframework.examples.shared.coordinator.PackagedCoordinatorProbe",
      classes.toString(), evidence.resolve("coordinator.json").toString(), forbiddenResources.toString());
  String report = Files.readString(evidence.resolve("coordinator.json"));
  assertTrue(report.contains("SCAFFOLD_ONLY"));
  assertFalse(report.contains("application-a1/target"));
  assertFalse(report.contains("application-a2/target"));
  assertFalse(report.contains("application-b/target"));
 }
 private static Path artifact(Path module) throws Exception {
  try (var files = Files.list(module.resolve("target/quarkus-app/app"))) {
   var artifacts = files.filter(p -> p.getFileName().toString().endsWith(".jar")).toList();
   assertEquals(1, artifacts.size(), "Exactly one application JAR must be packaged: " + module);
   return artifacts.get(0);
  }
 }

 private static void fork(Path module, String main, String... args) throws Exception {
  var closure = new ArrayList<String>(); closure.add(artifact(module).toString());
  try (var files = Files.list(module.resolve("target/lib"))) {
   files.filter(p -> p.getFileName().toString().endsWith(".jar")).sorted().forEach(p -> closure.add(p.toString()));
  }
  assertTrue(closure.stream().noneMatch(p -> p.contains("test-classes")));
  var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin/java").toString(),
      "-cp", String.join(java.io.File.pathSeparator, closure), main)); command.addAll(List.of(args));
  Path log = module.resolve("target/scaffold-process.log");
  Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
  try {
   assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Packaged probe timed out: " + module);
   assertEquals(0, process.exitValue(), () -> { try { return Files.readString(log); } catch (Exception e) { return e.toString(); } });
  } finally { if (process.isAlive()) process.destroyForcibly(); }
 }
}
