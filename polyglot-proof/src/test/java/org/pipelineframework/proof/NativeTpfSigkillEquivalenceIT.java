package org.pipelineframework.proof;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.PolyglotProofHarness;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.DynamoProofStoreHelper;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStatus;

/**
 * End-to-end proof (pipelineframework-cli#27) exercising the real native {@code tpf} executable
 * with zero JDK on {@code PATH}:
 * <ol>
 *   <li>Downloads (with SHA-256 verification) or resolves the native {@code tpf} binary.</li>
 *   <li>Runs offline {@code tpf generate --lang py} and launches the generated Python host.</li>
 *   <li>Runs {@code tpf dev}, kills it with {@code SIGKILL} across durable {@code WAITING_EXTERNAL}
 *       Await and {@code WAIT_RETRY} boundaries, restarts {@code tpf dev} against the same
 *       {@code --data-dir}, and completes execution. (Pre-dispatch {@code QUEUED} admission restart
 *       recovery is covered separately in {@link PolyglotCrossLanguageProofTest} because
 *       {@code tpf dev}'s {@code POST /runs} admits and drains synchronously in one call.)</li>
 *   <li>Verifies exact status and canonical JSON equivalence (29-digit {@code decimal} and
 *       odd {@code int64 > 2^53}) against production {@code DynamoExecutionStateStore} on LocalStack.</li>
 * </ol>
 */
class NativeTpfSigkillEquivalenceIT {

    private static final String SHARED_SECRET = "tpf-native-sigkill-eq-secret";
    private static final String EXACT_DECIMAL = "99999999999999999999.123456789";
    private static final String EXACT_INT64 = "9007199254740994";
    private static final String EXACT_INT64_NEXT = "9007199254740995";

    private static final ObjectMapper EXACT_JSON = PipelineJson.mapper().copy()
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);

    private static final HttpClient HTTP = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(15))
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();

    @Test
    void nativeTpfBinarySurvivesSigkillAndMatchesDynamoDbExecution(@TempDir Path temp) throws Exception {
        PolyglotHostProcess.requirePython3();
        Path nativeBinary = resolveNativeTpfBinary().orElse(null);
        boolean isCi = "true".equalsIgnoreCase(System.getenv("CI"));
        if (nativeBinary == null) {
            if (isCi && detectPlatform().isPresent()) {
                throw new AssertionError("Native tpf binary failed to download/resolve in CI for " + detectPlatform().orElseThrow());
            }
            Assumptions.assumeTrue(false, "Native tpf executable unavailable for this platform/network");
            return;
        }
        assertTrue(Files.isExecutable(nativeBinary), "Resolved tpf binary is not executable: " + nativeBinary);

        Path yaml = PolyglotCrossLanguageProofTest.resourcePath("sigkill-await-pipeline.yaml", temp);
        Path genDir = temp.resolve("sigkill_equivalence_proof");
        Path emptyPathDir = temp.resolve("empty-path");
        Path isolatedHome = temp.resolve("home");
        Files.createDirectories(emptyPathDir);
        Files.createDirectories(isolatedHome);

        // 1. Offline `tpf generate --lang py` with ZERO JDK on PATH
        ProcessResult genResult = runNativeCommand(
            nativeBinary, isolatedHome, emptyPathDir,
            List.of("generate", "--pipeline", yaml.toString(), "--lang", "py",
                "--out", genDir.toString(), "--output", "json"));
        assertEquals(0, genResult.exitCode(), "Native tpf generate failed:\n" + genResult.output());
        assertTrue(Files.isRegularFile(genDir.resolve("host.py")), "Generated Python host.py missing");

        // 2. Start live Python foreign host from generated stubs
        Path hostScript = temp.resolve("run_sigkill_host.py");
        Files.writeString(hostScript, pythonHostSource(), StandardCharsets.UTF_8);

        String awaitPayloadJson = "{\"paymentId\":\"pay-await-1\",\"sequenceNumber\":" + EXACT_INT64
            + ",\"amount\":" + EXACT_DECIMAL + ",\"failFirstAttempt\":false}";
        String retryPayloadJson = "{\"paymentId\":\"pay-retry-1\",\"sequenceNumber\":" + EXACT_INT64
            + ",\"amount\":" + EXACT_DECIMAL + ",\"failFirstAttempt\":true}";
        String awaitCompletionJson = "{\"paymentId\":\"pay-await-1\",\"sequenceNumber\":" + EXACT_INT64_NEXT
            + ",\"settledAmount\":" + EXACT_DECIMAL + "}";
        String retryCompletionJson = "{\"paymentId\":\"pay-retry-1\",\"sequenceNumber\":" + EXACT_INT64_NEXT
            + ",\"settledAmount\":" + EXACT_DECIMAL + "}";

        try (PolyglotHostProcess pyHost = PolyglotHostProcess.start(
                List.of("python3", "-u", hostScript.toString()),
                temp,
                Map.of("TPF_GEN_PARENT", temp.toString(), "TPF_DEV_SECRET", SHARED_SECRET, "PYTHONUNBUFFERED", "1"),
                Duration.ofSeconds(30));
             PolyglotHostProcess dynamoPyHost = PolyglotHostProcess.start(
                List.of("python3", "-u", hostScript.toString()),
                temp,
                Map.of("TPF_GEN_PARENT", temp.toString(), "TPF_DEV_SECRET", SHARED_SECRET, "PYTHONUNBUFFERED", "1"),
                Duration.ofSeconds(30));
             DynamoProofStoreHelper dynamoHelper = new DynamoProofStoreHelper()) {

            Path dataDir = temp.resolve("native-dev-state");

            // 3. Execute on real native `tpf dev` with SIGKILL across WAITING_EXTERNAL and WAIT_RETRY
            NativeDevProcess dev1 = startNativeDev(nativeBinary, isolatedHome, emptyPathDir, yaml, pyHost.baseUri(), dataDir);
            String nativeAwaitResultJson;
            String nativeRetryResultJson;
            try {
                JsonNode run1 = httpPostJson(dev1.baseUri().resolve("/runs"),
                    "{\"tenantId\":\"native-t\",\"executionKey\":\"await-sigkill-1\",\"payload\":" + awaitPayloadJson + "}");
                assertEquals("WAITING_EXTERNAL", run1.path("status").asText(), "Expected WAITING_EXTERNAL: " + run1);
                String exec1Id = run1.path("executionId").asText();
                String interaction1Id = run1.path("interactionId").asText();
                assertFalse(interaction1Id.isBlank());

                // Hard SIGKILL while suspended in WAITING_EXTERNAL
                long pid1 = dev1.osPid();
                sigkill(dev1);

                // Restart native tpf dev against the exact same --data-dir
                NativeDevProcess dev2 = startNativeDev(nativeBinary, isolatedHome, emptyPathDir, yaml, pyHost.baseUri(), dataDir);
                try {
                    assertNotEquals(pid1, dev2.osPid(), "Restarted native tpf dev must have a new PID");
                    JsonNode persisted1 = httpGetJson(dev2.baseUri().resolve("/runs/native-t/" + exec1Id));
                    assertEquals("WAITING_EXTERNAL", persisted1.path("status").asText());
                    assertEquals(interaction1Id, persisted1.path("interactionId").asText());
                    assertEquals(EXACT_INT64_NEXT,
                        persisted1.path("awaitRequestPayload").path("sequenceNumber").asText(),
                        "Recovered awaitRequestPayload must preserve exact int64 > 2^53 after SIGKILL");
                    assertEquals(EXACT_DECIMAL,
                        persisted1.path("awaitRequestPayload").path("amount").asText(),
                        "Recovered awaitRequestPayload must preserve 29-digit exact decimal after SIGKILL");

                    JsonNode completed1 = httpPostJson(dev2.baseUri().resolve("/awaits/complete"),
                        "{\"tenantId\":\"native-t\",\"interactionId\":\"" + interaction1Id
                            + "\",\"completionId\":\"comp-await-1\",\"payload\":" + awaitCompletionJson + "}");
                    assertEquals("SUCCEEDED", completed1.path("status").asText(), "Post-SIGKILL Await failed: " + completed1);
                    assertFalse(completed1.path("duplicate").asBoolean());
                    nativeAwaitResultJson = EXACT_JSON.writeValueAsString(completed1.path("result"));

                    // Trigger durable WAIT_RETRY, then hard SIGKILL before retry runs
                    JsonNode run2 = httpPostJson(dev2.baseUri().resolve("/runs"),
                        "{\"tenantId\":\"native-t\",\"executionKey\":\"retry-sigkill-1\",\"payload\":" + retryPayloadJson + "}");
                    assertEquals("WAIT_RETRY", run2.path("status").asText(), "Expected WAIT_RETRY: " + run2);
                    String exec2Id = run2.path("executionId").asText();

                    long pid2 = dev2.osPid();
                    sigkill(dev2);

                    // Restart native tpf dev Run #3, sweep recovery, and complete Await
                    NativeDevProcess dev3 = startNativeDev(nativeBinary, isolatedHome, emptyPathDir, yaml, pyHost.baseUri(), dataDir);
                    try {
                        assertNotEquals(pid2, dev3.osPid());
                        long futureMs = System.currentTimeMillis() + 60_000L;
                        httpPostJson(dev3.baseUri().resolve("/sweep"), "{\"nowEpochMs\":" + futureMs + "}");

                        JsonNode recovered2 = awaitStatus(dev3.baseUri().resolve("/runs/native-t/" + exec2Id),
                            "WAITING_EXTERNAL", Duration.ofSeconds(10));
                        assertEquals(EXACT_INT64_NEXT,
                            recovered2.path("awaitRequestPayload").path("sequenceNumber").asText(),
                            "Post-WAIT_RETRY SIGKILL awaitRequestPayload must preserve exact int64 > 2^53");
                        assertEquals(EXACT_DECIMAL,
                            recovered2.path("awaitRequestPayload").path("amount").asText(),
                            "Post-WAIT_RETRY SIGKILL awaitRequestPayload must preserve 29-digit exact decimal");
                        String interaction2Id = recovered2.path("interactionId").asText();
                        JsonNode completed2 = httpPostJson(dev3.baseUri().resolve("/awaits/complete"),
                            "{\"tenantId\":\"native-t\",\"interactionId\":\"" + interaction2Id
                                + "\",\"completionId\":\"comp-retry-1\",\"payload\":" + retryCompletionJson + "}");
                        assertEquals("SUCCEEDED", completed2.path("status").asText(), "Post-retry-SIGKILL failed: " + completed2);
                        nativeRetryResultJson = EXACT_JSON.writeValueAsString(completed2.path("result"));
                    } finally {
                        dev3.close();
                    }
                } finally {
                    dev2.close();
                }
            } finally {
                dev1.close();
            }

            // 4. Execute the exact same pipeline & inputs against production DynamoExecutionStateStore
            String dynamoAwaitResultJson;
            String dynamoRetryResultJson;
            try (PolyglotProofHarness dynamoHarness = new PolyglotProofHarness(
                yaml, dynamoPyHost.baseUri(), dynamoPyHost.baseUri(), SHARED_SECRET,
                temp.resolve("dynamo-state"), 2, Map.of(), dynamoHelper::createExecutionStateStore)) {

                ExecutionRecord<Object, Object> d1 = dynamoHarness.submitAndDrain(
                    "dynamo-t", "dynamo-await-1", awaitPayloadJson);
                assertEquals(ExecutionStatus.WAITING_EXTERNAL, d1.status());
                ExecutionRecord<Object, Object> d1Done = dynamoHarness.completeAwaitAndDrain(
                    "dynamo-t", dynamoHarness.awaitInteraction("dynamo-t", d1.awaitUnitId()), awaitCompletionJson);
                assertEquals(ExecutionStatus.SUCCEEDED, d1Done.status());
                dynamoAwaitResultJson = dynamoHarness.getResultCanonicalString("dynamo-t", d1.executionId());

                ExecutionRecord<Object, Object> d2 = dynamoHarness.submitAndDrain(
                    "dynamo-t", "dynamo-retry-1", retryPayloadJson);
                assertEquals(ExecutionStatus.WAIT_RETRY, d2.status());
                dynamoHarness.sweepAndDrain(System.currentTimeMillis() + 60_000L);
                ExecutionRecord<Object, Object> d2Wait = dynamoHarness.getExecution("dynamo-t", d2.executionId());
                assertEquals(ExecutionStatus.WAITING_EXTERNAL, d2Wait.status());
                ExecutionRecord<Object, Object> d2Done = dynamoHarness.completeAwaitAndDrain(
                    "dynamo-t", dynamoHarness.awaitInteraction("dynamo-t", d2Wait.awaitUnitId()), retryCompletionJson);
                assertEquals(ExecutionStatus.SUCCEEDED, d2Done.status());
                dynamoRetryResultJson = dynamoHarness.getResultCanonicalString("dynamo-t", d2.executionId());
            }

            // 5. Assert exact canonical JSON equivalence (including 29-digit decimal & int64 > 2^53)
            assertEquals(EXACT_JSON.readTree(dynamoAwaitResultJson), EXACT_JSON.readTree(nativeAwaitResultJson),
                "SIGKILL-recovered native tpf Await output must match DynamoDB");
            assertEquals(EXACT_JSON.readTree(dynamoRetryResultJson), EXACT_JSON.readTree(nativeRetryResultJson),
                "SIGKILL-recovered native tpf WAIT_RETRY output must match DynamoDB");
            assertTrue(nativeAwaitResultJson.contains(EXACT_DECIMAL));
            assertTrue(nativeAwaitResultJson.contains(EXACT_INT64_NEXT));
        }
    }

    static Optional<Path> resolveNativeTpfBinary() throws Exception {
        String configured = Optional.ofNullable(System.getProperty("tpf.native.bin"))
            .filter(v -> !v.isBlank())
            .orElseGet(() -> System.getenv("TPF_NATIVE_BIN"));
        if (configured != null && !configured.isBlank()) {
            Path path = Path.of(configured).toAbsolutePath().normalize();
            return Files.isExecutable(path) ? Optional.of(path) : Optional.empty();
        }
        Optional<String> platform = detectPlatform();
        if (platform.isEmpty()) {
            return Optional.empty();
        }
        String plat = platform.orElseThrow();

        // Resolve current `latest` release checksums directly from github.com release downloads first
        // (avoids unauthenticated api.github.com 60 req/hr rate limits on shared CI runners), falling back
        // to the GitHub Releases API only if checksums_sha256.txt is unavailable.
        try {
            String zipUrl = "";
            String expectedSha = "";
            String downloadBase = "https://github.com/The-Pipeline-Framework/pipelineframework-cli/releases/download/latest/";
            HttpResponse<String> checksumsResp = HTTP.send(
                HttpRequest.newBuilder(URI.create(downloadBase + "checksums_sha256.txt"))
                    .timeout(Duration.ofSeconds(30))
                    .header("User-Agent", "tpf-examples-sigkill-it")
                    .build(),
                HttpResponse.BodyHandlers.ofString());
            if (checksumsResp.statusCode() == 200) {
                for (String rawLine : checksumsResp.body().split("\\R")) {
                    String line = rawLine.trim();
                    if (line.isEmpty()) continue;
                    String[] parts = line.split("\\s+");
                    if (parts.length >= 2 && parts[1].endsWith("-" + plat + ".zip")
                        && parts[0].toLowerCase(Locale.ROOT).matches("[0-9a-f]{64}")) {
                        expectedSha = parts[0].toLowerCase(Locale.ROOT);
                        zipUrl = downloadBase + parts[1];
                        break;
                    }
                }
            }
            if (zipUrl.isBlank() || expectedSha.isBlank()) {
                HttpRequest.Builder metaReq = HttpRequest.newBuilder(
                        URI.create("https://api.github.com/repos/The-Pipeline-Framework/pipelineframework-cli/releases/tags/latest"))
                    .timeout(Duration.ofSeconds(20))
                    .header("Accept", "application/vnd.github+json")
                    .header("User-Agent", "tpf-examples-sigkill-it");
                String token = System.getenv("GITHUB_TOKEN");
                if (token != null && !token.isBlank()) {
                    metaReq.header("Authorization", "Bearer " + token);
                }
                HttpResponse<String> releaseResp = HTTP.send(metaReq.build(), HttpResponse.BodyHandlers.ofString());
                if (releaseResp.statusCode() != 200) {
                    return Optional.empty();
                }
                JsonNode release = EXACT_JSON.readTree(releaseResp.body());
                String jsonUrl = "";
                for (JsonNode asset : release.path("assets")) {
                    String name = asset.path("name").asText("");
                    if (name.endsWith("-" + plat + ".zip")) {
                        zipUrl = asset.path("browser_download_url").asText("");
                    } else if (name.endsWith("-" + plat + ".json")) {
                        jsonUrl = asset.path("browser_download_url").asText("");
                    }
                }
                if (zipUrl.isBlank() || jsonUrl.isBlank()) {
                    return Optional.empty();
                }
                HttpResponse<String> metaAssetResp = HTTP.send(
                    HttpRequest.newBuilder(URI.create(jsonUrl)).timeout(Duration.ofSeconds(30)).build(),
                    HttpResponse.BodyHandlers.ofString());
                if (metaAssetResp.statusCode() != 200) {
                    return Optional.empty();
                }
                JsonNode archiveMeta = EXACT_JSON.readTree(metaAssetResp.body());
                expectedSha = archiveMeta.path("sha256").asText("").trim().toLowerCase(Locale.ROOT);
                if (!expectedSha.matches("[0-9a-f]{64}")) {
                    return Optional.empty();
                }
            }

            Path cacheRoot = Path.of("target", "tpf-native-cache", plat, expectedSha).toAbsolutePath().normalize();
            Path cachedBin = cacheRoot.resolve("bin").resolve("tpf");
            if (Files.isExecutable(cachedBin)) {
                return Optional.of(cachedBin);
            }

            HttpResponse<byte[]> zipResp = HTTP.send(
                HttpRequest.newBuilder(URI.create(zipUrl)).timeout(Duration.ofMinutes(2)).build(),
                HttpResponse.BodyHandlers.ofByteArray());
            if (zipResp.statusCode() != 200) {
                return Optional.empty();
            }
            byte[] zipBytes = zipResp.body();
            String actualSha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(zipBytes));
            assertEquals(expectedSha, actualSha, "Downloaded native tpf archive SHA-256 mismatch");

            Files.createDirectories(cacheRoot);
            try (ZipInputStream zis = new ZipInputStream(new java.io.ByteArrayInputStream(zipBytes))) {
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    if (entry.isDirectory()) continue;
                    String entryName = entry.getName();
                    int slash = entryName.indexOf('/');
                    String rel = slash >= 0 ? entryName.substring(slash + 1) : entryName;
                    Path dest = cacheRoot.resolve(rel).normalize();
                    if (!dest.startsWith(cacheRoot)) {
                        throw new IOException("Unsafe zip entry: " + entryName);
                    }
                    Files.createDirectories(dest.getParent());
                    Files.write(dest, zis.readAllBytes());
                    if (rel.endsWith("bin/tpf")) {
                        Files.setPosixFilePermissions(dest, Set.of(
                            PosixFilePermission.OWNER_READ,
                            PosixFilePermission.OWNER_WRITE,
                            PosixFilePermission.OWNER_EXECUTE,
                            PosixFilePermission.GROUP_READ,
                            PosixFilePermission.GROUP_EXECUTE,
                            PosixFilePermission.OTHERS_READ,
                            PosixFilePermission.OTHERS_EXECUTE));
                    }
                }
            }
            return Files.isExecutable(cachedBin) ? Optional.of(cachedBin) : Optional.empty();
        } catch (IOException | InterruptedException downloadFailure) {
            if (downloadFailure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return Optional.empty();
        }
    }

    static Optional<String> detectPlatform() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        if (os.contains("mac") && (arch.contains("aarch64") || arch.contains("arm64"))) {
            return Optional.of("osx-aarch_64");
        }
        if (os.contains("linux")) {
            if (arch.contains("aarch64") || arch.contains("arm64")) {
                return Optional.of("linux-aarch_64");
            }
            if (arch.contains("amd64") || arch.contains("x86_64")) {
                return Optional.of("linux-x86_64");
            }
        }
        return Optional.empty();
    }

    private static ProcessBuilder nativeProcessBuilder(
        Path binary, Path home, Path emptyPathDir, List<String> args
    ) {
        List<String> cmd = new java.util.ArrayList<>();
        cmd.add(binary.toString());
        cmd.add("-Duser.home=" + home);
        cmd.addAll(args);
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        Map<String, String> env = pb.environment();
        env.keySet().removeIf(k -> k.startsWith("JAVA_") || k.startsWith("JDK_") || k.startsWith("TPF_"));
        env.put("HOME", home.toString());
        env.put("PATH", emptyPathDir.toString()); // zero JDK on PATH!
        return pb;
    }

    private static ProcessResult runNativeCommand(
        Path binary, Path home, Path emptyPathDir, List<String> args
    ) throws Exception {
        Process proc = nativeProcessBuilder(binary, home, emptyPathDir, args).start();
        CompletableFuture<String> outputFuture = new CompletableFuture<>();
        Thread drainer = new Thread(() -> {
            try (InputStream in = proc.getInputStream()) {
                outputFuture.complete(new String(in.readAllBytes(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                outputFuture.complete("[stdout read error: " + e.getMessage() + "]");
            }
        }, "native-tpf-cmd-stdout");
        drainer.setDaemon(true);
        drainer.start();
        try {
            boolean finished = proc.waitFor(60, TimeUnit.SECONDS);
            if (!finished) {
                terminateProcess(proc);
            }
            String out = outputFuture.get(5, TimeUnit.SECONDS);
            assertTrue(finished, "Native tpf command timed out:\n" + out);
            return new ProcessResult(proc.exitValue(), out);
        } finally {
            terminateProcess(proc);
        }
    }

    static NativeDevProcess startNativeDev(
        Path binary, Path home, Path emptyPathDir, Path yaml, URI functionHost, Path dataDir
    ) throws Exception {
        return startNativeDev(binary, home, emptyPathDir, yaml, functionHost, dataDir, SHARED_SECRET);
    }

    static NativeDevProcess startNativeDev(
        Path binary, Path home, Path emptyPathDir, Path yaml, URI functionHost, Path dataDir, String secret
    ) throws Exception {
        Process proc = nativeProcessBuilder(binary, home, emptyPathDir, List.of(
            "dev",
            "--pipeline", yaml.toString(),
            "--function-host", functionHost.toString(),
            "--data-dir", dataDir.toString(),
            "--secret", secret,
            "--port", "0",
            "--max-retries", "2")).start();
        StringBuilder log = new StringBuilder();
        CompletableFuture<Integer> portFuture = new CompletableFuture<>();
        Thread drainer = new Thread(() -> {
            try (var reader = proc.inputReader(StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    synchronized (log) {
                        log.append(line).append('\n');
                    }
                    if (!portFuture.isDone() && line.startsWith("READY ")) {
                        String[] parts = line.substring("READY ".length()).trim().split("\\s+");
                        portFuture.complete(Integer.parseInt(parts[0]));
                    }
                }
                if (!portFuture.isDone()) {
                    portFuture.completeExceptionally(new IllegalStateException(
                        "Native tpf dev exited before READY:\n" + log));
                }
            } catch (IOException e) {
                if (!portFuture.isDone()) {
                    portFuture.completeExceptionally(e);
                }
            }
        }, "native-tpf-dev-stdout");
        drainer.setDaemon(true);
        drainer.start();
        try {
            int port = portFuture.get(30, TimeUnit.SECONDS);
            URI baseUri = URI.create("http://127.0.0.1:" + port);
            JsonNode health = httpGetJson(baseUri.resolve("/health"));
            assertEquals("UP", health.path("status").asText());
            return new NativeDevProcess(proc, baseUri, health.path("pid").asLong(), log);
        } catch (Throwable startupFailure) {
            terminateProcess(proc);
            if (startupFailure instanceof Exception ex) {
                throw ex;
            }
            if (startupFailure instanceof Error err) {
                throw err;
            }
            throw new IllegalStateException("Failed starting native tpf dev", startupFailure);
        }
    }

    static void terminateProcess(Process proc) {
        if (proc != null && proc.isAlive()) {
            proc.destroy();
            try {
                if (!proc.waitFor(5, TimeUnit.SECONDS)) {
                    proc.destroyForcibly();
                    proc.waitFor(2, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                proc.destroyForcibly();
                Thread.currentThread().interrupt();
            }
        }
    }

    static void sigkill(NativeDevProcess dev) throws Exception {
        Process killer = new ProcessBuilder("/bin/kill", "-9", Long.toString(dev.osPid())).start();
        assertTrue(killer.waitFor(5, TimeUnit.SECONDS));
        assertTrue(dev.process().waitFor(10, TimeUnit.SECONDS), "Native tpf dev did not exit on SIGKILL");
        assertNotEquals(0, dev.process().exitValue(), "SIGKILL should produce non-zero exit status");
    }

    static JsonNode awaitStatus(URI url, String expectedStatus, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        JsonNode last = null;
        while (System.nanoTime() < deadline) {
            last = httpGetJson(url);
            if (expectedStatus.equals(last.path("status").asText())) {
                return last;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Timed out waiting for status " + expectedStatus + ", last=" + last);
    }

    static JsonNode httpGetJson(URI uri) throws Exception {
        HttpResponse<String> resp = HTTP.send(
            HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(15)).GET().build(),
            HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(), "GET " + uri + " failed: " + resp.body());
        return EXACT_JSON.readTree(resp.body());
    }

    static JsonNode httpPostJson(URI uri, String json) throws Exception {
        HttpResponse<String> resp = HTTP.send(
            HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                .build(),
            HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(), "POST " + uri + " failed: " + resp.body());
        return EXACT_JSON.readTree(resp.body());
    }

    private static String pythonHostSource() {
        return """
            import http.server, os, signal, socketserver, sys, threading, time
            sys.path.insert(0, os.environ["TPF_GEN_PARENT"])
            from sigkill_equivalence_proof import host, types as t

            # Avoid macOS reverse-DNS stall inside HTTPServer.server_bind -> socket.getfqdn("127.0.0.1")
            def _fast_server_bind(self):
                socketserver.TCPServer.server_bind(self)
                bound_host, bound_port = self.server_address[:2]
                self.server_name = bound_host
                self.server_port = bound_port
            http.server.HTTPServer.server_bind = _fast_server_bind

            lock = threading.Lock()
            seen = set()

            class Handlers:
                def validate_payment(self, inp: t.PaymentInput, ctx: t.FunctionContext) -> t.ValidatedPayment:
                    if inp.failFirstAttempt:
                        with lock:
                            key = (inp.paymentId, ctx.step_id)
                            first = key not in seen
                            seen.add(key)
                        if first:
                            raise t.TpfTransientError("TRANSIENT_OUTAGE", "Simulated first-attempt transient failure")
                    return t.ValidatedPayment(
                        paymentId=inp.paymentId,
                        sequenceNumber=inp.sequenceNumber + 1,
                        amount=inp.amount,
                    )

                def finalize_receipt(self, inp: t.SettlementNotice, ctx: t.FunctionContext) -> t.PaymentReceipt:
                    return t.PaymentReceipt(
                        paymentId=inp.paymentId,
                        sequenceNumber=inp.sequenceNumber,
                        settledAmount=inp.settledAmount,
                    )

            running = host.start_app_host(
                Handlers(),
                host="127.0.0.1",
                port=0,
                shared_secret=os.environ["TPF_DEV_SECRET"],
            )
            print("READY " + running.base_url, flush=True)
            def stop(_s, _f):
                running.close()
                sys.exit(0)
            signal.signal(signal.SIGTERM, stop)
            while True:
                time.sleep(0.2)
            """;
    }

    private record ProcessResult(int exitCode, String output) {
    }

    record NativeDevProcess(Process process, URI baseUri, long osPid, StringBuilder log)
        implements AutoCloseable {
        @Override
        public void close() {
            if (process.isAlive()) {
                process.destroy();
                try {
                    if (!process.waitFor(5, TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                        process.waitFor(2, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException e) {
                    process.destroyForcibly();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
