package org.pipelineframework.proof;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Assumptions;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Launches generated TypeScript (Node) or Python foreign hosts with bounded readiness timeouts
 * and clean subprocess teardown.
 */
public final class PolyglotHostProcess implements AutoCloseable {

    private final Process process;
    private final URI baseUri;
    private final StringBuilder outputLog;

    private PolyglotHostProcess(Process process, URI baseUri, StringBuilder outputLog) {
        this.process = process;
        this.baseUri = baseUri;
        this.outputLog = outputLog;
    }

    public URI baseUri() {
        return baseUri;
    }

    public String capturedOutput() {
        synchronized (outputLog) {
            return outputLog.toString();
        }
    }

    public static PolyglotHostProcess start(
        List<String> command,
        Path workingDir,
        Map<String, String> env,
        Duration readyTimeout
    ) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(command)
            .directory(workingDir.toFile())
            .redirectErrorStream(true);
        pb.environment().putAll(env);
        Process proc = pb.start();
        StringBuilder outputLog = new StringBuilder();
        try {
            String readyUrl = awaitReadyLine(proc, outputLog, readyTimeout);
            return new PolyglotHostProcess(proc, URI.create(readyUrl), outputLog);
        } catch (Throwable failure) {
            proc.destroyForcibly();
            proc.waitFor(5, TimeUnit.SECONDS);
            throw failure;
        }
    }

    public static void requireNodeWithTypeStripping() {
        boolean isCi = "true".equalsIgnoreCase(System.getenv("CI"));
        Path probeDir = null;
        Process proc = null;
        try {
            probeDir = Files.createTempDirectory("tpf-node-probe");
            Path probeScript = probeDir.resolve("probe.ts");
            Path probeOut = probeDir.resolve("probe.out");
            Files.writeString(probeScript, "const x: number = 42; console.log(x);\n");
            proc = new ProcessBuilder("node", probeScript.toString())
                .redirectErrorStream(true)
                .redirectOutput(probeOut.toFile())
                .start();
            boolean finished = proc.waitFor(5, TimeUnit.SECONDS);
            if (!finished) {
                proc.destroyForcibly();
                proc.waitFor(2, TimeUnit.SECONDS);
            }
            String out = Files.exists(probeOut) ? Files.readString(probeOut, StandardCharsets.UTF_8).trim() : "";
            boolean ok = finished && proc.exitValue() == 0 && "42".equals(out);
            String msg = "Node.js with default TypeScript type stripping (>= 22.18 / 23.6+) required (out=" + out + ")";
            if (isCi) assertTrue(ok, msg); else Assumptions.assumeTrue(ok, msg);
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            String msg = "Node.js with TypeScript type stripping required on PATH: " + e.getMessage();
            if (isCi) fail(msg); else Assumptions.assumeTrue(false, msg);
        } finally {
            if (proc != null && proc.isAlive()) proc.destroyForcibly();
            deleteTreeQuietly(probeDir);
        }
    }

    public static void requirePython3() {
        boolean isCi = "true".equalsIgnoreCase(System.getenv("CI"));
        Path probeDir = null;
        Process proc = null;
        try {
            probeDir = Files.createTempDirectory("tpf-py-probe");
            Path probeOut = probeDir.resolve("probe.out");
            proc = new ProcessBuilder("python3", "-c", "import sys; print(sys.version_info >= (3, 10))")
                .redirectErrorStream(true)
                .redirectOutput(probeOut.toFile())
                .start();
            boolean finished = proc.waitFor(5, TimeUnit.SECONDS);
            if (!finished) {
                proc.destroyForcibly();
                proc.waitFor(2, TimeUnit.SECONDS);
            }
            String out = Files.exists(probeOut) ? Files.readString(probeOut, StandardCharsets.UTF_8).trim() : "";
            boolean ok = finished && proc.exitValue() == 0 && "True".equals(out);
            String msg = "Python >= 3.10 required on PATH (out=" + out + ")";
            if (isCi) assertTrue(ok, msg); else Assumptions.assumeTrue(ok, msg);
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            String msg = "Python >= 3.10 required on PATH: " + e.getMessage();
            if (isCi) fail(msg); else Assumptions.assumeTrue(false, msg);
        } finally {
            if (proc != null && proc.isAlive()) proc.destroyForcibly();
            deleteTreeQuietly(probeDir);
        }
    }

    private static String awaitReadyLine(Process proc, StringBuilder outputLog, Duration timeout) throws Exception {
        var readyFuture = new java.util.concurrent.CompletableFuture<String>();
        Thread drainer = new Thread(() -> {
            try (var reader = proc.inputReader(StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    synchronized (outputLog) {
                        outputLog.append(line).append('\n');
                    }
                    if (!readyFuture.isDone() && line.startsWith("READY ")) {
                        readyFuture.complete(line.substring("READY ".length()).trim());
                    }
                }
                if (!readyFuture.isDone()) {
                    synchronized (outputLog) {
                        readyFuture.completeExceptionally(
                            new IllegalStateException("Foreign host exited before READY. Output:\n" + outputLog));
                    }
                }
            } catch (IOException e) {
                if (!readyFuture.isDone()) {
                    readyFuture.completeExceptionally(e);
                }
            }
        }, "polyglot-host-stdout");
        drainer.setDaemon(true);
        drainer.start();
        try {
            return readyFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            String captured;
            synchronized (outputLog) {
                captured = outputLog.toString();
            }
            throw new AssertionError("Timed out waiting for foreign host READY after " + timeout
                + ". Captured output:\n" + captured);
        } catch (ExecutionException e) {
            throw new AssertionError("Foreign host failed to start", e.getCause());
        }
    }

    private static void deleteTreeQuietly(Path dir) {
        if (dir == null) return;
        try (var stream = Files.walk(dir)) {
            stream.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (IOException ignored) { }
            });
        } catch (IOException ignored) {
        }
    }

    @Override
    public void close() {
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
