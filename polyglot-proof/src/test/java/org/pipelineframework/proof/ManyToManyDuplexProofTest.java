package org.pipelineframework.proof;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.PolyglotProofHarness;
import org.pipelineframework.compiler.polyglot.PolyglotCompiler;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStatus;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ADR-0092 cross-language proof verifying full-duplex {@code MANY_TO_MANY} streaming parity
 * across live Node (TypeScript) and Python hosts wired through TPF's
 * {@code CanonicalFunctionHostManyToManyStep} inside a 4-step
 * {@code ONE_TO_ONE -> ONE_TO_MANY -> MANY_TO_MANY -> MANY_TO_ONE} pipeline.
 */
class ManyToManyDuplexProofTest {

    private static final String SHARED_SECRET = PolyglotCrossLanguageProofTest.SHARED_SECRET;

    private static final String DUPLEX_OK_PAYLOAD = """
        {
          "batchSeq": 9007199254740993,
          "lines": [
            {"lineSeq": 9007199254740993, "sku": "SKU-A", "amount": 123456.789012, "mode": "OK"},
            {"lineSeq": 9007199254740994, "sku": "SKU-B", "amount": 10.25, "mode": "OK"}
          ]
        }
        """;

    private static final String DUPLEX_REJECT_PAYLOAD = """
        {
          "batchSeq": 100,
          "lines": [
            {"lineSeq": 101, "sku": "SKU-BAD", "amount": 5.00, "mode": "REJECT"}
          ]
        }
        """;

    private static final String DUPLEX_TRANSIENT_PAYLOAD = """
        {
          "batchSeq": 200,
          "lines": [
            {"lineSeq": 201, "sku": "SKU-RETRY", "amount": 7.50, "mode": "TRANSIENT_ONCE"}
          ]
        }
        """;

    @Test
    void compilerProducesByteIdenticalManyToManyContractForTypeScriptAndPython(@TempDir Path temp) throws Exception {
        Path yaml = resourcePath("invoice-many-to-many-pipeline.yaml", temp);
        Path tsOut = temp.resolve("ts");
        Path pyOut = temp.resolve("py");

        var tsRes = PolyglotCompiler.compile(PolyglotCompiler.Target.TYPESCRIPT, yaml, tsOut);
        var pyRes = PolyglotCompiler.compile(PolyglotCompiler.Target.PYTHON, yaml, pyOut);

        assertEquals(tsRes.contractVersion(), pyRes.contractVersion());
        assertEquals(tsRes.catalogFingerprint(), pyRes.catalogFingerprint());
        assertEquals(
            Files.readString(tsOut.resolve(PolyglotCompiler.CONTRACT_RELATIVE_PATH)),
            Files.readString(pyOut.resolve(PolyglotCompiler.CONTRACT_RELATIVE_PATH)));
        assertTrue(Files.readString(tsOut.resolve("host.ts")).contains("/tpf/functions/enrich-stream/duplex"));
        assertTrue(Files.readString(pyOut.resolve("host.py")).contains("/tpf/functions/enrich-stream/duplex"));
    }

    @Test
    void nodeAndPythonProduceByteIdenticalFullDuplexManyToManyResultsAndFailureSemantics(@TempDir Path temp)
        throws Exception {
        PolyglotHostProcess.requireNodeWithTypeStripping();
        PolyglotHostProcess.requirePython3();

        Path yaml = resourcePath("invoice-many-to-many-pipeline.yaml", temp);
        DuplexProofResult nodeRes = runDuplexProof(PolyglotCompiler.Target.TYPESCRIPT, yaml, temp.resolve("node"));
        DuplexProofResult pyRes = runDuplexProof(PolyglotCompiler.Target.PYTHON, yaml, temp.resolve("python"));

        // 1. Exact canonical JSON parity across Node and Python (int64 > 2^53 + 12-digit exact decimal)
        assertEquals(nodeRes.okCanonicalJson(), pyRes.okCanonicalJson(),
            "Node and Python must produce byte-identical DuplexSettlementSummary JSON");
        JsonNode summary = PolyglotProofHarness.JSON.readTree(nodeRes.okCanonicalJson());
        assertEquals("9007199254740994", summary.path("batchSeq").asText());
        assertEquals(2, summary.path("lineCount").asInt());
        assertEquals("9007199254740995", summary.path("maxLineSeq").asText());
        // (123456.789012 + 1.50) + (10.25 + 1.50) = 123470.039012
        assertEquals("123470.039012", summary.path("grandTotal").asText());

        // 2. Portable BUSINESS_REJECTION raised inside the MANY_TO_MANY step fails terminally with 0 retries
        assertEquals(ExecutionStatus.FAILED, nodeRes.rejected().status());
        assertEquals(ExecutionStatus.FAILED, pyRes.rejected().status());
        assertEquals(0, nodeRes.rejected().attempt());
        assertEquals(0, pyRes.rejected().attempt());
        assertEquals(2, nodeRes.rejected().failedStepIndex());
        assertEquals(2, pyRes.rejected().failedStepIndex());
        assertEquals("cmd-enrich-stream", nodeRes.rejected().failedCommandId().orElseThrow());
        assertEquals("cmd-enrich-stream", pyRes.rejected().failedCommandId().orElseThrow());

        // 3. Portable TRANSIENT raised inside the MANY_TO_MANY step transitions to WAIT_RETRY and recovers via sweep
        assertEquals(ExecutionStatus.WAIT_RETRY, nodeRes.retryBefore().status());
        assertEquals(ExecutionStatus.WAIT_RETRY, pyRes.retryBefore().status());
        assertEquals(ExecutionStatus.SUCCEEDED, nodeRes.retryAfter().status());
        assertEquals(ExecutionStatus.SUCCEEDED, pyRes.retryAfter().status());
        assertEquals(nodeRes.retryCanonicalJson(), pyRes.retryCanonicalJson());
        JsonNode retrySummary = PolyglotProofHarness.JSON.readTree(nodeRes.retryCanonicalJson());
        assertEquals("201", retrySummary.path("batchSeq").asText());
        assertEquals(1, retrySummary.path("lineCount").asInt());
        assertEquals("202", retrySummary.path("maxLineSeq").asText());
        assertEquals(0, new java.math.BigDecimal("9").compareTo(retrySummary.path("grandTotal").decimalValue()));
    }

    private static DuplexProofResult runDuplexProof(PolyglotCompiler.Target target, Path yaml, Path workDir)
        throws Exception {
        Path genDir = workDir.resolve("gen");
        Path dataDir = workDir.resolve("state");
        PolyglotCompiler.compile(target, yaml, genDir);

        try (PolyglotHostProcess host = startDuplexHost(target, genDir)) {
            try (PolyglotProofHarness harness = new PolyglotProofHarness(
                yaml, host.baseUri(), host.baseUri(), SHARED_SECRET, dataDir, 2, Map.of(), null)) {
                ExecutionRecord<Object, Object> ok = harness.submitAndDrain("t1", "duplex-ok", DUPLEX_OK_PAYLOAD);
                assertEquals(ExecutionStatus.SUCCEEDED, ok.status(),
                    "OK duplex run failed for " + target + ": " + ok.errorMessage() + "\nHost:\n" + host.capturedOutput());
                String okJson = harness.getResultCanonicalString("t1", ok.executionId());

                ExecutionRecord<Object, Object> rejected = harness.submitAndDrain(
                    "t1", "duplex-rej", DUPLEX_REJECT_PAYLOAD);

                ExecutionRecord<Object, Object> retryBefore = harness.submitAndDrain(
                    "t1", "duplex-retry", DUPLEX_TRANSIENT_PAYLOAD);
                Thread.sleep(25);
                harness.sweepAndDrain(System.currentTimeMillis() + 60_000L);
                ExecutionRecord<Object, Object> retryAfter = harness.getExecution("t1", retryBefore.executionId());
                assertEquals(ExecutionStatus.SUCCEEDED, retryAfter.status(),
                    "Retry duplex run failed for " + target + ": " + retryAfter.errorMessage()
                        + "\nHost:\n" + host.capturedOutput());
                String retryJson = harness.getResultCanonicalString("t1", retryAfter.executionId());

                return new DuplexProofResult(okJson, rejected, retryBefore, retryAfter, retryJson);
            }
        }
    }

    private static PolyglotHostProcess startDuplexHost(PolyglotCompiler.Target target, Path genDir) throws Exception {
        if (target == PolyglotCompiler.Target.TYPESCRIPT) {
            Path app = genDir.resolve("app.ts");
            Files.writeString(app, """
                import { startAppHost } from './host.ts';
                import { TpfBusinessRejectionError, TpfTransientError } from './types.ts';
                import type { EnrichedEntry, FunctionHandlers, PricedEntry } from './types.ts';

                function addDecimals(a: string, b: string): string {
                  const parse = (s: string) => {
                    const neg = s.startsWith('-');
                    const body = neg ? s.slice(1) : s;
                    const [intPart, fracPart = ''] = body.split('.');
                    return { neg, intPart, fracPart };
                  };
                  const pa = parse(a);
                  const pb = parse(b);
                  const scale = Math.max(pa.fracPart.length, pb.fracPart.length);
                  const toScaled = (p: ReturnType<typeof parse>) => {
                    const v = BigInt(p.intPart + p.fracPart.padEnd(scale, '0'));
                    return p.neg ? -v : v;
                  };
                  const sum = toScaled(pa) + toScaled(pb);
                  const neg = sum < 0n;
                  const digits = (neg ? -sum : sum).toString().padStart(scale + 1, '0');
                  if (scale === 0) return (neg ? '-' : '') + digits;
                  const intPart = digits.slice(0, digits.length - scale);
                  const fracPart = digits.slice(digits.length - scale).replace(/0+$/, '');
                  return (neg ? '-' : '') + (fracPart.length > 0 ? `${intPart}.${fracPart}` : intPart);
                }

                let transientOnce = 0;

                const handlers: FunctionHandlers = {
                  async validateBatch(input) {
                    return { batchSeq: input.batchSeq + 1n, lines: input.lines };
                  },
                  async *splitEntries(input) {
                    for (const line of input.lines) {
                      yield {
                        batchSeq: input.batchSeq,
                        lineSeq: line.lineSeq,
                        sku: line.sku,
                        amount: line.amount,
                        mode: line.mode,
                      };
                    }
                  },
                  async *enrichStream(items: AsyncIterable<PricedEntry>) {
                    for await (const item of items) {
                      if (item.mode === 'REJECT') {
                        throw new TpfBusinessRejectionError('BAD_DUPLEX_LINE', 'rejected inside MANY_TO_MANY');
                      }
                      if (item.mode === 'TRANSIENT_ONCE' && transientOnce === 0) {
                        transientOnce += 1;
                        throw new TpfTransientError('DUPLEX_BUSY', 'retry MANY_TO_MANY');
                      }
                      yield {
                        batchSeq: item.batchSeq,
                        lineSeq: item.lineSeq + 1n,
                        sku: item.sku,
                        settledAmount: addDecimals(item.amount, '1.50'),
                      };
                    }
                  },
                  async summarizeLedger(items: AsyncIterable<EnrichedEntry>) {
                    let batchSeq = 0n;
                    let lineCount = 0;
                    let maxLineSeq = 0n;
                    let grandTotal = '0';
                    for await (const item of items) {
                      batchSeq = item.batchSeq;
                      lineCount += 1;
                      if (item.lineSeq > maxLineSeq) maxLineSeq = item.lineSeq;
                      grandTotal = addDecimals(grandTotal, item.settledAmount);
                    }
                    return { batchSeq, lineCount, maxLineSeq, grandTotal };
                  },
                };

                const running = await startAppHost(handlers, { sharedSecret: process.env.TPF_SECRET! });
                console.log('READY ' + running.baseUrl);
                process.on('SIGTERM', async () => { await running.close(); process.exit(0); });
                """);
            return PolyglotHostProcess.start(
                java.util.List.of("node", app.toString()), genDir, Map.of("TPF_SECRET", SHARED_SECRET),
                Duration.ofSeconds(10));
        }

        Path appDir = genDir.getParent();
        String pkg = genDir.getFileName().toString();
        Path app = appDir.resolve("app.py");
        Files.writeString(app, """
            import os, signal, sys
            from decimal import Decimal
            from %s.host import start_app_host
            from %s import types as tpf_types

            transient_once = 0

            def normalize_decimal(d: Decimal) -> Decimal:
                normalized = d.normalize()
                if normalized == normalized.to_integral():
                    return normalized.quantize(Decimal("1"))
                return normalized

            class Handlers:
                def validate_batch(self, inp, ctx):
                    return tpf_types.ValidatedDuplexBatch(batchSeq=inp.batchSeq + 1, lines=inp.lines)

                def split_entries(self, inp, ctx):
                    for line in inp.lines:
                        yield tpf_types.PricedEntry(
                            batchSeq=inp.batchSeq,
                            lineSeq=line.lineSeq,
                            sku=line.sku,
                            amount=line.amount,
                            mode=line.mode,
                        )

                def enrich_stream(self, items, ctx):
                    global transient_once
                    for item in items:
                        if item.mode == "REJECT":
                            raise tpf_types.TpfBusinessRejectionError("BAD_DUPLEX_LINE", "rejected inside MANY_TO_MANY")
                        if item.mode == "TRANSIENT_ONCE" and transient_once == 0:
                            transient_once += 1
                            raise tpf_types.TpfTransientError("DUPLEX_BUSY", "retry MANY_TO_MANY")
                        yield tpf_types.EnrichedEntry(
                            batchSeq=item.batchSeq,
                            lineSeq=item.lineSeq + 1,
                            sku=item.sku,
                            settledAmount=normalize_decimal(item.amount + Decimal("1.50")),
                        )

                def summarize_ledger(self, items, ctx):
                    batch_seq = 0
                    line_count = 0
                    max_line_seq = 0
                    grand_total = Decimal("0")
                    for item in items:
                        batch_seq = item.batchSeq
                        line_count += 1
                        if item.lineSeq > max_line_seq:
                            max_line_seq = item.lineSeq
                        grand_total = normalize_decimal(grand_total + item.settledAmount)
                    return tpf_types.DuplexSettlementSummary(
                        batchSeq=batch_seq,
                        lineCount=line_count,
                        maxLineSeq=max_line_seq,
                        grandTotal=grand_total,
                    )

            running = start_app_host(Handlers(), shared_secret=os.environ["TPF_SECRET"])
            print("READY " + running.base_url, flush=True)
            def _stop(_sig, _frame):
                running.close()
                sys.exit(0)
            signal.signal(signal.SIGTERM, _stop)
            signal.pause()
            """.formatted(pkg, pkg));
        return PolyglotHostProcess.start(
            java.util.List.of("python3", "-u", app.toString()), appDir, Map.of("TPF_SECRET", SHARED_SECRET),
            Duration.ofSeconds(10));
    }

    private static Path resourcePath(String name, Path temp) throws Exception {
        Path dst = temp.resolve(name);
        try (var in = ManyToManyDuplexProofTest.class.getResourceAsStream("/" + name)) {
            assertNotNull(in, "Missing resource: " + name);
            Files.copy(in, dst);
        }
        return dst;
    }

    private record DuplexProofResult(
        String okCanonicalJson,
        ExecutionRecord<Object, Object> rejected,
        ExecutionRecord<Object, Object> retryBefore,
        ExecutionRecord<Object, Object> retryAfter,
        String retryCanonicalJson
    ) { }
}
