package org.pipelineframework.proof;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.PolyglotProofHarness;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandStepSupport;
import org.pipelineframework.compiler.polyglot.PolyglotCompiler;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.query.QueryStepDescriptor;
import org.pipelineframework.query.QueryStepSupport;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ADR-0092 cross-language proof verifying Node (TypeScript) and Python equivalence across:
 * <ul>
 *   <li>Class-free canonical v3 contract emission ({@link PolyglotCompiler})</li>
 *   <li>Multi-field canonical payloads ({@code int64} above {@code Number.MAX_SAFE_INTEGER}, exact {@code decimal}
 *       arithmetic, {@code bool}, {@code uuid}, {@code timestamp}, optional/nullable fields, repeated nested records,
 *       and constrained wrappers)</li>
 *   <li>All 3 cardinalities ({@code ONE_TO_ONE}, {@code ONE_TO_MANY}, {@code MANY_TO_ONE})</li>
 *   <li>Deterministic failure classification ({@code BUSINESS_REJECTION}, {@code TRANSIENT} retry)</li>
 *   <li>Tier-2 custom Polyglot {@code Query} and {@code Command} connectors with capture replay,
 *       {@code RETURN_RECORDED}, {@code AMBIGUOUS} reconciliation, and host secret isolation</li>
 *   <li>Crash-safe disk-backed restart recovery across durable {@code QUEUED} admission before dispatch
 *       and durable {@code WAITING_EXTERNAL} Await</li>
 * </ul>
 */
class PolyglotCrossLanguageProofTest {

    static final String SHARED_SECRET = "polyglot-proof-shared-hmac-secret";
    static final String STREAMING_PAYLOAD_OK = """
        {
          "batchSeq": 9007199254740993,
          "batchId": "550e8400-e29b-41d4-a716-446655440000",
          "submittedAt": "2026-11-26T12:00:00Z",
          "currency": "USD",
          "expedited": true,
          "note": "priority-settlement",
          "mode": "OK",
          "items": [
            {"sku": "SKU-A", "qty": 1, "unitPrice": 123456.789012},
            {"sku": "SKU-B", "qty": 2, "unitPrice": 10.25}
          ]
        }
        """;
    static final String STREAMING_PAYLOAD_REJECT = """
        {
          "batchSeq": 100,
          "batchId": "550e8400-e29b-41d4-a716-446655440001",
          "submittedAt": "2026-11-26T12:00:00Z",
          "currency": "EUR",
          "expedited": false,
          "note": null,
          "mode": "REJECT",
          "items": [{"sku": "SKU-X", "qty": 1, "unitPrice": 1.00}]
        }
        """;
    static final String STREAMING_PAYLOAD_TRANSIENT = """
        {
          "batchSeq": 200,
          "batchId": "550e8400-e29b-41d4-a716-446655440002",
          "submittedAt": "2026-11-26T12:00:00Z",
          "currency": "USD",
          "expedited": false,
          "mode": "TRANSIENT_ONCE",
          "items": [{"sku": "SKU-R", "qty": 3, "unitPrice": 5.50}]
        }
        """;

    @Test
    void compilerProducesByteIdenticalClassFreeContractsForTypeScriptAndPython(@TempDir Path temp) throws Exception {
        Path yaml = resourcePath("invoice-streaming-pipeline.yaml", temp);
        Path tsOut = temp.resolve("ts");
        Path pyOut = temp.resolve("py");

        var tsRes = PolyglotCompiler.compile(PolyglotCompiler.Target.TYPESCRIPT, yaml, tsOut);
        var pyRes = PolyglotCompiler.compile(PolyglotCompiler.Target.PYTHON, yaml, pyOut);

        assertEquals(tsRes.pipelineId(), pyRes.pipelineId());
        assertEquals(tsRes.contractVersion(), pyRes.contractVersion());
        assertEquals(tsRes.catalogFingerprint(), pyRes.catalogFingerprint());

        String tsContract = Files.readString(tsOut.resolve(PolyglotCompiler.CONTRACT_RELATIVE_PATH));
        String pyContract = Files.readString(pyOut.resolve(PolyglotCompiler.CONTRACT_RELATIVE_PATH));
        assertEquals(tsContract, pyContract, "Canonical v3 contract must be byte-identical across targets");
        assertFalse(tsContract.contains("\"runtimeClass\""), "Class-free contract must not emit Java runtimeClass");
        assertTrue(Files.isRegularFile(tsOut.resolve("types.ts")));
        assertTrue(Files.isRegularFile(tsOut.resolve("host.ts")));
        assertTrue(Files.isRegularFile(pyOut.resolve("types.py")));
        assertTrue(Files.isRegularFile(pyOut.resolve("host.py")));
    }

    @Test
    void nodeAndPythonProduceIdenticalMultiFieldStreamingResultsAndFailureSemantics(@TempDir Path temp) throws Exception {
        PolyglotHostProcess.requireNodeWithTypeStripping();
        PolyglotHostProcess.requirePython3();

        Path yaml = resourcePath("invoice-streaming-pipeline.yaml", temp);
        StreamingProofResult nodeResult = runStreamingProof(PolyglotCompiler.Target.TYPESCRIPT, yaml, temp.resolve("node"));
        StreamingProofResult pyResult = runStreamingProof(PolyglotCompiler.Target.PYTHON, yaml, temp.resolve("python"));

        // 1. Exact canonical JSON parity across Node (BigInt + exact decimal string) and Python (int + Decimal)
        assertEquals(nodeResult.okCanonicalJson(), pyResult.okCanonicalJson(),
            "Node and Python must produce byte-identical canonical SettlementSummary JSON");
        JsonNode summary = PolyglotProofHarness.JSON.readTree(nodeResult.okCanonicalJson());
        assertEquals("9007199254740994", summary.path("batchSeq").asText(),
            "int64 above Number.MAX_SAFE_INTEGER must increment without precision loss");
        assertEquals(2, summary.path("lineCount").asInt());
        assertEquals("123477.289012", summary.path("grandTotal").asText(),
            "Decimal arithmetic must survive 1:1 -> 1:N -> N:1 identically across Java/Node/Python");

        // 2. Identical BUSINESS_REJECTION terminal classification (0 retries, step index + commandId + error preserved)
        assertEquals(ExecutionStatus.FAILED, nodeResult.rejected().status());
        assertEquals(ExecutionStatus.FAILED, pyResult.rejected().status());
        assertEquals(0, nodeResult.rejected().attempt(), "BUSINESS_REJECTION must not retry on Node");
        assertEquals(0, pyResult.rejected().attempt(), "BUSINESS_REJECTION must not retry on Python");
        assertEquals(0, nodeResult.rejected().failedStepIndex());
        assertEquals(0, pyResult.rejected().failedStepIndex());
        assertEquals("cmd-validate-invoice", nodeResult.rejected().failedCommandId().orElseThrow());
        assertEquals("cmd-validate-invoice", pyResult.rejected().failedCommandId().orElseThrow());
        assertTrue(errorDetail(nodeResult.rejected()).contains("INVALID_INVOICE"),
            "Node rejection must preserve INVALID_INVOICE detail, got: " + errorDetail(nodeResult.rejected()));
        assertTrue(errorDetail(pyResult.rejected()).contains("INVALID_INVOICE"),
            "Python rejection must preserve INVALID_INVOICE detail, got: " + errorDetail(pyResult.rejected()));
        assertEquals(1, nodeResult.deadLetters());
        assertEquals(1, pyResult.deadLetters());

        // 3. Identical TRANSIENT -> WAIT_RETRY -> sweep -> SUCCEEDED
        assertEquals(ExecutionStatus.WAIT_RETRY, nodeResult.retryBeforeSweep().status());
        assertEquals(ExecutionStatus.WAIT_RETRY, pyResult.retryBeforeSweep().status());
        assertEquals(ExecutionStatus.SUCCEEDED, nodeResult.retryAfterSweep().status());
        assertEquals(ExecutionStatus.SUCCEEDED, pyResult.retryAfterSweep().status());
        assertEquals(nodeResult.retryCanonicalJson(), pyResult.retryCanonicalJson());
        assertTrue(nodeResult.dispatchIntents() > 0 && pyResult.dispatchIntents() > 0);
    }

    @Test
    void diskStoreRecoversDurableQueuedAdmissionAcrossRestartForNodeAndPython(@TempDir Path temp) throws Exception {
        PolyglotHostProcess.requireNodeWithTypeStripping();
        PolyglotHostProcess.requirePython3();

        Path yaml = resourcePath("invoice-streaming-pipeline.yaml", temp);
        String nodeRecovered = runQueuedAdmissionRestartProof(
            PolyglotCompiler.Target.TYPESCRIPT, yaml, temp.resolve("node-queued"));
        String pyRecovered = runQueuedAdmissionRestartProof(
            PolyglotCompiler.Target.PYTHON, yaml, temp.resolve("py-queued"));

        assertEquals(nodeRecovered, pyRecovered,
            "Recovered QUEUED admission output must be byte-identical across Node and Python");
        JsonNode summary = PolyglotProofHarness.JSON.readTree(nodeRecovered);
        assertEquals("9007199254740994", summary.path("batchSeq").asText());
        assertEquals(2, summary.path("lineCount").asInt());
        assertEquals("123477.289012", summary.path("grandTotal").asText());
    }

    @Test
    void nodeAndPythonCustomConnectorsPreserveCaptureReplayReconciliationAndSecretIsolation(@TempDir Path temp)
        throws Exception {
        PolyglotHostProcess.requireNodeWithTypeStripping();
        PolyglotHostProcess.requirePython3();

        Path yaml = resourcePath("invoice-connector-await-pipeline.yaml", temp);
        ConnectorProofResult nodeRes = runConnectorAndAwaitProof(
            PolyglotCompiler.Target.TYPESCRIPT, yaml, temp.resolve("node-conn"));
        ConnectorProofResult pyRes = runConnectorAndAwaitProof(
            PolyglotCompiler.Target.PYTHON, yaml, temp.resolve("py-conn"));

        // Both languages reach identical final result after Query + Command + Restart + Await resume
        assertEquals(nodeRes.finalCanonicalJson(), pyRes.finalCanonicalJson(),
            "Node and Python connector + Await pipelines must produce byte-identical canonical JSON");
        JsonNode notice = PolyglotProofHarness.JSON.readTree(nodeRes.finalCanonicalJson());
        assertEquals("9007199254740994", notice.path("orderSeq").asText());
        assertEquals("LEDGER-9007199254740994", notice.path("entryId").asText());
        assertEquals("123456.789012", notice.path("settledAmount").asText());
        assertEquals("alice", notice.path("approver").asText());

        // Secret isolation: both fx.quote and ledger.post received their host secrets; pure business functions saw none
        assertTrue(nodeRes.fxQuerySawSecret(), "Node fx.quote connector must receive fx-api-key");
        assertTrue(pyRes.fxQuerySawSecret(), "Python fx.quote connector must receive fx-api-key");
        assertTrue(nodeRes.ledgerCommandSawSecret(), "Node ledger.post connector must receive ledger-api-key");
        assertTrue(pyRes.ledgerCommandSawSecret(), "Python ledger.post connector must receive ledger-api-key");
        assertFalse(nodeRes.functionSawSecret(), "Node pure function must never see connector secrets");
        assertFalse(pyRes.functionSawSecret(), "Python pure function must never see connector secrets");

        // Query capture replay, Command RETURN_RECORDED replay, and AMBIGUOUS -> /reconcile recovery
        assertEquals(nodeRes.reconciledReceiptJson(), pyRes.reconciledReceiptJson(),
            "AMBIGUOUS reconciliation result must match across Node and Python");
        assertEquals(2, nodeRes.fxQueryCalls(), "Node Query replay must execute 0 duplicate remote queries");
        assertEquals(2, pyRes.fxQueryCalls(), "Python Query replay must execute 0 duplicate remote queries");
        assertEquals(3, nodeRes.ledgerDispatchCalls(), "Node Command RETURN_RECORDED + AMBIGUOUS must not re-dispatch");
        assertEquals(3, pyRes.ledgerDispatchCalls(), "Python Command RETURN_RECORDED + AMBIGUOUS must not re-dispatch");
        assertEquals(1, nodeRes.ledgerReconcileCalls(), "Node AMBIGUOUS recovery must call /reconcile exactly once");
        assertEquals(1, pyRes.ledgerReconcileCalls(), "Python AMBIGUOUS recovery must call /reconcile exactly once");

        // Crash/restart across WAITING_EXTERNAL Await did not re-run Step 0 (Prepare Transfer)
        assertEquals(1, nodeRes.prepareCalls());
        assertEquals(1, pyRes.prepareCalls());
        assertEquals(1, nodeRes.finalizeCalls());
        assertEquals(1, pyRes.finalizeCalls());
    }

    static StreamingProofResult runStreamingProof(PolyglotCompiler.Target target, Path yaml, Path workDir)
        throws Exception {
        Path genDir = workDir.resolve("gen");
        Path dataDir = workDir.resolve("state");
        PolyglotCompiler.compile(target, yaml, genDir);

        try (PolyglotHostProcess host = startStreamingHost(target, genDir)) {
            try (PolyglotProofHarness harness = new PolyglotProofHarness(
                yaml, host.baseUri(), host.baseUri(), SHARED_SECRET, dataDir, 2, Map.of(), null)) {
                ExecutionRecord<Object, Object> ok = harness.submitAndDrain("t1", "ok-1", STREAMING_PAYLOAD_OK);
                assertEquals(ExecutionStatus.SUCCEEDED, ok.status(), "OK run failed: " + ok.errorMessage());
                String okJson = harness.getResultCanonicalString("t1", ok.executionId());

                ExecutionRecord<Object, Object> rejected = harness.submitAndDrain("t1", "rej-1", STREAMING_PAYLOAD_REJECT);

                ExecutionRecord<Object, Object> retryBefore = harness.submitAndDrain("t1", "ret-1", STREAMING_PAYLOAD_TRANSIENT);
                Thread.sleep(25);
                harness.sweepAndDrain(System.currentTimeMillis() + 60_000L);
                ExecutionRecord<Object, Object> retryAfter = harness.getExecution("t1", retryBefore.executionId());
                assertEquals(ExecutionStatus.SUCCEEDED, retryAfter.status(),
                    "Retry failed for " + target + ": " + retryAfter.errorMessage() + "\nHost log:\n" + host.capturedOutput());
                String retryJson = harness.getResultCanonicalString("t1", retryAfter.executionId());

                return new StreamingProofResult(
                    okJson, rejected, retryBefore, retryAfter, retryJson,
                    harness.deadLetterCount(), harness.dispatchIntentCount());
            }
        }
    }

    private static String runQueuedAdmissionRestartProof(
        PolyglotCompiler.Target target, Path yaml, Path workDir) throws Exception {
        Path genDir = workDir.resolve("gen");
        Path dataDir = workDir.resolve("state");
        Path metricsFile = workDir.resolve("streaming-metrics.json");
        PolyglotCompiler.compile(target, yaml, genDir);

        try (PolyglotHostProcess host = startStreamingHost(target, genDir, metricsFile)) {
            String executionId;
            // Instance #1: durably admits QUEUED execution and crashes before draining/dispatching workQueue
            try (PolyglotProofHarness dev1 = new PolyglotProofHarness(
                yaml, host.baseUri(), host.baseUri(), SHARED_SECRET, dataDir, 2, Map.of(), null)) {
                ExecutionRecord<Object, Object> admitted = dev1.admitWithoutDrain("t1", "queued-1", STREAMING_PAYLOAD_OK);
                assertEquals(ExecutionStatus.QUEUED, admitted.status(), "Admitted execution must remain QUEUED");
                assertEquals(0, dev1.dispatchIntentCount(), "No dispatch intent may be recorded before worker claim");
                JsonNode beforeRestartMetrics = PolyglotProofHarness.JSON.readTree(Files.readString(metricsFile));
                assertEquals(0, beforeRestartMetrics.path("validateCalls").asInt(),
                    "Foreign function host must not be invoked before worker dispatch");
                executionId = admitted.executionId();
            }

            // Instance #2: fresh coordinator reloads QUEUED state from disk and completes via ordinary sweep
            try (PolyglotProofHarness dev2 = new PolyglotProofHarness(
                yaml, host.baseUri(), host.baseUri(), SHARED_SECRET, dataDir, 2, Map.of(), null)) {
                ExecutionRecord<Object, Object> reloaded = dev2.getExecution("t1", executionId);
                assertEquals(ExecutionStatus.QUEUED, reloaded.status(), "Restarted store must preserve QUEUED status");

                dev2.sweepAndDrain(System.currentTimeMillis() + 60_000L);
                ExecutionRecord<Object, Object> finished = dev2.getExecution("t1", executionId);
                assertEquals(ExecutionStatus.SUCCEEDED, finished.status(),
                    "Ordinary sweep recovery failed for " + target + ": " + finished.errorMessage()
                        + "\nHost log:\n" + host.capturedOutput());
                JsonNode afterMetrics = PolyglotProofHarness.JSON.readTree(Files.readString(metricsFile));
                assertEquals(1, afterMetrics.path("validateCalls").asInt(),
                    "Recovered QUEUED execution must invoke Step 0 exactly once");
                assertTrue(dev2.dispatchIntentCount() > 0, "Recovered execution must record dispatch intents");
                return dev2.getResultCanonicalString("t1", executionId);
            }
        }
    }

    private static ConnectorProofResult runConnectorAndAwaitProof(
        PolyglotCompiler.Target target, Path yaml, Path workDir) throws Exception {
        Path genDir = workDir.resolve("gen");
        Path dataDir = workDir.resolve("state");
        Path metricsFile = workDir.resolve("metrics.json");
        PolyglotCompiler.compile(target, yaml, genDir);

        Map<String, String> connectorSecrets = Map.of("fx-api-key", "secret-fx-42", "ledger-api-key", "secret-ledger-99");
        try (PolyglotHostProcess host = startConnectorHost(target, genDir, metricsFile)) {
            String executionId;
            String awaitUnitId;
            String reconciledReceiptJson;
            // Instance #1: runs Step 0 (fn) -> Step 1 (Query) -> Step 2 (Command) -> Step 3 (Await WAITING_EXTERNAL)
            // plus explicit Query capture replay, Command RETURN_RECORDED replay, and AMBIGUOUS -> /reconcile proofs.
            try (PolyglotProofHarness dev1 = new PolyglotProofHarness(
                yaml, host.baseUri(), host.baseUri(), SHARED_SECRET, dataDir, 2, connectorSecrets, null)) {
                ExecutionRecord<Object, Object> waiting = dev1.submitAndDrain("t1", "conn-await-1", """
                    {"orderSeq":9007199254740993,"currency":"USD","amount":123456.789012}""");
                assertEquals(ExecutionStatus.WAITING_EXTERNAL, waiting.status(),
                    "Expected WAITING_EXTERNAL for " + target + ", got " + waiting.status() + ": " + waiting.errorMessage()
                        + "\nHost log:\n" + host.capturedOutput());
                executionId = waiting.executionId();
                awaitUnitId = waiting.awaitUnitId();
                assertNotNull(awaitUnitId);

                QueryStepSupport querySupport = dev1.querySupport().orElseThrow();
                CommandStepSupport commandSupport = dev1.commandSupport().orElseThrow();
                QueryStepDescriptor fxDesc = dev1.queryDescriptor("lookup-fx-quote");
                CommandDescriptor ledgerDesc = dev1.commandDescriptor("post-ledger-entry");

                // (a) Query capture replay: two executions with identical executionId/input invoke remote host only once
                JsonNode fxReq = PolyglotProofHarness.JSON.readTree(
                    """
                    {"orderSeq":9007199254740994,"currency":"USD","amount":123456.789012}""");
                JsonNode q1 = dev1.withExecutionContext("t1", "replay-query-exec", 1,
                    () -> querySupport.queryOneToOne(fxDesc, fxReq, JsonNode.class).await().atMost(Duration.ofSeconds(10)));
                JsonNode q2 = dev1.withExecutionContext("t1", "replay-query-exec", 1,
                    () -> querySupport.queryOneToOne(fxDesc, fxReq, JsonNode.class).await().atMost(Duration.ofSeconds(10)));
                assertEquals(q1, q2, "Captured Query replay must return identical FxQuote");
                JsonNode afterQueryMetrics = PolyglotProofHarness.JSON.readTree(Files.readString(metricsFile));
                assertEquals(2, afterQueryMetrics.path("fxQueryCalls").asInt(),
                    "Query capture replay must avoid duplicate remote query calls");

                // (b) Command RETURN_RECORDED replay: same commandId + input returns recorded receipt without re-dispatch
                JsonNode cmd1 = dev1.withExecutionContext("t1", "replay-cmd-exec-1", 2,
                    () -> commandSupport.<JsonNode, JsonNode>execute(ledgerDesc, (d, i) -> "cmd-replay-fixed", q1)
                        .await().atMost(Duration.ofSeconds(10)));
                JsonNode cmd2 = dev1.withExecutionContext("t1", "replay-cmd-exec-2", 2,
                    () -> commandSupport.<JsonNode, JsonNode>execute(ledgerDesc, (d, i) -> "cmd-replay-fixed", q1)
                        .await().atMost(Duration.ofSeconds(10)));
                assertEquals(cmd1, cmd2, "Command RETURN_RECORDED replay must return identical LedgerReceipt");
                JsonNode afterCmdReplayMetrics = PolyglotProofHarness.JSON.readTree(Files.readString(metricsFile));
                assertEquals(2, afterCmdReplayMetrics.path("ledgerDispatchCalls").asInt(),
                    "Command RETURN_RECORDED replay must avoid duplicate remote dispatches");

                // (c) Mid-dispatch AMBIGUOUS recovery via /reconcile -> CONFIRMED_SUCCEEDED (zero re-dispatch)
                JsonNode ambigQuote = PolyglotProofHarness.JSON.readTree(
                    """
                    {"orderSeq":9007199254740995,"convertedAmount":99.5,"rateSource":"fx-live"}""");
                assertThrows(RuntimeException.class, () -> dev1.withExecutionContext("t1", "ambig-exec-1", 2,
                    () -> commandSupport.<JsonNode, JsonNode>execute(ledgerDesc, (d, i) -> "cmd-ambiguous-1", ambigQuote)
                        .await().atMost(Duration.ofSeconds(10))),
                    "First AMBIGUOUS dispatch must not silently succeed");
                JsonNode afterAmbigMetrics = PolyglotProofHarness.JSON.readTree(Files.readString(metricsFile));
                assertEquals(3, afterAmbigMetrics.path("ledgerDispatchCalls").asInt());
                assertEquals(0, afterAmbigMetrics.path("ledgerReconcileCalls").asInt());

                JsonNode reconciled = dev1.withExecutionContext("t1", "ambig-exec-1", 2,
                    () -> commandSupport.<JsonNode, JsonNode>execute(ledgerDesc, (d, i) -> "cmd-ambiguous-1", ambigQuote)
                        .await().atMost(Duration.ofSeconds(10)));
                assertEquals("LEDGER-9007199254740995", reconciled.path("entryId").asText());
                assertEquals("99.5", reconciled.path("postedAmount").asText());
                reconciledReceiptJson = PolyglotProofHarness.JSON.writeValueAsString(reconciled);
                JsonNode afterReconcileMetrics = PolyglotProofHarness.JSON.readTree(Files.readString(metricsFile));
                assertEquals(3, afterReconcileMetrics.path("ledgerDispatchCalls").asInt(),
                    "AMBIGUOUS recovery must use /reconcile and never re-dispatch");
                assertEquals(1, afterReconcileMetrics.path("ledgerReconcileCalls").asInt(),
                    "AMBIGUOUS recovery must invoke /reconcile exactly once");
            }

            // Instance #2 (fresh process state from disk): reloads WAITING_EXTERNAL, verifies persisted requestPayload,
            // constructs completion from decoded Await requestPayload, and completes Step 4 only!
            try (PolyglotProofHarness dev2 = new PolyglotProofHarness(
                yaml, host.baseUri(), host.baseUri(), SHARED_SECRET, dataDir, 2, connectorSecrets, null)) {
                ExecutionRecord<Object, Object> reloaded = dev2.getExecution("t1", executionId);
                assertEquals(ExecutionStatus.WAITING_EXTERNAL, reloaded.status());
                AwaitInteractionRecord interaction = dev2.awaitInteraction("t1", awaitUnitId);

                JsonNode awaitRequest = dev2.decodeTypedPayloadJson(interaction.requestPayload());
                assertEquals("9007199254740994", awaitRequest.path("orderSeq").asText(),
                    "Persisted Await requestPayload must carry Step 2 orderSeq");
                assertEquals("LEDGER-9007199254740994", awaitRequest.path("entryId").asText(),
                    "Persisted Await requestPayload must carry Step 2 entryId");
                assertEquals("123456.789012", awaitRequest.path("postedAmount").asText(),
                    "Persisted Await requestPayload must carry Step 2 postedAmount");

                ObjectNode completionPayload = ((ObjectNode) awaitRequest.deepCopy()).put("approver", "alice");
                ExecutionRecord<Object, Object> finished = dev2.completeAwaitAndDrain(
                    "t1", interaction, PolyglotProofHarness.JSON.writeValueAsString(completionPayload));
                assertEquals(ExecutionStatus.SUCCEEDED, finished.status(),
                    "Resume failed: " + finished.errorMessage());
                String finalJson = dev2.getResultCanonicalString("t1", executionId);
                JsonNode metrics = PolyglotProofHarness.JSON.readTree(Files.readString(metricsFile));
                return new ConnectorProofResult(
                    finalJson,
                    reconciledReceiptJson,
                    metrics.path("prepareCalls").asInt(),
                    metrics.path("finalizeCalls").asInt(),
                    metrics.path("fxQueryCalls").asInt(),
                    metrics.path("ledgerDispatchCalls").asInt(),
                    metrics.path("ledgerReconcileCalls").asInt(),
                    metrics.path("fxQuerySawSecret").asBoolean(),
                    metrics.path("ledgerCommandSawSecret").asBoolean(),
                    metrics.path("functionSawSecret").asBoolean());
            }
        }
    }

    static String errorDetail(ExecutionRecord<Object, Object> record) {
        String code = record.errorCode() == null ? "" : record.errorCode();
        String msg = record.errorMessage() == null ? "" : record.errorMessage();
        return code + " " + msg;
    }

    static PolyglotHostProcess startStreamingHost(PolyglotCompiler.Target target, Path genDir) throws Exception {
        return startStreamingHost(target, genDir, null);
    }

    static PolyglotHostProcess startStreamingHost(PolyglotCompiler.Target target, Path genDir, Path metricsFile)
        throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put("TPF_SECRET", SHARED_SECRET);
        if (metricsFile != null) env.put("METRICS_FILE", metricsFile.toString());
        if (target == PolyglotCompiler.Target.TYPESCRIPT) {
            Path app = genDir.resolve("app.ts");
            Files.writeString(app, """
                import fs from 'node:fs';
                import { startAppHost } from './host.ts';
                import { TpfBusinessRejectionError, TpfTransientError } from './types.ts';
                import type { FunctionHandlers, PricedLine } from './types.ts';

                const metricsPath = process.env.METRICS_FILE;
                const metrics = { validateCalls: 0 };
                const saveMetrics = () => { if (metricsPath) fs.writeFileSync(metricsPath, JSON.stringify(metrics)); };
                saveMetrics();

                // Exact base-10 decimal addition & integer multiplication without float64 loss.
                function parseDec(raw: string): { unscaled: bigint; scale: number } {
                  const s = raw.trim();
                  const neg = s.startsWith('-');
                  const body = neg ? s.slice(1) : s;
                  const dot = body.indexOf('.');
                  const digits = dot === -1 ? body : body.slice(0, dot) + body.slice(dot + 1);
                  const scale = dot === -1 ? 0 : body.length - dot - 1;
                  const unscaled = BigInt(digits) * (neg ? -1n : 1n);
                  return { unscaled, scale };
                }
                function formatDec(unscaled: bigint, scale: number): string {
                  const neg = unscaled < 0n;
                  let digits = (neg ? -unscaled : unscaled).toString();
                  if (scale <= 0) return (neg ? '-' : '') + digits;
                  if (digits.length <= scale) digits = '0'.repeat(scale - digits.length + 1) + digits;
                  const intPart = digits.slice(0, digits.length - scale);
                  let fracPart = digits.slice(digits.length - scale).replace(/0+$/, '');
                  return (neg ? '-' : '') + (fracPart.length > 0 ? `${intPart}.${fracPart}` : intPart);
                }
                function mulDecInt(raw: string, factor: number): string {
                  const d = parseDec(raw);
                  return formatDec(d.unscaled * BigInt(factor), d.scale);
                }
                function addDec(aRaw: string, bRaw: string): string {
                  const a = parseDec(aRaw);
                  const b = parseDec(bRaw);
                  const scale = Math.max(a.scale, b.scale);
                  const au = a.unscaled * (10n ** BigInt(scale - a.scale));
                  const bu = b.unscaled * (10n ** BigInt(scale - b.scale));
                  return formatDec(au + bu, scale);
                }

                let transientOnce = 0;
                const handlers: FunctionHandlers = {
                  async validateInvoice(input) {
                    metrics.validateCalls += 1;
                    saveMetrics();
                    if (input.mode === 'REJECT') {
                      throw new TpfBusinessRejectionError('INVALID_INVOICE', 'rejected by rule');
                    }
                    if (input.mode === 'TRANSIENT_ONCE' && transientOnce === 0) {
                      transientOnce += 1;
                      throw new TpfTransientError('BUSY', 'transient failure');
                    }
                    return {
                      batchSeq: input.batchSeq + 1n,
                      currency: input.currency,
                      expedited: input.expedited,
                      items: input.items,
                    };
                  },
                  async *splitLineItems(input) {
                    for (const item of input.items) {
                      yield {
                        batchSeq: input.batchSeq,
                        sku: item.sku,
                        lineTotal: mulDecInt(item.unitPrice, item.qty),
                      };
                    }
                  },
                  async aggregateSettlement(items: AsyncIterable<PricedLine>) {
                    let batchSeq = 0n;
                    let lineCount = 0;
                    let grandTotal = '0';
                    for await (const line of items) {
                      batchSeq = line.batchSeq;
                      lineCount += 1;
                      grandTotal = addDec(grandTotal, line.lineTotal);
                    }
                    return { batchSeq, lineCount, grandTotal };
                  },
                };

                const running = await startAppHost(handlers, { sharedSecret: process.env.TPF_SECRET! });
                console.log('READY ' + running.baseUrl);
                process.on('SIGTERM', async () => { await running.close(); process.exit(0); });
                """);
            return PolyglotHostProcess.start(
                List.of("node", app.toString()), genDir, env, Duration.ofSeconds(10));
        } else {
            Path appDir = genDir.getParent();
            String pkg = genDir.getFileName().toString();
            Path app = appDir.resolve("app.py");
            Files.writeString(app, """
                import json, os, signal, sys
                from decimal import Decimal
                from %s.host import start_app_host
                from %s import types as t

                metrics_path = os.environ.get("METRICS_FILE")
                metrics = {"validateCalls": 0}
                def save_metrics():
                    if metrics_path:
                        with open(metrics_path, "w") as f:
                            json.dump(metrics, f)
                save_metrics()

                def _norm_dec(d: Decimal) -> Decimal:
                    norm = d.normalize()
                    if norm == norm.to_integral():
                        return norm.quantize(Decimal(1))
                    return norm

                class Handlers:
                    def __init__(self):
                        self.transient_once = 0

                    def validate_invoice(self, inp, ctx):
                        metrics["validateCalls"] += 1
                        save_metrics()
                        if inp.mode == "REJECT":
                            raise t.TpfBusinessRejectionError("INVALID_INVOICE", "rejected by rule")
                        if inp.mode == "TRANSIENT_ONCE" and self.transient_once == 0:
                            self.transient_once += 1
                            raise t.TpfTransientError("BUSY", "transient failure")
                        return t.ValidatedBatch(
                            batchSeq=inp.batchSeq + 1,
                            currency=inp.currency,
                            expedited=inp.expedited,
                            items=inp.items,
                        )

                    def split_line_items(self, inp, ctx):
                        for item in inp.items:
                            yield t.PricedLine(
                                batchSeq=inp.batchSeq,
                                sku=item.sku,
                                lineTotal=_norm_dec(item.unitPrice * item.qty),
                            )

                    def aggregate_settlement(self, lines, ctx):
                        batch_seq = 0
                        line_count = 0
                        grand_total = Decimal("0")
                        for line in lines:
                            batch_seq = line.batchSeq
                            line_count += 1
                            grand_total += line.lineTotal
                        return t.SettlementSummary(
                            batchSeq=batch_seq,
                            lineCount=line_count,
                            grandTotal=_norm_dec(grand_total),
                        )

                running = start_app_host(Handlers(), shared_secret=os.environ["TPF_SECRET"])
                print("READY " + running.base_url, flush=True)
                signal.signal(signal.SIGTERM, lambda *_: (running.close(), sys.exit(0)))
                signal.pause()
                """.formatted(pkg, pkg));
            return PolyglotHostProcess.start(
                List.of("python3", app.toString()), appDir, env, Duration.ofSeconds(10));
        }
    }

    private static PolyglotHostProcess startConnectorHost(
        PolyglotCompiler.Target target, Path genDir, Path metricsFile) throws Exception {
        if (target == PolyglotCompiler.Target.TYPESCRIPT) {
            Path app = genDir.resolve("app.ts");
            Files.writeString(app, """
                import fs from 'node:fs';
                import { startAppHost } from './host.ts';
                import type { FunctionHandlers, ConnectorHandlers } from './types.ts';

                const metricsPath = process.env.METRICS_FILE!;
                const metrics = {
                  prepareCalls: 0,
                  finalizeCalls: 0,
                  fxQueryCalls: 0,
                  ledgerDispatchCalls: 0,
                  ledgerReconcileCalls: 0,
                  fxQuerySawSecret: false,
                  ledgerCommandSawSecret: false,
                  functionSawSecret: false,
                };
                const save = () => fs.writeFileSync(metricsPath, JSON.stringify(metrics));
                save();

                function checkNoFunctionSecret(ctx: unknown) {
                  if (process.env['fx-api-key'] || process.env['ledger-api-key']) {
                    metrics.functionSawSecret = true;
                  }
                  if (ctx && typeof ctx === 'object') {
                    const serialized = JSON.stringify(ctx);
                    if ('secrets' in ctx || serialized.includes('secret-fx-42') || serialized.includes('secret-ledger-99')) {
                      metrics.functionSawSecret = true;
                    }
                  }
                }

                const handlers: FunctionHandlers = {
                  async prepareTransfer(input, ctx) {
                    metrics.prepareCalls += 1;
                    checkNoFunctionSecret(ctx);
                    save();
                    return { orderSeq: input.orderSeq + 1n, currency: input.currency, amount: input.amount };
                  },
                  async awaitSignoff() { throw new Error('Await owned by TPF'); },
                  async finalizeNotice(input, ctx) {
                    metrics.finalizeCalls += 1;
                    checkNoFunctionSecret(ctx);
                    save();
                    return {
                      orderSeq: input.orderSeq,
                      entryId: input.entryId,
                      settledAmount: input.postedAmount,
                      approver: input.approver,
                    };
                  },
                };

                const posted = new Map<string, { orderSeq: bigint; entryId: string; postedAmount: string }>();
                let ambiguousTriggered = false;
                const connectors: ConnectorHandlers = {
                  queries: {
                    async fxQuote(input, ctx) {
                      metrics.fxQueryCalls += 1;
                      if (ctx.secrets['fx-api-key'] === 'secret-fx-42') metrics.fxQuerySawSecret = true;
                      save();
                      return {
                        outcome: 'FOUND',
                        payload: { orderSeq: input.orderSeq, convertedAmount: input.amount, rateSource: 'fx-live' },
                      };
                    },
                  },
                  commands: {
                    ledgerPost: {
                      async dispatch(input, ctx) {
                        metrics.ledgerDispatchCalls += 1;
                        if (ctx.secrets['ledger-api-key'] === 'secret-ledger-99') metrics.ledgerCommandSawSecret = true;
                        const rec = {
                          orderSeq: input.orderSeq,
                          entryId: 'LEDGER-' + input.orderSeq.toString(),
                          postedAmount: input.convertedAmount,
                        };
                        posted.set(ctx.dispatchIdentity.providerIdempotencyKey, rec);
                        if (input.orderSeq === 9007199254740995n && !ambiguousTriggered) {
                          ambiguousTriggered = true;
                          save();
                          return { outcome: 'AMBIGUOUS', message: 'simulated post-commit timeout' };
                        }
                        save();
                        return {
                          outcome: 'SUCCEEDED',
                          payload: rec,
                          receiptReference: rec.entryId,
                        };
                      },
                      async reconcile(ctx) {
                        metrics.ledgerReconcileCalls += 1;
                        save();
                        const existing = posted.get(ctx.dispatchIdentity.providerIdempotencyKey);
                        if (!existing) return { outcome: 'NOT_FOUND' };
                        return {
                          outcome: 'CONFIRMED_SUCCEEDED',
                          payload: existing,
                          receiptReference: existing.entryId,
                        };
                      },
                    },
                  },
                };

                const running = await startAppHost(handlers, connectors, { sharedSecret: process.env.TPF_SECRET! });
                console.log('READY ' + running.baseUrl);
                process.on('SIGTERM', async () => { await running.close(); process.exit(0); });
                """);
            return PolyglotHostProcess.start(
                List.of("node", app.toString()), genDir,
                Map.of("TPF_SECRET", SHARED_SECRET, "METRICS_FILE", metricsFile.toString()),
                Duration.ofSeconds(10));
        } else {
            Path appDir = genDir.getParent();
            String pkg = genDir.getFileName().toString();
            Path app = appDir.resolve("app.py");
            Files.writeString(app, """
                import json, os, signal, sys
                from %s.host import start_app_host
                from %s import types as t

                metrics_path = os.environ["METRICS_FILE"]
                metrics = {
                    "prepareCalls": 0,
                    "finalizeCalls": 0,
                    "fxQueryCalls": 0,
                    "ledgerDispatchCalls": 0,
                    "ledgerReconcileCalls": 0,
                    "fxQuerySawSecret": False,
                    "ledgerCommandSawSecret": False,
                    "functionSawSecret": False,
                }
                def save():
                    with open(metrics_path, "w") as f:
                        json.dump(metrics, f)
                save()

                def check_no_function_secret(ctx):
                    if "fx-api-key" in os.environ or "ledger-api-key" in os.environ:
                        metrics["functionSawSecret"] = True
                    ctx_repr = repr(ctx)
                    if hasattr(ctx, "secrets") or "secret-fx-42" in ctx_repr or "secret-ledger-99" in ctx_repr:
                        metrics["functionSawSecret"] = True

                class Handlers:
                    def prepare_transfer(self, inp, ctx):
                        metrics["prepareCalls"] += 1
                        check_no_function_secret(ctx)
                        save()
                        return t.FxRequest(orderSeq=inp.orderSeq + 1, currency=inp.currency, amount=inp.amount)

                    def await_signoff(self, inp, ctx):
                        raise RuntimeError("Await owned by TPF")

                    def finalize_notice(self, inp, ctx):
                        metrics["finalizeCalls"] += 1
                        check_no_function_secret(ctx)
                        save()
                        return t.FinalNotice(
                            orderSeq=inp.orderSeq,
                            entryId=inp.entryId,
                            settledAmount=inp.postedAmount,
                            approver=inp.approver,
                        )

                class Connectors:
                    def __init__(self):
                        self.posted = {}
                        self.ambiguous_triggered = False

                    def query_fx_quote(self, inp, ctx):
                        metrics["fxQueryCalls"] += 1
                        if ctx.secrets.get("fx-api-key") == "secret-fx-42":
                            metrics["fxQuerySawSecret"] = True
                        save()
                        return {
                            "outcome": "FOUND",
                            "payload": t.FxQuote(orderSeq=inp.orderSeq, convertedAmount=inp.amount, rateSource="fx-live"),
                        }

                    def dispatch_ledger_post(self, inp, ctx):
                        metrics["ledgerDispatchCalls"] += 1
                        if ctx.secrets.get("ledger-api-key") == "secret-ledger-99":
                            metrics["ledgerCommandSawSecret"] = True
                        rec = t.LedgerReceipt(
                            orderSeq=inp.orderSeq,
                            entryId=f"LEDGER-{inp.orderSeq}",
                            postedAmount=inp.convertedAmount,
                        )
                        self.posted[ctx.dispatch_identity.provider_idempotency_key] = rec
                        if inp.orderSeq == 9007199254740995 and not self.ambiguous_triggered:
                            self.ambiguous_triggered = True
                            save()
                            return {"outcome": "AMBIGUOUS", "message": "simulated post-commit timeout"}
                        save()
                        return {
                            "outcome": "SUCCEEDED",
                            "payload": rec,
                            "receiptReference": rec.entryId,
                        }

                    def reconcile_ledger_post(self, ctx):
                        metrics["ledgerReconcileCalls"] += 1
                        save()
                        rec = self.posted.get(ctx.dispatch_identity.provider_idempotency_key)
                        if rec is None:
                            return {"outcome": "NOT_FOUND"}
                        return {
                            "outcome": "CONFIRMED_SUCCEEDED",
                            "payload": rec,
                            "receiptReference": rec.entryId,
                        }

                running = start_app_host(Handlers(), Connectors(), shared_secret=os.environ["TPF_SECRET"])
                print("READY " + running.base_url, flush=True)
                signal.signal(signal.SIGTERM, lambda *_: (running.close(), sys.exit(0)))
                signal.pause()
                """.formatted(pkg, pkg));
            return PolyglotHostProcess.start(
                List.of("python3", app.toString()), appDir,
                Map.of("TPF_SECRET", SHARED_SECRET, "METRICS_FILE", metricsFile.toString()),
                Duration.ofSeconds(10));
        }
    }

    static Path resourcePath(String name, Path targetDir) throws Exception {
        try (var stream = PolyglotCrossLanguageProofTest.class.getClassLoader().getResourceAsStream(name)) {
            assertNotNull(stream, "Missing test resource: " + name);
            Path out = targetDir.resolve(name);
            Files.createDirectories(out.getParent());
            Files.copy(stream, out, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            return out;
        }
    }

    record StreamingProofResult(
        String okCanonicalJson,
        ExecutionRecord<Object, Object> rejected,
        ExecutionRecord<Object, Object> retryBeforeSweep,
        ExecutionRecord<Object, Object> retryAfterSweep,
        String retryCanonicalJson,
        int deadLetters,
        int dispatchIntents
    ) { }

    record ConnectorProofResult(
        String finalCanonicalJson,
        String reconciledReceiptJson,
        int prepareCalls,
        int finalizeCalls,
        int fxQueryCalls,
        int ledgerDispatchCalls,
        int ledgerReconcileCalls,
        boolean fxQuerySawSecret,
        boolean ledgerCommandSawSecret,
        boolean functionSawSecret
    ) { }
}
