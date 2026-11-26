package org.pipelineframework.proof;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.PolyglotProofHarness;
import org.pipelineframework.compiler.polyglot.PolyglotCompiler;
import org.pipelineframework.orchestrator.DynamoProofStoreHelper;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStatus;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies exact behavioral equivalence between the local crash-safe file store (modeling {@code tpf dev})
 * and TPF's production {@code DynamoExecutionStateStore} (on LocalStack DynamoDB) across both
 * Node (TypeScript) and Python foreign function hosts.
 */
class PolyglotDynamoEquivalenceIT {

    @Test
    void localDiskStoreAndProductionDynamoDbStoreAreEquivalentAcrossNodeAndPython(@TempDir Path temp)
        throws Exception {
        PolyglotHostProcess.requireNodeWithTypeStripping();
        PolyglotHostProcess.requirePython3();

        Path yaml = PolyglotCrossLanguageProofTest.resourcePath("invoice-streaming-pipeline.yaml", temp);

        try (DynamoProofStoreHelper dynamoHelper = new DynamoProofStoreHelper()) {
            for (PolyglotCompiler.Target target : List.of(
                PolyglotCompiler.Target.TYPESCRIPT, PolyglotCompiler.Target.PYTHON)) {
                String label = target.id();
                Path workDir = temp.resolve(label);
                Path genDir = workDir.resolve("gen");
                PolyglotCompiler.compile(target, yaml, genDir);

                // 1. Run against local crash-safe disk store (tpf dev model)
                StoreSnapshot diskSnap = runWithStore(
                    target, yaml, genDir, workDir.resolve("disk-state"), null, "disk-" + label);

                // 2. Run against production DynamoExecutionStateStore on LocalStack DynamoDB
                StoreSnapshot dynamoSnap = runWithStore(
                    target, yaml, genDir, workDir.resolve("dynamo-state"),
                    dynamoHelper::createExecutionStateStore, "dynamo-" + label);

                // 3. Assert 100% behavioral & payload equivalence
                assertEquals(diskSnap.okStatus(), dynamoSnap.okStatus(), label + " OK status");
                assertEquals(diskSnap.okVersion(), dynamoSnap.okVersion(), label + " OK version");
                assertEquals(diskSnap.okCanonicalJson(), dynamoSnap.okCanonicalJson(),
                    label + " canonical JSON (29-digit decimal + int64) must match between disk and DynamoDB");

                assertEquals(diskSnap.rejectedStatus(), dynamoSnap.rejectedStatus(), label + " rejection status");
                assertEquals(diskSnap.rejectedStepIndex(), dynamoSnap.rejectedStepIndex(), label + " failedStepIndex");
                assertEquals(diskSnap.rejectedCommandId(), dynamoSnap.rejectedCommandId(), label + " failedCommandId");

                assertEquals(diskSnap.retryBeforeStatus(), dynamoSnap.retryBeforeStatus(), label + " WAIT_RETRY");
                assertEquals(diskSnap.retryAfterStatus(), dynamoSnap.retryAfterStatus(), label + " retry SUCCEEDED");
                assertEquals(diskSnap.retryCanonicalJson(), dynamoSnap.retryCanonicalJson(), label + " retry JSON");
            }
        }
    }

    private static StoreSnapshot runWithStore(
        PolyglotCompiler.Target target,
        Path yaml,
        Path genDir,
        Path dataDir,
        java.util.function.Function<org.pipelineframework.orchestrator.PipelineOrchestratorConfig,
            org.pipelineframework.orchestrator.ExecutionStateStore> storeOverride,
        String tenant
    ) throws Exception {
        try (PolyglotHostProcess host = PolyglotCrossLanguageProofTest.startStreamingHost(target, genDir);
             PolyglotProofHarness harness = new PolyglotProofHarness(
                 yaml, host.baseUri(), host.baseUri(), PolyglotCrossLanguageProofTest.SHARED_SECRET,
                 dataDir, 2, Map.of(), storeOverride)) {

            ExecutionRecord<Object, Object> ok = harness.submitAndDrain(
                tenant, "ok-key", PolyglotCrossLanguageProofTest.STREAMING_PAYLOAD_OK);
            assertEquals(ExecutionStatus.SUCCEEDED, ok.status(), ok.errorMessage());
            String okJson = harness.getResultCanonicalString(tenant, ok.executionId());

            ExecutionRecord<Object, Object> rejected = harness.submitAndDrain(
                tenant, "rej-key", PolyglotCrossLanguageProofTest.STREAMING_PAYLOAD_REJECT);

            ExecutionRecord<Object, Object> retryBefore = harness.submitAndDrain(
                tenant, "ret-key", PolyglotCrossLanguageProofTest.STREAMING_PAYLOAD_TRANSIENT);
            Thread.sleep(25);
            harness.sweepAndDrain(System.currentTimeMillis() + 60_000L);
            ExecutionRecord<Object, Object> retryAfter = harness.getExecution(tenant, retryBefore.executionId());
            assertEquals(ExecutionStatus.SUCCEEDED, retryAfter.status(),
                "Retry failed: " + retryAfter.errorMessage() + "\nHost log:\n" + host.capturedOutput());
            String retryJson = harness.getResultCanonicalString(tenant, retryAfter.executionId());

            return new StoreSnapshot(
                ok.status(), ok.version(), okJson,
                rejected.status(), rejected.failedStepIndex(), rejected.failedCommandId().orElse(""),
                retryBefore.status(), retryAfter.status(), retryJson);
        }
    }

    private record StoreSnapshot(
        ExecutionStatus okStatus,
        long okVersion,
        String okCanonicalJson,
        ExecutionStatus rejectedStatus,
        int rejectedStepIndex,
        String rejectedCommandId,
        ExecutionStatus retryBeforeStatus,
        ExecutionStatus retryAfterStatus,
        String retryCanonicalJson
    ) { }
}
