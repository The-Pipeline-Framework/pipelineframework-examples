package org.pipelineframework.proof;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.PolyglotProofHarness;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.compiler.polyglot.PolyglotCompiler;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TypedDurablePayload;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tier-3 mixed-topology polyglot proof (ADR-0092 §4, issue #1124):
 * combines a JVM ecosystem Connector ({@code http.client} {@code HttpConnector}),
 * a live Node (TypeScript) foreign function host, and a live Python foreign function host
 * inside ONE pipeline — proving:
 * <ul>
 *   <li>JVM ecosystem Connector {@code Query} and {@code Command} with host-owned bearer tokens
 *       and {@code Idempotency-Key} headers never leaked to Node or Python hosts</li>
 *   <li>Polyglot {@code SIDE_EFFECT} step executing in Node while preserving the original
 *       canonical payload unchanged downstream</li>
 *   <li>Canonical v3 {@code union} branching ({@code express} vs {@code standard}) executed
 *       deterministically across Node and Python</li>
 *   <li>Cross-pipeline {@link CheckpointPublicationService} handoff from upstream ingress pipeline
 *       to an independently owned downstream settlement pipeline with exact {@code int64 > 2^53}
 *       and {@code decimal} fidelity across the JVM {@code HttpConnector} boundary</li>
 * </ul>
 */
class Tier3MixedTopologyProofTest {

    private static final String SHARED_SECRET = "tier3-mixed-topology-hmac-secret";
    private static final String RISK_API_TOKEN = "jvm-only-risk-bearer-secret-111";
    private static final String SETTLEMENT_API_TOKEN = "jvm-only-settlement-bearer-secret-222";
    private static final String EXACT_INT64 = "9007199254740993";
    private static final String EXACT_DECIMAL = "123456.789012";

    @Test
    void combinesJvmHttpConnectorNodeSideEffectPythonUnionBranchingAndCrossPipelineCheckpointHandoff(
        @TempDir Path temp
    ) throws Exception {
        PolyglotHostProcess.requireNodeWithTypeStripping();
        PolyglotHostProcess.requirePython3();

        AtomicInteger riskCalls = new AtomicInteger();
        List<String> riskAuthsSeen = new CopyOnWriteArrayList<>();
        AtomicInteger settlementCalls = new AtomicInteger();
        List<String> settlementAuthsSeen = new CopyOnWriteArrayList<>();
        List<String> settlementIdemKeysSeen = new CopyOnWriteArrayList<>();

        SSLContext loopbackTls = createLoopbackSslContext(temp);
        HttpsServer jvmUpstream = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        jvmUpstream.setHttpsConfigurator(new HttpsConfigurator(loopbackTls));
        jvmUpstream.createContext("/api/risk/", ex -> {
            riskCalls.incrementAndGet();
            String auth = ex.getRequestHeaders().getFirst("Authorization");
            riskAuthsSeen.add(auth == null ? "" : auth);
            if (!("Bearer " + RISK_API_TOKEN).equals(auth)) {
                ex.sendResponseHeaders(401, -1);
                ex.close();
                return;
            }
            String accountId = ex.getRequestURI().getPath().substring("/api/risk/".length());
            Map<String, String> q = parseQuery(ex.getRequestURI().getRawQuery());
            String riskTier = accountId.startsWith("vip-") ? "LOW" : "MEDIUM";
            String body = "{\"orderSeq\":" + q.get("orderSeq")
                + ",\"accountId\":\"" + accountId
                + "\",\"amount\":" + q.get("amount")
                + ",\"expedited\":" + q.get("expedited")
                + ",\"riskTier\":\"" + riskTier + "\"}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(bytes);
            }
        });
        jvmUpstream.createContext("/api/settlements", ex -> {
            settlementCalls.incrementAndGet();
            String auth = ex.getRequestHeaders().getFirst("Authorization");
            settlementAuthsSeen.add(auth == null ? "" : auth);
            String idem = ex.getRequestHeaders().getFirst("Idempotency-Key");
            settlementIdemKeysSeen.add(idem == null ? "" : idem);
            if (!("Bearer " + SETTLEMENT_API_TOKEN).equals(auth)) {
                ex.sendResponseHeaders(401, -1);
                ex.close();
                return;
            }
            if (idem == null || idem.isBlank()) {
                ex.sendResponseHeaders(422, -1);
                ex.close();
                return;
            }
            JsonNode req = PolyglotProofHarness.JSON.readTree(ex.getRequestBody().readAllBytes());
            String body = "{\"settlementId\":\"SET-" + req.path("branch").asText() + "-" + req.path("orderSeq").asText()
                + "\",\"orderSeq\":" + req.path("orderSeq").asText()
                + ",\"branch\":\"" + req.path("branch").asText()
                + "\",\"settledAmount\":" + req.path("amount").asText() + "}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(201, bytes.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(bytes);
            }
        });
        jvmUpstream.start();

        try {
            String apiBase = "https://127.0.0.1:" + jvmUpstream.getAddress().getPort() + "/api";
            Path ingressYaml = materializeYaml("tier3-mixed-ingress-pipeline.yaml", apiBase, temp);
            Path downstreamYaml = materializeYaml("tier3-downstream-settlement-pipeline.yaml", apiBase, temp);

            Path nodeIngressGen = temp.resolve("gen-node-ingress");
            Path pyIngressGen = temp.resolve("gen-py-ingress");
            Path pyDownstreamGen = temp.resolve("gen-py-downstream");
            PolyglotCompiler.compile(PolyglotCompiler.Target.TYPESCRIPT, ingressYaml, nodeIngressGen);
            PolyglotCompiler.compile(PolyglotCompiler.Target.PYTHON, ingressYaml, pyIngressGen);
            PolyglotCompiler.compile(PolyglotCompiler.Target.PYTHON, downstreamYaml, pyDownstreamGen);

            Path nodeAuditLog = temp.resolve("node-side-effect-audit.jsonl");
            Path nodeLeakFile = temp.resolve("node-leak.txt");
            Path pyLeakFile = temp.resolve("py-leak.txt");
            Path pyDownstreamLeakFile = temp.resolve("py-downstream-leak.txt");

            try (PolyglotHostProcess nodeHost = startNodeIngressHost(nodeIngressGen, nodeAuditLog, nodeLeakFile);
                 PolyglotHostProcess pyIngressHost = startPythonIngressHost(pyIngressGen, pyLeakFile);
                 PolyglotHostProcess pyDownstreamHost = startPythonDownstreamHost(pyDownstreamGen, pyDownstreamLeakFile)) {

                Map<String, String> connectorSecrets = Map.of(
                    "risk-api-token", RISK_API_TOKEN,
                    "settlement-api-token", SETTLEMENT_API_TOKEN);

                // Independent downstream settlement pipeline with its own QueueAsyncCoordinator + disk store
                try (PolyglotProofHarness downstream = new PolyglotProofHarness(
                    downstreamYaml,
                    pyDownstreamHost.baseUri(),
                    Map.of(),
                    pyDownstreamHost.baseUri(),
                    SHARED_SECRET,
                    temp.resolve("downstream-state"),
                    1,
                    connectorSecrets,
                    null,
                    Optional.empty(),
                    Optional.of(loopbackTls))) {

                    List<HandoffRecord> handoffs = new CopyOnWriteArrayList<>();
                    CheckpointPublicationService handoffService = new CheckpointPublicationService() {
                        @Override
                        public boolean enabled() {
                            return true;
                        }

                        @Override
                        public Uni<Void> publishIfConfigured(ExecutionRecord<Object, Object> upstreamRec, Object payload) {
                            Object decoded = payload instanceof SerializedTransitionPayload s
                                ? new JsonTransitionPayloadCodec().decode(s)
                                : payload;
                            TypedDurablePayload typed = (TypedDurablePayload) decoded;
                            String json = new String(typed.payload(), StandardCharsets.UTF_8);
                            ExecutionRecord<Object, Object> downstreamRec = downstream.submitAndDrain(
                                upstreamRec.tenantId(),
                                "handoff-" + upstreamRec.executionId(),
                                json);
                            handoffs.add(new HandoffRecord(
                                upstreamRec.executionId(),
                                downstreamRec.executionId(),
                                downstreamRec.status(),
                                downstream.getResultCanonicalString(upstreamRec.tenantId(), downstreamRec.executionId())));
                            return Uni.createFrom().voidItem();
                        }
                    };

                    // Per-step polyglot routing: Node owns score-payment & audit-scored-payment (SIDE_EFFECT);
                    // Python owns route-settlement (union) & prepare-checkpoint-dispatch!
                    Map<String, URI> stepHosts = Map.of(
                        "score-payment", nodeHost.baseUri(),
                        "audit-scored-payment", nodeHost.baseUri(),
                        "route-settlement", pyIngressHost.baseUri(),
                        "prepare-checkpoint-dispatch", pyIngressHost.baseUri());

                    try (PolyglotProofHarness upstream = new PolyglotProofHarness(
                        ingressYaml,
                        nodeHost.baseUri(),
                        stepHosts,
                        nodeHost.baseUri(),
                        SHARED_SECRET,
                        temp.resolve("upstream-state"),
                        1,
                        connectorSecrets,
                        null,
                        Optional.of(handoffService),
                        Optional.of(loopbackTls))) {

                        // 1. EXPRESS branch (vip account -> LOW risk + expedited=true)
                        ExecutionRecord<Object, Object> expressUpstream = upstream.submitAndDrain(
                            "t1", "tier3-express-1",
                            "{\"orderSeq\":" + EXACT_INT64 + ",\"accountId\":\"vip-100\",\"amount\":"
                                + EXACT_DECIMAL + ",\"expedited\":true}");
                        assertEquals(ExecutionStatus.SUCCEEDED, expressUpstream.status(),
                            "Express upstream failed: " + expressUpstream.errorMessage());
                        JsonNode expressDispatch = upstream.getResultJson("t1", expressUpstream.executionId());
                        assertEquals("EXPRESS", expressDispatch.path("branch").asText());
                        assertEquals("INSTANT_WIRE", expressDispatch.path("channel").asText());
                        assertEquals(95, expressDispatch.path("score").asInt(),
                            "SIDE_EFFECT mutation (-999) must NOT leak into downstream canonical payload");
                        assertEquals(EXACT_INT64, expressDispatch.path("orderSeq").asText());
                        assertEquals(EXACT_DECIMAL, expressDispatch.path("amount").asText());

                        // 2. STANDARD branch (regular account -> MEDIUM risk + expedited=false)
                        ExecutionRecord<Object, Object> standardUpstream = upstream.submitAndDrain(
                            "t1", "tier3-standard-1",
                            "{\"orderSeq\":42,\"accountId\":\"std-200\",\"amount\":250.75,\"expedited\":false}");
                        assertEquals(ExecutionStatus.SUCCEEDED, standardUpstream.status(),
                            "Standard upstream failed: " + standardUpstream.errorMessage());
                        JsonNode standardDispatch = upstream.getResultJson("t1", standardUpstream.executionId());
                        assertEquals("STANDARD", standardDispatch.path("branch").asText());
                        assertEquals("ACH_BATCH", standardDispatch.path("channel").asText());
                        assertEquals(60, standardDispatch.path("score").asInt(),
                            "SIDE_EFFECT mutation (-999) must NOT leak into standard branch payload");

                        // Verify cross-pipeline Checkpoint Handoff executed independently for both runs
                        assertEquals(2, handoffs.size(), "Expected 2 cross-pipeline checkpoint handoffs");

                        HandoffRecord expressHandoff = handoffs.get(0);
                        assertNotEquals(expressHandoff.upstreamExecutionId(), expressHandoff.downstreamExecutionId(),
                            "Downstream pipeline must own an independent executionId after checkpoint handoff");
                        assertEquals(ExecutionStatus.SUCCEEDED, expressHandoff.downstreamStatus());
                        JsonNode expressConf = PolyglotProofHarness.JSON.readTree(expressHandoff.downstreamResultJson());
                        assertEquals("SET-EXPRESS-" + EXACT_INT64, expressConf.path("settlementId").asText());
                        assertEquals("EXPRESS", expressConf.path("branch").asText());
                        assertEquals(EXACT_INT64, expressConf.path("orderSeq").asText());
                        assertEquals(EXACT_DECIMAL, expressConf.path("settledAmount").asText());

                        HandoffRecord standardHandoff = handoffs.get(1);
                        assertNotEquals(standardHandoff.upstreamExecutionId(), standardHandoff.downstreamExecutionId());
                        assertEquals(ExecutionStatus.SUCCEEDED, standardHandoff.downstreamStatus());
                        JsonNode standardConf = PolyglotProofHarness.JSON.readTree(standardHandoff.downstreamResultJson());
                        assertEquals("SET-STANDARD-42", standardConf.path("settlementId").asText());
                        assertEquals("STANDARD", standardConf.path("branch").asText());
                        assertEquals("250.75", standardConf.path("settledAmount").asText());

                        // Verify Node SIDE_EFFECT ran exactly twice (once per run) with exact canonical payloads
                        List<String> auditLines = Files.readAllLines(nodeAuditLog, StandardCharsets.UTF_8);
                        assertEquals(2, auditLines.size(), "Expected 2 Node SIDE_EFFECT audit records");
                        assertTrue(auditLines.get(0).contains(EXACT_INT64) && auditLines.get(0).contains("\"score\":95"));
                        assertTrue(auditLines.get(1).contains("\"accountId\":\"std-200\"") && auditLines.get(1).contains("\"score\":60"));

                        // Verify JVM ecosystem HttpConnector received host-owned secrets + Idempotency-Key on EVERY request
                        assertEquals(2, riskCalls.get());
                        assertEquals(List.of("Bearer " + RISK_API_TOKEN, "Bearer " + RISK_API_TOKEN), riskAuthsSeen);
                        assertEquals(2, settlementCalls.get());
                        assertEquals(
                            List.of("Bearer " + SETTLEMENT_API_TOKEN, "Bearer " + SETTLEMENT_API_TOKEN),
                            settlementAuthsSeen);
                        assertEquals(2, settlementIdemKeysSeen.size());
                        assertTrue(settlementIdemKeysSeen.stream().allMatch(k -> !k.isBlank()),
                            "Every JVM HttpConnector Command must send a non-blank Idempotency-Key");
                        assertNotEquals(settlementIdemKeysSeen.get(0), settlementIdemKeysSeen.get(1),
                            "Distinct commands must carry distinct Idempotency-Key headers");

                        // Verify ZERO credential leakage (env, payload, or context) across all 3 foreign hosts
                        assertEquals("CLEAN", Files.readString(nodeLeakFile, StandardCharsets.UTF_8).trim());
                        assertEquals("CLEAN", Files.readString(pyLeakFile, StandardCharsets.UTF_8).trim());
                        assertEquals("CLEAN", Files.readString(pyDownstreamLeakFile, StandardCharsets.UTF_8).trim());
                    }
                }
            }
        } finally {
            jvmUpstream.stop(0);
        }
    }

    private record HandoffRecord(
        String upstreamExecutionId,
        String downstreamExecutionId,
        ExecutionStatus downstreamStatus,
        String downstreamResultJson
    ) { }

    private static SSLContext createLoopbackSslContext(Path temp) throws Exception {
        Path keystorePath = temp.resolve("loopback-tls.p12");
        char[] password = "changeit".toCharArray();
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        Process proc = new ProcessBuilder(
            keytool, "-genkeypair", "-alias", "loopback",
            "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
            "-storetype", "PKCS12", "-keystore", keystorePath.toString(),
            "-storepass", "changeit", "-keypass", "changeit",
            "-dname", "CN=127.0.0.1", "-ext", "SAN=IP:127.0.0.1,DNS:localhost")
            .redirectErrorStream(true)
            .start();
        String output = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, proc.waitFor(), "keytool failed: " + output);
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(keystorePath)) {
            ks.load(in, password);
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, password);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return ctx;
    }

    private static Path materializeYaml(String resource, String apiBase, Path temp) throws IOException {
        try (var in = Tier3MixedTopologyProofTest.class.getResourceAsStream("/" + resource)) {
            assertNotNull(in, "Missing resource /" + resource);
            String raw = new String(in.readAllBytes(), StandardCharsets.UTF_8)
                .replace("http://127.0.0.1:0/api", apiBase);
            Path out = temp.resolve(resource);
            Files.writeString(out, raw, StandardCharsets.UTF_8);
            return out;
        }
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> map = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) return map;
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                map.put(
                    URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                    URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        return map;
    }

    private static PolyglotHostProcess startNodeIngressHost(
        Path genDir, Path auditLog, Path leakFile
    ) throws Exception {
        Files.writeString(leakFile, "CLEAN", StandardCharsets.UTF_8);
        Path runner = genDir.resolve("node_ingress.ts");
        Files.writeString(runner, """
            import fs from 'node:fs';
            import { startAppHost } from './host.ts';
            import type { FunctionHandlers } from './types.ts';

            const auditLog = process.env.AUDIT_LOG!;
            const leakFile = process.env.LEAK_FILE!;
            const checkIsolation = (input: unknown, ctx: unknown) => {
              const serialized = JSON.stringify(
                { env: process.env, input, ctx },
                (_k, v) => (typeof v === 'bigint' ? v.toString() : v));
              if (serialized.includes('%s') || serialized.includes('%s')) {
                fs.writeFileSync(leakFile, 'LEAKED');
              }
            };

            const handlers: FunctionHandlers = {
              async scorePayment(input, ctx) {
                checkIsolation(input, ctx);
                const score = input.riskTier === 'LOW' ? 95 : 60;
                return {
                  orderSeq: input.orderSeq,
                  accountId: input.accountId,
                  amount: input.amount,
                  expedited: input.expedited,
                  riskTier: input.riskTier,
                  score,
                };
              },
              async auditScoredPayment(input, ctx) {
                checkIsolation(input, ctx);
                fs.appendFileSync(auditLog, JSON.stringify({
                  orderSeq: input.orderSeq.toString(),
                  accountId: input.accountId,
                  score: input.score,
                }) + '\\n');
                // Deliberately return mutated score (-999) to prove SIDE_EFFECT pass-through discards mutation!
                return { ...input, score: -999 };
              },
              async routeSettlement() {
                throw new Error('Owned by Python host in mixed topology');
              },
              async prepareCheckpointDispatch() {
                throw new Error('Owned by Python host in mixed topology');
              },
            };

            const running = await startAppHost(handlers, { sharedSecret: process.env.TPF_SECRET! });
            console.log('READY ' + running.baseUrl);
            process.on('SIGTERM', async () => { await running.close(); process.exit(0); });
            """.formatted(RISK_API_TOKEN, SETTLEMENT_API_TOKEN));
        return PolyglotHostProcess.start(
            List.of("node", runner.toString()),
            genDir,
            Map.of(
                "TPF_SECRET", SHARED_SECRET,
                "AUDIT_LOG", auditLog.toString(),
                "LEAK_FILE", leakFile.toString()),
            Duration.ofSeconds(15));
    }

    private static PolyglotHostProcess startPythonIngressHost(Path genDir, Path leakFile) throws Exception {
        Files.writeString(leakFile, "CLEAN", StandardCharsets.UTF_8);
        Path pkgDir = genDir.resolve("py_ingress_pkg");
        Files.createDirectories(pkgDir);
        Files.copy(genDir.resolve("types.py"), pkgDir.resolve("types.py"));
        Files.copy(genDir.resolve("host.py"), pkgDir.resolve("host.py"));
        Files.writeString(pkgDir.resolve("__init__.py"), "");
        Path runner = genDir.resolve("py_ingress.py");
        Files.writeString(runner, """
            import os, signal, socketserver, sys, time
            from py_ingress_pkg import host, types as t

            def _fast_bind(self):
                socketserver.TCPServer.server_bind(self)
                self.server_name, self.server_port = self.server_address[:2]
            host.ThreadingHTTPServer.server_bind = _fast_bind

            leak_file = os.environ["LEAK_FILE"]
            def check_isolation(inp, ctx):
                joined = " ".join(os.environ.values()) + " " + repr(inp) + " " + repr(ctx)
                if "%s" in joined or "%s" in joined:
                    with open(leak_file, "w", encoding="utf-8") as f:
                        f.write("LEAKED")

            class Handlers:
                def score_payment(self, inp, ctx):
                    raise RuntimeError("Owned by Node host in mixed topology")

                def audit_scored_payment(self, inp, ctx):
                    raise RuntimeError("Owned by Node host in mixed topology")

                def route_settlement(self, inp: t.ScoredPayment, ctx: t.FunctionContext) -> t.RoutedSettlement:
                    check_isolation(inp, ctx)
                    if inp.expedited and inp.riskTier == "LOW":
                        return t.RoutedSettlement_Express(
                            discriminator="express",
                            value=t.ExpressSettlement(
                                orderSeq=inp.orderSeq,
                                accountId=inp.accountId,
                                amount=inp.amount,
                                channel="INSTANT_WIRE",
                                score=inp.score,
                            ),
                        )
                    return t.RoutedSettlement_Standard(
                        discriminator="standard",
                        value=t.StandardSettlement(
                            orderSeq=inp.orderSeq,
                            accountId=inp.accountId,
                            amount=inp.amount,
                            channel="ACH_BATCH",
                            score=inp.score,
                        ),
                    )

                def prepare_checkpoint_dispatch(self, inp: t.RoutedSettlement, ctx: t.FunctionContext) -> t.CheckpointDispatch:
                    check_isolation(inp, ctx)
                    branch = "EXPRESS" if inp.discriminator == "express" else "STANDARD"
                    val = inp.value
                    return t.CheckpointDispatch(
                        orderSeq=val.orderSeq,
                        accountId=val.accountId,
                        amount=val.amount,
                        branch=branch,
                        channel=val.channel,
                        score=val.score,
                    )

            running = host.start_app_host(Handlers(), shared_secret=os.environ["TPF_SECRET"])
            print(f"READY {running.base_url}", flush=True)
            def stop(_sig, _frame):
                running.close()
                sys.exit(0)
            signal.signal(signal.SIGTERM, stop)
            while True:
                time.sleep(0.2)
            """.formatted(RISK_API_TOKEN, SETTLEMENT_API_TOKEN));
        return PolyglotHostProcess.start(
            List.of("python3", "-u", runner.toString()),
            genDir,
            Map.of("TPF_SECRET", SHARED_SECRET, "LEAK_FILE", leakFile.toString()),
            Duration.ofSeconds(15));
    }

    private static PolyglotHostProcess startPythonDownstreamHost(Path genDir, Path leakFile) throws Exception {
        Files.writeString(leakFile, "CLEAN", StandardCharsets.UTF_8);
        Path pkgDir = genDir.resolve("py_downstream_pkg");
        Files.createDirectories(pkgDir);
        Files.copy(genDir.resolve("types.py"), pkgDir.resolve("types.py"));
        Files.copy(genDir.resolve("host.py"), pkgDir.resolve("host.py"));
        Files.writeString(pkgDir.resolve("__init__.py"), "");
        Path runner = genDir.resolve("py_downstream.py");
        Files.writeString(runner, """
            import os, signal, socketserver, sys, time
            from py_downstream_pkg import host, types as t

            def _fast_bind(self):
                socketserver.TCPServer.server_bind(self)
                self.server_name, self.server_port = self.server_address[:2]
            host.ThreadingHTTPServer.server_bind = _fast_bind

            leak_file = os.environ["LEAK_FILE"]
            def check_isolation(inp, ctx):
                joined = " ".join(os.environ.values()) + " " + repr(inp) + " " + repr(ctx)
                if "%s" in joined or "%s" in joined:
                    with open(leak_file, "w", encoding="utf-8") as f:
                        f.write("LEAKED")

            class Handlers:
                def format_settlement_instruction(self, inp: t.CheckpointDispatch, ctx: t.FunctionContext) -> t.SettlementInstruction:
                    check_isolation(inp, ctx)
                    return t.SettlementInstruction(
                        orderSeq=inp.orderSeq,
                        accountId=inp.accountId,
                        amount=inp.amount,
                        branch=inp.branch,
                        channel=inp.channel,
                        instructionRef=f"INST-{inp.branch}-{inp.orderSeq}",
                    )

            running = host.start_app_host(Handlers(), shared_secret=os.environ["TPF_SECRET"])
            print(f"READY {running.base_url}", flush=True)
            def stop(_sig, _frame):
                running.close()
                sys.exit(0)
            signal.signal(signal.SIGTERM, stop)
            while True:
                time.sleep(0.2)
            """.formatted(RISK_API_TOKEN, SETTLEMENT_API_TOKEN));
        return PolyglotHostProcess.start(
            List.of("python3", "-u", runner.toString()),
            genDir,
            Map.of("TPF_SECRET", SHARED_SECRET, "LEAK_FILE", leakFile.toString()),
            Duration.ofSeconds(15));
    }
}
