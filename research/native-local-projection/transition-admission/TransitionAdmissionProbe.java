import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.RestTransitionWorkerProtocol;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionPayloadEncoding;
import org.pipelineframework.orchestrator.TransitionWireResult;
import org.pipelineframework.orchestrator.TransitionWorkerSignature;
import org.pipelineframework.orchestrator.worker.PipelineWorkerCapability;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Research consumer. Uses the released wire values without starting the Java runtime. */
public final class TransitionAdmissionProbe {
    private static final ObjectMapper JSON = PipelineJson.mapper();
    private static final String PROFILE = "tpf-polyglot-transition-admission/1";
    private static final String POLICY = "tpf-polyglot-unary-policy/1";
    private static final String ENCODING = "tpf-polyglot-research-json/1";
    private static final String FAILED_RESULT_PROFILE = "tpf-polyglot-failed-result/1";
    private static final Map<String, String> FAILURE_DIAGNOSTICS = Map.of(
        "TRANSIENT_EXHAUSTED", "Local retry budget exhausted",
        "BUSINESS_REJECTION", "Business function rejected input",
        "UNCLASSIFIED", "Unclassified business function failure");
    private static final List<String> IDENTITY = List.of("pipelineId", "contractVersion", "releaseVersion");
    private static final List<String> ARTIFACT = List.of("artifactId", "artifactDigest");
    private static final List<String> ATTEMPT = List.of("executionId", "attempt", "executionVersion", "transitionKey");
    private static final List<String> CONTEXT = List.of("executionId", "traceId");
    private static final List<String> INTS = List.of(
        "currentStepIndex", "stopBeforeStepIndex", "attempt", "executionVersion", "redriveStepIndex");
    private static final List<String> STRINGS = List.of(
        "pipelineId", "contractVersion", "releaseVersion", "tenantId", "executionId", "traceId",
        "transitionKey", "resultShape", "payloadTypeId", "payloadEncoding", "payload", "redriveIntent");

    private final JsonNode corpus;
    private final JsonNode target;
    private final ObjectNode capability;
    private final ObjectNode grant;
    private final ObjectNode claims;
    private final JsonNode requests;
    private final JsonNode workerFaults;
    private final boolean observeResultDecoders;
    private final byte[] projection;
    private final Set<String> nonces = new HashSet<>();
    private final Map<String, JsonNode> replies = new HashMap<>();
    private final ArrayNode diagnostics = JSON.createArrayNode();
    private final ArrayNode bInputs = JSON.createArrayNode();
    private final ArrayNode contexts = JSON.createArrayNode();
    private final ArrayNode settlements = JSON.createArrayNode();
    private int physicalRequests;
    private int coordinatorDeliveries;
    private int envelopeReads;
    private int payloadReads;
    private int aAttempts;
    private int bAttempts;
    private int cAttempts;
    private int releasedResultReads;
    private int portableFailureReads;
    private String selectedResultProfile = "";
    private String state = "READY";
    private String current = "";
    private boolean disposition;
    private boolean authorized;

    private TransitionAdmissionProbe(JsonNode corpus, JsonNode testCase, Path generated) throws IOException {
        this.corpus = corpus;
        this.target = corpus.path("target");
        this.capability = overlay(corpus.path("capability"), testCase.path("capability"));
        this.grant = overlay(corpus.path("grant"), testCase.path("grant"));
        this.claims = overlay(corpus.path("attempts"), testCase.path("claims"));
        this.requests = testCase.path("requests");
        this.workerFaults = testCase.path("workerFaults");
        this.observeResultDecoders = testCase.path("observeResultDecoders").asBoolean();
        String name = testCase.path("projection").asText("downstream-retry");
        if (!Set.of("downstream-retry", "changed-order").contains(name)) {
            throw new IllegalArgumentException("unknown projection fixture");
        }
        this.projection = Files.readAllBytes(generated.resolve(name).resolve("projection.json"));
    }

    private static ObjectNode overlay(JsonNode base, JsonNode patch) {
        ObjectNode copy = base.deepCopy();
        if (patch instanceof ObjectNode object) {
            copy.setAll(object);
        }
        return copy;
    }

    private static String text(JsonNode node, String key) {
        return node.path(key).asText();
    }

    private static boolean matches(JsonNode left, JsonNode right, List<String> keys) {
        return keys.stream().allMatch(key -> left.path(key).equals(right.path(key)));
    }

    private static long timestamp(String value) {
        long milliseconds = TransitionWorkerSignature.parseTimestamp(value);
        if (!Instant.ofEpochMilli(milliseconds).toString().equals(value) || value.length() != 20) {
            throw new IllegalArgumentException("expected UTC seconds");
        }
        return milliseconds;
    }

    private String authenticate(JsonNode frame) {
        if (List.of("timestamp", "nonce", "signature").stream().anyMatch(key ->
            !frame.path(key).isTextual() || text(frame, key).isBlank())) {
            return "MISSING_AUTH_HEADER";
        }
        long signedAt;
        try {
            signedAt = timestamp(text(frame, "timestamp"));
        } catch (IllegalArgumentException invalid) {
            return "INVALID_TIMESTAMP";
        }
        long now = timestamp(text(corpus, "now"));
        if (Math.abs(now - signedAt) > corpus.path("toleranceSeconds").asLong() * 1_000L) {
            return "TIMESTAMP_OUTSIDE_WINDOW";
        }
        String expected = TransitionWorkerSignature.sign(
            text(corpus, "secret"), RestTransitionWorkerProtocol.EXECUTE_METHOD,
            RestTransitionWorkerProtocol.EXECUTE_PATH, text(frame, "timestamp"),
            text(frame, "nonce"), text(frame, "body").getBytes(StandardCharsets.UTF_8));
        if (!TransitionWorkerSignature.matches(expected, text(frame, "signature"))) {
            return "BAD_SIGNATURE";
        }
        return nonces.add(text(frame, "nonce")) ? "OK" : "NONCE_REPLAY";
    }

    private String verifyBinding() throws NoSuchAlgorithmException {
        if (!matches(capability, target, IDENTITY) || !matches(capability, target, ARTIFACT)) {
            return "UNVERIFIED_WORKER";
        }
        PipelineWorkerCapability advertised;
        try {
            advertised = JSON.treeToValue(capability, PipelineWorkerCapability.class);
        } catch (JsonProcessingException invalid) {
            return "UNVERIFIED_WORKER";
        }
        if (!PipelineWorkerCapability.PROTOCOL_VERSION.equals(advertised.protocolVersion())
            || !"rest".equals(advertised.providerName()) || !advertised.workerProtocols().contains("rest")
            || !advertised.payloadEncodings().contains(TransitionPayloadEncoding.JSON)) {
            return "UNVERIFIED_WORKER";
        }
        if (!POLICY.equals(text(grant, "profile"))) {
            return "UNSUPPORTED_POLICY_PROFILE";
        }
        if (!matches(grant, target, List.of("artifactDigest"))
            || !ENCODING.equals(text(grant, "payloadEncoding")) || !"ProbeValue".equals(text(grant, "payloadTypeId"))) {
            return "UNVERIFIED_POLICY_BINDING";
        }
        if (grant.has("resultProfile")) {
            JsonNode selected = grant.path("resultProfile");
            if (!selected.isTextual()) {
                return "MALFORMED_RESULT_PROFILE";
            }
            if (!FAILED_RESULT_PROFILE.equals(selected.textValue())) {
                return "UNSUPPORTED_RESULT_PROFILE";
            }
        }
        String loadedDigest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(projection));
        return loadedDigest.equals(text(target, "artifactDigest")) ? "OK" : "ARTIFACT_DIGEST_MISMATCH";
    }

    private static boolean commandShape(JsonNode node) {
        return node.isObject() && node.size() == INTS.size() + STRINGS.size()
            && INTS.stream().allMatch(key -> node.path(key).isIntegralNumber())
            && STRINGS.stream().allMatch(key -> node.path(key).isTextual()
                && (!text(node, key).isEmpty() || CONTEXT.contains(key)));
    }

    private String receive(String name, Optional<JsonNode> claim) throws Exception {
        physicalRequests++;
        JsonNode frame = corpus.path("frames").path(requests.path(name).asText(name));
        String auth = authenticate(frame);
        if (!auth.equals("OK")) {
            return auth;
        }
        envelopeReads++;
        JsonNode envelope;
        TransitionCommandEnvelope command;
        try {
            envelope = JSON.readTree(text(frame, "body"));
            if (!commandShape(envelope)) {
                return "INVALID_ENVELOPE";
            }
        } catch (JsonProcessingException invalid) {
            return "INVALID_ENVELOPE";
        }
        if (!text(envelope, "tenantId").equals(text(target, "tenantId"))) {
            return "TENANT_FORBIDDEN";
        }
        if (!matches(envelope, target, IDENTITY)) {
            return "IDENTITY_MISMATCH";
        }
        // The released constructor normalizes a REPLAY redrive index. Check the signed field first.
        if (!envelope.path("currentStepIndex").equals(JSON.getNodeFactory().numberNode(0))
            || !envelope.path("stopBeforeStepIndex").equals(JSON.getNodeFactory().numberNode(-1))
            || !text(envelope, "resultShape").equals("SINGLE") || !text(envelope, "redriveIntent").equals("REPLAY")
            || !envelope.path("redriveStepIndex").equals(JSON.getNodeFactory().numberNode(-1))) {
            return "UNSUPPORTED_RANGE";
        }
        if (CONTEXT.stream().anyMatch(key -> text(envelope, key).chars()
            .allMatch(code -> " \t\r\n".indexOf(code) >= 0))) {
            return "INVALID_CONTEXT";
        }
        if (claim.isPresent() && !matches(envelope, claim.orElseThrow(), ATTEMPT)) {
            return "ATTEMPT_MISMATCH";
        }
        String binding = verifyBinding();
        if (!binding.equals("OK")) {
            return binding;
        }
        try {
            command = JSON.treeToValue(envelope, TransitionCommandEnvelope.class);
        } catch (JsonProcessingException invalid) {
            return "INVALID_ENVELOPE";
        }
        var payload = command.serializedPayload();
        if (!payload.payloadEncoding().equals(ENCODING)) {
            return "UNSUPPORTED_PAYLOAD_ENCODING";
        }
        if (!payload.payloadTypeId().equals("ProbeValue")) {
            return "UNSUPPORTED_PAYLOAD_TYPE";
        }
        payloadReads++;
        JsonNode value;
        try {
            value = JSON.readTree(payload.payload());
        } catch (JsonProcessingException invalid) {
            return "INVALID_PAYLOAD";
        }
        if (!value.isObject() || value.size() != 1 || !value.path("value").isInt() || value.path("value").intValue() != 3) {
            return "INVALID_PAYLOAD";
        }
        String fault = claim.isPresent() ? text(workerFaults, name) : "";
        if (!fault.isEmpty() && !FAILED_RESULT_PROFILE.equals(selectedResultProfile)) {
            throw new IllegalArgumentException("modeled worker fault requires the selected failed result profile");
        }
        ObjectNode result = runFixture(value.path("value").intValue(), command, fault);
        if (claim.isPresent()) {
            replies.put(name, result);
        }
        return text(result, "outcome");
    }

    private ObjectNode runFixture(int value, TransitionCommandEnvelope command, String fault) throws IOException {
        // Model the admitted worker's observations. The LOCAL binding exercises real calls and retry.
        contexts.addObject().put("executionId", command.executionId()).put("traceId", command.traceId());
        aAttempts++;
        int bInput = value + 1;
        bAttempts++;
        bInputs.add(bInput);
        JsonNode admittedProjection = JSON.readTree(projection);
        JsonNode step = admittedProjection.path("steps").path(1);
        if (!text(admittedProjection.path("policy"), "retryOwner").equals("LOCAL_SHELL")
            || step.path("retryLimit").asInt() != 1) {
            throw new IllegalArgumentException("worker fixture lacks its B retry policy");
        }
        if (fault.equals("BUSINESS_REJECTION")) {
            return failedResult("BUSINESS_REJECTION", text(step, "id"));
        }
        if (fault.equals("HOST_THROW")) {
            return failedResult("UNCLASSIFIED", text(step, "id"));
        }
        bAttempts++;
        bInputs.add(bInput);
        if (fault.equals("TRANSIENT")) {
            return failedResult("TRANSIENT_EXHAUSTED", text(step, "id"));
        }
        if (!fault.isEmpty()) {
            throw new IllegalArgumentException("unknown modeled worker fault");
        }
        cAttempts++;
        return completedResult(bInput * 2 - 3);
    }

    private ObjectNode failedResult(String code, String stepId) {
        ObjectNode result = JSON.createObjectNode()
            .put("resultProfile", selectedResultProfile).put("outcome", "FAILED");
        result.putObject("failure").put("code", code).put("stepId", stepId)
            .put("diagnostic", FAILURE_DIAGNOSTICS.get(code));
        return result;
    }

    private static ObjectNode completedResult(int result) {
        ObjectNode wire = JSON.createObjectNode()
            .put("outcome", "COMPLETED").put("terminalOutputPublished", false).put("terminalInputPassthrough", false);
        wire.putArray("outputPayloads").addObject()
            .put("payloadTypeId", "ProbeValue").put("payloadEncoding", ENCODING)
            .put("payload", JSON.createObjectNode().put("value", result).toString());
        return wire;
    }

    private String dispatch(String name) throws Exception {
        boolean initial = state.equals("READY") && name.equals("original");
        boolean redrive = state.equals("REMOTE_OUTCOME_UNKNOWN") && authorized && name.equals("redrive");
        if (!initial && !redrive) {
            return "REDRIVE_NOT_AUTHORIZED";
        }
        String binding = verifyBinding();
        if (!binding.equals("OK")) {
            return binding;
        }
        authorized = false;
        disposition = false;
        current = name;
        selectedResultProfile = text(grant, "resultProfile");
        state = "IN_FLIGHT";
        coordinatorDeliveries++;
        String observed = receive(name, Optional.of(claims.path(name)));
        return observed.equals("COMPLETED") || observed.equals("FAILED") ? "DISPATCHED" : observed;
    }

    private String complete(JsonNode event) throws IOException {
        String name = text(event, "frame");
        if (!name.equals(current) || !replies.containsKey(name)) {
            return "STALE_COMPLETION";
        }
        if (state.equals("SETTLED")) {
            return "ALREADY_SETTLED";
        }
        if (!state.equals("IN_FLIGHT")) {
            return "STALE_COMPLETION";
        }
        JsonNode response = event.has("response") ? event.path("response") : replies.get(name);
        if (!response.isObject()) {
            return "INVALID_WORKER_RESULT";
        }
        if (response.has("resultProfile")) {
            if (!FAILED_RESULT_PROFILE.equals(selectedResultProfile)
                || !response.path("resultProfile").isTextual()
                || !selectedResultProfile.equals(text(response, "resultProfile"))) {
                return "RESULT_PROFILE_MISMATCH";
            }
            return completeFailure(name, response);
        }
        if (text(response, "outcome").equals("FAILED") && FAILED_RESULT_PROFILE.equals(selectedResultProfile)) {
            return "RESULT_PROFILE_MISMATCH";
        }
        if (!text(response, "outcome").equals("COMPLETED")) {
            return "UNSUPPORTED_WORKER_OUTCOME";
        }
        JsonNode outputs = response.path("outputPayloads");
        if (response.size() != 4 || !outputs.isArray() || outputs.size() != 1
            || !outputs.path(0).isObject() || outputs.path(0).size() != 3
            || !response.path("terminalOutputPublished").isBoolean() || response.path("terminalOutputPublished").asBoolean()
            || !response.path("terminalInputPassthrough").isBoolean() || response.path("terminalInputPassthrough").asBoolean()) {
            return "INVALID_WORKER_RESULT";
        }
        JsonNode output = outputs.path(0);
        if (!ENCODING.equals(text(output, "payloadEncoding")) || !"ProbeValue".equals(text(output, "payloadTypeId"))) {
            return "UNSUPPORTED_RESULT_ENCODING";
        }
        releasedResultReads++;
        JsonNode value;
        try {
            var result = JSON.treeToValue(response, TransitionWireResult.class);
            value = JSON.readTree(result.outputPayloads().getFirst().payload());
        } catch (JsonProcessingException invalid) {
            return "INVALID_WORKER_RESULT";
        }
        if (!value.isObject() || value.size() != 1 || !value.path("value").isInt()) {
            return "INVALID_WORKER_RESULT";
        }
        JsonNode pin = claims.path(name);
        settlements.addObject().put("transitionKey", text(pin, "transitionKey"))
            .put("executionVersion", pin.path("executionVersion").asInt()).put("value", value.path("value").asInt());
        state = "SETTLED";
        return "SETTLED";
    }

    private String completeFailure(String name, JsonNode response) throws IOException {
        portableFailureReads++;
        JsonNode failure = response.path("failure");
        if (response.size() != 3 || !text(response, "outcome").equals("FAILED")
            || !failure.isObject() || failure.size() != 3
            || !List.of("code", "stepId", "diagnostic").stream().allMatch(key ->
                failure.path(key).isTextual() && !text(failure, key).isEmpty())) {
            return "INVALID_PORTABLE_FAILURE";
        }
        String code = text(failure, "code");
        if (!FAILURE_DIAGNOSTICS.containsKey(code)) {
            return "UNKNOWN_FAILURE_CLASSIFICATION";
        }
        if (!FAILURE_DIAGNOSTICS.get(code).equals(text(failure, "diagnostic"))) {
            return "UNSAFE_FAILURE_DIAGNOSTIC";
        }
        boolean knownStep = false;
        for (JsonNode step : JSON.readTree(projection).path("steps")) {
            knownStep |= text(step, "id").equals(text(failure, "stepId"));
        }
        if (!knownStep) {
            return "UNKNOWN_FAILED_STEP";
        }
        JsonNode pin = claims.path(name);
        settlements.addObject().put("transitionKey", text(pin, "transitionKey"))
            .put("executionVersion", pin.path("executionVersion").asInt()).set("failure", failure);
        state = "SETTLED";
        return "SETTLED_FAILED";
    }

    private String loseResponse() {
        if (!state.equals("IN_FLIGHT")) {
            throw new IllegalStateException("no in-flight response");
        }
        state = "REMOTE_OUTCOME_UNKNOWN";
        return state;
    }

    private String confirmDisposition(JsonNode event) {
        if (!state.equals("REMOTE_OUTCOME_UNKNOWN")) {
            throw new IllegalStateException("no uncertain attempt");
        }
        if (!text(event, "authority").equals("independent-observer")
            || !text(event, "frame").equals(current)
            || !text(event, "disposition").equals("ORIGINAL_ATTEMPT_INACTIVE")) {
            return "DISPOSITION_UNVERIFIED";
        }
        disposition = true;
        return "DISPOSITION_CONFIRMED";
    }

    private String authorizeRedrive(JsonNode event) {
        if (!state.equals("REMOTE_OUTCOME_UNKNOWN")) {
            throw new IllegalStateException("no uncertain attempt");
        }
        if (!text(event, "authority").equals("operator")) {
            return "REDRIVE_FORBIDDEN";
        }
        if (!current.equals("original")) {
            return "REDRIVE_NOT_AUTHORIZED";
        }
        if (!disposition) {
            return "DISPOSITION_REQUIRED";
        }
        if (!text(event, "prior").equals(current) || !text(event, "next").equals("redrive")) {
            return "REDRIVE_BINDING_MISMATCH";
        }
        JsonNode prior = claims.path(current);
        JsonNode next = claims.path(text(event, "next"));
        if (!matches(prior, next, List.of("executionId"))
            || next.path("attempt").asInt() <= prior.path("attempt").asInt()
            || next.path("executionVersion").asLong() <= prior.path("executionVersion").asLong()
            || matches(prior, next, List.of("transitionKey"))) {
            return "REDRIVE_BINDING_MISMATCH";
        }
        authorized = true;
        return "REDRIVE_AUTHORIZED";
    }

    private String apply(JsonNode event) throws Exception {
        return switch (text(event, "op")) {
            case "receive" -> receive(text(event, "frame"), Optional.empty());
            case "dispatch" -> dispatch(text(event, "frame"));
            case "complete" -> complete(event);
            case "change-result-grant" -> {
                grant.set("resultProfile", event.path("profile"));
                yield "HOST_RESULT_GRANT_CHANGED";
            }
            case "lose-response" -> loseResponse();
            case "confirm-disposition" -> confirmDisposition(event);
            case "authorize-redrive" -> authorizeRedrive(event);
            default -> throw new IllegalArgumentException("unsupported transcript event");
        };
    }

    private ObjectNode report(JsonNode events) throws Exception {
        for (JsonNode event : events) {
            diagnostics.add(apply(event));
        }
        ObjectNode result = JSON.createObjectNode().put("physicalRequests", physicalRequests)
            .put("coordinatorDeliveries", coordinatorDeliveries).put("envelopeReads", envelopeReads)
            .put("payloadReads", payloadReads).put("state", state);
        result.putObject("attempts").put("A", aAttempts).put("B", bAttempts).put("C", cAttempts);
        result.set("diagnostics", diagnostics);
        result.set("bInputs", bInputs);
        result.set("contexts", contexts);
        result.set("settlements", settlements);
        if (observeResultDecoders) {
            result.putObject("resultDecoders").put("released", releasedResultReads)
                .put("portableFailed", portableFailureReads);
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        JsonNode corpus = JSON.readTree(Files.readAllBytes(Path.of(args[0])));
        if (!corpus.path("schemaVersion").isInt() || corpus.path("schemaVersion").asInt() != 1
            || !PROFILE.equals(text(corpus, "profile"))) {
            throw new IllegalArgumentException("unsupported transcript corpus");
        }
        JsonNode encodings = corpus.path("capability").path("payloadEncodings");
        if (!encodings.isArray() || encodings.size() != 1
            || !TransitionPayloadEncoding.JSON.equals(encodings.path(0).asText())) {
            throw new IllegalArgumentException("fixture must advertise the released outer envelope encoding");
        }
        for (JsonNode testCase : corpus.path("cases")) {
            JsonNode observed = new TransitionAdmissionProbe(corpus, testCase, Path.of(args[1]))
                .report(testCase.path("events"));
            if (!observed.equals(testCase.path("expected"))) {
                System.err.println("TRANSITION_ADMISSION_MISMATCH:" + text(testCase, "id"));
                System.err.println("expected " + testCase.path("expected") + "\nobserved " + observed);
                System.exit(1);
            }
        }
        System.out.println("TRANSITION_ADMISSION_PASS:java:" + corpus.path("cases").size());
    }
}
