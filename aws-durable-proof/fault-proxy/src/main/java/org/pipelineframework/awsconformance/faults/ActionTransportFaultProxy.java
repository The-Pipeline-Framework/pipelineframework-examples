package org.pipelineframework.awsconformance.faults;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

/** Fixture-only decorator of the actual Lambda transport. It never interprets TPF outcomes. */
public final class ActionTransportFaultProxy implements RequestStreamHandler {
    private final LambdaClient lambda;
    private final DynamoDbClient dynamo;
    private final String target;
    private final String faultTable;
    private final ObjectMapper mapper = new ObjectMapper();

    public ActionTransportFaultProxy() {
        this(LambdaClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build(),
            DynamoDbClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build(),
            requiredEnvironment("TPF_CONFORMANCE_TARGET_ACTION_VERSION"),
            requiredEnvironment("TPF_CONFORMANCE_FAULT_TABLE"));
    }

    ActionTransportFaultProxy(LambdaClient lambda, DynamoDbClient dynamo, String target, String faultTable) {
        this.lambda = Objects.requireNonNull(lambda, "lambda");
        this.dynamo = Objects.requireNonNull(dynamo, "dynamo");
        this.target = Objects.requireNonNull(target, "target");
        this.faultTable = Objects.requireNonNull(faultTable, "faultTable");
        if (!target.matches("arn:[^:]+:lambda:[^:]+:[0-9]{12}:function:[^:]+:[0-9]+")) {
            throw new IllegalArgumentException("Proxy must forward to a pinned numbered production action version");
        }
    }

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        byte[] request = input.readAllBytes();
        JsonNode envelope = mapper.readTree(request);
        String operation = envelope.path("operation").asText();
        String tenant = envelope.path("tenantId").asText();
        if (operation.isBlank() || tenant.isBlank()) {
            throw new IllegalArgumentException("Proxy requires a production action operation and tenant correlation");
        }
        injectOnce("BEFORE_FORWARD", operation, tenant, envelope, request);
        var response = lambda.invoke(InvokeRequest.builder().functionName(target).payload(SdkBytes.fromByteArray(request)).build());
        if (response.functionError() != null && !response.functionError().isBlank()) {
            throw new IOException("Production target failed: " + response.functionError());
        }
        injectOnce("AFTER_RESPONSE", operation, tenant, envelope, request);
        output.write(response.payload().asByteArray());
    }

    private void injectOnce(String point, String operation, String tenant, JsonNode envelope, byte[] request) throws IOException {
        String armId = tenant + "#" + operation + "#" + point;
        var arm = dynamo.getItem(GetItemRequest.builder().tableName(faultTable).consistentRead(true)
            .key(Map.of("fault_id", AttributeValue.fromS(armId))).build());
        if (!arm.hasItem()) {
            return;
        }
        String mode = arm.item().getOrDefault("mode", AttributeValue.fromS("ONCE")).s();
        if ("HOLD".equals(mode)) {
            // A HOLD is fixture-only and valid at just this transport cut. Unlike
            // ONCE, a previously recorded claim never authorises a retry forward.
            if (!"BEFORE_FORWARD".equals(point) || !"REGISTER_CALLBACK".equals(operation)) {
                return;
            }
            holdRegistration(armId, tenant, envelope, request);
            return;
        }
        if (!"ONCE".equals(mode)) {
            throw new IllegalArgumentException("Unknown external fault mode");
        }
        try {
            dynamo.putItem(PutItemRequest.builder().tableName(faultTable)
                .item(Map.of("fault_id", AttributeValue.fromS(armId + "#CLAIM"),
                    "point", AttributeValue.fromS(point), "target", AttributeValue.fromS(target)))
                .conditionExpression("attribute_not_exists(fault_id)").build());
        } catch (ConditionalCheckFailedException alreadyInjected) {
            return;
        }
        throw new IOException("Injected external action transport failure: " + point + " " + operation);
    }

    private void holdRegistration(String armId, String tenant, JsonNode envelope, byte[] request) throws IOException {
        var release = dynamo.getItem(GetItemRequest.builder().tableName(faultTable).consistentRead(true)
            .key(Map.of("fault_id", AttributeValue.fromS(armId + "#RELEASE"))).build());
        if (release.hasItem()) {
            return;
        }
        String executionId = requiredText(envelope, "executionId");
        long generation = envelope.path("generation").asLong(0);
        if (generation < 1) {
            throw new IllegalArgumentException("Held registration requires positive generation");
        }
        var claim = Map.ofEntries(
            Map.entry("fault_id", AttributeValue.fromS(armId + "#CLAIM")),
            Map.entry("mode", AttributeValue.fromS("HOLD")),
            Map.entry("point", AttributeValue.fromS("BEFORE_FORWARD")),
            Map.entry("operation", AttributeValue.fromS("REGISTER_CALLBACK")),
            Map.entry("target", AttributeValue.fromS(target)),
            Map.entry("tenant_id", AttributeValue.fromS(tenant)),
            Map.entry("execution_id", AttributeValue.fromS(executionId)),
            Map.entry("generation", AttributeValue.fromN(Long.toString(generation))),
            Map.entry("request_sha256", AttributeValue.fromS(sha256(request))),
            Map.entry("request_json", AttributeValue.fromS(new String(request, StandardCharsets.UTF_8))));
        try {
            dynamo.putItem(PutItemRequest.builder().tableName(faultTable).item(claim)
                .conditionExpression("attribute_not_exists(fault_id)").build());
        } catch (ConditionalCheckFailedException alreadyHeld) {
            // Every retry still fails before the real target, including retries
            // with different serialized bytes. Only the fixture RELEASE opens it.
        }
        throw new IOException("Held external REGISTER_CALLBACK before production forwarding");
    }

    private String requiredText(JsonNode envelope, String field) {
        String value = envelope.path(field).asText();
        if (value.isBlank()) {
            throw new IllegalArgumentException("Held registration requires " + field);
        }
        return value;
    }

    private String sha256(byte[] request) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(request));
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is required", unavailable);
        }
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required");
        }
        return value;
    }
}
