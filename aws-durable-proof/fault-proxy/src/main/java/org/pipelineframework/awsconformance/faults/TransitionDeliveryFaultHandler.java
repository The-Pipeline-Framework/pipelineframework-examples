package org.pipelineframework.awsconformance.faults;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.aws.durable.AwsDurableTransitionWorkerHandler;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

/** Fixture-only delivery faults around the unchanged production SQS handler.
 * An acknowledged delegate is NOT publication evidence: the runner must additionally
 * observe the authenticated native caller advance on the correlated response.
 */
@Named("conformance-transition-delivery")
@ApplicationScoped
public final class TransitionDeliveryFaultHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
    @Inject
    AwsDurableTransitionWorkerHandler delegate;
    private DynamoDbClient client;
    private String table;

    public TransitionDeliveryFaultHandler() { }

    public TransitionDeliveryFaultHandler(AwsDurableTransitionWorkerHandler delegate, DynamoDbClient client, String table) {
        this.delegate = java.util.Objects.requireNonNull(delegate);
        this.client = java.util.Objects.requireNonNull(client);
        this.table = java.util.Objects.requireNonNull(table);
    }

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        if (event.getRecords().size() != 1) {
            return new SQSBatchResponse(event.getRecords().stream()
                .map(record -> new SQSBatchResponse.BatchItemFailure(record.getMessageId())).toList());
        }
        var record = event.getRecords().get(0);
        String tenant;
        String requestId;
        try {
            var json = new ObjectMapper();
            var request = json.readTree(record.getBody());
            var command = json.readTree(request.path("commandEnvelope").asText());
            tenant = command.path("tenantId").asText();
            requestId = request.path("requestId").asText();
            if (tenant.isBlank() || requestId.isBlank()) {
                // Malformed/authentication decisions remain exclusively production-owned.
                return delegate.handleRequest(event, context);
            }
        } catch (Exception malformed) {
            return delegate.handleRequest(event, context);
        }
        try {
            if (claim(tenant, requestId, record, "BEFORE_DELEGATE")) {
                return failed(record);
            }
            var response = delegate.handleRequest(event, context);
            if ((response.getBatchItemFailures() == null || response.getBatchItemFailures().isEmpty())
                && claim(tenant, requestId, record, "AFTER_ACK")) {
                return failed(record);
            }
            return response;
        } catch (Exception failure) {
            return failed(record);
        }
    }

    private boolean claim(String tenant, String requestId, SQSEvent.SQSMessage record, String point) {
        String faultId = tenant + "#TRANSITION#" + point;
        var armed = dynamo().getItem(GetItemRequest.builder().tableName(faultTable()).consistentRead(true)
            .key(Map.of("fault_id", AttributeValue.fromS(faultId))).build());
        if (!armed.hasItem()) { return false; }
        try {
            dynamo().putItem(PutItemRequest.builder().tableName(faultTable())
                .conditionExpression("attribute_not_exists(fault_id)")
                .item(Map.of("fault_id", AttributeValue.fromS(faultId + "#CLAIM"),
                    "point", AttributeValue.fromS(point), "request_id", AttributeValue.fromS(requestId),
                    "body_sha256", AttributeValue.fromS(bodyDigest(record.getBody())),
                    "message_id", AttributeValue.fromS(record.getMessageId()),
                    "delegate", AttributeValue.fromS(AwsDurableTransitionWorkerHandler.class.getName()))).build());
            return true;
        } catch (ConditionalCheckFailedException consumed) {
            return false;
        }
    }

    private SQSBatchResponse failed(SQSEvent.SQSMessage record) {
        return new SQSBatchResponse(List.of(new SQSBatchResponse.BatchItemFailure(record.getMessageId())));
    }

    private synchronized DynamoDbClient dynamo() {
        if (client == null) {
            client = DynamoDbClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build();
        }
        return client;
    }

    private String faultTable() {
        if (table == null) { table = System.getenv("TPF_CONFORMANCE_TRANSITION_FAULT_TABLE"); }
        if (table == null || table.isBlank()) { throw new IllegalStateException("Transition fault table is required"); }
        return table;
    }

    static String bodyDigest(String body) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    @PreDestroy
    synchronized void close() { if (client != null) { client.close(); } }
}
