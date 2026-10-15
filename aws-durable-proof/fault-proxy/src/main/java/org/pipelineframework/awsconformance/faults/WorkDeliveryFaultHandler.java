package org.pipelineframework.awsconformance.faults;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.aws.durable.AwsDurableWorkItemHandler;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

/** Fixture-only delivery cuts. An ACK is not evidence of semantic progress. */
@Named("conformance-work-delivery")
@ApplicationScoped
public final class WorkDeliveryFaultHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
    @Inject
    AwsDurableWorkItemHandler delegate;
    @Inject
    PipelineControlPlane controlPlane;
    private Optional<DynamoDbClient> client = Optional.empty();
    private Optional<String> table = Optional.empty();

    public WorkDeliveryFaultHandler() { }

    public WorkDeliveryFaultHandler(AwsDurableWorkItemHandler delegate, DynamoDbClient client, String table,
        PipelineControlPlane controlPlane) {
        this.delegate = Objects.requireNonNull(delegate);
        this.client = Optional.of(Objects.requireNonNull(client));
        this.table = Optional.of(Objects.requireNonNull(table));
        this.controlPlane = Objects.requireNonNull(controlPlane);
    }

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        if (event.getRecords().size() != 1) {
            return new SQSBatchResponse(event.getRecords().stream()
                .map(record -> new SQSBatchResponse.BatchItemFailure(record.getMessageId())).toList());
        }
        var record = event.getRecords().get(0);
        ExecutionWorkItem work;
        try {
            var json = new ObjectMapper().readTree(record.getBody());
            if (!json.isObject() || json.size() != 2 || !json.path("tenantId").isTextual()
                || !json.path("executionId").isTextual() || json.path("tenantId").asText().isBlank()
                || json.path("executionId").asText().isBlank()) {
                return delegate.handleRequest(event, context);
            }
            work = new ExecutionWorkItem(json.path("tenantId").asText(), json.path("executionId").asText());
        } catch (Exception malformed) {
            return delegate.handleRequest(event, context);
        }
        try {
            if (claim(work, record, "BEFORE_DELEGATE")) { return failed(record); }
            var response = delegate.handleRequest(event, context);
            if ((response.getBatchItemFailures() == null || response.getBatchItemFailures().isEmpty())
                && claim(work, record, "AFTER_ACK")) { return failed(record); }
            return response;
        } catch (Exception failure) { return failed(record); }
    }

    private boolean claim(ExecutionWorkItem work, SQSEvent.SQSMessage record, String point) {
        String faultId = work.tenantId() + "#WORK#" + point;
        String digest = digest(record.getBody());
        var armed = dynamo().getItem(GetItemRequest.builder().tableName(faultTable()).consistentRead(true)
            .key(Map.of("fault_id", AttributeValue.fromS(faultId))).build());
        var identity = Map.of("tenant_id", AttributeValue.fromS(work.tenantId()),
            "execution_id", AttributeValue.fromS(work.executionId()),
            "work_item_type", AttributeValue.fromS(ExecutionWorkItem.class.getName()),
            "body_sha256", AttributeValue.fromS(digest));
        if (!armed.hasItem() || identity.entrySet().stream()
            .anyMatch(entry -> !entry.getValue().equals(armed.item().get(entry.getKey())))) { return false; }
        var claim = new java.util.HashMap<>(identity);
        if (point.equals("AFTER_ACK")) {
            // An ACK can be a lease-losing no-op while another delivery still runs.
            // Observe committed native state before consuming this cut; never drive it.
            var checkpoint = controlPlane.getExecutionSemanticCheckpoint(work.tenantId(), work.executionId())
                .await().atMost(Duration.ofSeconds(15));
            if (checkpoint.status() != ExecutionStatus.WAITING_EXTERNAL
                || !checkpoint.tenantId().equals(work.tenantId()) || !checkpoint.executionId().equals(work.executionId())
                || !AttributeValue.fromS(checkpoint.pipelineId()).equals(armed.item().get("pipeline_id"))
                || !AttributeValue.fromS(checkpoint.contractVersion()).equals(armed.item().get("contract_version"))
                || !AttributeValue.fromS(checkpoint.releaseVersion()).equals(armed.item().get("release_version"))) { return false; }
            var current = controlPlane.getCurrentAwaitSemanticCheckpoint(work.tenantId(), work.executionId())
                .await().atMost(Duration.ofSeconds(15));
            if (current.isEmpty()) { return false; }
            var awaiting = current.orElseThrow();
            if (!awaiting.tenantId().equals(work.tenantId()) || !awaiting.executionId().equals(work.executionId())
                || !awaiting.pipelineId().equals(checkpoint.pipelineId()) || !awaiting.contractVersion().equals(checkpoint.contractVersion())
                || !awaiting.releaseVersion().equals(checkpoint.releaseVersion())
                || !awaiting.stepId().contains("FirstApproval")) { return false; }
            claim.put("execution_status", AttributeValue.fromS(checkpoint.status().name()));
            claim.put("pipeline_id", AttributeValue.fromS(checkpoint.pipelineId()));
            claim.put("contract_version", AttributeValue.fromS(checkpoint.contractVersion()));
            claim.put("release_version", AttributeValue.fromS(checkpoint.releaseVersion()));
            claim.put("interaction_id", AttributeValue.fromS(awaiting.interactionId()));
            claim.put("await_step_id", AttributeValue.fromS(awaiting.stepId()));
            claim.put("commit_observation", AttributeValue.fromS("PRODUCTION_CONTROL_PLANE_POINT_READS"));
        }
        claim.put("fault_id", AttributeValue.fromS(faultId + "#CLAIM"));
        claim.put("point", AttributeValue.fromS(point));
        claim.put("message_id", AttributeValue.fromS(record.getMessageId()));
        claim.put("delegate", AttributeValue.fromS(AwsDurableWorkItemHandler.class.getName()));
        try {
            dynamo().putItem(PutItemRequest.builder().tableName(faultTable())
                .conditionExpression("attribute_not_exists(fault_id)").item(Map.copyOf(claim)).build());
            return true;
        } catch (ConditionalCheckFailedException consumed) { return false; }
    }

    private SQSBatchResponse failed(SQSEvent.SQSMessage record) {
        return new SQSBatchResponse(List.of(new SQSBatchResponse.BatchItemFailure(record.getMessageId())));
    }

    private synchronized DynamoDbClient dynamo() {
        if (client.isEmpty()) {
            client = Optional.of(DynamoDbClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).build());
        }
        return client.orElseThrow();
    }

    private String faultTable() {
        if (table.isEmpty()) { table = Optional.ofNullable(System.getenv("TPF_CONFORMANCE_WORK_FAULT_TABLE")); }
        return table.filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalStateException("Work fault table is required"));
    }

    private String digest(String body) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    @PreDestroy
    synchronized void close() { client.ifPresent(DynamoDbClient::close); }
}
