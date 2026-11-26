package org.pipelineframework.orchestrator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Provisions LocalStack DynamoDB tables matching TPF's production {@link DynamoExecutionStateStore}
 * schema and constructs a real {@link DynamoExecutionStateStore} for equivalence verification.
 */
public final class DynamoProofStoreHelper implements AutoCloseable {

    public static final String EXECUTION_TABLE = "polyglot-proof-execution";
    public static final String EXECUTION_KEY_TABLE = "polyglot-proof-execution-key";
    public static final String EXECUTION_PAYLOAD_TABLE = "polyglot-proof-execution-payload";
    public static final String AWAIT_COMPLETION_TABLE = "polyglot-proof-await-completion";

    private final LocalStackContainer container = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:3.8")).withServices("dynamodb");
    private final DynamoDbClient client;

    public DynamoProofStoreHelper() {
        container.start();
        try {
            this.client = DynamoDbClient.builder()
                .endpointOverride(container.getEndpoint())
                .region(Region.of(container.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(container.getAccessKey(), container.getSecretKey())))
                .build();
            createTable(EXECUTION_TABLE, "tenant_id", "execution_id");
            createTable(EXECUTION_KEY_TABLE, "tenant_execution_key", null);
            createTable(EXECUTION_PAYLOAD_TABLE, "payload_id", "payload_part");
            createTable(AWAIT_COMPLETION_TABLE, "pk", "sk");
        } catch (RuntimeException | Error failure) {
            close();
            throw failure;
        }
    }

    public DynamoExecutionStateStore createExecutionStateStore(PipelineOrchestratorConfig config) {
        return new DynamoExecutionStateStore(client, config);
    }

    public static PipelineOrchestratorConfig.DynamoConfig dynamoConfig() {
        return new PipelineOrchestratorConfig.DynamoConfig() {
            @Override public String executionTable() { return EXECUTION_TABLE; }
            @Override public String executionKeyTable() { return EXECUTION_KEY_TABLE; }
            @Override public String executionPayloadTable() { return EXECUTION_PAYLOAD_TABLE; }
            @Override public String awaitCompletionLifecycleTable() { return AWAIT_COMPLETION_TABLE; }
            @Override public String awaitUnitTable() { return ""; }
            @Override public String awaitInteractionTable() { return ""; }
            @Override public String awaitInteractionKeyTable() { return ""; }
            @Override public String awaitAdmissionTable() { return ""; }
            @Override public String releaseTable() { return ""; }
            @Override public String workerTable() { return ""; }
            @Override public Optional<String> endpointOverride() { return Optional.empty(); }
            @Override public Optional<String> region() { return Optional.empty(); }
        };
    }

    private void createTable(String tableName, String hashKey, String rangeKey) {
        var attrs = new LinkedHashMap<String, ScalarAttributeType>();
        var keys = new ArrayList<KeySchemaElement>();
        attrs.put(hashKey, ScalarAttributeType.S);
        keys.add(KeySchemaElement.builder().attributeName(hashKey).keyType(KeyType.HASH).build());
        if (rangeKey != null) {
            attrs.put(rangeKey, ScalarAttributeType.S);
            keys.add(KeySchemaElement.builder().attributeName(rangeKey).keyType(KeyType.RANGE).build());
        }
        client.createTable(CreateTableRequest.builder()
            .tableName(tableName)
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .keySchema(keys)
            .attributeDefinitions(attrs.entrySet().stream()
                .map(e -> AttributeDefinition.builder().attributeName(e.getKey()).attributeType(e.getValue()).build())
                .toList())
            .build());
        client.waiter().waitUntilTableExists(r -> r.tableName(tableName));
    }

    @Override
    public void close() {
        try {
            if (client != null) client.close();
        } finally {
            container.close();
        }
    }
}
