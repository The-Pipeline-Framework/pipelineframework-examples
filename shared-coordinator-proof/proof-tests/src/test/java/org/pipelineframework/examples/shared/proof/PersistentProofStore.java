package org.pipelineframework.examples.shared.proof;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/** Local disposable persistence, deliberately owned only by the Coordinator subprocess. */
final class PersistentProofStore implements AutoCloseable {
    private final LocalStackContainer container = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
        .withServices("dynamodb");
    private DynamoDbClient client;
    private final Map<String, String> properties = new LinkedHashMap<>();

    PersistentProofStore() {
        try {
            container.start(); // Required proof: unavailable persistence fails, never silently skips acceptance.
            client = DynamoDbClient.builder().endpointOverride(container.getEndpoint()).region(Region.of(container.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(container.getAccessKey(), container.getSecretKey()))).build();
            properties.put("pipeline.orchestrator.state-provider", "dynamo");
            properties.put("pipeline.orchestrator.releases.registry.provider", "dynamo");
            properties.put("pipeline.orchestrator.worker.lifecycle.provider", "dynamo");
            properties.put("pipeline.orchestrator.dynamo.endpoint-override", container.getEndpoint().toString());
            properties.put("pipeline.orchestrator.dynamo.region", container.getRegion());
            properties.put("aws.accessKeyId", container.getAccessKey());
            properties.put("aws.secretAccessKey", container.getSecretKey());
            table("execution", "execution-table", "tenant_id", "execution_id", ScalarAttributeType.S, List.of());
            table("execution-key", "execution-key-table", "tenant_execution_key", null, null, List.of());
            table("payload", "execution-payload-table", "payload_id", "payload_part", ScalarAttributeType.S, List.of());
            table("unit", "await-unit-table", "tenant_id", "unit_id", ScalarAttributeType.S, List.of());
            table("interaction-key", "await-interaction-key-table", "lookup_key", null, null, List.of());
            table("completion", "await-completion-lifecycle-table", "pk", "sk", ScalarAttributeType.S, List.of());
            table("admission", "await-admission-table", "scope_key", "slot", ScalarAttributeType.N, List.of());
            table("release", "release-table", "registry_key", "registry_sort", ScalarAttributeType.S, List.of());
            table("worker", "worker-table", "registry_key", "registry_sort", ScalarAttributeType.S, List.of());
            table("interaction", "await-interaction-table", "tenant_id", "interaction_id", ScalarAttributeType.S, List.of(
                index("await-interaction-by-unit", "query_unit_key", "query_unit_sort", ScalarAttributeType.S),
                index("await-interaction-by-execution", "query_execution_key", "query_execution_sort", ScalarAttributeType.S),
                index("await-interaction-pending-by-tenant", "query_pending_tenant_key", "query_pending_deadline_sort", ScalarAttributeType.S),
                index("await-interaction-pending-by-assignee", "query_pending_assignee_key", "query_pending_deadline_sort", ScalarAttributeType.S),
                index("await-interaction-pending-by-group", "query_pending_group_key", "query_pending_deadline_sort", ScalarAttributeType.S),
                index("await-interaction-pending-by-step", "query_pending_step_key", "query_pending_deadline_sort", ScalarAttributeType.S),
                index("await-interaction-pending-by-deadline", "query_deadline_key", "query_deadline_sort", ScalarAttributeType.S),
                index("await-interaction-continuation-work", "query_continuation_key", "query_continuation_due_epoch_ms", ScalarAttributeType.N)));
        } catch (RuntimeException | Error failure) {
            try { close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    Map<String, String> properties() { return Map.copyOf(properties); }
    private record Index(String name, String hash, String range, ScalarAttributeType rangeType) {}
    private static Index index(String name, String hash, String range, ScalarAttributeType type) { return new Index(name, hash, range, type); }
    private void table(String name, String property, String hash, String range, ScalarAttributeType rangeType, List<Index> indexes) {
        String table = "proof-" + name;
        properties.put("pipeline.orchestrator.dynamo." + property, table);
        var attrs = new LinkedHashMap<String, ScalarAttributeType>(); attrs.put(hash, ScalarAttributeType.S);
        var keys = new ArrayList<KeySchemaElement>(); keys.add(key(hash, KeyType.HASH));
        if (range != null) { attrs.put(range, rangeType); keys.add(key(range, KeyType.RANGE)); }
        var gsis = new ArrayList<GlobalSecondaryIndex>();
        for (var index : indexes) {
            attrs.put(index.hash(), ScalarAttributeType.S); attrs.put(index.range(), index.rangeType());
            gsis.add(GlobalSecondaryIndex.builder().indexName(index.name()).keySchema(key(index.hash(), KeyType.HASH), key(index.range(), KeyType.RANGE))
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build()).build());
        }
        var request = CreateTableRequest.builder().tableName(table).billingMode(BillingMode.PAY_PER_REQUEST).keySchema(keys)
            .attributeDefinitions(attrs.entrySet().stream().map(entry -> AttributeDefinition.builder().attributeName(entry.getKey()).attributeType(entry.getValue()).build()).toList());
        if (!gsis.isEmpty()) request.globalSecondaryIndexes(gsis);
        client.createTable(request.build()); client.waiter().waitUntilTableExists(r -> r.tableName(table));
    }
    private static KeySchemaElement key(String name, KeyType type) { return KeySchemaElement.builder().attributeName(name).keyType(type).build(); }
    public void close() {
        try { if (client != null) client.close(); }
        finally { container.close(); }
    }
}
