package org.pipelineframework.examples.openapiproof.connector;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pipelineframework.blocks.openapi.mapping.OpenApiMappingCandidate;
import org.pipelineframework.blocks.openapi.mapping.OpenApiMappingTurn;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;

/** Supplies external proposals; the packaged Block owns validation and recursive control flow. */
@ApplicationScoped
public class MappingProposalConnector implements ConnectorProvider<ConnectorConfigurationDocument> {
    @Inject MappingProposalRecorder recorder;

    @Override
    public ConnectorProviderId id() { return ConnectorProviderId.of("proof.mapping"); }

    @Override
    public ConnectorProviderVersion version() { return new ConnectorProviderVersion(1, 0); }

    @Override
    public Collection<? extends ConnectorOperation> operations() { return List.of(new Propose()); }

    public record ProposalConfiguration(String instructions, List<String> modelInputExcludes, Map<String, String> completion) {}

    private final class Propose
            implements QueryOperation<OpenApiMappingTurn, ProposalConfiguration, OpenApiMappingCandidate> {
        @Override
        public String id() { return "decide"; }

        @Override
        public Optional<ConnectorConfigSchema<ProposalConfiguration>> configurationSchema() {
            return Optional.of(ConnectorConfigSchema.record(ProposalConfiguration.class, "proof.mapping.proposal", 1));
        }

        @Override
        public CompletionStage<QueryOutcome<OpenApiMappingCandidate>> query(
                QueryInvocation<OpenApiMappingTurn, ProposalConfiguration, OpenApiMappingCandidate> invocation) {
            var turn = invocation.input();
            recorder.record(turn);
            String options = turn.diagnostics().isEmpty()
                ? "{\"fields\":{\"evidence\":\"payload.missing\"}}"
                : "{\"fields\":{\"evidence\":\"payload.value\"}}";
            return CompletableFuture.completedStage(new QueryOutcome.Found<>(
                new OpenApiMappingCandidate(turn.state(), options)));
        }
    }
}
