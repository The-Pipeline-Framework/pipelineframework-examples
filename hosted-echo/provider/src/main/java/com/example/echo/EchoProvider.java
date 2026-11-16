package com.example.echo;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.pipelineframework.connector.*;

@ApplicationScoped
public class EchoProvider implements ConnectorProvider<Void> {
    public record Config() {}

    @Override public ConnectorProviderId id() { return ConnectorProviderId.of("fixture.echo"); }
    @Override public ConnectorProviderVersion version() { return new ConnectorProviderVersion(1, 0); }

    @Override public Collection<? extends ConnectorOperation> operations() {
        return List.of(new DeferredEcho());
    }

    static final class DeferredEcho implements CommandOperation<EchoInput, Config, EchoAcknowledgement> {
        @Override public String id() { return "echo"; }
        @Override public Optional<ConnectorConfigSchema<Config>> configurationSchema() {
            return Optional.of(ConnectorConfigSchema.record(Config.class, "fixture.echo.config", 1));
        }
        @Override public List<ConnectorOperationCallbackDescriptor> callbacks() {
            return List.of(new ConnectorOperationCallbackDescriptor("completed",
                    new ConnectorOperationTypeContract("EchoCompletion", Optional.empty()), true));
        }
        @Override public CommandCapabilities capabilities() {
            return new CommandCapabilities(false, false, false, CommandExecutionPosture.AUTOMATED,
                    CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false, Set.of());
        }
        @Override public CompletionStage<CommandOutcome<EchoAcknowledgement>> dispatch(
                CommandInvocation<EchoInput, Config> invocation) {
            return CompletableFuture.completedStage(new CommandOutcome.Succeeded<>(
                    new EchoAcknowledgement(invocation.input().value()),
                    new CommandConfirmation(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false), List.of()));
        }
    }
}
