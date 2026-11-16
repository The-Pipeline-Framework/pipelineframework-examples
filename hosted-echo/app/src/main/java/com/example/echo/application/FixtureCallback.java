package com.example.echo.application;

import jakarta.enterprise.context.ApplicationScoped;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.pipelineframework.connector.*;

/** Local fixture waits for host-managed completion and has no public HTTP callback. */
@ApplicationScoped
public class FixtureCallback implements ProviderCallbackEndpointResolver, ProviderCallbackAuthenticator {
    @Override public URI resolve(ProviderCallbackRequest request) {
        return URI.create("https://hosted-echo.invalid");
    }

    @Override public CompletionStage<Optional<ProviderCallbackActor>> authenticate(
            ProviderCallbackAuthenticationRequest request) {
        return CompletableFuture.completedStage(Optional.empty());
    }
}
