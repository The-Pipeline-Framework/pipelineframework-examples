package com.example.echo.application;

import com.example.echo.EchoAcknowledgement;
import com.example.echo.EchoCompletion;
import com.example.echo.EchoInput;
import com.example.echo.EchoOutput;

import org.pipelineframework.awaitable.AwaitCompletionMetadata;
import org.pipelineframework.awaitable.CommandCompletionProjector;
import org.pipelineframework.awaitable.CommandCompletionRejectedException;

public final class EchoProjector implements CommandCompletionProjector<EchoInput, EchoAcknowledgement, EchoCompletion, EchoOutput> {
    @Override public EchoOutput project(EchoInput input, EchoAcknowledgement acknowledgement,
            EchoCompletion completion, AwaitCompletionMetadata metadata) {
        if (!input.value().equals(acknowledgement.value()) || !input.value().equals(completion.value())) {
            throw new CommandCompletionRejectedException("Fixture completion must match the acknowledged input");
        }
        return new EchoOutput("release-A:" + completion.value());
    }
}
