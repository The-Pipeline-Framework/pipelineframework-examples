package com.example.echo.application;

import com.example.echo.EchoInput;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandIdGenerator;
import org.pipelineframework.execution.PipelineExecutionContextHolder;

/** One Echo effect per execution, including retries of that execution. */
@ApplicationScoped
public class StableId implements CommandIdGenerator<EchoInput> {
    @Override
    public String commandId(CommandDescriptor descriptor, EchoInput input) {
        return PipelineExecutionContextHolder.get()
                .orElseThrow(() -> new IllegalStateException("Echo requires an execution context"))
                .executionId();
    }
}
