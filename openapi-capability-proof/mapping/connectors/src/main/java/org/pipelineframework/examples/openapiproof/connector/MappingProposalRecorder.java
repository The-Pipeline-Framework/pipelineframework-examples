package org.pipelineframework.examples.openapiproof.connector;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.blocks.openapi.mapping.OpenApiMappingTurn;

/** Test observations shared across provider instances; never used to select proposals. */
@ApplicationScoped
public class MappingProposalRecorder {
    private final List<OpenApiMappingTurn> inputs = new CopyOnWriteArrayList<>();

    public void record(OpenApiMappingTurn turn) { inputs.add(turn); }

    public List<OpenApiMappingTurn> inputs() { return List.copyOf(inputs); }

    public void reset() { inputs.clear(); }
}
