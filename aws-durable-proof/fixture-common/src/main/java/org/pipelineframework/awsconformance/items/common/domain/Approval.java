package org.pipelineframework.awsconformance.items.common.domain;

/** Canonical approval value owned by the item application. */
public record Approval(String request) {
    public Approval {
        if (request == null || request.isBlank()) throw new IllegalArgumentException("request is required");
    }
}
