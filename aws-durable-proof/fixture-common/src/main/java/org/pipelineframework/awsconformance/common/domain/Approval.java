package org.pipelineframework.awsconformance.common.domain;

/** The fixture's typed value; completion carries the same canonical contract. */
public record Approval(String request) {
    public Approval {
        if (request == null || request.isBlank()) {
            throw new IllegalArgumentException("request must not be blank");
        }
    }
}
