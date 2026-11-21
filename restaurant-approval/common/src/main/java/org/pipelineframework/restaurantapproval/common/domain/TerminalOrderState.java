package org.pipelineframework.restaurantapproval.common.domain;

import java.io.Serializable;
import java.util.UUID;
import java.time.Instant;

public record TerminalOrderState(
    UUID orderId,
    String outcome,
    @com.fasterxml.jackson.annotation.JsonFormat(shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING)
    Instant resolvedAt,
    String restaurantStatus,
    String summary
) implements Serializable {}
