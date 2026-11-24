package org.pipelineframework.restaurantapproval.common.domain;

import java.io.Serializable;
import java.util.UUID;
import java.time.Instant;
import java.math.BigDecimal;

public record ValidatedRestaurantOrderRequest(
    UUID requestId,
    String customerName,
    String restaurantName,
    String items,
    BigDecimal totalAmount,
    String currency,
    @com.fasterxml.jackson.annotation.JsonFormat(shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING)
    Instant validatedAt
) implements Serializable {}
