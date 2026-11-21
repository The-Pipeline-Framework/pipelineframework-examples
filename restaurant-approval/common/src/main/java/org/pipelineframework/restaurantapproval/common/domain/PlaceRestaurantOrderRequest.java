package org.pipelineframework.restaurantapproval.common.domain;

import java.io.Serializable;
import java.util.UUID;
import java.math.BigDecimal;

public record PlaceRestaurantOrderRequest(
    UUID requestId,
    String customerName,
    String restaurantName,
    String items,
    BigDecimal totalAmount,
    String currency
) implements Serializable {}
