package org.pipelineframework.restaurantapproval.common.domain;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@JsonTypeName("declined")
public record RestaurantOrderDeclined(
    UUID orderId,
    @com.fasterxml.jackson.annotation.JsonFormat(shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING)
    Instant decidedAt,
    String note,
    String declineReason
) implements java.io.Serializable {

  public RestaurantOrderDeclined {
    Objects.requireNonNull(orderId, "orderId must not be null");
    Objects.requireNonNull(decidedAt, "decidedAt must not be null");
    note = note == null ? "" : note.trim();
    declineReason = Objects.requireNonNullElse(declineReason, "no reason provided").trim();
    if (declineReason.isBlank()) {
      declineReason = "no reason provided";
    }
  }

  public String discriminator() { return "declined"; }

  public <R> R accept(RestaurantDecisionVisitor<R> visitor) {
    return visitor.declined(this);
  }

  public TerminalOrderState toTerminalOrderState() {
    return new TerminalOrderState(orderId, "DECLINED", decidedAt, "DECLINED",
        note.isBlank() ? declineReason : note + " (" + declineReason + ")");
  }
}
