package org.pipelineframework.restaurantapproval.common.domain;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@JsonTypeName("accepted")
public record RestaurantOrderAccepted(
    UUID orderId,
    @com.fasterxml.jackson.annotation.JsonFormat(shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING)
    Instant decidedAt,
    String note
) {

  public RestaurantOrderAccepted {
    Objects.requireNonNull(orderId, "orderId must not be null");
    Objects.requireNonNull(decidedAt, "decidedAt must not be null");
    note = note == null ? "" : note.trim();
  }

  public String discriminator() { return "accepted"; }

  public <R> R accept(RestaurantDecisionVisitor<R> visitor) {
    return visitor.accepted(this);
  }

  public TerminalOrderState toTerminalOrderState() {
    return new TerminalOrderState(orderId, "APPROVED", decidedAt, "ACCEPTED",
        note.isBlank() ? "Restaurant approved the order." : note);
  }
}
