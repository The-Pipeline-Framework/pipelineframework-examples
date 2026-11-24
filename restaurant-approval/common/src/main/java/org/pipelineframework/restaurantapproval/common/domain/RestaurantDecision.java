package org.pipelineframework.restaurantapproval.common.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.Serializable;
import java.util.UUID;

@JsonSerialize(using = RestaurantDecisionJsonSerializer.class)
@JsonDeserialize(using = RestaurantDecisionJsonDeserializer.class)
public sealed interface RestaurantDecision extends Serializable
    permits RestaurantDecision.Accepted, RestaurantDecision.Declined {

  UUID orderId();

  /** Canonical union identity; legacy DTO/JSON presentation remains unchanged. */
  @com.fasterxml.jackson.annotation.JsonIgnore
  String discriminator();

  <R> R accept(RestaurantDecisionVisitor<R> visitor);

  TerminalOrderState toTerminalOrderState();
  record Accepted(RestaurantOrderAccepted value) implements RestaurantDecision {
    public Accepted { java.util.Objects.requireNonNull(value, "value"); }
    @Override public UUID orderId() { return value.orderId(); }
    @Override public String discriminator() { return "accepted"; }
    @Override public <R> R accept(RestaurantDecisionVisitor<R> visitor) { return visitor.accepted(value); }
    @Override public TerminalOrderState toTerminalOrderState() { return value.toTerminalOrderState(); }
  }
  record Declined(RestaurantOrderDeclined value) implements RestaurantDecision {
    public Declined { java.util.Objects.requireNonNull(value, "value"); }
    @Override public UUID orderId() { return value.orderId(); }
    @Override public String discriminator() { return "declined"; }
    @Override public <R> R accept(RestaurantDecisionVisitor<R> visitor) { return visitor.declined(value); }
    @Override public TerminalOrderState toTerminalOrderState() { return value.toTerminalOrderState(); }
  }
}
