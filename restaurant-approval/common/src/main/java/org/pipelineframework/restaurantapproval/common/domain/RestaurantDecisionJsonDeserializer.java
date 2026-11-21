package org.pipelineframework.restaurantapproval.common.domain;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

public final class RestaurantDecisionJsonDeserializer extends JsonDeserializer<RestaurantDecision> {

  @Override
  public RestaurantDecision deserialize(JsonParser parser, DeserializationContext context)
      throws IOException {
    JsonNode node = parser.getCodec().readTree(parser);
    if (node.has("value") && node.size() == 1) node = node.get("value");
    if (!node.isObject()) throw new IOException("Decision must be an object");
    boolean accepted = node.hasNonNull("accepted");
    boolean declined = node.hasNonNull("declined");
    if (accepted && declined) throw new IOException("Decision must contain exactly one variant");
    if (accepted || declined) {
      String selected = accepted ? "accepted" : "declined";
      for (String field : new String[] {"type", "discriminator"}) {
        if (node.hasNonNull(field) && !selected.equals(node.get(field).asText()))
          throw new IOException("Decision discriminator conflicts with its payload");
      }
    }
    String type = resolveType(node);
    if ("accepted".equals(type)) {
      JsonNode payload = node.has("accepted") ? node.get("accepted") : node;
      return new RestaurantDecision.Accepted(new RestaurantOrderAccepted(
          uuid(payload, "orderId"),
          instant(payload, "decidedAt"),
          text(payload, "note")));
    }
    if ("declined".equals(type)) {
      JsonNode payload = node.has("declined") ? node.get("declined") : node;
      return new RestaurantDecision.Declined(new RestaurantOrderDeclined(
          uuid(payload, "orderId"),
          instant(payload, "decidedAt"),
          text(payload, "note"),
          text(payload, "declineReason")));
    }
    throw new IOException("Unsupported RestaurantDecision type: " + type);
  }

  private String resolveType(JsonNode node) {
    String type = node.path("type").asText(null);
    if (type != null && !type.isBlank()) {
      return type;
    }
    if (node.hasNonNull("accepted")) {
      return "accepted";
    }
    if (node.hasNonNull("declined")) {
      return "declined";
    }
    if (node.hasNonNull("declineReason")) {
      return "declined";
    }
    if (node.hasNonNull("orderId") && node.hasNonNull("decidedAt")) {
      return "accepted";
    }
    return null;
  }

  private static UUID uuid(JsonNode node, String fieldName) throws IOException {
    String value = text(node, fieldName);
    if (value == null || value.isBlank()) {
      throw new IOException("Required field '" + fieldName + "' is missing or empty");
    }
    try {
      return UUID.fromString(value);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid UUID format for field '" + fieldName + "': " + value, e);
    }
  }

  private static Instant instant(JsonNode node, String fieldName) throws IOException {
    String value = text(node, fieldName);
    if (value == null || value.isBlank()) {
      throw new IOException("Required field '" + fieldName + "' is missing or empty");
    }
    try {
      return Instant.parse(value);
    } catch (Exception e) {
      throw new IOException("Invalid Instant format for field '" + fieldName + "': " + value, e);
    }
  }

  private static String text(JsonNode node, String fieldName) {
    JsonNode value = node.get(fieldName);
    return value == null || value.isNull() ? null : value.asText();
  }
}
