package org.pipelineframework.restaurantapproval.common.domain;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

public final class RestaurantDecisionJsonSerializer extends JsonSerializer<RestaurantDecision> {

  /** Honors Jackson's caller-owned type envelope at compiled canonical boundaries. */
  @Override
  public void serializeWithType(RestaurantDecision value, JsonGenerator gen, SerializerProvider serializers,
      com.fasterxml.jackson.databind.jsontype.TypeSerializer typeSerializer) throws IOException {
    var id = typeSerializer.writeTypePrefix(gen, typeSerializer.typeId(value, com.fasterxml.jackson.core.JsonToken.START_OBJECT));
    if (value instanceof RestaurantDecision.Accepted variant) {
      var accepted = variant.value();
      gen.writeStringField("orderId", accepted.orderId().toString());
      gen.writeStringField("decidedAt", accepted.decidedAt().toString());
      gen.writeStringField("note", accepted.note());
    } else if (value instanceof RestaurantDecision.Declined variant) {
      var declined = variant.value();
      gen.writeStringField("orderId", declined.orderId().toString());
      gen.writeStringField("decidedAt", declined.decidedAt().toString());
      gen.writeStringField("note", declined.note());
      gen.writeStringField("declineReason", declined.declineReason());
    }
    typeSerializer.writeTypeSuffix(gen, id);
  }

  @Override
  public void serialize(RestaurantDecision value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeStartObject();
    if (value instanceof RestaurantDecision.Accepted variant) {
      var accepted = variant.value();
      gen.writeStringField("type", "accepted");
      gen.writeObjectFieldStart("accepted");
      gen.writeStringField("orderId", accepted.orderId().toString());
      gen.writeStringField("decidedAt", accepted.decidedAt().toString());
      gen.writeStringField("note", accepted.note());
      gen.writeEndObject();
    } else if (value instanceof RestaurantDecision.Declined variant) {
      var declined = variant.value();
      gen.writeStringField("type", "declined");
      gen.writeObjectFieldStart("declined");
      gen.writeStringField("orderId", declined.orderId().toString());
      gen.writeStringField("decidedAt", declined.decidedAt().toString());
      gen.writeStringField("note", declined.note());
      gen.writeStringField("declineReason", declined.declineReason());
      gen.writeEndObject();
    } else {
      throw new IOException("Unsupported RestaurantDecision subtype: " + value.getClass().getName());
    }
    gen.writeEndObject();
  }
}
