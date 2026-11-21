package org.pipelineframework.restaurantapproval.orchestrator.compat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.pipelineframework.config.pipeline.PipelineJson;

/** Keeps the existing Restaurant UI completion JSON at the generated v3 REST boundary. */
@jakarta.enterprise.context.ApplicationScoped
public class LegacyDecisionCompletionFilter {
  @org.jboss.resteasy.reactive.server.ServerRequestFilter(readBody = true, priority = Priorities.ENTITY_CODER)
  public void filter(ContainerRequestContext request) throws IOException {
    if (!"POST".equals(request.getMethod())
        || !"pipeline/interactions/complete".equals(request.getUriInfo().getPath().replaceFirst("^/", ""))) return;
    byte[] original = request.getEntityStream().readAllBytes();
    final JsonNode body;
    try { body = PipelineJson.mapper().readTree(original); }
    catch (IOException invalid) { throw new BadRequestException("Invalid completion JSON"); }
    if (body instanceof ObjectNode object && object.get("responsePayload") instanceof ObjectNode payload) {
      boolean accepted = payload.hasNonNull("accepted");
      boolean declined = payload.hasNonNull("declined");
      if (accepted && declined) throw new BadRequestException("Decision must contain exactly one variant");
      if (accepted != declined) {
        String selected = accepted ? "accepted" : "declined";
        JsonNode discriminator = payload.get("discriminator");
        JsonNode legacyType = payload.get("type");
        if (discriminator != null && !selected.equals(discriminator.asText())
            || legacyType != null && !selected.equals(legacyType.asText()))
          throw new BadRequestException("Decision discriminator conflicts with its payload");
        payload.put("discriminator", selected);
        payload.remove("type");
        original = PipelineJson.mapper().writeValueAsBytes(body);
      }
    }
    request.setEntityStream(new ByteArrayInputStream(original));
    request.getHeaders().putSingle("Content-Length", Integer.toString(original.length));
  }
}
