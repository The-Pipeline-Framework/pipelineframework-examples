package org.example.pipeline;

import io.quarkus.security.Authenticated;
import io.smallrye.common.annotation.Blocking;
import io.vertx.core.http.HttpServerResponse;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.io.InputStream;
import java.lang.IllegalArgumentException;
import java.lang.IllegalStateException;
import java.lang.SecurityException;
import java.lang.String;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.pipelineframework.config.boundary.PipelineHttpPayloadBoundaryConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.OwnedPayloadMultipart;
import org.pipelineframework.connector.OwnedPayloadTransfer;
import org.pipelineframework.connector.PayloadBoundaryAuthorizer;
import org.pipelineframework.connector.PayloadBoundaryRuntimeConfig;
import org.pipelineframework.connector.PayloadMediaTypeException;
import org.pipelineframework.connector.PayloadTooLargeException;
import org.pipelineframework.repository.PayloadReference;

@Path("/tpf/payloads/invoice/upload")
@Authenticated
@Blocking
public class GeneratedPayloadBoundary0 {
  private static final PipelineHttpPayloadBoundaryConfig BOUNDARY = new PipelineHttpPayloadBoundaryConfig("invoice", PipelineHttpPayloadBoundaryConfig.Direction.UPLOAD, "invoices", "InvoiceInput", "payload_ref", java.util.List.of("application/pdf"), 1024L, "invoice.write");

  private static final PipelineYamlConfig CONFIG = PayloadBoundaryRuntimeConfig.load();

  @Inject
  private ConnectorBindingRegistry bindings;

  @Inject
  private ConnectorRuntimeContext runtimeContext;

  @Inject
  private Instance<PayloadBoundaryAuthorizer> authorizers;

  private OwnedPayloadTransfer transfer() {
    if (authorizers.isUnsatisfied() || authorizers.isAmbiguous()) throw new IllegalStateException("exactly one payload boundary authorizer is required");
    return new OwnedPayloadTransfer(bindings, runtimeContext, authorizers.get());
  }

  private static String principal(SecurityContext security) {
    if (security == null || security.getUserPrincipal() == null) throw new WebApplicationException(401);
    return security.getUserPrincipal().getName();
  }

  @POST
  @Consumes("*/*")
  @Produces("application/json")
  @Operation(
      operationId = "uploadPayloadinvoice"
  )
  public Map<String, PayloadReference> upload(
      @RequestBody(content = @Content(mediaType = "multipart/form-data")) InputStream body,
      @HeaderParam("Content-Type") String contentType, @HeaderParam("X-Tenant-Id") String tenant,
      @HeaderParam("X-Scope-Id") String scope, @Context SecurityContext security,
      @Context HttpServerResponse httpResponse) throws IOException {
    var target = CONFIG.publish().get(BOUNDARY.objectName());
    if (target == null) throw new IllegalStateException("payload target is unavailable");
    var cancelled = new AtomicBoolean();
    httpResponse.closeHandler(ignored -> cancelled.set(true));
    try (var input = new OwnedPayloadMultipart(body, contentType)) {
      var reference = transfer().upload(BOUNDARY, target, principal(security), tenant, scope, input.mediaType(), input, cancelled::get);
      return Map.of(BOUNDARY.referenceField(), reference);
    } catch (PayloadTooLargeException oversized) {
      throw new WebApplicationException(413);
    } catch (PayloadMediaTypeException media) {
      throw new WebApplicationException(415);
    } catch (SecurityException denied) {
      throw new ForbiddenException();
    } catch (IllegalArgumentException invalid) {
      throw new BadRequestException();
    }
  }
}
