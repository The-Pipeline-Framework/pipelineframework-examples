package org.pipelineframework.restaurantapproval.create_pending_approval.service;

import org.pipelineframework.restaurantapproval.common.domain.ValidatedRestaurantOrderRequest;
import org.pipelineframework.restaurantapproval.common.domain.PendingRestaurantApproval;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@PipelineStep
@ApplicationScoped
public class ProcessCreatePendingApprovalService
    implements ReactiveService<ValidatedRestaurantOrderRequest, PendingRestaurantApproval> {

  private final Clock clock = Clock.systemUTC();

  @Override
  public Uni<PendingRestaurantApproval> process(ValidatedRestaurantOrderRequest input) {
    return Uni.createFrom().item(() -> {
      Objects.requireNonNull(input, "input must not be null");
      return new PendingRestaurantApproval(UUID.randomUUID(), input.requestId(), input.customerName(),
          input.restaurantName(), input.items(), input.totalAmount(), input.currency(), Instant.now(clock));
    });
  }
}
