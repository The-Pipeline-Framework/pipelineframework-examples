package org.pipelineframework.examples.shared.expense;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
@PipelineStep @ApplicationScoped
public class FinishService implements ReactiveService<Approved, Receipt> {
 public Uni<Receipt> process(Approved input) { return Uni.createFrom().item(new Receipt(input.requestId(), input.approved() ? "approved" : "declined", "A1")); }
}
