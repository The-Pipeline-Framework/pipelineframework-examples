package org.pipelineframework.examples.shared.expense;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
@PipelineStep @ApplicationScoped
public class PrepareService implements ReactiveService<Request, Pending> {
 public Uni<Pending> process(Request input) { return Uni.createFrom().item(new Pending(input.requestId(), input.amount(), "A2")); }
}
