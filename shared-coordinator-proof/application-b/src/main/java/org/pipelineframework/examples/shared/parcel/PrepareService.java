package org.pipelineframework.examples.shared.parcel;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
@PipelineStep @ApplicationScoped
public class PrepareService implements ReactiveService<ValidatedRequest, Measured> {
 public Uni<Measured> process(ValidatedRequest input) { return Uni.createFrom().item(new Measured(input.parcelId(), input.weight() * 3L)); }
}
