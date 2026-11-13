package org.pipelineframework.examples.shared.parcel;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
@PipelineStep @ApplicationScoped
public class FinishService implements ReactiveService<Label, Receipt> {
 public Uni<Receipt> process(Label input) { return Uni.createFrom().item(new Receipt(input.parcelId(), "B:" + input.label())); }
}
