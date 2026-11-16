package org.pipelineframework.examples.shared.parcel;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
@PipelineStep @ApplicationScoped
public class LabelService implements ReactiveService<Measured, Label> {
 public Uni<Label> process(Measured input) { return Uni.createFrom().item(new Label(input.parcelId(), "POST-" + input.charge())); }
}
