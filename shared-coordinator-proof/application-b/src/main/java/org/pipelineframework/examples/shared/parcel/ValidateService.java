package org.pipelineframework.examples.shared.parcel;

import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.service.ReactiveService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@PipelineStep @ApplicationScoped
public class ValidateService implements ReactiveService<Request, ValidatedRequest> {
    public Uni<ValidatedRequest> process(Request input) {
        if (input.parcelId() == null || input.parcelId().isBlank() || input.weight() <= 0) {
            return Uni.createFrom().failure(new IllegalArgumentException("Parcel identity and positive weight are required"));
        }
        return Uni.createFrom().item(new ValidatedRequest(input.parcelId(), input.weight()));
    }
}
