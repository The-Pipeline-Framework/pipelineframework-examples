package org.pipelineframework.awsconformance;

import jakarta.enterprise.context.ApplicationScoped;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.awsconformance.common.domain.Approval;

/** Initiates the next approval in the same TPF execution. */
@ApplicationScoped
@PipelineStep
public final class SecondApprovalService implements ReactiveService<Approval, Approval> {
    @Override
    public Uni<Approval> process(Approval input) {
        return Uni.createFrom().item(input);
    }
}
