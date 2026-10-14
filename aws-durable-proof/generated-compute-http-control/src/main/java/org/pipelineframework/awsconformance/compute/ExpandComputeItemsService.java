package org.pipelineframework.awsconformance.compute;

import jakarta.enterprise.context.ApplicationScoped;
import io.smallrye.mutiny.Multi;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.awsconformance.items.common.domain.Approval;
import org.pipelineframework.service.ReactiveStreamingService;

/** Pure bounded expansion used to exercise the ordinary COMPUTE SSE boundary. */
@ApplicationScoped
@PipelineStep
public final class ExpandComputeItemsService implements ReactiveStreamingService<Approval, Approval> {
    @Override
    public Multi<Approval> process(Approval input) {
        if (input.request().equals("prefix-failure")) {
            return Multi.createBy().concatenating().streams(
                Multi.createFrom().item(new Approval("approval-0")),
                Multi.createFrom().failure(new IllegalStateException("fixture prefix failure")));
        }
        int count = Integer.parseInt(input.request());
        if (count < 0 || count > 300) throw new IllegalArgumentException("approval count must be between 0 and 300");
        return Multi.createFrom().range(0, count).map(index -> new Approval("approval-" + index));
    }
}
