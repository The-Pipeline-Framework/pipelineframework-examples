package org.pipelineframework.telemetry;

import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.branching.BranchVariantIdentity;
import org.pipelineframework.config.ParallelismPolicy;

/** Lightweight no-op telemetry implementation for standalone proof tests. */
public final class NoOpProofTelemetry implements PipelineRunTelemetry, PipelineStepTelemetry.Seam {
    @Override
    public PipelineRunContext startRun(Object input, int stepCount, ParallelismPolicy policy, int maxConcurrency) {
        return PipelineRunTelemetry.nonOwningContext();
    }
    @Override public Object instrumentInput(Object input, PipelineRunContext context) { return input; }
    @Override public Object instrumentRunCompletion(Object output, PipelineRunContext context) { return output; }
    @Override public void abortRun(PipelineRunContext context, Throwable failure) { }
    @Override public void abortActiveRun(Throwable failure) { }
    @Override public <T> Uni<T> instrumentItemConsumed(Class<?> s, Object c, Uni<T> u) { return u; }
    @Override public <T> Multi<T> instrumentItemConsumed(Class<?> s, Object c, Multi<T> m) { return m; }
    @Override public <T> Uni<T> instrumentItemConsumed(Class<?> s, Uni<T> u) { return u; }
    @Override public <T> Multi<T> instrumentItemConsumed(Class<?> s, Multi<T> m) { return m; }
    @Override public <T> Uni<T> instrumentItemProduced(Class<?> s, Object c, Uni<T> u) { return u; }
    @Override public <T> Multi<T> instrumentItemProduced(Class<?> s, Object c, Multi<T> m) { return m; }
    @Override public <T> Uni<T> instrumentItemProduced(Class<?> s, Uni<T> u) { return u; }
    @Override public <T> Multi<T> instrumentItemProduced(Class<?> s, Multi<T> m) { return m; }
    @Override public ExecutionReplayTracker.StepExecutionScope beginReplayStep(Class<?> s, Object c, boolean a, Object i) { return null; }
    @Override public ExecutionReplayTracker.StepExecutionScope beginPendingReplayStep(Class<?> s, Object c, boolean a) { return null; }
    @Override public void recordReplayInput(ExecutionReplayTracker.StepExecutionScope s, Object i) { }
    @Override public void recordReplayOutput(ExecutionReplayTracker.StepExecutionScope s, Object o) { }
    @Override public void recordReplayCacheHit(Object c) { }
    @Override public void recordReplaySkip(Class<?> s, Object c, Object i, List<String> r, Optional<BranchVariantIdentity> v) { }
    @Override public <T> Uni<T> instrumentStepUni(Class<?> s, Uni<T> u, Object c, boolean a, ExecutionReplayTracker.StepExecutionScope sc) { return u; }
    @Override public <T> Uni<T> instrumentStepUni(Class<?> s, Uni<T> u, Object c, boolean a) { return u; }
    @Override public <T> Multi<T> instrumentStepMulti(Class<?> s, Multi<T> m, Object c, boolean a, ExecutionReplayTracker.StepExecutionScope sc) { return m; }
    @Override public <T> Multi<T> instrumentStepMulti(Class<?> s, Multi<T> m, Object c, boolean a) { return m; }
}
