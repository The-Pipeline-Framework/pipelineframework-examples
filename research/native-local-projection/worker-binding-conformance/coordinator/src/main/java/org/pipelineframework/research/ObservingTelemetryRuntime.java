package org.pipelineframework.research;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.function.Consumer;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleCounterBuilder;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.telemetry.TelemetryRuntime;
import org.pipelineframework.telemetry.TelemetryRuntimes;

/** Observes generic Java transition dispatches externally so native cases can prove zero fallback. */
@Alternative
@Priority(1)
@ApplicationScoped
public class ObservingTelemetryRuntime implements TelemetryRuntime {
    private static final String ORCHESTRATOR_SCOPE = "org.pipelineframework.orchestrator";
    private static final String GENERIC_DISPATCH_COUNTER = "tpf.orchestrator.transition.dispatched.total";

    private final TelemetryRuntime delegate = TelemetryRuntimes.global();
    private final Optional<Path> dispatchLog;

    @Inject
    public ObservingTelemetryRuntime(
            @ConfigProperty(name = "proof.generic-dispatch-log") Optional<String> dispatchLogPath) {
        this.dispatchLog = dispatchLogPath
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .map(Path::of);
    }

    @Override
    public Meter meter(String instrumentationScope) {
        Meter base = delegate.meter(instrumentationScope);
        if (!ORCHESTRATOR_SCOPE.equals(instrumentationScope) || dispatchLog.isEmpty()) {
            return base;
        }
        Path target = dispatchLog.orElseThrow();
        return new Meter() {
            @Override
            public LongCounterBuilder counterBuilder(String name) {
                LongCounterBuilder builder = base.counterBuilder(name);
                if (!GENERIC_DISPATCH_COUNTER.equals(name)) {
                    return builder;
                }
                return new ObservingCounterBuilder(builder, target);
            }

            @Override
            public LongUpDownCounterBuilder upDownCounterBuilder(String name) {
                return base.upDownCounterBuilder(name);
            }

            @Override
            public DoubleHistogramBuilder histogramBuilder(String name) {
                return base.histogramBuilder(name);
            }

            @Override
            public DoubleGaugeBuilder gaugeBuilder(String name) {
                return base.gaugeBuilder(name);
            }
        };
    }

    @Override
    public Tracer tracer(String instrumentationScope) {
        return delegate.tracer(instrumentationScope);
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    private static final class ObservingCounterBuilder implements LongCounterBuilder {
        private final LongCounterBuilder delegate;
        private final Path target;

        private ObservingCounterBuilder(LongCounterBuilder delegate, Path target) {
            this.delegate = delegate;
            this.target = target;
        }

        @Override
        public LongCounterBuilder setDescription(String description) {
            delegate.setDescription(description);
            return this;
        }

        @Override
        public LongCounterBuilder setUnit(String unit) {
            delegate.setUnit(unit);
            return this;
        }

        @Override
        public DoubleCounterBuilder ofDoubles() {
            return delegate.ofDoubles();
        }

        @Override
        public LongCounter build() {
            return new ObservingCounter(delegate.build(), target);
        }

        @Override
        public ObservableLongCounter buildWithCallback(Consumer<ObservableLongMeasurement> callback) {
            return delegate.buildWithCallback(callback);
        }
    }

    private static final class ObservingCounter implements LongCounter {
        private final LongCounter delegate;
        private final Path target;

        private ObservingCounter(LongCounter delegate, Path target) {
            this.delegate = delegate;
            this.target = target;
        }

        @Override
        public void add(long value) {
            record(value);
            delegate.add(value);
        }

        @Override
        public void add(long value, Attributes attributes) {
            record(value);
            delegate.add(value, attributes);
        }

        @Override
        public void add(long value, Attributes attributes, Context context) {
            record(value);
            delegate.add(value, attributes, context);
        }

        private synchronized void record(long value) {
            try {
                Path parent = target.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                Files.writeString(
                    target,
                    Long.toString(value) + "\n",
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.SYNC);
            } catch (IOException error) {
                throw new UncheckedIOException("Failed recording generic transition dispatch observation", error);
            }
        }
    }
}
