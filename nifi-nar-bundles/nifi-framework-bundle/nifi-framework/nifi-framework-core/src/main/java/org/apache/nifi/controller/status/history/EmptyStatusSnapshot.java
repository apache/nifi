package org.apache.nifi.controller.status.history;

import java.util.Date;
import java.util.List;
import java.util.Set;

public class EmptyStatusSnapshot implements StatusSnapshot {
    private static final ValueReducer<StatusSnapshot, StatusSnapshot> VALUE_REDUCER = new EmptyValueReducer();
    private static final Long METRIC_VALUE = 0L;

    private final Date timestamp;
    private final Set<MetricDescriptor<?>> metricsDescriptors;

    public EmptyStatusSnapshot(final Date timestamp, final Set<MetricDescriptor<?>> metricsDescriptors) {
        this.timestamp = timestamp;
        this.metricsDescriptors = metricsDescriptors;
    }

    @Override
    public Date getTimestamp() {
        return timestamp;
    }

    @Override
    public Set<MetricDescriptor<?>> getMetricDescriptors() {
        return metricsDescriptors;
    }

    @Override
    public Long getStatusMetric(final MetricDescriptor<?> descriptor) {
        return METRIC_VALUE;
    }

    @Override
    public StatusSnapshot withoutCounters() {
        return this;
    }

    @Override
    public ValueReducer<StatusSnapshot, StatusSnapshot> getValueReducer() {
        return VALUE_REDUCER;
    }

    private static class EmptyValueReducer implements ValueReducer<StatusSnapshot, StatusSnapshot> {
        @Override
        public StatusSnapshot reduce(final List<StatusSnapshot> values) {
            return (values == null || values.isEmpty()) ? null : values.get(0);
        }
    }
}
