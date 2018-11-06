/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.controller.status.history;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StandardStatusSnapshot implements StatusSnapshot {

    private final Set<MetricDescriptor<?>> metricDescriptors;
    private final long[] values;

    private Map<MetricDescriptor<?>, Long> counterValues = null;
    private Date timestamp = new Date();
    private Set<MetricDescriptor<?>> metricDescriptorsWithCounters = null;


    public StandardStatusSnapshot(final Set<MetricDescriptor<?>> metricDescriptors) {
        this.metricDescriptors = metricDescriptors;
        values = new long[metricDescriptors.size()];
    }

    private StandardStatusSnapshot(final Set<MetricDescriptor<?>> metricDescriptors, final long[] values) {
        this.metricDescriptors = metricDescriptors;
        this.values = values;
    }

    @Override
    public Date getTimestamp() {
        return timestamp;
    }

    @Override
    public Set<MetricDescriptor<?>> getMetricDescriptors() {
        if (counterValues == null || counterValues.isEmpty()) {
            return metricDescriptors;
        } else {
            if (metricDescriptorsWithCounters == null) {
                metricDescriptorsWithCounters = new LinkedHashSet<>();
                metricDescriptorsWithCounters.addAll(metricDescriptors);
                metricDescriptorsWithCounters.addAll(counterValues.keySet());
            }

            return metricDescriptorsWithCounters;
        }
    }

    @Override
    public Long getStatusMetric(final MetricDescriptor<?> descriptor) {
        if (descriptor.isCounter()) {
            return counterValues.get(descriptor);
        } else {
            return values[descriptor.getMetricIdentifier()];
        }
    }

    public void setTimestamp(final Date timestamp) {
        this.timestamp = timestamp;
    }


    public void addStatusMetric(final MetricDescriptor<?> metric, final Long value) {
        values[metric.getMetricIdentifier()] = value;
    }

    public void addCounterStatusMetric(final MetricDescriptor<?> metric, final Long value) {
        if (counterValues == null) {
            counterValues = new HashMap<>();
        }

        counterValues.put(metric, value);
    }

    public StandardStatusSnapshot withoutCounters() {
        if (counterValues == null || counterValues.isEmpty()) {
            return this;
        }

        final StandardStatusSnapshot without = new StandardStatusSnapshot(metricDescriptors, values);
        without.setTimestamp(timestamp);
        return without;
    }

    @Override
    public ValueReducer<StatusSnapshot, StatusSnapshot> getValueReducer() {
        return new ValueReducer<StatusSnapshot, StatusSnapshot>() {
            @Override
            public StatusSnapshot reduce(final List<StatusSnapshot> values) {
                Date reducedTimestamp = null;
                final Set<MetricDescriptor<?>> allDescriptors = new LinkedHashSet<>(getMetricDescriptors());

                for (final StatusSnapshot statusSnapshot : values) {
                    if (reducedTimestamp == null) {
                        reducedTimestamp = statusSnapshot.getTimestamp();
                    }
                    allDescriptors.addAll(statusSnapshot.getMetricDescriptors());
                }

                final StandardStatusSnapshot reduced = new StandardStatusSnapshot(allDescriptors);
                if (reducedTimestamp != null) {
                    reduced.setTimestamp(reducedTimestamp);
                }

                for (final MetricDescriptor<?> descriptor : allDescriptors) {
                    final Long descriptorValue = descriptor.getValueReducer().reduce(values);
                    reduced.addStatusMetric(descriptor, descriptorValue);
                }

                return reduced;
            }
        };
    }
}
