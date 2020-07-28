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
