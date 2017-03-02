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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StandardStatusSnapshot implements StatusSnapshot {

    private final Map<MetricDescriptor<?>, Long> metricValues = new LinkedHashMap<>();
    private Date timestamp = new Date();

    @Override
    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final Date timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public Map<MetricDescriptor<?>, Long> getStatusMetrics() {
        return metricValues;
    }

    public void addStatusMetric(final MetricDescriptor<?> metric, final Long value) {
        metricValues.put(metric, value);
    }

    @Override
    public ValueReducer<StatusSnapshot, StatusSnapshot> getValueReducer() {
        return new ValueReducer<StatusSnapshot, StatusSnapshot>() {
            @Override
            public StatusSnapshot reduce(final List<StatusSnapshot> values) {
                Date reducedTimestamp = null;
                final Set<MetricDescriptor<?>> allDescriptors = new LinkedHashSet<>(metricValues.keySet());

                for (final StatusSnapshot statusSnapshot : values) {
                    if (reducedTimestamp == null) {
                        reducedTimestamp = statusSnapshot.getTimestamp();
                    }
                    allDescriptors.addAll(statusSnapshot.getStatusMetrics().keySet());
                }

                final StandardStatusSnapshot reduced = new StandardStatusSnapshot();
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
