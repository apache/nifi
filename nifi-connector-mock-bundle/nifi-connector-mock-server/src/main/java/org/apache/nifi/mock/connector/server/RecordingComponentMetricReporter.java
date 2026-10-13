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
package org.apache.nifi.mock.connector.server;

import org.apache.nifi.controller.metrics.ComponentMetricReporter;
import org.apache.nifi.controller.metrics.CounterRecord;
import org.apache.nifi.controller.metrics.GaugeRecord;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

class RecordingComponentMetricReporter implements ComponentMetricReporter {
    private final ConcurrentMap<MetricKey, AtomicLong> counterValues = new ConcurrentHashMap<>();
    private final Queue<GaugeMeasurement> gaugeMeasurements = new ConcurrentLinkedQueue<>();

    @Override
    public void recordGauge(final GaugeRecord gaugeRecord) {
        final String componentId = gaugeRecord.componentMetricContext().id();
        final MetricKey metricKey = new MetricKey(componentId, gaugeRecord.name(), gaugeRecord.attributes());
        gaugeMeasurements.add(new GaugeMeasurement(metricKey, gaugeRecord.value()));
    }

    @Override
    public void recordCounter(final CounterRecord counterRecord) {
        final String componentId = counterRecord.componentMetricContext().id();
        final MetricKey metricKey = new MetricKey(componentId, counterRecord.name(), counterRecord.attributes());
        counterValues.computeIfAbsent(metricKey, key -> new AtomicLong()).addAndGet(counterRecord.value());
    }

    Long getCounterValue(final String componentId, final String name) {
        Long counterValue = null;

        for (final Map.Entry<MetricKey, AtomicLong> counterEntry : counterValues.entrySet()) {
            final MetricKey metricKey = counterEntry.getKey();
            if (metricKey.componentId().equals(componentId) && metricKey.name().equals(name)) {
                final long recorded = counterEntry.getValue().get();
                counterValue = counterValue == null ? recorded : counterValue + recorded;
            }
        }

        return counterValue;
    }

    Long getCounterValue(final String componentId, final String name, final Map<String, String> attributes) {
        final MetricKey metricKey = new MetricKey(componentId, name, attributes);
        final AtomicLong counterValue = counterValues.get(metricKey);
        return counterValue == null ? null : counterValue.get();
    }

    List<Double> getGaugeValues(final String componentId, final String name) {
        return gaugeMeasurements.stream()
            .filter(measurement -> measurement.key().componentId().equals(componentId))
            .filter(measurement -> measurement.key().name().equals(name))
            .map(GaugeMeasurement::value)
            .toList();
    }

    List<Double> getGaugeValues(final String componentId, final String name, final Map<String, String> attributes) {
        final MetricKey metricKey = new MetricKey(componentId, name, attributes);
        return gaugeMeasurements.stream()
            .filter(measurement -> measurement.key().equals(metricKey))
            .map(GaugeMeasurement::value)
            .toList();
    }

    private record MetricKey(String componentId, String name, Map<String, String> attributes) {
        private MetricKey {
            attributes = Map.copyOf(attributes);
        }
    }

    private record GaugeMeasurement(MetricKey key, double value) {
    }
}
