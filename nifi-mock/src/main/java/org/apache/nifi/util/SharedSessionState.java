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
package org.apache.nifi.util;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceReporter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class SharedSessionState {

    private final MockFlowFileQueue flowFileQueue;
    private final ProvenanceReporter provenanceReporter;
    @SuppressWarnings("unused")
    private final Processor processor;
    private final AtomicLong flowFileIdGenerator;
    private final ConcurrentMap<MetricKey, AtomicLong> counterMap = new ConcurrentHashMap<>();
    private final Queue<GaugeMeasurement> gaugeMeasurements = new ConcurrentLinkedQueue<>();
    // list of provenance events as they were in the provenance repository (events emitted with force=true or committed with the session)
    private final List<ProvenanceEventRecord> events = new ArrayList<>();

    public SharedSessionState(final Processor processor, final AtomicLong flowFileIdGenerator) {
        flowFileQueue = new MockFlowFileQueue();
        provenanceReporter = new MockProvenanceReporter(null, this, UUID.randomUUID().toString(), "N/A");
        this.flowFileIdGenerator = flowFileIdGenerator;
        this.processor = processor;
    }

    void addProvenanceEvents(final Collection<ProvenanceEventRecord> events) {
        this.events.addAll(events);
    }

    void clearProvenanceEvents() {
        this.events.clear();
    }

    public List<ProvenanceEventRecord> getProvenanceEvents() {
        return new ArrayList<>(this.events);
    }

    public MockFlowFileQueue getFlowFileQueue() {
        return flowFileQueue;
    }

    public ProvenanceReporter getProvenanceReporter() {
        return provenanceReporter;
    }

    public long nextFlowFileId() {
        return flowFileIdGenerator.getAndIncrement();
    }

    public void adjustCounter(final String name, final long delta) {
        adjustCounter(new MetricKey(name, Map.of()), delta);
    }

    void adjustCounter(final MetricKey counterKey, final long delta) {
        AtomicLong counter = counterMap.get(counterKey);
        if (counter == null) {
            counter = new AtomicLong(0L);
            final AtomicLong existingCounter = counterMap.putIfAbsent(counterKey, counter);
            if (existingCounter != null) {
                counter = existingCounter;
            }
        }

        counter.addAndGet(delta);
    }

    /**
     * Get the value recorded for the named Counter, summing the measurements recorded with differing attributes
     *
     * @param name Counter Name
     * @return Counter value, or null when the named Counter was not used
     */
    public Long getCounterValue(final String name) {
        Long counterValue = null;

        for (final Map.Entry<MetricKey, AtomicLong> counterEntry : counterMap.entrySet()) {
            if (counterEntry.getKey().name().equals(name)) {
                final long recorded = counterEntry.getValue().get();
                counterValue = counterValue == null ? recorded : counterValue + recorded;
            }
        }

        return counterValue;
    }

    /**
     * Get the value recorded for the named Counter with the specified attributes
     *
     * @param name Counter Name
     * @param attributes Map of keys and values associated with the Counter
     * @return Counter value, or null when the named Counter was not used with the specified attributes
     */
    public Long getCounterValue(final String name, final Map<String, String> attributes) {
        final AtomicLong counterValue = counterMap.get(new MetricKey(name, Map.copyOf(attributes)));
        return counterValue == null ? null : counterValue.get();
    }

    public void recordGauge(final String name, final double value) {
        recordGauge(new MetricKey(name, Map.of()), value);
    }

    void recordGauge(final MetricKey gaugeKey, final double value) {
        gaugeMeasurements.add(new GaugeMeasurement(gaugeKey, value));
    }

    /**
     * Get list of values recorded for the named Gauge, including the measurements recorded with differing attributes
     *
     * @param name Gauge Name
     * @return List of recorded values, or empty when the named Gauge was not used
     */
    public List<Double> getGaugeValues(final String name) {
        final List<Double> gaugeValues = new ArrayList<>();

        for (final GaugeMeasurement gaugeMeasurement : gaugeMeasurements) {
            if (gaugeMeasurement.key().name().equals(name)) {
                gaugeValues.add(gaugeMeasurement.value());
            }
        }

        return gaugeValues;
    }

    /**
     * Get list of values recorded for the named Gauge with the specified attributes
     *
     * @param name Gauge Name
     * @param attributes Map of keys and values associated with the Gauge
     * @return List of recorded values, or empty when the named Gauge was not used with the specified attributes
     */
    public List<Double> getGaugeValues(final String name, final Map<String, String> attributes) {
        final MetricKey gaugeKey = new MetricKey(name, Map.copyOf(attributes));
        final List<Double> gaugeValues = new ArrayList<>();

        for (final GaugeMeasurement gaugeMeasurement : gaugeMeasurements) {
            if (gaugeMeasurement.key().equals(gaugeKey)) {
                gaugeValues.add(gaugeMeasurement.value());
            }
        }

        return List.copyOf(gaugeValues);
    }
}
