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

import org.apache.nifi.controller.metrics.ComponentMetricContext;
import org.apache.nifi.controller.metrics.CounterRecord;
import org.apache.nifi.controller.metrics.GaugeRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordingComponentMetricReporterTest {
    private static final String COMPONENT_ID = "component-id";
    private static final String OTHER_COMPONENT_ID = "other-component-id";
    private static final String METRIC_NAME = "metric";
    private static final Map<String, String> ATTRIBUTES = Map.of("key", "value");

    private final RecordingComponentMetricReporter reporter = new RecordingComponentMetricReporter();

    @Test
    void testCounterValues() {
        reporter.recordCounter(new CounterRecord(METRIC_NAME, 2, Map.of(), Instant.now(), getContext(COMPONENT_ID)));
        reporter.recordCounter(new CounterRecord(METRIC_NAME, 3, ATTRIBUTES, Instant.now(), getContext(COMPONENT_ID)));
        reporter.recordCounter(new CounterRecord(METRIC_NAME, 7, ATTRIBUTES, Instant.now(), getContext(OTHER_COMPONENT_ID)));

        assertEquals(5L, reporter.getCounterValue(COMPONENT_ID, METRIC_NAME));
        assertEquals(2L, reporter.getCounterValue(COMPONENT_ID, METRIC_NAME, Map.of()));
        assertEquals(3L, reporter.getCounterValue(COMPONENT_ID, METRIC_NAME, ATTRIBUTES));
        assertNull(reporter.getCounterValue(COMPONENT_ID, "unrecorded"));
        assertNull(reporter.getCounterValue(COMPONENT_ID, METRIC_NAME, Map.of("unrecorded", "attribute")));
    }

    @Test
    void testGaugeValues() {
        reporter.recordGauge(new GaugeRecord(METRIC_NAME, 2.0, Map.of(), Instant.now(), getContext(COMPONENT_ID)));
        reporter.recordGauge(new GaugeRecord(METRIC_NAME, 3.0, ATTRIBUTES, Instant.now(), getContext(COMPONENT_ID)));
        reporter.recordGauge(new GaugeRecord(METRIC_NAME, 7.0, ATTRIBUTES, Instant.now(), getContext(OTHER_COMPONENT_ID)));

        assertEquals(List.of(2.0, 3.0), reporter.getGaugeValues(COMPONENT_ID, METRIC_NAME));
        assertEquals(List.of(2.0), reporter.getGaugeValues(COMPONENT_ID, METRIC_NAME, Map.of()));
        assertEquals(List.of(3.0), reporter.getGaugeValues(COMPONENT_ID, METRIC_NAME, ATTRIBUTES));
        assertTrue(reporter.getGaugeValues(COMPONENT_ID, "unrecorded").isEmpty());
        assertTrue(reporter.getGaugeValues(COMPONENT_ID, METRIC_NAME, Map.of("unrecorded", "attribute")).isEmpty());
    }

    private ComponentMetricContext getContext(final String componentId) {
        return new ComponentMetricContext(componentId, "name", "type", Map.of());
    }
}
