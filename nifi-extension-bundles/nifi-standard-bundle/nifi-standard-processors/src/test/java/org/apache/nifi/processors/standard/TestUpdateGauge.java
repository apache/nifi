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
package org.apache.nifi.processors.standard;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestUpdateGauge {
    private static final String GAUGE_NAME = TestUpdateGauge.class.getSimpleName();

    private static final double GAUGE_VALUE = 1.2345;

    private static final double INVALID_GAUGE_VALUE = 0;

    private static final String VALUE_ATTRIBUTE = "value";

    private static final String FILENAME_ATTRIBUTE = "filename";
    private static final String FILENAME_VALUE = "test";

    private static final String SERVICE_ATTRIBUTE = "service";
    private static final String SERVICE_VALUE = "payments";

    private static final String EMPTY_ATTRIBUTE = "emptyAttribute";
    private static final String EMPTY_VALUE = "emptyValue";

    private final TestRunner runner = TestRunners.newTestRunner(UpdateGauge.class);

    @Test
    void testRunRecordGauge() {
        runner.setProperty(UpdateGauge.GAUGE_NAME, GAUGE_NAME);
        runner.setProperty(UpdateGauge.GAUGE_VALUE, Double.toString(GAUGE_VALUE));

        assertGaugeValueRecorded(GAUGE_VALUE);
    }

    @Test
    void testRunRecordGaugeExpressionLanguageConfigured() {
        runner.setProperty(UpdateGauge.GAUGE_NAME, "${literal('%s')}".formatted(GAUGE_NAME));
        runner.setProperty(UpdateGauge.GAUGE_VALUE, "${literal(%s)}".formatted(GAUGE_VALUE));

        assertGaugeValueRecorded(GAUGE_VALUE);
    }

    @Test
    void testRunRecordGaugeExpressionLanguageInvalidGaugeValue() {
        runner.setProperty(UpdateGauge.GAUGE_NAME, "${literal('%s')}".formatted(GAUGE_NAME));
        runner.setProperty(UpdateGauge.GAUGE_VALUE, "${literal('')}");

        assertGaugeValueRecorded(INVALID_GAUGE_VALUE);
    }

    @Test
    void testRunRecordGaugeAttributes() {
        runner.setProperty(UpdateGauge.GAUGE_NAME, GAUGE_NAME);
        runner.setProperty(UpdateGauge.GAUGE_VALUE, "${%s}".formatted(VALUE_ATTRIBUTE));
        runner.setProperty(FILENAME_ATTRIBUTE, "${%s}".formatted(FILENAME_ATTRIBUTE));
        runner.setProperty(EMPTY_ATTRIBUTE, "${%s}".formatted(EMPTY_VALUE));
        runner.setProperty(SERVICE_ATTRIBUTE, SERVICE_VALUE);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(VALUE_ATTRIBUTE, Double.toString(GAUGE_VALUE));
        attributes.put(FILENAME_ATTRIBUTE, FILENAME_VALUE);

        runner.enqueue(new byte[0], attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateGauge.SUCCESS);
        final Map<String, String> gaugeAttributes = Map.of(FILENAME_ATTRIBUTE, FILENAME_VALUE, SERVICE_ATTRIBUTE, SERVICE_VALUE);
        assertEquals(List.of(GAUGE_VALUE), runner.getGaugeValues(GAUGE_NAME, gaugeAttributes));
        assertTrue(runner.getGaugeValues(GAUGE_NAME, Map.of()).isEmpty());
    }

    private void assertGaugeValueRecorded(final double expectedGaugeValue) {
        runner.enqueue(new byte[]{});

        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateGauge.SUCCESS);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(UpdateGauge.SUCCESS);
        assertFalse(flowFiles.isEmpty());

        final List<Double> gaugeValues = runner.getGaugeValues(GAUGE_NAME);
        assertFalse(gaugeValues.isEmpty());

        final Double firstGaugeValue = gaugeValues.getFirst();
        assertEquals(expectedGaugeValue, firstGaugeValue);
    }
}
