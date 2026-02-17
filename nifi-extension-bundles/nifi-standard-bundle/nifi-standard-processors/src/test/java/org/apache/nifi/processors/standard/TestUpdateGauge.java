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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TestUpdateGauge {
    private static final String GAUGE_NAME = TestUpdateGauge.class.getSimpleName();

    private static final double GAUGE_VALUE = 1.2345;

    private static final double INVALID_GAUGE_VALUE = 0;

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
        runner.setProperty(UpdateGauge.GAUGE_VALUE, "${literal(1.2345)}");

        assertGaugeValueRecorded(GAUGE_VALUE);
    }

    @Test
    void testRunRecordGaugeExpressionLanguageInvalidGaugeValue() {
        runner.setProperty(UpdateGauge.GAUGE_NAME, "${literal('%s')}".formatted(GAUGE_NAME));
        runner.setProperty(UpdateGauge.GAUGE_VALUE, "${literal('')}");

        assertGaugeValueRecorded(INVALID_GAUGE_VALUE);
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
