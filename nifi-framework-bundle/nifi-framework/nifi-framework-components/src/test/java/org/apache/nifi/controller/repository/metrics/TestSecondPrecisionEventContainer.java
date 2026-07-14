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
package org.apache.nifi.controller.repository.metrics;

import org.apache.nifi.controller.metrics.ComponentMetricContext;
import org.apache.nifi.controller.metrics.ProcessSessionEvent;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSecondPrecisionEventContainer {

    private static final String COMPONENT_ID = "component-1";
    private static final ComponentMetricContext COMPONENT_CONTEXT = new ComponentMetricContext(
            COMPONENT_ID, COMPONENT_ID, "Processor", Map.of());

    @Test
    public void testUpdateOncePerSecond() {
        final SecondPrecisionEventContainer container = new SecondPrecisionEventContainer(5);
        final long startTime = System.currentTimeMillis();

        final ProcessSessionEvent event = ProcessSessionEventBuilder.forComponent(COMPONENT_CONTEXT)
                .bytesRead(100L)
                .bytesWritten(100L)
                .build();

        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 300; j++) {
                container.addEvent(event, startTime + (i * 300_000) + (j * 1000));
            }

            final long timestamp = startTime + 300_000 * i + 300_000;
            final ProcessSessionEvent result = container.generateReport(timestamp);
            assertEquals(300 * 100, result.getBytesRead(), "Failure at i=" + i);
            assertEquals(300 * 100, result.getBytesWritten(), "Failure at i=" + i);
        }
    }

    @Test
    public void testExpiresOnReportGeneration() {
        final SecondPrecisionEventContainer container = new SecondPrecisionEventContainer(5);
        final long startTime = System.currentTimeMillis();

        final ProcessSessionEvent event = ProcessSessionEventBuilder.forComponent(COMPONENT_CONTEXT)
                .bytesRead(100L)
                .bytesWritten(100L)
                .build();

        for (int j = 0; j < 100; j++) {
            container.addEvent(event, startTime + (j * 1000));
        }

        final ProcessSessionEvent resultAt5Mins = container.generateReport(startTime + 300_000);
        assertEquals(100 * 100, resultAt5Mins.getBytesRead());
        assertEquals(100 * 100, resultAt5Mins.getBytesWritten());

        final ProcessSessionEvent resultAt5MinsPlus50Seconds = container.generateReport(startTime + 350_000);
        assertEquals(50 * 100, resultAt5MinsPlus50Seconds.getBytesRead());
        assertEquals(50 * 100, resultAt5MinsPlus50Seconds.getBytesWritten());

        final ProcessSessionEvent resultAt5MinsPlus99Seconds = container.generateReport(startTime + 399_000);
        assertEquals(100, resultAt5MinsPlus99Seconds.getBytesRead());
        assertEquals(100, resultAt5MinsPlus99Seconds.getBytesWritten());

        final ProcessSessionEvent resultAt5MinsPlus100Seconds = container.generateReport(startTime + 400_000);
        assertEquals(0, resultAt5MinsPlus100Seconds.getBytesRead());
        assertEquals(0, resultAt5MinsPlus100Seconds.getBytesWritten());

        final ProcessSessionEvent resultAt5MinsPlus101Seconds = container.generateReport(startTime + 401_000);
        assertEquals(0, resultAt5MinsPlus101Seconds.getBytesRead());
        assertEquals(0, resultAt5MinsPlus101Seconds.getBytesWritten());

        final ProcessSessionEvent resultsAt5MinsPlus300seconds = container.generateReport(startTime + 600_000);
        assertEquals(0, resultsAt5MinsPlus300seconds.getBytesRead());
        assertEquals(0, resultsAt5MinsPlus300seconds.getBytesWritten());

        final ProcessSessionEvent resultsAt5MinsPlus600seconds = container.generateReport(startTime + 900_000);
        assertEquals(0, resultsAt5MinsPlus600seconds.getBytesRead());
        assertEquals(0, resultsAt5MinsPlus600seconds.getBytesWritten());
    }

    @Test
    public void testExpiresOnReportGenerationWithSkipsBetweenUpdates() {
        final SecondPrecisionEventContainer container = new SecondPrecisionEventContainer(5);
        final long startTime = System.currentTimeMillis();

        final ProcessSessionEvent event = ProcessSessionEventBuilder.forComponent(COMPONENT_CONTEXT)
                .bytesRead(100L)
                .bytesWritten(100L)
                .build();

        for (int j = 0; j < 20; j++) {
            container.addEvent(event, startTime + (j * 5000));
        }

        final ProcessSessionEvent resultAt5Mins = container.generateReport(startTime + 300_000);
        assertEquals(20 * 100, resultAt5Mins.getBytesRead());
        assertEquals(20 * 100, resultAt5Mins.getBytesWritten());

        final ProcessSessionEvent resultAt5MinsPlus50Seconds = container.generateReport(startTime + 350_000);
        assertEquals(10 * 100, resultAt5MinsPlus50Seconds.getBytesRead());
        assertEquals(10 * 100, resultAt5MinsPlus50Seconds.getBytesWritten());

        final ProcessSessionEvent resultAt5MinsPlus94Seconds = container.generateReport(startTime + 394_000);
        assertEquals(100, resultAt5MinsPlus94Seconds.getBytesRead());
        assertEquals(100, resultAt5MinsPlus94Seconds.getBytesWritten());

        final ProcessSessionEvent resultAt5MinsPlus95Seconds = container.generateReport(startTime + 395_000);
        assertEquals(100, resultAt5MinsPlus95Seconds.getBytesRead());
        assertEquals(100, resultAt5MinsPlus95Seconds.getBytesWritten());

        final ProcessSessionEvent resultAt5MinsPlus100Seconds = container.generateReport(startTime + 400_000);
        assertEquals(0, resultAt5MinsPlus100Seconds.getBytesRead());
        assertEquals(0, resultAt5MinsPlus100Seconds.getBytesWritten());

        final ProcessSessionEvent resultAt5MinsPlus101Seconds = container.generateReport(startTime + 401_000);
        assertEquals(0, resultAt5MinsPlus101Seconds.getBytesRead());
        assertEquals(0, resultAt5MinsPlus101Seconds.getBytesWritten());

        final ProcessSessionEvent resultsAt5MinsPlus300seconds = container.generateReport(startTime + 600_000);
        assertEquals(0, resultsAt5MinsPlus300seconds.getBytesRead());
        assertEquals(0, resultsAt5MinsPlus300seconds.getBytesWritten());

        final ProcessSessionEvent resultsAt5MinsPlus600seconds = container.generateReport(startTime + 900_000);
        assertEquals(0, resultsAt5MinsPlus600seconds.getBytesRead());
        assertEquals(0, resultsAt5MinsPlus600seconds.getBytesWritten());
    }

}
