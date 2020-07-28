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

import org.apache.nifi.controller.repository.FlowFileEvent;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestSecondPrecisionEventContainer {

    @Test
    public void testUpdateOncePerSecond() {
        final SecondPrecisionEventContainer container = new SecondPrecisionEventContainer(5);
        final long startTime = System.currentTimeMillis();

        final StandardFlowFileEvent event = new StandardFlowFileEvent();
        event.setBytesRead(100L);
        event.setBytesWritten(100L);

        for (int i=0; i < 5; i++) {
            for (int j=0; j < 300; j++) {
                container.addEvent(event, startTime + (i * 300_000) + (j * 1000));
            }

            final long timestamp = startTime + 300_000 * i + 300_000;
            final FlowFileEvent result = container.generateReport(timestamp);
            assertEquals("Failure at i=" + i, 300 * 100, result.getBytesRead());
            assertEquals("Failure at i=" + i, 300 * 100, result.getBytesWritten());
        }
    }

    @Test
    public void testExpiresOnReportGeneration() {
        final SecondPrecisionEventContainer container = new SecondPrecisionEventContainer(5);
        final long startTime = System.currentTimeMillis();

        final StandardFlowFileEvent event = new StandardFlowFileEvent();
        event.setBytesRead(100L);
        event.setBytesWritten(100L);

        for (int j=0; j < 100; j++) {
            container.addEvent(event, startTime + (j * 1000));
        }

        final FlowFileEvent resultAt5Mins = container.generateReport(startTime + 300_000);
        assertEquals(100 * 100, resultAt5Mins.getBytesRead());
        assertEquals(100 * 100, resultAt5Mins.getBytesWritten());

        final FlowFileEvent resultAt5MinsPlus50Seconds = container.generateReport(startTime + 350_000);
        assertEquals(50 * 100, resultAt5MinsPlus50Seconds.getBytesRead());
        assertEquals(50 * 100, resultAt5MinsPlus50Seconds.getBytesWritten());

        final FlowFileEvent resultAt5MinsPlus99Seconds = container.generateReport(startTime + 399_000);
        assertEquals(100, resultAt5MinsPlus99Seconds.getBytesRead());
        assertEquals(100, resultAt5MinsPlus99Seconds.getBytesWritten());

        final FlowFileEvent resultAt5MinsPlus100Seconds = container.generateReport(startTime + 400_000);
        assertEquals(0, resultAt5MinsPlus100Seconds.getBytesRead());
        assertEquals(0, resultAt5MinsPlus100Seconds.getBytesWritten());

        final FlowFileEvent resultAt5MinsPlus101Seconds = container.generateReport(startTime + 401_000);
        assertEquals(0, resultAt5MinsPlus101Seconds.getBytesRead());
        assertEquals(0, resultAt5MinsPlus101Seconds.getBytesWritten());

        final FlowFileEvent resultsAt5MinsPlus300seconds = container.generateReport(startTime + 600_000);
        assertEquals(0, resultsAt5MinsPlus300seconds.getBytesRead());
        assertEquals(0, resultsAt5MinsPlus300seconds.getBytesWritten());

        final FlowFileEvent resultsAt5MinsPlus600seconds = container.generateReport(startTime + 900_000);
        assertEquals(0, resultsAt5MinsPlus600seconds.getBytesRead());
        assertEquals(0, resultsAt5MinsPlus600seconds.getBytesWritten());
    }

    @Test
    public void testExpiresOnReportGenerationWithSkipsBetweenUpdates() {
        final SecondPrecisionEventContainer container = new SecondPrecisionEventContainer(5);
        final long startTime = System.currentTimeMillis();

        final StandardFlowFileEvent event = new StandardFlowFileEvent();
        event.setBytesRead(100L);
        event.setBytesWritten(100L);

        for (int j=0; j < 20; j++) {
            container.addEvent(event, startTime + (j * 5000));
        }

        final FlowFileEvent resultAt5Mins = container.generateReport(startTime + 300_000);
        assertEquals(20 * 100, resultAt5Mins.getBytesRead());
        assertEquals(20 * 100, resultAt5Mins.getBytesWritten());

        final FlowFileEvent resultAt5MinsPlus50Seconds = container.generateReport(startTime + 350_000);
        assertEquals(10 * 100, resultAt5MinsPlus50Seconds.getBytesRead());
        assertEquals(10 * 100, resultAt5MinsPlus50Seconds.getBytesWritten());

        final FlowFileEvent resultAt5MinsPlus94Seconds = container.generateReport(startTime + 394_000);
        assertEquals(100, resultAt5MinsPlus94Seconds.getBytesRead());
        assertEquals(100, resultAt5MinsPlus94Seconds.getBytesWritten());

        final FlowFileEvent resultAt5MinsPlus95Seconds = container.generateReport(startTime + 395_000);
        assertEquals(100, resultAt5MinsPlus95Seconds.getBytesRead());
        assertEquals(100, resultAt5MinsPlus95Seconds.getBytesWritten());

        final FlowFileEvent resultAt5MinsPlus100Seconds = container.generateReport(startTime + 400_000);
        assertEquals(0, resultAt5MinsPlus100Seconds.getBytesRead());
        assertEquals(0, resultAt5MinsPlus100Seconds.getBytesWritten());

        final FlowFileEvent resultAt5MinsPlus101Seconds = container.generateReport(startTime + 401_000);
        assertEquals(0, resultAt5MinsPlus101Seconds.getBytesRead());
        assertEquals(0, resultAt5MinsPlus101Seconds.getBytesWritten());

        final FlowFileEvent resultsAt5MinsPlus300seconds = container.generateReport(startTime + 600_000);
        assertEquals(0, resultsAt5MinsPlus300seconds.getBytesRead());
        assertEquals(0, resultsAt5MinsPlus300seconds.getBytesWritten());

        final FlowFileEvent resultsAt5MinsPlus600seconds = container.generateReport(startTime + 900_000);
        assertEquals(0, resultsAt5MinsPlus600seconds.getBytesRead());
        assertEquals(0, resultsAt5MinsPlus600seconds.getBytesWritten());
    }

}
