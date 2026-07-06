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
package org.apache.nifi.controller.repository;

import org.apache.nifi.controller.metrics.ComponentMetricContext;
import org.apache.nifi.controller.metrics.ProcessSessionEvent;
import org.apache.nifi.controller.repository.metrics.ProcessSessionEventBuilder;
import org.apache.nifi.controller.repository.metrics.RingBufferEventRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestRingBufferEventRepository {

    private static final String FIRST_COMPONENT_ID = "component-1";
    private static final String SECOND_COMPONENT_ID = "component-2";

    private final RingBufferEventRepository repository = new RingBufferEventRepository(1);

    @AfterEach
    void closeRepository() throws IOException {
        repository.close();
    }

    @Test
    void testReportTransferEvents() {
        final ProcessSessionEvent event = getProcessSessionEvent(FIRST_COMPONENT_ID);
        repository.updateRepository(event);

        final StandardRepositoryStatusReport report = repository.reportTransferEvents(System.currentTimeMillis());
        assertNotNull(report);

        final ProcessSessionEvent reportEntry = report.getReportEntry(FIRST_COMPONENT_ID);
        assertNotNull(reportEntry);
        assertEquals(event.getFlowFilesIn(), reportEntry.getFlowFilesIn());
    }

    @Test
    void testReportTransferEventsForComponentId() {
        final ProcessSessionEvent event = getProcessSessionEvent(FIRST_COMPONENT_ID);
        repository.updateRepository(event);

        final ProcessSessionEvent reportEvent = repository.reportTransferEvents(FIRST_COMPONENT_ID, System.currentTimeMillis());
        assertNotNull(reportEvent);
        assertEquals(event.getFlowFilesIn(), reportEvent.getFlowFilesIn());
    }

    @Test
    void testPurgeTransferEvents() {
        repository.updateRepository(getProcessSessionEvent(FIRST_COMPONENT_ID));
        repository.updateRepository(getProcessSessionEvent(SECOND_COMPONENT_ID));

        final RepositoryStatusReport report = repository.reportTransferEvents(System.currentTimeMillis());
        final ProcessSessionEvent firstReportEntry = report.getReportEntry(FIRST_COMPONENT_ID);
        assertNotNull(firstReportEntry);
        final ProcessSessionEvent secondReportEntry = report.getReportEntry(SECOND_COMPONENT_ID);
        assertNotNull(secondReportEntry);

        repository.purgeTransferEvents(FIRST_COMPONENT_ID);
        final RepositoryStatusReport firstReportPurged = repository.reportTransferEvents(System.currentTimeMillis());
        assertNull(firstReportPurged.getReportEntry(FIRST_COMPONENT_ID));
        assertNotNull(firstReportPurged.getReportEntry(SECOND_COMPONENT_ID));

        repository.purgeTransferEvents(SECOND_COMPONENT_ID);
        final RepositoryStatusReport secondReportPurged = repository.reportTransferEvents(System.currentTimeMillis());
        assertNull(secondReportPurged.getReportEntry(SECOND_COMPONENT_ID));
    }

    @Test
    void testReportAggregateEvent() {
        final ProcessSessionEvent firstEvent = getProcessSessionEvent(FIRST_COMPONENT_ID);
        final ProcessSessionEvent secondEvent = getProcessSessionEvent(SECOND_COMPONENT_ID);

        repository.updateRepository(firstEvent);
        repository.updateRepository(secondEvent);

        final int totalFlowFilesIn = firstEvent.getFlowFilesIn() + secondEvent.getFlowFilesIn();
        final ProcessSessionEvent aggregateEvent = repository.reportAggregateEvent();
        assertEquals(totalFlowFilesIn, aggregateEvent.getFlowFilesIn());
    }

    private ProcessSessionEvent getProcessSessionEvent(final String componentId) {
        final ComponentMetricContext componentMetricContext = new ComponentMetricContext(componentId, componentId, "Processor", Map.of());
        return ProcessSessionEventBuilder.forComponent(componentMetricContext)
                .flowFilesIn(1)
                .build();
    }
}
