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

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.metrics.ComponentMetricContext;
import org.apache.nifi.controller.metrics.ComponentMetricReporter;
import org.apache.nifi.controller.metrics.CounterRecord;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardRepositoryContextTest {
    private static final String COMPONENT_ID = "component-id";

    private static final String COMPONENT_NAME = "Component Name";

    private static final String COMPONENT_TYPE = "ComponentType";

    private static final String COUNTER_NAME = "records.ingested";

    private static final long COUNTER_DELTA = 5;

    private static final Map<String, String> COUNTER_ATTRIBUTES = Map.of("table", "orders");

    private static final Map<String, String> INITIAL_LOGGING_ATTRIBUTES = Map.of("registeredFlowIdentifierPath", "/root:1/connector:1.0.0");

    private static final Map<String, String> UPDATED_LOGGING_ATTRIBUTES = Map.of("registeredFlowIdentifierPath", "/root:1/connector:2.0.0");

    @Mock
    private Connectable connectable;

    @Mock
    private ProcessGroup processGroup;

    @Mock
    private ContentRepository contentRepository;

    @Mock
    private FlowFileRepository flowFileRepository;

    @Mock
    private FlowFileEventRepository flowFileEventRepository;

    @Mock
    private CounterRepository counterRepository;

    @Mock
    private ComponentMetricReporter componentMetricReporter;

    @Mock
    private ProvenanceEventRepository provenanceEventRepository;

    @Mock
    private StateManager stateManager;

    @Captor
    private ArgumentCaptor<CounterRecord> counterRecordCaptor;

    @Test
    void testAdjustCounterRecordsCurrentProcessGroupLoggingAttributes() {
        when(connectable.getIdentifier()).thenReturn(COMPONENT_ID);
        when(connectable.getName()).thenReturn(COMPONENT_NAME);
        when(connectable.getComponentType()).thenReturn(COMPONENT_TYPE);
        when(connectable.getProcessGroup()).thenReturn(processGroup);
        when(processGroup.getLoggingAttributes()).thenReturn(INITIAL_LOGGING_ATTRIBUTES);

        final StandardRepositoryContext repositoryContext = new StandardRepositoryContext(
            connectable,
            new AtomicLong(),
            contentRepository,
            flowFileRepository,
            flowFileEventRepository,
            counterRepository,
            componentMetricReporter,
            provenanceEventRepository,
            stateManager
        );
        repositoryContext.adjustCounter(COUNTER_NAME, COUNTER_DELTA, COUNTER_ATTRIBUTES);

        when(processGroup.getLoggingAttributes()).thenReturn(UPDATED_LOGGING_ATTRIBUTES);
        repositoryContext.adjustCounter(COUNTER_NAME, COUNTER_DELTA, COUNTER_ATTRIBUTES);

        verify(componentMetricReporter, times(2)).recordCounter(counterRecordCaptor.capture());
        final List<CounterRecord> counterRecords = counterRecordCaptor.getAllValues();
        assertCounterRecordEquals(INITIAL_LOGGING_ATTRIBUTES, counterRecords.getFirst());
        assertCounterRecordEquals(UPDATED_LOGGING_ATTRIBUTES, counterRecords.getLast());
    }

    private void assertCounterRecordEquals(final Map<String, String> expectedLoggingAttributes, final CounterRecord counterRecord) {
        assertEquals(COUNTER_NAME, counterRecord.name());
        assertEquals(COUNTER_DELTA, counterRecord.value());
        assertEquals(COUNTER_ATTRIBUTES, counterRecord.attributes());
        assertEquals(new ComponentMetricContext(COMPONENT_ID, COMPONENT_NAME, COMPONENT_TYPE, expectedLoggingAttributes), counterRecord.componentMetricContext());
    }
}
