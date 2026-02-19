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

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.lifecycle.TaskTermination;
import org.apache.nifi.controller.metrics.GaugeRecord;
import org.apache.nifi.controller.repository.metrics.NopPerformanceTracker;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.metrics.CommitTiming;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BatchingSessionFactoryTest {
    private static final String CONNECTABLE_ID = "connectable-id";

    private static final String GAUGE_NAME = "recording";

    private static final double GAUGE_VALUE = 64.5;

    private final TaskTermination taskTermination = () -> false;

    @Mock
    private RepositoryContext repositoryContext;

    @Mock
    private Connectable connectable;

    @Captor
    private ArgumentCaptor<GaugeRecord> gaugeRecordCaptor;

    private BatchingSessionFactory factory;

    @BeforeEach
    void setFactory() {
        when(repositoryContext.getConnectable()).thenReturn(connectable);
        when(connectable.getIdentifier()).thenReturn(CONNECTABLE_ID);
        final StandardProcessSession standardProcessSession = new StandardProcessSession(repositoryContext, taskTermination, new NopPerformanceTracker());
        factory = new BatchingSessionFactory(standardProcessSession);
    }

    @Test
    void testCreateSession() {
        final ProcessSession session = factory.createSession();
        assertNotNull(session);
    }

    @Test
    void testCreateSessionRecordGauge() {
        final ProcessSession session = factory.createSession();
        assertNotNull(session);

        session.recordGauge(GAUGE_NAME, GAUGE_VALUE, CommitTiming.NOW);

        verify(repositoryContext).recordGauge(gaugeRecordCaptor.capture());
        final GaugeRecord gaugeRecord = gaugeRecordCaptor.getValue();
        assertEquals(GAUGE_NAME, gaugeRecord.name());
        assertEquals(GAUGE_VALUE, gaugeRecord.value());
    }
}
