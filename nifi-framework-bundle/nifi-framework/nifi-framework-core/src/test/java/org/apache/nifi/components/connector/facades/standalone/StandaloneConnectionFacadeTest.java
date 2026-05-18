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
package org.apache.nifi.components.connector.facades.standalone;

import org.apache.nifi.components.connector.components.QueueSnapshot;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.MockFlowFileRecord;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.FlowFileQueueSnapshot;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.flow.VersionedConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StandaloneConnectionFacadeTest {

    private FlowFileQueue flowFileQueue;
    private StandaloneConnectionFacade facade;

    @BeforeEach
    public void setUp() {
        final Connection connection = mock(Connection.class);
        flowFileQueue = mock(FlowFileQueue.class);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);

        final VersionedConnection versionedConnection = mock(VersionedConnection.class);
        facade = new StandaloneConnectionFacade(connection, versionedConnection);
    }

    @Test
    public void testGetQueueSnapshotWithActiveFlowFilesIsExhaustive() {
        final List<FlowFileRecord> activeFlowFiles = List.of(new MockFlowFileRecord(10L), new MockFlowFileRecord(20L), new MockFlowFileRecord(30L));
        when(flowFileQueue.getQueueSnapshot()).thenReturn(new FlowFileQueueSnapshot(new QueueSize(3, 60L), activeFlowFiles));

        final QueueSnapshot snapshot = facade.getQueueSnapshot();

        assertEquals(new QueueSize(3, 60L), snapshot.getQueueSize());
        assertEquals(activeFlowFiles, snapshot.getActiveFlowFiles());
        assertTrue(snapshot.isActiveListExhaustive());
    }

    @Test
    public void testGetQueueSnapshotEmptyQueueIsExhaustive() {
        when(flowFileQueue.getQueueSnapshot()).thenReturn(new FlowFileQueueSnapshot(new QueueSize(0, 0L), Collections.emptyList()));

        final QueueSnapshot snapshot = facade.getQueueSnapshot();

        assertEquals(new QueueSize(0, 0L), snapshot.getQueueSize());
        assertTrue(snapshot.getActiveFlowFiles().isEmpty());
        assertTrue(snapshot.isActiveListExhaustive());
    }

    @Test
    public void testIsActiveListNotExhaustiveWhenQueueSizeExceedsActiveList() {
        // The total queue size counts FlowFiles that are swapped to disk or held in remote/rebalancing partitions,
        // none of which appear in the active in-memory list. When the total exceeds the active list, the active list
        // is not exhaustive.
        final List<FlowFileRecord> activeFlowFiles = List.of(new MockFlowFileRecord(10L), new MockFlowFileRecord(20L));
        when(flowFileQueue.getQueueSnapshot()).thenReturn(new FlowFileQueueSnapshot(new QueueSize(5, 150L), activeFlowFiles));

        final QueueSnapshot snapshot = facade.getQueueSnapshot();

        assertEquals(5, snapshot.getQueueSize().getObjectCount());
        assertEquals(2, snapshot.getActiveFlowFiles().size());
        assertFalse(snapshot.isActiveListExhaustive());
    }
}
