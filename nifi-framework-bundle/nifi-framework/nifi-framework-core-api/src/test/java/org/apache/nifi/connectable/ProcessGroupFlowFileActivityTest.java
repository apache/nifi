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

package org.apache.nifi.connectable;

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.StatelessGroupNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProcessGroupFlowFileActivityTest {

    @Mock
    private ProcessGroup processGroup;

    private ProcessGroupFlowFileActivity activity;

    @BeforeEach
    void setUp() {
        activity = new ProcessGroupFlowFileActivity(processGroup);

        lenient().when(processGroup.getProcessors()).thenReturn(Collections.emptyList());
        lenient().when(processGroup.getInputPorts()).thenReturn(Collections.emptySet());
        lenient().when(processGroup.getOutputPorts()).thenReturn(Collections.emptySet());
        lenient().when(processGroup.getFunnels()).thenReturn(Collections.emptySet());
        lenient().when(processGroup.getProcessGroups()).thenReturn(Collections.emptySet());
        lenient().when(processGroup.getStatelessGroupNode()).thenReturn(Optional.empty());
    }

    @Test
    void testGetTransferCountsWithNoComponents() {
        final FlowFileTransferCounts counts = activity.getTransferCounts();

        assertEquals(0L, counts.getReceivedCount());
        assertEquals(0L, counts.getReceivedBytes());
        assertEquals(0L, counts.getSentCount());
        assertEquals(0L, counts.getSentBytes());
    }

    @Test
    void testGetTransferCountsAggregatesProcessors() {
        final ProcessorNode processor = createMockConnectable(ProcessorNode.class, 10, 100L, 5, 50L);
        when(processGroup.getProcessors()).thenReturn(List.of(processor));

        final FlowFileTransferCounts counts = activity.getTransferCounts();

        assertEquals(10L, counts.getReceivedCount());
        assertEquals(100L, counts.getReceivedBytes());
        assertEquals(5L, counts.getSentCount());
        assertEquals(50L, counts.getSentBytes());
    }

    @Test
    void testGetTransferCountsIncludesStatelessGroupNode() {
        final StatelessGroupNode statelessGroupNode = mock(StatelessGroupNode.class);
        final FlowFileActivity statelessActivity = new ConnectableFlowFileActivity();
        statelessActivity.updateTransferCounts(20, 2000L, 15, 1500L);

        when(statelessGroupNode.getFlowFileActivity()).thenReturn(statelessActivity);
        when(processGroup.getStatelessGroupNode()).thenReturn(Optional.of(statelessGroupNode));

        final FlowFileTransferCounts counts = activity.getTransferCounts();

        assertEquals(20L, counts.getReceivedCount());
        assertEquals(2000L, counts.getReceivedBytes());
        assertEquals(15L, counts.getSentCount());
        assertEquals(1500L, counts.getSentBytes());
    }

    @Test
    void testGetTransferCountsAggregatesProcessorsAndStatelessGroupNode() {
        final ProcessorNode processor = createMockConnectable(ProcessorNode.class, 10, 100L, 5, 50L);
        when(processGroup.getProcessors()).thenReturn(List.of(processor));

        final StatelessGroupNode statelessGroupNode = mock(StatelessGroupNode.class);
        final FlowFileActivity statelessActivity = new ConnectableFlowFileActivity();
        statelessActivity.updateTransferCounts(20, 2000L, 15, 1500L);
        when(statelessGroupNode.getFlowFileActivity()).thenReturn(statelessActivity);
        when(processGroup.getStatelessGroupNode()).thenReturn(Optional.of(statelessGroupNode));

        final FlowFileTransferCounts counts = activity.getTransferCounts();

        assertEquals(30L, counts.getReceivedCount());
        assertEquals(2100L, counts.getReceivedBytes());
        assertEquals(20L, counts.getSentCount());
        assertEquals(1550L, counts.getSentBytes());
    }

    @Test
    void testGetTransferCountsAggregatesChildGroups() {
        final ProcessGroup childGroup = mock(ProcessGroup.class);
        final FlowFileActivity childActivity = mock(FlowFileActivity.class);
        when(childActivity.getTransferCounts()).thenReturn(new FlowFileTransferCounts(8, 800L, 3, 300L));
        when(childGroup.getFlowFileActivity()).thenReturn(childActivity);
        when(processGroup.getProcessGroups()).thenReturn(Set.of(childGroup));

        final FlowFileTransferCounts counts = activity.getTransferCounts();

        assertEquals(8L, counts.getReceivedCount());
        assertEquals(800L, counts.getReceivedBytes());
        assertEquals(3L, counts.getSentCount());
        assertEquals(300L, counts.getSentBytes());
    }

    @Test
    void testGetLatestActivityTimeEmpty() {
        final OptionalLong result = activity.getLatestActivityTime();
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetLatestActivityTimeIncludesStatelessGroupNode() {
        final StatelessGroupNode statelessGroupNode = mock(StatelessGroupNode.class);
        final ConnectableFlowFileActivity statelessActivity = new ConnectableFlowFileActivity();
        statelessActivity.updateLatestActivityTime();
        when(statelessGroupNode.getFlowFileActivity()).thenReturn(statelessActivity);
        when(processGroup.getStatelessGroupNode()).thenReturn(Optional.of(statelessGroupNode));

        final OptionalLong result = activity.getLatestActivityTime();
        assertTrue(result.isPresent());
    }

    private <T extends Connectable> T createMockConnectable(final Class<T> type,
            final int receivedCount, final long receivedBytes, final int sentCount, final long sentBytes) {
        final T connectable = mock(type);
        final FlowFileActivity connectableActivity = new ConnectableFlowFileActivity();
        connectableActivity.updateTransferCounts(receivedCount, receivedBytes, sentCount, sentBytes);
        when(connectable.getFlowFileActivity()).thenReturn(connectableActivity);
        return connectable;
    }
}
