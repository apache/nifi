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
package org.apache.nifi.controller.tasks;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Test;
import org.mockito.Mockito;

public class TestContinuallyRunProcessorTask {

    @Test
    public void testIsWorkToDo() {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, TestContinuallyRunProcessorTask.class.getResource("/conf/nifi.properties").getFile());

        final ProcessorNode procNode = Mockito.mock(ProcessorNode.class);
        Mockito.when(procNode.hasIncomingConnection()).thenReturn(false);

        // There is work to do because there are no incoming connections.
        assertTrue(ContinuallyRunProcessorTask.isWorkToDo(procNode));

        // Test with only a single connection that is self-looping and empty
        final Connection selfLoopingConnection = Mockito.mock(Connection.class);
        when(selfLoopingConnection.getSource()).thenReturn(procNode);
        when(selfLoopingConnection.getDestination()).thenReturn(procNode);

        when(procNode.hasIncomingConnection()).thenReturn(true);
        when(procNode.getIncomingConnections()).thenReturn(Collections.singletonList(selfLoopingConnection));
        assertTrue(ContinuallyRunProcessorTask.isWorkToDo(procNode));

        // Test with only a single connection that is self-looping and empty
        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
        when(flowFileQueue.isActiveQueueEmpty()).thenReturn(true);

        final FlowFileQueue nonEmptyQueue = Mockito.mock(FlowFileQueue.class);
        when(nonEmptyQueue.isActiveQueueEmpty()).thenReturn(false);

        when(selfLoopingConnection.getFlowFileQueue()).thenReturn(nonEmptyQueue);
        assertTrue(ContinuallyRunProcessorTask.isWorkToDo(procNode));

        // Test with only a non-looping Connection that has no FlowFiles
        final Connection emptyConnection = Mockito.mock(Connection.class);
        when(emptyConnection.getSource()).thenReturn(Mockito.mock(ProcessorNode.class));
        when(emptyConnection.getDestination()).thenReturn(procNode);

        when(emptyConnection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(procNode.getIncomingConnections()).thenReturn(Collections.singletonList(emptyConnection));
        assertFalse(ContinuallyRunProcessorTask.isWorkToDo(procNode));

        // test when the queue has data
        final Connection nonEmptyConnection = Mockito.mock(Connection.class);
        when(nonEmptyConnection.getSource()).thenReturn(Mockito.mock(ProcessorNode.class));
        when(nonEmptyConnection.getDestination()).thenReturn(procNode);
        when(nonEmptyConnection.getFlowFileQueue()).thenReturn(nonEmptyQueue);
        when(procNode.getIncomingConnections()).thenReturn(Collections.singletonList(nonEmptyConnection));
        assertTrue(ContinuallyRunProcessorTask.isWorkToDo(procNode));
    }

}
