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

package org.apache.nifi.stateless.session;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.groups.ProcessGroup;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestAsynchronousCommitTracker {

    @Test
    public void testAddAndGetProvidesCorrectOrder() {
        final AsynchronousCommitTracker tracker = new AsynchronousCommitTracker();

        final Connectable connectable1 = Mockito.mock(Connectable.class);
        final Connectable connectable2 = Mockito.mock(Connectable.class);
        final Connectable connectable3 = Mockito.mock(Connectable.class);

        tracker.addConnectable(connectable1);
        assertEquals(Collections.singletonList(connectable1), tracker.getReady());

        tracker.addConnectable(connectable2);
        assertEquals(Arrays.asList(connectable2, connectable1), tracker.getReady());

        tracker.addConnectable(connectable3);
        assertEquals(Arrays.asList(connectable3, connectable2, connectable1), tracker.getReady());

        // connectable1 should now be moved to the start of the List
        tracker.addConnectable(connectable1);
        assertEquals(Arrays.asList(connectable1, connectable3, connectable2), tracker.getReady());

        // Adding connectable1 again should now have effect since it is already first
        tracker.addConnectable(connectable1);
        assertEquals(Arrays.asList(connectable1, connectable3, connectable2), tracker.getReady());
    }

    @Test
    public void testIsReadyRemovesConnectablesWithNoData() {
        final AsynchronousCommitTracker tracker = new AsynchronousCommitTracker();

        final Connectable connectable1 = Mockito.mock(Connectable.class);
        final Connectable connectable2 = Mockito.mock(Connectable.class);

        tracker.addConnectable(connectable1);
        tracker.addConnectable(connectable2);
        assertEquals(Arrays.asList(connectable2, connectable1), tracker.getReady());

        assertTrue(tracker.isAnyReady());

        // If no incoming connections, should not be considered ready. Calling isReady should then remove it from the collection of ready components.
        Mockito.when(connectable1.getIncomingConnections()).thenReturn(Collections.emptyList());
        assertFalse(tracker.isReady(connectable1));

        // Still should have connectable2 ready.
        assertTrue(tracker.isAnyReady());
        assertEquals(Collections.singletonList(connectable2), tracker.getReady());

        // Mock out a Connection and a FlowFileQueue so that this is used when checking if connectable2 is ready.
        // If the queue is not empty, we should see that connectable2 is ready and remains in the collection.
        final Connection connection = Mockito.mock(Connection.class);
        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
        Mockito.when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        Mockito.when(flowFileQueue.isEmpty()).thenReturn(false);
        Mockito.when(connectable2.getIncomingConnections()).thenReturn(Collections.singletonList(connection));

        assertTrue(tracker.isReady(connectable2));
        assertTrue(tracker.isAnyReady());
        assertEquals(Collections.singletonList(connectable2), tracker.getReady());

        // If we then indicate that the FlowFileQueue is empty, we should see that Connectable2 is no longer ready and it should be evicted from the collection of ready components.
        // This should then also result in isAnyReady() being false.
        Mockito.when(flowFileQueue.isEmpty()).thenReturn(true);
        assertFalse(tracker.isReady(connectable2));
        assertFalse(tracker.isAnyReady());
        assertEquals(Collections.emptyList(), tracker.getReady());
    }

    @Test
    public void testRootGroupOutputPortNotReady() {
        final AsynchronousCommitTracker tracker = new AsynchronousCommitTracker();

        final Connectable outputPort = Mockito.mock(Connectable.class);

        tracker.addConnectable(outputPort);
        assertEquals(Collections.singletonList(outputPort), tracker.getReady());
        assertTrue(tracker.isAnyReady());

        Mockito.when(outputPort.getConnectableType()).thenReturn(ConnectableType.OUTPUT_PORT);

        // Create Process Group that is not the root process group
        final ProcessGroup processGroup = Mockito.mock(ProcessGroup.class);
        Mockito.when(processGroup.getParent()).thenReturn(Mockito.mock(ProcessGroup.class));
        Mockito.when(outputPort.getProcessGroup()).thenReturn(processGroup);

        // Mock out a Connection and a FlowFileQueue so that this is used when checking if connectable2 is ready.
        // If the queue is not empty, we should see that connectable2 is ready and remains in the collection.
        final Connection connection = Mockito.mock(Connection.class);
        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
        Mockito.when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        Mockito.when(flowFileQueue.isEmpty()).thenReturn(false);
        Mockito.when(outputPort.getIncomingConnections()).thenReturn(Collections.singletonList(connection));

        // Output Port should be ready
        assertTrue(tracker.isReady(outputPort));

        // If Process Group's parent is null, that means that the Process Group is the root group. As a result, the Output Port
        // is now a Root Group Output Port and therefore it should not be considered ready.
        Mockito.when(processGroup.getParent()).thenReturn(null);
        assertFalse(tracker.isReady(outputPort));
        assertEquals(Collections.emptyList(), tracker.getReady());
        assertFalse(tracker.isAnyReady());
    }
}
