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

import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.FlowFileQueueFactory;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.remote.RemoteGroupPort;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StandardConnectionTest {

    private FlowFileQueue queue;

    private StandardConnection connectionWithDestination(final Connectable source, final Connectable destination) {
        queue = mock(FlowFileQueue.class);
        final FlowFileQueueFactory queueFactory = mock(FlowFileQueueFactory.class);
        when(queueFactory.createFlowFileQueue(any(), any(), any())).thenReturn(queue);

        return new StandardConnection.Builder(mock(ProcessScheduler.class))
                .id("connection-1")
                .source(source)
                .destination(destination)
                .processGroup(mock(ProcessGroup.class))
                .flowFileQueueFactory(queueFactory)
                .relationships(List.of(Relationship.ANONYMOUS))
                .build();
    }

    private StandardConnection connectionWithDestination(final Connectable destination) {
        return connectionWithDestination(mock(Connectable.class), destination);
    }

    @Test
    void testVerifyCanUpdateDestinationThrowsWhenCurrentDestinationRunning() {
        final Connectable runningProcessor = mock(Connectable.class);
        when(runningProcessor.isRunning()).thenReturn(true);
        final StandardConnection connection = connectionWithDestination(runningProcessor);

        assertThrows(IllegalStateException.class, connection::verifyCanUpdateDestination);
    }

    @Test
    void testVerifyCanUpdateDestinationAllowsRunningFunnel() {
        final Funnel runningFunnel = mock(Funnel.class);
        when(runningFunnel.isRunning()).thenReturn(true);
        final StandardConnection connection = connectionWithDestination(runningFunnel);

        assertDoesNotThrow(connection::verifyCanUpdateDestination);
    }

    @Test
    void testVerifyCanUpdateDestinationAllowsRunningLocalPort() {
        final LocalPort runningLocalPort = mock(LocalPort.class);
        when(runningLocalPort.isRunning()).thenReturn(true);
        final StandardConnection connection = connectionWithDestination(runningLocalPort);

        assertDoesNotThrow(connection::verifyCanUpdateDestination);
    }

    @Test
    void testVerifyCanUpdateDestinationAllowsRunningRemoteGroupPort() {
        final RemoteGroupPort runningRemotePort = mock(RemoteGroupPort.class);
        when(runningRemotePort.isRunning()).thenReturn(true);
        final StandardConnection connection = connectionWithDestination(runningRemotePort);

        assertDoesNotThrow(connection::verifyCanUpdateDestination);
    }

    @Test
    void testVerifyCanUpdateDestinationThrowsWhenFlowFilesHeld() {
        final Funnel stoppedFunnel = mock(Funnel.class);
        final StandardConnection connection = connectionWithDestination(stoppedFunnel);
        when(queue.isUnacknowledgedFlowFile()).thenReturn(true);

        assertThrows(IllegalStateException.class, connection::verifyCanUpdateDestination);
    }

    @Test
    void testVerifyCanUpdateDestinationAllowsStoppedDestinationWithNoUnacknowledgedFlowFiles() {
        final Connectable stoppedProcessor = mock(Connectable.class);
        final StandardConnection connection = connectionWithDestination(stoppedProcessor);
        when(queue.isUnacknowledgedFlowFile()).thenReturn(false);

        assertDoesNotThrow(connection::verifyCanUpdateDestination);
    }

    @Test
    void testSetDestinationNoOpWhenDestinationUnchanged() {
        final Connectable runningProcessor = mock(Connectable.class);
        when(runningProcessor.isRunning()).thenReturn(true);
        final StandardConnection connection = connectionWithDestination(runningProcessor);

        assertDoesNotThrow(() -> connection.setDestination(runningProcessor));
    }

    @Test
    void testSetDestinationRejectsSelfLoopingFunnel() {
        final Funnel funnel = mock(Funnel.class);
        final Connectable currentDestination = mock(Connectable.class);
        final StandardConnection connection = connectionWithDestination(funnel, currentDestination);

        assertThrows(IllegalStateException.class, () -> connection.setDestination(funnel));
    }
}
