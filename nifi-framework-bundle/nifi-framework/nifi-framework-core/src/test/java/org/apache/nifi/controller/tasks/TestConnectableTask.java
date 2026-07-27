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

import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.GarbageCollectionLog;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.RepositoryContext;
import org.apache.nifi.controller.repository.StandardRepositoryContext;
import org.apache.nifi.controller.scheduling.LifecycleState;
import org.apache.nifi.controller.scheduling.RepositoryContextFactory;
import org.apache.nifi.controller.scheduling.SchedulingAgent;
import org.apache.nifi.controller.status.FlowFileAvailability;
import org.apache.nifi.processor.Processor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestConnectableTask {

    private ConnectableTask createTask(final Connectable connectable) {
        final FlowController flowController = mock(FlowController.class);
        when(flowController.getStateManagerProvider()).thenReturn(mock(StateManagerProvider.class));

        final RepositoryContext repoContext = mock(StandardRepositoryContext.class);
        when(repoContext.getFlowFileEventRepository()).thenReturn(mock(FlowFileEventRepository.class));

        when(flowController.getGarbageCollectionLog()).thenReturn(mock(GarbageCollectionLog.class));

        final RepositoryContextFactory contextFactory = mock(RepositoryContextFactory.class);
        when(contextFactory.newProcessContext(any(Connectable.class), any(AtomicLong.class))).thenReturn(repoContext);

        final LifecycleState scheduleState = new LifecycleState(connectable.getIdentifier());

        return new ConnectableTask(mock(SchedulingAgent.class), connectable,
                flowController, contextFactory, scheduleState);
    }

    @Test
    public void testIsWorkToDo() {
        final ProcessorNode procNode = mock(ProcessorNode.class);
        when(procNode.hasIncomingConnection()).thenReturn(false);

        final Processor processor = mock(Processor.class);
        when(procNode.getIdentifier()).thenReturn("123");
        when(procNode.getRunnableComponent()).thenReturn(processor);

        // There is work to do because there are no incoming connections.
        final ConnectableTask task = createTask(procNode);
        assertFalse(task.invoke().isYield());

        // Test with only a single connection that is self-looping and empty
        final Connection selfLoopingConnection = mock(Connection.class);
        when(selfLoopingConnection.getSource()).thenReturn(procNode);
        when(selfLoopingConnection.getDestination()).thenReturn(procNode);

        when(procNode.hasIncomingConnection()).thenReturn(true);
        when(procNode.getIncomingConnections()).thenReturn(Collections.singletonList(selfLoopingConnection));
        assertFalse(task.invoke().isYield());

        // Test with only a single connection that is self-looping and empty
        final FlowFileQueue flowFileQueue = mock(FlowFileQueue.class);
        when(flowFileQueue.getFlowFileAvailability()).thenReturn(FlowFileAvailability.ACTIVE_QUEUE_EMPTY);

        final FlowFileQueue nonEmptyQueue = mock(FlowFileQueue.class);
        when(nonEmptyQueue.getFlowFileAvailability()).thenReturn(FlowFileAvailability.FLOWFILE_AVAILABLE);

        when(selfLoopingConnection.getFlowFileQueue()).thenReturn(nonEmptyQueue);
        assertFalse(task.invoke().isYield());

        // Test with only a non-looping Connection that has no FlowFiles
        final Connection emptyConnection = mock(Connection.class);
        when(emptyConnection.getSource()).thenReturn(mock(ProcessorNode.class));
        when(emptyConnection.getDestination()).thenReturn(procNode);

        when(emptyConnection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(procNode.getIncomingConnections()).thenReturn(Collections.singletonList(emptyConnection));

        assertTrue(task.invoke().isYield());

        // test when the queue has data
        final Connection nonEmptyConnection = mock(Connection.class);
        when(nonEmptyConnection.getSource()).thenReturn(mock(ProcessorNode.class));
        when(nonEmptyConnection.getDestination()).thenReturn(procNode);
        when(nonEmptyConnection.getFlowFileQueue()).thenReturn(nonEmptyQueue);
        when(procNode.getIncomingConnections()).thenReturn(Collections.singletonList(nonEmptyConnection));
        assertFalse(task.invoke().isYield());
    }

    @Test
    public void testIsWorkToDoFunnels() {
        final Funnel funnel = mock(Funnel.class);
        when(funnel.hasIncomingConnection()).thenReturn(false);
        when(funnel.getRunnableComponent()).thenReturn(funnel);
        when(funnel.getConnectableType()).thenReturn(ConnectableType.FUNNEL);
        when(funnel.getIdentifier()).thenReturn("funnel-1");

        final ConnectableTask task = createTask(funnel);
        assertTrue(task.invoke().isYield(),
                "If there is no incoming connection, it should be yielded.");

        // Test with only a single connection that is self-looping and empty.
        // Actually, this self-loop input can not be created for Funnels using NiFi API because an outer layer check condition does not allow it.
        // But test it anyways.
        final Connection selfLoopingConnection = mock(Connection.class);
        when(selfLoopingConnection.getSource()).thenReturn(funnel);
        when(selfLoopingConnection.getDestination()).thenReturn(funnel);

        when(funnel.hasIncomingConnection()).thenReturn(true);
        when(funnel.getIncomingConnections()).thenReturn(Collections.singletonList(selfLoopingConnection));

        final FlowFileQueue emptyQueue = mock(FlowFileQueue.class);
        when(emptyQueue.getFlowFileAvailability()).thenReturn(FlowFileAvailability.ACTIVE_QUEUE_EMPTY);
        when(selfLoopingConnection.getFlowFileQueue()).thenReturn(emptyQueue);

        final Set<Connection> outgoingConnections = new HashSet<>();
        outgoingConnections.add(selfLoopingConnection);
        when(funnel.getConnections()).thenReturn(outgoingConnections);

        assertTrue(task.invoke().isYield(),
                "If there is no incoming connection from other components, it should be yielded.");

        // Add an incoming connection from another component.
        final ProcessorNode inputProcessor = mock(ProcessorNode.class);
        final Connection incomingFromAnotherComponent = mock(Connection.class);
        when(incomingFromAnotherComponent.getSource()).thenReturn(inputProcessor);
        when(incomingFromAnotherComponent.getDestination()).thenReturn(funnel);
        when(incomingFromAnotherComponent.getFlowFileQueue()).thenReturn(emptyQueue);

        when(funnel.hasIncomingConnection()).thenReturn(true);
        when(funnel.getIncomingConnections()).thenReturn(Arrays.asList(selfLoopingConnection, incomingFromAnotherComponent));

        assertTrue(task.invoke().isYield(), "Even if there is an incoming connection from another component," +
                " it should be yielded because there's no outgoing connections.");

        // Add an outgoing connection to another component.
        final ProcessorNode outputProcessor = mock(ProcessorNode.class);
        final Connection outgoingToAnotherComponent = mock(Connection.class);
        when(outgoingToAnotherComponent.getSource()).thenReturn(funnel);
        when(outgoingToAnotherComponent.getDestination()).thenReturn(outputProcessor);
        outgoingConnections.add(outgoingToAnotherComponent);

        assertTrue(task.invoke().isYield(), "Even if there is an incoming connection from another component and an outgoing connection as well," +
                " it should be yielded because there's no incoming FlowFiles to process.");

        // Adding input FlowFiles.
        final FlowFileQueue nonEmptyQueue = mock(FlowFileQueue.class);
        when(nonEmptyQueue.getFlowFileAvailability()).thenReturn(FlowFileAvailability.FLOWFILE_AVAILABLE);
        when(incomingFromAnotherComponent.getFlowFileQueue()).thenReturn(nonEmptyQueue);
        assertFalse(task.invoke().isYield(),
                "When a Funnel has both incoming and outgoing connections and FlowFiles to process," +
                        " then it should be executed.");
    }
}
