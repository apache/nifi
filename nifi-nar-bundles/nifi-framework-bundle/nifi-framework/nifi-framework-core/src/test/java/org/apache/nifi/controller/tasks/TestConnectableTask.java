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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.RepositoryContext;
import org.apache.nifi.controller.scheduling.RepositoryContextFactory;
import org.apache.nifi.controller.scheduling.LifecycleState;
import org.apache.nifi.controller.scheduling.SchedulingAgent;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.processor.Processor;
import org.junit.Test;
import org.mockito.Mockito;

public class TestConnectableTask {
    @Test
    public void testIsWorkToDo() {
        final ProcessorNode procNode = Mockito.mock(ProcessorNode.class);
        Mockito.when(procNode.hasIncomingConnection()).thenReturn(false);

        final Processor processor = Mockito.mock(Processor.class);
        Mockito.when(procNode.getIdentifier()).thenReturn("123");
        Mockito.when(procNode.getRunnableComponent()).thenReturn(processor);

        final FlowController flowController = Mockito.mock(FlowController.class);
        Mockito.when(flowController.getStateManagerProvider()).thenReturn(Mockito.mock(StateManagerProvider.class));

        final RepositoryContext repoContext = Mockito.mock(RepositoryContext.class);
        Mockito.when(repoContext.getFlowFileEventRepository()).thenReturn(Mockito.mock(FlowFileEventRepository.class));

        final RepositoryContextFactory contextFactory = Mockito.mock(RepositoryContextFactory.class);
        Mockito.when(contextFactory.newProcessContext(Mockito.any(Connectable.class), Mockito.any(AtomicLong.class))).thenReturn(repoContext);

        final LifecycleState scheduleState = new LifecycleState();
        final StringEncryptor encryptor = Mockito.mock(StringEncryptor.class);
        ConnectableTask task = new ConnectableTask(Mockito.mock(SchedulingAgent.class), procNode, flowController, contextFactory, scheduleState, encryptor);

        // There is work to do because there are no incoming connections.
        assertFalse(task.invoke().isYield());

        // Test with only a single connection that is self-looping and empty
        final Connection selfLoopingConnection = Mockito.mock(Connection.class);
        when(selfLoopingConnection.getSource()).thenReturn(procNode);
        when(selfLoopingConnection.getDestination()).thenReturn(procNode);

        when(procNode.hasIncomingConnection()).thenReturn(true);
        when(procNode.getIncomingConnections()).thenReturn(Collections.singletonList(selfLoopingConnection));
        assertFalse(task.invoke().isYield());

        // Test with only a single connection that is self-looping and empty
        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
        when(flowFileQueue.isActiveQueueEmpty()).thenReturn(true);

        final FlowFileQueue nonEmptyQueue = Mockito.mock(FlowFileQueue.class);
        when(nonEmptyQueue.isActiveQueueEmpty()).thenReturn(false);

        when(selfLoopingConnection.getFlowFileQueue()).thenReturn(nonEmptyQueue);
        assertFalse(task.invoke().isYield());

        // Test with only a non-looping Connection that has no FlowFiles
        final Connection emptyConnection = Mockito.mock(Connection.class);
        when(emptyConnection.getSource()).thenReturn(Mockito.mock(ProcessorNode.class));
        when(emptyConnection.getDestination()).thenReturn(procNode);

        when(emptyConnection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(procNode.getIncomingConnections()).thenReturn(Collections.singletonList(emptyConnection));

        // Create a new ConnectableTask because we want to have a different value for the 'hasNonLoopConnection' value, which is calculated in the task's constructor.
        task = new ConnectableTask(Mockito.mock(SchedulingAgent.class), procNode, flowController, contextFactory, scheduleState, encryptor);
        assertTrue(task.invoke().isYield());

        // test when the queue has data
        final Connection nonEmptyConnection = Mockito.mock(Connection.class);
        when(nonEmptyConnection.getSource()).thenReturn(Mockito.mock(ProcessorNode.class));
        when(nonEmptyConnection.getDestination()).thenReturn(procNode);
        when(nonEmptyConnection.getFlowFileQueue()).thenReturn(nonEmptyQueue);
        when(procNode.getIncomingConnections()).thenReturn(Collections.singletonList(nonEmptyConnection));
        assertFalse(task.invoke().isYield());
    }

}
