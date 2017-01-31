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

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.ProcessContext;
import org.apache.nifi.controller.scheduling.ProcessContextFactory;
import org.apache.nifi.controller.scheduling.ScheduleState;
import org.apache.nifi.controller.scheduling.SchedulingAgent;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.StandardProcessContext;
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

    /**
     * Verifies that the invocation count is incremented after the processor runs once.
     */
    @Test
    public void testIncrementsInvocationCount() {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, TestContinuallyRunProcessorTask.class.getResource("/conf/nifi.properties").getFile());

        final Processor processor = Mockito.mock(Processor.class);

        final ProcessorNode processorNode = Mockito.mock(ProcessorNode.class);
        when(processorNode.hasIncomingConnection()).thenReturn(false);
        when(processorNode.getRelationships()).thenReturn(Collections.EMPTY_LIST);
        when(processorNode.getIdentifier()).thenReturn("an identifier");
        when(processorNode.hasIncomingConnection()).thenReturn(true);
        when(processorNode.getProcessor()).thenReturn(processor);
        Mockito.doNothing().when(processorNode).onTrigger(Mockito.anyObject(), Mockito.anyObject());

        // Set up with a single connection that is self-looping and empty as
        // above
        final Connection selfLoopingConnection = Mockito.mock(Connection.class);
        when(selfLoopingConnection.getSource()).thenReturn(processorNode);
        when(selfLoopingConnection.getDestination()).thenReturn(processorNode);
        when(processorNode.getIncomingConnections()).thenReturn(Collections.singletonList(selfLoopingConnection));

        // Create mock runtime environment for an instance of a
        // ContinuallyRunProcessorTask
        final FlowController flowController = Mockito.mock(FlowController.class);
        when(flowController.isConfiguredForClustering()).thenReturn(false);
        when(flowController.isPrimary()).thenReturn(true);

        final SchedulingAgent schedulingAgent = Mockito.mock(SchedulingAgent.class);
        when(schedulingAgent.getAdministrativeYieldDuration(TimeUnit.NANOSECONDS)).thenReturn(10L);
        when(schedulingAgent.getAdministrativeYieldDuration()).thenReturn("1 ns");

        // unused in this test
        final StandardProcessContext standardProcessContext = null;

        final FlowFileEventRepository eventRepo = Mockito.mock(FlowFileEventRepository.class);
        try {
            Mockito.doNothing().when(eventRepo).updateRepository(Mockito.anyObject());
        } catch (IOException ignore) {
            // our mock code won't do this
        }

        final ProcessContext processContext = Mockito.mock(ProcessContext.class);
        when(processContext.isRelationshipAvailabilitySatisfied(Mockito.anyInt())).thenReturn(true);
        when(processContext.getFlowFileEventRepository()).thenReturn(eventRepo);

        final ProcessContextFactory contextFactory = Mockito.mock(ProcessContextFactory.class);
        when(contextFactory.newProcessContext(Mockito.eq(processorNode), Mockito.anyObject())).thenReturn(processContext);

        final ScheduleState scheduleState = Mockito.mock(ScheduleState.class);
        when(scheduleState.incrementActiveThreadCount()).thenReturn(1);
        ContinuallyRunProcessorTask crpt = new ContinuallyRunProcessorTask(schedulingAgent, processorNode, flowController, contextFactory, scheduleState, standardProcessContext);

        // The RunOnceSchedulingAgent uses the invocation count to check if the
        // processor has run at least once.

        // In this first case the processor node should run and the count should
        // increase.
        final int invocationCountBefore1 = crpt.getInvocationCount();
        crpt.call();
        final int invocationCountAfter1 = crpt.getInvocationCount();
        assertTrue(invocationCountAfter1 > invocationCountBefore1);

        // In this second case it should not increase because the processor node
        // should believe it has missing connections
        when(processorNode.hasIncomingConnection()).thenReturn(false);
        final int invocationCountBefore2 = crpt.getInvocationCount();
        crpt.call();
        final int invocationCountAfter2 = crpt.getInvocationCount();
        assertFalse(invocationCountAfter2 > invocationCountBefore2);

    }
}
