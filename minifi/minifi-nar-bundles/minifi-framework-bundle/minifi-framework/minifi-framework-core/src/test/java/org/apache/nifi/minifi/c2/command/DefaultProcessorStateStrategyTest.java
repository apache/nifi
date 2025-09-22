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

package org.apache.nifi.minifi.c2.command;

import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DefaultProcessorStateStrategyTest {

    private static final String PROCESSOR_ID = "e2f2b9f6-1a7a-4b65-9e58-9a2b3f6a0c01";
    private static final String GROUP_ID = "0b0d53b7-3b5d-4d88-a3f0-2d6c5dc9d8bb";

    @Mock
    private FlowController flowController;

    @Mock
    private FlowManager flowManager;

    @InjectMocks
    private DefaultProcessorStateStrategy victim;

    @BeforeEach
    public void init() {
        when(flowController.getFlowManager()).thenReturn(flowManager);
    }

    @Test
    public void testStartProcessorFullyApplied() {
        ProcessorNode node = mock(ProcessorNode.class);
        when(flowManager.getProcessorNode(PROCESSOR_ID)).thenReturn(node);
        when(node.getProcessGroupIdentifier()).thenReturn(GROUP_ID);

        OperationState result = victim.startProcessor(PROCESSOR_ID);

        assertEquals(FULLY_APPLIED, result);
        verify(flowController).startProcessor(GROUP_ID, PROCESSOR_ID, true);
    }

    @Test
    public void testStartProcessorNotAppliedWhenMissing() {
        when(flowManager.getProcessorNode(PROCESSOR_ID)).thenReturn(null);

        OperationState result = victim.startProcessor(PROCESSOR_ID);

        assertEquals(NOT_APPLIED, result);
    }

    @Test
    public void testStartProcessorNotAppliedOnError() {
        ProcessorNode node = mock(ProcessorNode.class);
        when(flowManager.getProcessorNode(PROCESSOR_ID)).thenReturn(node);
        when(node.getProcessGroupIdentifier()).thenReturn(GROUP_ID);
        doThrow(new RuntimeException("boom")).when(flowController).startProcessor(GROUP_ID, PROCESSOR_ID, true);

        OperationState result = victim.startProcessor(PROCESSOR_ID);

        assertEquals(NOT_APPLIED, result);
    }

    @Test
    public void testStopProcessorFullyApplied() {
        ProcessorNode node = mock(ProcessorNode.class);
        when(flowManager.getProcessorNode(PROCESSOR_ID)).thenReturn(node);
        when(node.getProcessGroupIdentifier()).thenReturn(GROUP_ID);

        OperationState result = victim.stopProcessor(PROCESSOR_ID);

        assertEquals(FULLY_APPLIED, result);
        verify(flowController).stopProcessor(GROUP_ID, PROCESSOR_ID);
    }

    @Test
    public void testStopProcessorNotAppliedOnError() {
        ProcessorNode node = mock(ProcessorNode.class);
        when(flowManager.getProcessorNode(PROCESSOR_ID)).thenReturn(node);
        when(node.getProcessGroupIdentifier()).thenReturn(GROUP_ID);
        doThrow(new RuntimeException("boom")).when(flowController).stopProcessor(GROUP_ID, PROCESSOR_ID);

        OperationState result = victim.stopProcessor(PROCESSOR_ID);

        assertEquals(NOT_APPLIED, result);
    }

    @Test
    public void testStopProcessorNotAppliedWhenMissing() {
        when(flowManager.getProcessorNode(PROCESSOR_ID)).thenReturn(null);

        OperationState result = victim.stopProcessor(PROCESSOR_ID);

        assertEquals(NOT_APPLIED, result);
    }
}
