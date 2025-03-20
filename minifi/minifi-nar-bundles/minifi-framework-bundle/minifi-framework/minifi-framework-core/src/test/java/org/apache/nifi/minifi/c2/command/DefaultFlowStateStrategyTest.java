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
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.PARTIALLY_APPLIED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DefaultFlowStateStrategyTest {

    @Mock
    private FlowController flowController;
    @Mock
    private FlowManager flowManager;

    @InjectMocks
    private DefaultFlowStateStrategy victim;

    @BeforeEach
    public void initTests() {
        when(flowController.getFlowManager()).thenReturn(flowManager);
    }

    @Test
    public void testStartFullyApplied() {
        ProcessGroup rootProcessGroup = mock(ProcessGroup.class);
        ProcessGroup nestedProcessGroup = mock(ProcessGroup.class);
        RemoteProcessGroup remoteProcessGroup = mock(RemoteProcessGroup.class);
        Set<RemoteProcessGroup> remoteProcessGroups = Set.of(remoteProcessGroup);
        RemoteProcessGroup nestedRemoteProcessGroup = mock(RemoteProcessGroup.class);
        Set<RemoteProcessGroup> nestedRemoteProcessGroups = Set.of(nestedRemoteProcessGroup);

        when(flowManager.getRootGroup()).thenReturn(rootProcessGroup);
        when(rootProcessGroup.getRemoteProcessGroups()).thenReturn(remoteProcessGroups);
        when(rootProcessGroup.getProcessGroups()).thenReturn(Set.of(nestedProcessGroup));
        when(nestedProcessGroup.getRemoteProcessGroups()).thenReturn(nestedRemoteProcessGroups);

        OperationState result = victim.start();

        assertEquals(FULLY_APPLIED, result);
        verify(rootProcessGroup).startProcessing();
        verify(remoteProcessGroup).startTransmitting();
        verify(nestedProcessGroup).startProcessing();
        verify(nestedRemoteProcessGroup).startTransmitting();
    }

    @Test
    public void testStartPartiallyApplied() {
        ProcessGroup rootProcessGroup = mock(ProcessGroup.class);
        RemoteProcessGroup remoteProcessGroup = mock(RemoteProcessGroup.class);
        Set<RemoteProcessGroup> remoteProcessGroups = Set.of(remoteProcessGroup);

        when(flowManager.getRootGroup()).thenReturn(rootProcessGroup);
        when(rootProcessGroup.getRemoteProcessGroups()).thenReturn(remoteProcessGroups);
        doThrow(new RuntimeException()).when(remoteProcessGroup).startTransmitting();

        OperationState result = victim.start();

        assertEquals(PARTIALLY_APPLIED, result);
    }

    @Test
    public void testStartNotApplied() {
        when(flowManager.getRootGroup()).thenReturn(null);

        OperationState result = victim.start();

        assertEquals(NOT_APPLIED, result);
    }

    @Test
    public void testStopFullyApplied() {
        ProcessGroup rootProcessGroup = mock(ProcessGroup.class);
        RemoteProcessGroup remoteProcessGroup = mock(RemoteProcessGroup.class);
        Set<RemoteProcessGroup> remoteProcessGroups = Set.of(remoteProcessGroup);
        CompletableFuture<Void> rootProcessGroupStopFuture = mock(CompletableFuture.class);
        Future remoteProcessGroupStopFuture = mock(Future.class);
        ProcessGroup nestedProcessGroup = mock(ProcessGroup.class);
        RemoteProcessGroup nestedRemoteProcessGroup = mock(RemoteProcessGroup.class);
        Set<RemoteProcessGroup> nestedRemoteProcessGroups = Set.of(nestedRemoteProcessGroup);
        CompletableFuture<Void> nestedProcessGroupStopFuture = mock(CompletableFuture.class);
        Future nestedRemoteProcessGroupStopFuture = mock(Future.class);

        when(flowManager.getRootGroup()).thenReturn(rootProcessGroup);
        when(rootProcessGroup.getRemoteProcessGroups()).thenReturn(remoteProcessGroups);
        when(rootProcessGroup.stopProcessing()).thenReturn(rootProcessGroupStopFuture);
        when(remoteProcessGroup.stopTransmitting()).thenReturn(remoteProcessGroupStopFuture);
        when(rootProcessGroup.getProcessGroups()).thenReturn(Set.of(nestedProcessGroup));
        when(nestedProcessGroup.getRemoteProcessGroups()).thenReturn(nestedRemoteProcessGroups);
        when(nestedProcessGroup.stopProcessing()).thenReturn(nestedProcessGroupStopFuture);
        when(nestedRemoteProcessGroup.stopTransmitting()).thenReturn(nestedRemoteProcessGroupStopFuture);

        OperationState result = victim.stop();

        assertEquals(FULLY_APPLIED, result);
        assertDoesNotThrow(() -> rootProcessGroupStopFuture.get());
        assertDoesNotThrow(() -> remoteProcessGroupStopFuture.get());
        assertDoesNotThrow(() -> nestedProcessGroupStopFuture.get());
        assertDoesNotThrow(() -> nestedRemoteProcessGroupStopFuture.get());
    }

    @Test
    public void testStopPartiallyApplied() {
        ProcessGroup rootProcessGroup = mock(ProcessGroup.class);
        RemoteProcessGroup remoteProcessGroup = mock(RemoteProcessGroup.class);
        Set<RemoteProcessGroup> remoteProcessGroups = Set.of(remoteProcessGroup);
        CompletableFuture<Void> rootProcessGroupStopFuture = mock(CompletableFuture.class);

        when(flowManager.getRootGroup()).thenReturn(rootProcessGroup);
        when(rootProcessGroup.getRemoteProcessGroups()).thenReturn(remoteProcessGroups);
        when(rootProcessGroup.stopProcessing()).thenReturn(rootProcessGroupStopFuture);
        when(remoteProcessGroup.stopTransmitting()).thenThrow(new RuntimeException());

        OperationState result = victim.stop();

        assertEquals(PARTIALLY_APPLIED, result);
    }

    @Test
    public void testStopNotApplied() {
        when(flowManager.getRootGroup()).thenReturn(null);

        OperationState result = victim.stop();

        assertEquals(NOT_APPLIED, result);
    }
}