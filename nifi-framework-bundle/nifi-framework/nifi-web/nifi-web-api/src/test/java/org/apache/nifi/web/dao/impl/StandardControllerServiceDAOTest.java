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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.ProcessGroupDrain;
import org.apache.nifi.groups.StatelessGroupScheduledState;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardControllerServiceDAOTest {

    private static final String SERVICE_ID = "service-1";

    @Mock
    private ControllerServiceProvider serviceProvider;

    @Mock
    private ControllerServiceNode controllerService;

    @Mock
    private ControllerServiceReference references;

    @Mock
    private ProcessorNode processor;

    @Mock
    private ProcessGroup processGroup;

    @Mock
    private ProcessGroupDrain drain;

    private StandardControllerServiceDAO dao;

    @BeforeEach
    void setUp() {
        dao = new StandardControllerServiceDAO();
        dao.setServiceProvider(serviceProvider);

        when(serviceProvider.getControllerServiceNode(SERVICE_ID)).thenReturn(controllerService);
        when(controllerService.getReferences()).thenReturn(references);
        when(references.findRecursiveReferences(ProcessorNode.class)).thenReturn(List.of(processor));
        when(processor.getProcessGroup()).thenReturn(processGroup);
        when(processGroup.findActiveDrain()).thenReturn(Optional.of(drain));
    }

    @Test
    void testDrainRulesApplyToProcessorsStartedWithReferencingComponents() {
        when(processor.getScheduledState()).thenReturn(ScheduledState.STOPPED);
        when(processor.getPhysicalScheduledState()).thenReturn(ScheduledState.STOPPED);
        when(drain.keepsStopped(processor)).thenReturn(true);
        when(drain.getProcessGroup()).thenReturn(processGroup);

        // A drain that keeps the referencing Processor stopped prevents it from being started while data is queued, in both the verify and perform phases
        doThrow(new IllegalStateException("Data is queued")).when(drain).verifyCanStart(processor);
        assertIllegalStateThrown(() -> dao.verifyUpdateReferencingComponents(SERVICE_ID, ScheduledState.RUNNING, null));
        assertIllegalStateThrown(() -> dao.updateControllerServiceReferencingComponents(SERVICE_ID, ScheduledState.RUNNING, null));
        verify(processGroup, never()).endDrain();
        verify(serviceProvider, never()).scheduleReferencingComponents(any(ControllerServiceNode.class));

        // Once no data is queued, the drain is ended before the referencing Processor is started
        doNothing().when(drain).verifyCanStart(processor);
        dao.verifyUpdateReferencingComponents(SERVICE_ID, ScheduledState.RUNNING, null);
        dao.updateControllerServiceReferencingComponents(SERVICE_ID, ScheduledState.RUNNING, null);
        final InOrder inOrder = inOrder(processGroup, serviceProvider);
        inOrder.verify(processGroup).endDrain();
        inOrder.verify(serviceProvider).scheduleReferencingComponents(controllerService);
    }

    @Test
    void testDrainRulesApplyToStatelessGroupStoppedByDisablingService() {
        when(controllerService.getState()).thenReturn(ControllerServiceState.ENABLED);
        when(processGroup.resolveExecutionEngine()).thenReturn(ExecutionEngine.STATELESS);
        when(processGroup.getStatelessScheduledState()).thenReturn(StatelessGroupScheduledState.RUNNING);

        // A Processor in a running Stateless group is stopped through that group, so a drain that keeps the group running prevents the service from being disabled while data is queued
        doThrow(new IllegalStateException("Data is queued")).when(drain).verifyCanStop(processGroup);
        final ControllerServiceDTO serviceDTO = new ControllerServiceDTO();
        serviceDTO.setId(SERVICE_ID);
        serviceDTO.setState(ControllerServiceState.DISABLED.name());
        assertIllegalStateThrown(() -> dao.verifyUpdate(serviceDTO));
        assertIllegalStateThrown(() -> dao.updateControllerService(serviceDTO));
        verify(serviceProvider, never()).unscheduleReferencingComponents(any(ControllerServiceNode.class));
        verify(serviceProvider, never()).disableControllerService(any(ControllerServiceNode.class));
    }

    private static void assertIllegalStateThrown(final Runnable action) {
        try {
            action.run();
        } catch (final IllegalStateException expected) {
            return;
        }

        fail("Expected an IllegalStateException");
    }
}
