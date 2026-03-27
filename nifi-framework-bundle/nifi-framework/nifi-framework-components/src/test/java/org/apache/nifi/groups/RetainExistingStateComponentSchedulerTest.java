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
package org.apache.nifi.groups;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.flow.ExecutionEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RetainExistingStateComponentSchedulerTest {

    private ComponentScheduler delegate;

    @BeforeEach
    void setup() {
        delegate = mock(ComponentScheduler.class);
    }

    @Test
    void testEmptyProcessGroupIsNotActive() {
        final ProcessGroup group = createProcessGroup(Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
        final RetainExistingStateComponentScheduler scheduler = new RetainExistingStateComponentScheduler(group, delegate);
        assertFalse(scheduler.isProcessGroupActive());
        assertFalse(RetainExistingStateComponentScheduler.hasActiveRuntimeState(group));
    }

    @Test
    void testProcessGroupWithRunningProcessorIsActive() {
        final ProcessorNode processor = createMockProcessor("proc-1", ScheduledState.RUNNING);
        final ProcessGroup group = createProcessGroup(Set.of(processor), Collections.emptySet(), Collections.emptySet());
        assertTrue(RetainExistingStateComponentScheduler.hasActiveRuntimeState(group));
        assertTrue(new RetainExistingStateComponentScheduler(group, delegate).isProcessGroupActive());
    }

    @Test
    void testProcessGroupWithStartingProcessorIsActive() {
        final ProcessorNode processor = createMockProcessor("proc-1", ScheduledState.STARTING);
        final ProcessGroup group = createProcessGroup(Set.of(processor), Collections.emptySet(), Collections.emptySet());
        assertTrue(RetainExistingStateComponentScheduler.hasActiveRuntimeState(group));
    }

    @Test
    void testProcessGroupWithEnabledServiceIsActive() {
        final ControllerServiceNode service = createMockService("svc-1", ControllerServiceState.ENABLED);
        final ProcessGroup group = createProcessGroup(Collections.emptySet(), Collections.emptySet(), Set.of(service));
        assertTrue(RetainExistingStateComponentScheduler.hasActiveRuntimeState(group));
        assertTrue(new RetainExistingStateComponentScheduler(group, delegate).isProcessGroupActive());
    }

    @Test
    void testProcessGroupWithEnablingServiceIsActive() {
        final ControllerServiceNode service = createMockService("svc-1", ControllerServiceState.ENABLING);
        final ProcessGroup group = createProcessGroup(Collections.emptySet(), Collections.emptySet(), Set.of(service));
        assertTrue(RetainExistingStateComponentScheduler.hasActiveRuntimeState(group));
    }

    @Test
    void testProcessGroupWithStoppedComponentsIsNotActive() {
        final ProcessorNode processor = createMockProcessor("proc-1", ScheduledState.STOPPED);
        final ControllerServiceNode service = createMockService("svc-1", ControllerServiceState.DISABLED);
        final ProcessGroup group = createProcessGroup(Set.of(processor), Collections.emptySet(), Set.of(service));
        assertFalse(RetainExistingStateComponentScheduler.hasActiveRuntimeState(group));
        assertFalse(new RetainExistingStateComponentScheduler(group, delegate).isProcessGroupActive());
    }

    @Test
    void testProcessGroupWithRunningPortIsActive() {
        final Port runningPort = mock(Port.class);
        when(runningPort.getIdentifier()).thenReturn("port-1");
        when(runningPort.getScheduledState()).thenReturn(ScheduledState.RUNNING);
        final ProcessGroup group = createProcessGroup(Collections.emptySet(), Set.of(runningPort), Collections.emptySet());
        assertTrue(RetainExistingStateComponentScheduler.hasActiveRuntimeState(group));
    }

    @Test
    void testNewComponentStartedWhenProcessGroupActive() {
        final ProcessorNode runningProcessor = createMockProcessor("running-proc", ScheduledState.RUNNING);
        final ProcessGroup group = createProcessGroup(Set.of(runningProcessor), Collections.emptySet(), Collections.emptySet());
        final RetainExistingStateComponentScheduler scheduler = new RetainExistingStateComponentScheduler(group, delegate);
        assertTrue(scheduler.isProcessGroupActive());

        final Connectable newComponent = mock(Connectable.class);
        when(newComponent.getIdentifier()).thenReturn("new-component-id");

        scheduler.startComponent(newComponent);
        verify(delegate).startComponent(newComponent);
    }

    @Test
    void testNewComponentNotStartedWhenProcessGroupInactive() {
        final ProcessGroup group = createProcessGroup(Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
        final RetainExistingStateComponentScheduler scheduler = new RetainExistingStateComponentScheduler(group, delegate);
        assertFalse(scheduler.isProcessGroupActive());

        final Connectable newComponent = mock(Connectable.class);
        when(newComponent.getIdentifier()).thenReturn("new-component-id");

        scheduler.startComponent(newComponent);
        verify(delegate, never()).startComponent(newComponent);
    }

    @Test
    void testExistingRunningComponentStarted() {
        final ProcessorNode runningProcessor = createMockProcessor("running-proc", ScheduledState.RUNNING);
        final ProcessGroup group = createProcessGroup(Set.of(runningProcessor), Collections.emptySet(), Collections.emptySet());
        final RetainExistingStateComponentScheduler scheduler = new RetainExistingStateComponentScheduler(group, delegate);

        scheduler.startComponent(runningProcessor);
        verify(delegate).startComponent(runningProcessor);
    }

    @Test
    void testExistingStoppedComponentNotStarted() {
        final ProcessorNode stoppedProcessor = createMockProcessor("stopped-proc", ScheduledState.STOPPED);
        final ProcessorNode runningProcessor = createMockProcessor("running-proc", ScheduledState.RUNNING);
        final ProcessGroup group = createProcessGroup(Set.of(stoppedProcessor, runningProcessor), Collections.emptySet(), Collections.emptySet());
        final RetainExistingStateComponentScheduler scheduler = new RetainExistingStateComponentScheduler(group, delegate);

        scheduler.startComponent(stoppedProcessor);
        verify(delegate, never()).startComponent(stoppedProcessor);
    }

    @Test
    void testNewServiceEnabledWhenProcessGroupActive() {
        final ControllerServiceNode existingService = createMockService("existing-svc", ControllerServiceState.ENABLED);
        final ProcessGroup group = createProcessGroup(Collections.emptySet(), Collections.emptySet(), Set.of(existingService));
        final RetainExistingStateComponentScheduler scheduler = new RetainExistingStateComponentScheduler(group, delegate);
        assertTrue(scheduler.isProcessGroupActive());

        final ControllerServiceNode newService = createMockService("new-svc", ControllerServiceState.DISABLED);
        scheduler.enableControllerServicesAsync(List.of(newService));
        verify(delegate).enableControllerServicesAsync(Set.of(newService));
    }

    @Test
    void testNewServiceNotEnabledWhenProcessGroupInactive() {
        final ProcessGroup group = createProcessGroup(Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
        final RetainExistingStateComponentScheduler scheduler = new RetainExistingStateComponentScheduler(group, delegate);

        final ControllerServiceNode newService = createMockService("new-svc", ControllerServiceState.DISABLED);
        scheduler.enableControllerServicesAsync(List.of(newService));
        verify(delegate).enableControllerServicesAsync(Collections.emptySet());
    }

    @Test
    void testExistingEnabledServiceReEnabled() {
        final ControllerServiceNode enabledService = createMockService("enabled-svc", ControllerServiceState.ENABLED);
        final ProcessGroup group = createProcessGroup(Collections.emptySet(), Collections.emptySet(), Set.of(enabledService));
        final RetainExistingStateComponentScheduler scheduler = new RetainExistingStateComponentScheduler(group, delegate);

        scheduler.enableControllerServicesAsync(List.of(enabledService));
        verify(delegate).enableControllerServicesAsync(Set.of(enabledService));
    }

    @Test
    void testExistingDisabledServiceNotReEnabled() {
        final ControllerServiceNode disabledService = createMockService("disabled-svc", ControllerServiceState.DISABLED);
        final ProcessorNode runningProcessor = createMockProcessor("running-proc", ScheduledState.RUNNING);
        final ProcessGroup group = createProcessGroup(Set.of(runningProcessor), Collections.emptySet(), Set.of(disabledService));
        final RetainExistingStateComponentScheduler scheduler = new RetainExistingStateComponentScheduler(group, delegate);

        scheduler.enableControllerServicesAsync(List.of(disabledService));
        verify(delegate).enableControllerServicesAsync(Collections.emptySet());
    }

    @Test
    void testNewStatelessGroupStartedWhenProcessGroupActive() {
        final ProcessorNode runningProcessor = createMockProcessor("running-proc", ScheduledState.RUNNING);
        final ProcessGroup parentGroup = createProcessGroup(Set.of(runningProcessor), Collections.emptySet(), Collections.emptySet());
        final RetainExistingStateComponentScheduler scheduler = new RetainExistingStateComponentScheduler(parentGroup, delegate);
        assertTrue(scheduler.isProcessGroupActive());

        final ProcessGroup newStatelessGroup = mock(ProcessGroup.class);
        when(newStatelessGroup.getIdentifier()).thenReturn("new-stateless-group");

        scheduler.startStatelessGroup(newStatelessGroup);
        verify(delegate).startStatelessGroup(newStatelessGroup);
    }

    @Test
    void testNewStatelessGroupNotStartedWhenProcessGroupInactive() {
        final ProcessGroup parentGroup = createProcessGroup(Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
        final RetainExistingStateComponentScheduler scheduler = new RetainExistingStateComponentScheduler(parentGroup, delegate);

        final ProcessGroup newStatelessGroup = mock(ProcessGroup.class);
        when(newStatelessGroup.getIdentifier()).thenReturn("new-stateless-group");

        scheduler.startStatelessGroup(newStatelessGroup);
        verify(delegate, never()).startStatelessGroup(newStatelessGroup);
    }

    @Test
    void testTransitionComponentStateStartsNewComponentWhenActive() {
        final ProcessorNode runningProcessor = createMockProcessor("running-proc", ScheduledState.RUNNING);
        final ProcessGroup group = createProcessGroup(Set.of(runningProcessor), Collections.emptySet(), Collections.emptySet());
        final RetainExistingStateComponentScheduler scheduler = new RetainExistingStateComponentScheduler(group, delegate);

        final Connectable newComponent = mock(Connectable.class);
        when(newComponent.getIdentifier()).thenReturn("new-component-id");

        scheduler.transitionComponentState(newComponent, org.apache.nifi.flow.ScheduledState.ENABLED);
        verify(delegate).startComponent(newComponent);
        verify(delegate).transitionComponentState(newComponent, org.apache.nifi.flow.ScheduledState.ENABLED);
    }

    @Test
    void testTransitionComponentStateDoesNotStartNewDisabledComponent() {
        final ProcessorNode runningProcessor = createMockProcessor("running-proc", ScheduledState.RUNNING);
        final ProcessGroup group = createProcessGroup(Set.of(runningProcessor), Collections.emptySet(), Collections.emptySet());
        final RetainExistingStateComponentScheduler scheduler = new RetainExistingStateComponentScheduler(group, delegate);

        final Connectable newComponent = mock(Connectable.class);
        when(newComponent.getIdentifier()).thenReturn("new-component-id");

        scheduler.transitionComponentState(newComponent, org.apache.nifi.flow.ScheduledState.DISABLED);
        verify(delegate, never()).startComponent(newComponent);
        verify(delegate).transitionComponentState(newComponent, org.apache.nifi.flow.ScheduledState.DISABLED);
    }

    @Test
    void testTransitionComponentStateDoesNotStartNewComponentWhenInactive() {
        final ProcessGroup group = createProcessGroup(Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
        final RetainExistingStateComponentScheduler scheduler = new RetainExistingStateComponentScheduler(group, delegate);

        final Connectable newComponent = mock(Connectable.class);
        when(newComponent.getIdentifier()).thenReturn("new-component-id");

        scheduler.transitionComponentState(newComponent, org.apache.nifi.flow.ScheduledState.ENABLED);
        verify(delegate, never()).startComponent(newComponent);
        verify(delegate).transitionComponentState(newComponent, org.apache.nifi.flow.ScheduledState.ENABLED);
    }

    @Test
    void testTransitionComponentStateDelegatesForExistingComponents() {
        final ProcessorNode runningProcessor = createMockProcessor("running-proc", ScheduledState.RUNNING);
        final ProcessGroup group = createProcessGroup(Set.of(runningProcessor), Collections.emptySet(), Collections.emptySet());
        final RetainExistingStateComponentScheduler scheduler = new RetainExistingStateComponentScheduler(group, delegate);

        scheduler.transitionComponentState(runningProcessor, org.apache.nifi.flow.ScheduledState.ENABLED);
        verify(delegate, never()).startComponent(runningProcessor);
        verify(delegate).transitionComponentState(runningProcessor, org.apache.nifi.flow.ScheduledState.ENABLED);
    }

    private ProcessorNode createMockProcessor(final String identifier, final ScheduledState state) {
        final ProcessorNode processor = mock(ProcessorNode.class);
        when(processor.getIdentifier()).thenReturn(identifier);
        when(processor.getScheduledState()).thenReturn(state);
        return processor;
    }

    private ControllerServiceNode createMockService(final String identifier, final ControllerServiceState state) {
        final ControllerServiceNode service = mock(ControllerServiceNode.class);
        when(service.getIdentifier()).thenReturn(identifier);
        when(service.getState()).thenReturn(state);
        return service;
    }

    private ProcessGroup createProcessGroup(final Set<ProcessorNode> processors, final Set<Port> inputPorts, final Set<ControllerServiceNode> services) {
        final ProcessGroup group = Mockito.mock(ProcessGroup.class);
        when(group.getProcessors()).thenReturn(processors);
        when(group.getInputPorts()).thenReturn(inputPorts);
        when(group.getOutputPorts()).thenReturn(Collections.emptySet());
        when(group.getFunnels()).thenReturn(Collections.emptySet());
        when(group.getRemoteProcessGroups()).thenReturn(Collections.emptySet());
        when(group.getProcessGroups()).thenReturn(Collections.emptySet());
        when(group.findAllControllerServices()).thenReturn(new HashSet<>(services));
        when(group.resolveExecutionEngine()).thenReturn(ExecutionEngine.STANDARD);
        return group;
    }
}
