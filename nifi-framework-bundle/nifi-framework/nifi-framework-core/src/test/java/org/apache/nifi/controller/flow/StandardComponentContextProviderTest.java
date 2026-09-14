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

package org.apache.nifi.controller.flow;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.MockStateManagerProvider;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.mock.DummyProcessor;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.processor.ProcessContext;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StandardComponentContextProviderTest {

    @Test
    void createProcessContextResolvesAncestorControllerServiceOverrideFromVersionedIdToRuntimeId() {
        final String runtimeServiceId = "runtime-service-id";
        final String versionedServiceId = "versioned-service-id";
        final PropertyDescriptor serviceProperty = DummyProcessor.SERVICE;

        final ControllerServiceNode ancestorService = mock(ControllerServiceNode.class);
        when(ancestorService.getIdentifier()).thenReturn(runtimeServiceId);
        when(ancestorService.getVersionedComponentId()).thenReturn(Optional.of(versionedServiceId));

        final ProcessGroup childGroup = mock(ProcessGroup.class);
        when(childGroup.getControllerServices(true)).thenReturn(Set.of(ancestorService));

        final ProcessorNode processorNode = mock(ProcessorNode.class);
        when(processorNode.getIdentifier()).thenReturn("processor-id");
        when(processorNode.getProcessGroup()).thenReturn(childGroup);
        when(processorNode.getPropertyDescriptors()).thenReturn(List.of(serviceProperty));
        when(processorNode.getPropertyDescriptor(serviceProperty.getName())).thenReturn(serviceProperty);
        when(processorNode.getEffectivePropertyValues()).thenReturn(Map.of(serviceProperty, versionedServiceId));
        when(processorNode.getParameterLookup()).thenReturn(ParameterLookup.EMPTY);

        final FlowController flowController = mock(FlowController.class);
        when(flowController.getStateManagerProvider()).thenReturn(new MockStateManagerProvider());
        when(flowController.getControllerServiceProvider()).thenReturn(mock(ControllerServiceProvider.class));

        final StandardComponentContextProvider contextProvider = new StandardComponentContextProvider(flowController);
        final ProcessContext context = contextProvider.createProcessContext(
            processorNode,
            Map.of(serviceProperty.getName(), versionedServiceId),
            ParameterLookup.EMPTY
        );

        assertEquals(runtimeServiceId, context.getAllProperties().get(serviceProperty.getName()));
        assertEquals(runtimeServiceId, context.getProperty(serviceProperty).getValue());
    }

    @Test
    void createProcessContextResolvesCurrentGroupControllerServiceOverrideFromVersionedIdToRuntimeId() {
        final String runtimeServiceId = "runtime-service-id";
        final String versionedServiceId = "versioned-service-id";
        final PropertyDescriptor serviceProperty = DummyProcessor.SERVICE;

        final ControllerServiceNode currentGroupService = mock(ControllerServiceNode.class);
        when(currentGroupService.getIdentifier()).thenReturn(runtimeServiceId);
        when(currentGroupService.getVersionedComponentId()).thenReturn(Optional.of(versionedServiceId));

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroup.getControllerServices(true)).thenReturn(Set.of(currentGroupService));

        final ProcessorNode processorNode = mock(ProcessorNode.class);
        when(processorNode.getIdentifier()).thenReturn("processor-id");
        when(processorNode.getProcessGroup()).thenReturn(processGroup);
        when(processorNode.getPropertyDescriptors()).thenReturn(List.of(serviceProperty));
        when(processorNode.getPropertyDescriptor(serviceProperty.getName())).thenReturn(serviceProperty);
        when(processorNode.getEffectivePropertyValues()).thenReturn(Map.of(serviceProperty, versionedServiceId));
        when(processorNode.getParameterLookup()).thenReturn(ParameterLookup.EMPTY);

        final FlowController flowController = mock(FlowController.class);
        when(flowController.getStateManagerProvider()).thenReturn(new MockStateManagerProvider());
        when(flowController.getControllerServiceProvider()).thenReturn(mock(ControllerServiceProvider.class));

        final StandardComponentContextProvider contextProvider = new StandardComponentContextProvider(flowController);
        final ProcessContext context = contextProvider.createProcessContext(
            processorNode,
            Map.of(serviceProperty.getName(), versionedServiceId),
            ParameterLookup.EMPTY
        );

        assertEquals(runtimeServiceId, context.getAllProperties().get(serviceProperty.getName()));
        assertEquals(runtimeServiceId, context.getProperty(serviceProperty).getValue());
    }
}
