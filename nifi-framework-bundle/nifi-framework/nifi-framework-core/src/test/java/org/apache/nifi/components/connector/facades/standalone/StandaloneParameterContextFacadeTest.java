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
package org.apache.nifi.components.connector.facades.standalone;

import org.apache.nifi.components.connector.components.ParameterValue;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StandaloneParameterContextFacadeTest {

    private static final String PARAMETER_NAME = "Classpath Resource";

    @Test
    public void testUpdateParametersReloadsStoppedProcessorsThatReferenceUpdatedServices() {
        final FlowController flowController = mock(FlowController.class);
        final FlowManager flowManager = mock(FlowManager.class);
        final ControllerServiceProvider controllerServiceProvider = mock(ControllerServiceProvider.class);
        when(flowController.getFlowManager()).thenReturn(flowManager);
        when(flowController.getControllerServiceProvider()).thenReturn(controllerServiceProvider);

        final ParameterContext parameterContext = mock(ParameterContext.class);
        final ParameterDescriptor parameterDescriptor = new ParameterDescriptor.Builder()
                .name(PARAMETER_NAME)
                .sensitive(false)
                .build();
        final Parameter existingParameter = new Parameter.Builder()
                .descriptor(parameterDescriptor)
                .value("old-asset")
                .build();
        when(parameterContext.getParameter(PARAMETER_NAME)).thenReturn(Optional.of(existingParameter));

        final ProcessorNode referencingProcessor = mock(ProcessorNode.class);
        when(referencingProcessor.getPhysicalScheduledState()).thenReturn(ScheduledState.STOPPED);
        when(referencingProcessor.isRunning()).thenReturn(false);
        when(referencingProcessor.isReferencingParameter(PARAMETER_NAME)).thenReturn(false);

        final ControllerServiceReference serviceReferences = mock(ControllerServiceReference.class);
        when(serviceReferences.findRecursiveReferences(ComponentNode.class)).thenReturn(List.of(referencingProcessor));

        final ControllerServiceNode referencingService = mock(ControllerServiceNode.class);
        when(referencingService.isReferencingParameter(PARAMETER_NAME)).thenReturn(true);
        when(referencingService.getState()).thenReturn(ControllerServiceState.DISABLED);
        when(referencingService.getReferences()).thenReturn(serviceReferences);

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroup.getParameterContext()).thenReturn(parameterContext);
        when(processGroup.getExecutionEngine()).thenReturn(ExecutionEngine.STANDARD);
        when(processGroup.referencesParameterContext(parameterContext)).thenReturn(true);
        when(processGroup.getControllerServices(false)).thenReturn(Set.of(referencingService));
        when(processGroup.getProcessors()).thenReturn(Set.of());
        when(processGroup.findAllProcessGroups(any())).thenAnswer(invocation -> {
            final Predicate<ProcessGroup> predicate = invocation.getArgument(0);
            if (predicate.test(processGroup)) {
                return List.of(processGroup);
            }
            return List.of();
        });
        when(referencingProcessor.getProcessGroup()).thenReturn(processGroup);
        when(referencingService.getProcessGroup()).thenReturn(processGroup);

        final StandaloneParameterContextFacade facade = new StandaloneParameterContextFacade(flowController, processGroup);
        final ParameterValue updatedParameter = new ParameterValue.Builder()
                .name(PARAMETER_NAME)
                .value("new-asset")
                .sensitive(false)
                .build();

        facade.updateParameters(List.of(updatedParameter));

        verify(referencingService).reloadAdditionalResourcesIfNecessary();
        verify(referencingProcessor).reloadAdditionalResourcesIfNecessary();
    }
}
