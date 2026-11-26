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

import org.apache.nifi.components.connector.components.ComponentHierarchyScope;
import org.apache.nifi.components.connector.components.StatelessGroupLifecycle;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.mock.MockProcessGroup;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TestStandaloneProcessGroupLifecycle {

    @Mock
    private ControllerServiceProvider controllerServiceProvider;

    @Mock
    private StatelessGroupLifecycle statelessGroupLifecycle;

    @Test
    void testStartProcessorsIncludesStandardGroupsOnly() throws Exception {
        final MockProcessGroup rootGroup = spy(new MockProcessGroup(null, "root"));
        final MockProcessGroup standardChildGroup = spy(new MockProcessGroup(null, "standard"));
        final MockProcessGroup statelessChildGroup = spy(new MockProcessGroup(null, "stateless"));
        final MockProcessGroup inheritedStatelessChildGroup = spy(new MockProcessGroup(null, "inherited-stateless"));
        final ProcessorNode rootProcessor = createStartableProcessor(rootGroup);
        final ProcessorNode standardChildProcessor = createStartableProcessor(standardChildGroup);
        final ProcessorNode statelessChildProcessor = createStartableProcessor(statelessChildGroup);
        final ProcessorNode inheritedStatelessChildProcessor = createStartableProcessor(inheritedStatelessChildGroup);

        rootGroup.setExecutionEngine(ExecutionEngine.STANDARD);
        standardChildGroup.setExecutionEngine(ExecutionEngine.INHERITED);
        statelessChildGroup.setExecutionEngine(ExecutionEngine.STATELESS);
        inheritedStatelessChildGroup.setExecutionEngine(ExecutionEngine.INHERITED);

        rootGroup.addProcessor(rootProcessor);
        standardChildGroup.addProcessor(standardChildProcessor);
        statelessChildGroup.addProcessor(statelessChildProcessor);
        inheritedStatelessChildGroup.addProcessor(inheritedStatelessChildProcessor);
        statelessChildGroup.addProcessGroup(inheritedStatelessChildGroup);
        rootGroup.addProcessGroup(standardChildGroup);
        rootGroup.addProcessGroup(statelessChildGroup);

        final StandaloneProcessGroupLifecycle rootLifecycle = new StandaloneProcessGroupLifecycle(
            rootGroup, controllerServiceProvider, statelessGroupLifecycle, id -> null);

        rootLifecycle.startProcessors(ComponentHierarchyScope.INCLUDE_CHILD_GROUPS).get();

        verify(rootGroup).startProcessor(rootProcessor, true);
        verify(standardChildGroup).startProcessor(standardChildProcessor, true);
        verify(statelessChildGroup, never()).startProcessor(statelessChildProcessor, true);
        verify(inheritedStatelessChildGroup, never()).startProcessor(inheritedStatelessChildProcessor, true);
        verify(rootGroup, never()).findAllProcessors();
    }

    private ProcessorNode createStartableProcessor(final ProcessGroup owningGroup) {
        final ProcessorNode processor = mock(ProcessorNode.class);
        final String processorIdentifier = owningGroup.getIdentifier() + "-processor";
        when(processor.getIdentifier()).thenReturn(processorIdentifier);
        when(processor.getValidationStatus()).thenReturn(ValidationStatus.VALID);
        when(processor.getScheduledState()).thenReturn(ScheduledState.STOPPED);
        when(processor.getProcessGroup()).thenReturn(owningGroup);
        return processor;
    }
}
