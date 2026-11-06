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

package org.apache.nifi.components.connector;

import org.apache.nifi.components.connector.components.FlowContextType;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestStandardFlowContext {

    @Test
    void testReloadReplacesStateAfterCreatingRootGroup() {
        final ProcessGroup managedProcessGroup = mock(ProcessGroup.class);
        final ProcessGroupFacadeFactory groupFacadeFactory = mock(ProcessGroupFacadeFactory.class);
        final ParameterContextFacadeFactory parameterContextFacadeFactory = mock(ParameterContextFacadeFactory.class);
        final ComponentLog originalLog = mock(ComponentLog.class);
        final ComponentLog replacementLog = mock(ComponentLog.class);
        final MutableConnectorConfigurationContext originalConfiguration = mock(MutableConnectorConfigurationContext.class);
        final MutableConnectorConfigurationContext replacementConfiguration = mock(MutableConnectorConfigurationContext.class);
        final MutableConnectorConfigurationContext failedConfiguration = mock(MutableConnectorConfigurationContext.class);
        final Bundle originalBundle = new Bundle("group", "artifact", "1.0.0");
        final Bundle replacementBundle = new Bundle("group", "artifact", "2.0.0");
        final Bundle failedBundle = new Bundle("group", "artifact", "3.0.0");
        final ProcessGroupFacade originalRootGroup = mock(ProcessGroupFacade.class);
        final ProcessGroupFacade replacementRootGroup = mock(ProcessGroupFacade.class);
        when(groupFacadeFactory.create(managedProcessGroup, originalLog)).thenReturn(originalRootGroup);
        when(groupFacadeFactory.create(managedProcessGroup, replacementLog)).thenReturn(replacementRootGroup);

        final StandardFlowContext flowContext = new StandardFlowContext(managedProcessGroup, originalConfiguration, groupFacadeFactory,
            parameterContextFacadeFactory, originalLog, FlowContextType.ACTIVE, originalBundle);

        flowContext.reload(replacementBundle, replacementLog, replacementConfiguration);

        assertSame(replacementBundle, flowContext.getBundle());
        assertSame(replacementConfiguration, flowContext.getConfigurationContext());
        assertSame(replacementRootGroup, flowContext.getRootGroup());

        final ComponentLog failedLog = mock(ComponentLog.class);
        when(groupFacadeFactory.create(managedProcessGroup, failedLog)).thenThrow(new IllegalStateException("Root group creation failed"));

        assertThrows(IllegalStateException.class, () -> flowContext.reload(failedBundle, failedLog, failedConfiguration));
        assertSame(replacementBundle, flowContext.getBundle());
        assertSame(replacementConfiguration, flowContext.getConfigurationContext());
        assertSame(replacementRootGroup, flowContext.getRootGroup());
    }
}
