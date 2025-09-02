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

import org.apache.nifi.components.connector.FlowContextFactory;
import org.apache.nifi.components.connector.FrameworkFlowContext;
import org.apache.nifi.components.connector.MutableConnectorConfigurationContext;
import org.apache.nifi.components.connector.ParameterContextFacadeFactory;
import org.apache.nifi.components.connector.ProcessGroupFacadeFactory;
import org.apache.nifi.components.connector.ProcessGroupFactory;
import org.apache.nifi.components.connector.StandardFlowContext;
import org.apache.nifi.components.connector.components.FlowContextType;
import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.facades.standalone.StandaloneParameterContextFacade;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.registry.flow.mapping.ComponentIdLookup;
import org.apache.nifi.registry.flow.mapping.FlowMappingOptions;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedProcessGroup;
import org.apache.nifi.registry.flow.mapping.VersionedComponentFlowMapper;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

public class FlowControllerFlowContextFactory implements FlowContextFactory {
    private final FlowController flowController;
    private final ProcessGroup activeManagedProcessGroup;
    private final MutableConnectorConfigurationContext activeConfigurationContext;
    private final ParameterContextFacadeFactory parameterContextFacadeFactory;
    private final ProcessGroupFacadeFactory processGroupFacadeFactory;
    private final ProcessGroupFactory processGroupFactory;

    public FlowControllerFlowContextFactory(final FlowController flowController, final ProcessGroup activeManagedProcessGroup,
            final MutableConnectorConfigurationContext activeConfigurationContext, final ProcessGroupFactory processGroupFactory) {

        this.flowController = flowController;
        this.activeManagedProcessGroup = activeManagedProcessGroup;
        this.activeConfigurationContext = activeConfigurationContext;
        this.processGroupFactory = processGroupFactory;

        this.processGroupFacadeFactory = new FlowControllerProcessGroupFacadeFactory(flowController);
        this.parameterContextFacadeFactory = processGroup -> new StandaloneParameterContextFacade(flowController, processGroup);
    }

    @Override
    public FrameworkFlowContext createActiveFlowContext(final String connectorId, final ComponentLog connectorLogger, final Bundle bundle) {
        return new StandardFlowContext(activeManagedProcessGroup, activeConfigurationContext, processGroupFacadeFactory,
            parameterContextFacadeFactory, connectorLogger, FlowContextType.ACTIVE, bundle);
    }

    @Override
    public FrameworkFlowContext createWorkingFlowContext(final String connectorId, final ComponentLog connectorLogger,
                final MutableConnectorConfigurationContext activeConfigurationContext, final Bundle bundle) {

        final String workingGroupId = UUID.nameUUIDFromBytes((connectorId + "-working").getBytes(StandardCharsets.UTF_8)).toString();
        final ProcessGroup processGroup = processGroupFactory.create(workingGroupId);
        copyGroupContents(activeManagedProcessGroup, processGroup, connectorId + "-working-context");

        final ProcessGroupFacade processGroupFacade = processGroupFacadeFactory.create(processGroup, connectorLogger);
        final ParameterContextFacade parameterContextFacade = new StandaloneParameterContextFacade(flowController, processGroup);
        final MutableConnectorConfigurationContext workingConfigurationContext = activeConfigurationContext.clone();

        return new StandardFlowContext(processGroup, workingConfigurationContext, processGroupFacadeFactory,
            parameterContextFacadeFactory, connectorLogger, FlowContextType.WORKING, bundle);
    }

    private void copyGroupContents(final ProcessGroup sourceGroup, final ProcessGroup destinationGroup, final String componentIdSeed) {
        final FlowMappingOptions flowMappingOptions = new FlowMappingOptions.Builder()
            .mapSensitiveConfiguration(true)
            .mapPropertyDescriptors(true)
            .stateLookup(VersionedComponentStateLookup.ENABLED_OR_DISABLED)
            .sensitiveValueEncryptor(value -> value)
            .componentIdLookup(ComponentIdLookup.VERSIONED_OR_GENERATE)
            .mapInstanceIdentifiers(true)
            .mapControllerServiceReferencesToVersionedId(true)
            .mapFlowRegistryClientId(true)
            .mapAssetReferences(true)
            .build();

        final VersionedComponentFlowMapper flowMapper = new VersionedComponentFlowMapper(flowController.getExtensionManager(), flowMappingOptions);
        final Map<String, VersionedParameterContext> parameterContexts = flowMapper.mapParameterContexts(sourceGroup, true, Map.of());
        final InstantiatedVersionedProcessGroup versionedGroup = flowMapper.mapProcessGroup(sourceGroup, flowController.getControllerServiceProvider(),
            flowController.getFlowManager(), true);
        final VersionedExternalFlow versionedExternalFlow = new VersionedExternalFlow();
        versionedExternalFlow.setFlowContents(versionedGroup);
        versionedExternalFlow.setExternalControllerServices(Map.of());
        versionedExternalFlow.setParameterProviders(Map.of());
        versionedExternalFlow.setParameterContexts(parameterContexts);

        destinationGroup.updateFlow(versionedExternalFlow, componentIdSeed, false, true, true);
    }
}
