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

import org.apache.nifi.components.connector.ProcessGroupFacadeFactory;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.facades.standalone.ComponentContextProvider;
import org.apache.nifi.components.connector.facades.standalone.StandaloneProcessGroupFacade;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.registry.flow.mapping.ComponentIdLookup;
import org.apache.nifi.registry.flow.mapping.FlowMappingOptions;
import org.apache.nifi.registry.flow.mapping.VersionedComponentFlowMapper;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;

public class FlowControllerProcessGroupFacadeFactory implements ProcessGroupFacadeFactory {
    private final FlowController flowController;

    public FlowControllerProcessGroupFacadeFactory(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    public ProcessGroupFacade create(final ProcessGroup processGroup, final ComponentLog connectorLogger) {
        final FlowMappingOptions flowMappingOptions = new FlowMappingOptions.Builder()
            .mapSensitiveConfiguration(true)
            .mapPropertyDescriptors(true)
            .stateLookup(VersionedComponentStateLookup.IDENTITY_LOOKUP)
            .sensitiveValueEncryptor(value -> value)
            .componentIdLookup(ComponentIdLookup.VERSIONED_OR_GENERATE)
            .mapInstanceIdentifiers(true)
            .mapControllerServiceReferencesToVersionedId(true)
            .mapFlowRegistryClientId(true)
            .mapAssetReferences(true)
            .build();

        final VersionedComponentFlowMapper flowMapper = new VersionedComponentFlowMapper(flowController.getExtensionManager(), flowMappingOptions);

        final VersionedProcessGroup versionedManagedGroup = flowMapper.mapProcessGroup(processGroup, flowController.getControllerServiceProvider(),
            flowController.getFlowManager(), true);
        final ComponentContextProvider componentContextProvider = new StandardComponentContextProvider(flowController);
        final ParameterContext parameterContext = processGroup.getParameterContext();
        final ProcessScheduler processScheduler = flowController.getProcessScheduler();
        final ExtensionManager extensionManager = flowController.getExtensionManager();

        return new StandaloneProcessGroupFacade(processGroup, versionedManagedGroup,
            processScheduler, parameterContext, flowController.getControllerServiceProvider(),
            componentContextProvider, connectorLogger, extensionManager, flowController.getAssetManager());
    }
}
