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
package org.apache.nifi.controller.flowanalysis;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.VersionedControllerServiceLookup;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flowanalysis.FlowAnalysisContext;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

public class StandardFlowAnalysisContext implements FlowAnalysisContext {
    private final FlowController flowController;

    public StandardFlowAnalysisContext(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    public VersionedControllerServiceLookup getVersionedControllerServiceLookup() {
        VersionedControllerServiceLookup versionedControllerServiceLookup = id -> {
            ControllerServiceProvider controllerServiceProvider = flowController.getControllerServiceProvider();
            ExtensionManager extensionManager = flowController.getExtensionManager();

            NiFiRegistryFlowMapper mapper = FlowAnalysisUtil.createMapper(extensionManager);

            ControllerServiceNode controllerServiceNode = controllerServiceProvider.getControllerServiceNode(id);

            VersionedControllerService versionedControllerService = mapper.mapControllerService(
                    controllerServiceNode,
                    controllerServiceProvider,
                    Collections.emptySet(),
                    new HashMap<>()
            );

            return versionedControllerService;
        };

        return versionedControllerServiceLookup;
    }

    @Override
    public int getMaxTimerDrivenThreadCount() {
        return flowController.getMaxTimerDrivenThreadCount();
    }

    @Override
    public boolean isClustered() {
        return flowController.isConfiguredForClustering();
    }

    @Override
    public Optional<String> getClusterNodeIdentifier() {
        final NodeIdentifier nodeId = flowController.getNodeId();

        final Optional<String> nodeIdOptional = Optional.ofNullable(nodeId)
                .map(NodeIdentifier::getId);

        return nodeIdOptional;
    }
}
