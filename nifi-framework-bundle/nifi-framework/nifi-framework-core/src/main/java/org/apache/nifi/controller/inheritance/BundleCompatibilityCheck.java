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
package org.apache.nifi.controller.inheritance;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.flow.VersionedFlowAnalysisRule;
import org.apache.nifi.flow.VersionedFlowRegistryClient;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedParameterProvider;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.nar.ExtensionManager;
import org.w3c.dom.Document;

import java.util.Set;

public class BundleCompatibilityCheck implements FlowInheritabilityCheck {

    @Override
    public FlowInheritability checkInheritability(final DataFlow existingFlow, final DataFlow proposedFlow, final FlowController flowController) {
        return checkBundles(proposedFlow, flowController.getExtensionManager());
    }

    private FlowInheritability checkBundles(final DataFlow proposedFlow, ExtensionManager extensionManager) {
        final VersionedDataflow dataflow = proposedFlow.getVersionedDataflow();
        if (dataflow == null) {
            return FlowInheritability.inheritable();
        }

        final Set<String> missingComponents = proposedFlow.getMissingComponents();

        if (dataflow.getControllerServices() != null) {
            for (final VersionedControllerService service : dataflow.getControllerServices()) {
                if (missingComponents.contains(service.getInstanceIdentifier())) {
                    continue;
                }

                if (isMissing(service.getBundle(), extensionManager)) {
                    return FlowInheritability.notInheritable(String.format("Controller Service with ID %s and type %s requires bundle %s, but that bundle cannot be found in this NiFi instance",
                        service.getInstanceIdentifier(), service.getType(), service.getBundle()));
                }
            }
        }

        if (dataflow.getReportingTasks() != null) {
            for (final VersionedReportingTask task : dataflow.getReportingTasks()) {
                if (missingComponents.contains(task.getInstanceIdentifier())) {
                    continue;
                }

                if (isMissing(task.getBundle(), extensionManager)) {
                    return FlowInheritability.notInheritable(String.format("Reporting Task with ID %s and type %s requires bundle %s, but that bundle cannot be found in this NiFi instance",
                        task.getInstanceIdentifier(), task.getType(), task.getBundle()));
                }
            }
        }

        if (dataflow.getFlowAnalysisRules() != null) {
            for (final VersionedFlowAnalysisRule rule : dataflow.getFlowAnalysisRules()) {
                if (missingComponents.contains(rule.getInstanceIdentifier())) {
                    continue;
                }

                if (isMissing(rule.getBundle(), extensionManager)) {
                    return FlowInheritability.notInheritable(String.format("Flow Analysis Rule with ID %s and type %s requires bundle %s, but that bundle cannot be found in this NiFi instance",
                        rule.getInstanceIdentifier(), rule.getType(), rule.getBundle()));
                }
            }
        }

        if (dataflow.getParameterProviders() != null) {
            for (final VersionedParameterProvider task : dataflow.getParameterProviders()) {
                if (missingComponents.contains(task.getInstanceIdentifier())) {
                    continue;
                }

                if (isMissing(task.getBundle(), extensionManager)) {
                    return FlowInheritability.notInheritable(String.format("Parameter Provider with ID %s and type %s requires bundle %s, but that bundle cannot be found in this NiFi instance",
                            task.getInstanceIdentifier(), task.getType(), task.getBundle()));
                }
            }
        }

        if (dataflow.getRegistries() != null) {
            for (final VersionedFlowRegistryClient registryClient : dataflow.getRegistries()) {
                if (registryClient.getBundle() == null) {
                    // Bundle will be missing if dataflow is using an older format, before Registry Clients became extension points.
                    continue;
                }
                if (missingComponents.contains(registryClient.getInstanceIdentifier())) {
                    continue;
                }

                if (isMissing(registryClient.getBundle(), extensionManager)) {
                    return FlowInheritability.notInheritable(String.format("Flow Registry Client with ID %s and type %s requires bundle %s, but that bundle cannot be found in this NiFi instance",
                            registryClient.getInstanceIdentifier(), registryClient.getType(), registryClient.getBundle()));
                }
            }

        }

        return checkBundles(dataflow.getRootGroup(), extensionManager, missingComponents);
    }

    private FlowInheritability checkBundles(final VersionedProcessGroup group, final ExtensionManager extensionManager, final Set<String> missingComponents) {
        for (final VersionedProcessor processor : group.getProcessors()) {
            if (missingComponents.contains(processor.getInstanceIdentifier())) {
                continue;
            }

            if (isMissing(processor.getBundle(), extensionManager)) {
                return FlowInheritability.notInheritable(String.format("Processor with ID %s and type %s requires bundle %s, but that bundle cannot be found in this NiFi instance",
                    processor.getInstanceIdentifier(), processor.getType(), processor.getBundle()));
            }
        }

        for (final VersionedControllerService service : group.getControllerServices()) {
            if (missingComponents.contains(service.getInstanceIdentifier())) {
                continue;
            }

            if (isMissing(service.getBundle(), extensionManager)) {
                return FlowInheritability.notInheritable(String.format("Controller Service with ID %s and type %s requires bundle %s, but that bundle cannot be found in this NiFi instance",
                    service.getInstanceIdentifier(), service.getType(), service.getBundle()));
            }
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            final FlowInheritability childInheritability = checkBundles(childGroup, extensionManager, missingComponents);
            if (!childInheritability.isInheritable()) {
                return childInheritability;
            }
        }

        return FlowInheritability.inheritable();
    }

    private boolean isMissing(final Bundle bundle, final ExtensionManager extensionManager) {
        final BundleCoordinate coordinate = new BundleCoordinate(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
        final org.apache.nifi.bundle.Bundle existingBundle = extensionManager.getBundle(coordinate);
        return existingBundle == null;
    }

    private FlowInheritability checkInheritability(final Document configuration, final FlowController flowController) {
        return FlowInheritability.inheritable();
    }
}
