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
import org.apache.nifi.controller.serialization.FlowFromDOMFactory;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.util.DomUtils;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.Set;

public class BundleCompatibilityCheck implements FlowInheritabilityCheck {
    private static final Logger logger = LoggerFactory.getLogger(BundleCompatibilityCheck.class);

    @Override
    public FlowInheritability checkInheritability(final DataFlow existingFlow, final DataFlow proposedFlow, final FlowController flowController) {
        if (proposedFlow.isXml()) {
            return checkInheritability(proposedFlow.getFlowDocument(), flowController);
        } else {
            return checkVersionedFlowInheritability(proposedFlow, flowController);
        }
    }

    private FlowInheritability checkVersionedFlowInheritability(final DataFlow proposedFlow, final FlowController flowController) {
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
        if (configuration == null) {
            return FlowInheritability.inheritable();
        }

        final ExtensionManager extensionManager = flowController.getExtensionManager();
        final NodeList bundleNodes = configuration.getElementsByTagName("bundle");
        for (int i = 0; i < bundleNodes.getLength(); i++) {
            final Node bundleNode = bundleNodes.item(i);
            if (bundleNode instanceof Element) {
                final Element bundleElement = (Element) bundleNode;

                final Node componentNode = bundleElement.getParentNode();
                if (componentNode instanceof Element) {
                    final Element componentElement = (Element) componentNode;
                    if (!withinTemplate(componentElement)) {
                        final String componentType = DomUtils.getChildText(componentElement, "class");
                        final BundleDTO bundleDto = FlowFromDOMFactory.getBundle(bundleElement);

                        try {
                            BundleUtils.getBundle(extensionManager, componentType, bundleDto);
                        } catch (final IllegalStateException e) {
                            final String bundleDescription = bundleDto.getGroup() + ":" + bundleDto.getArtifact() + ":" + bundleDto.getVersion();
                            return FlowInheritability.notInheritable("Could not find Bundle " + bundleDescription + ": " + e.getMessage());
                        }
                    }
                }
            }
        }

        return FlowInheritability.inheritable();
    }

    private boolean withinTemplate(final Element element) {
        if ("template".equals(element.getTagName())) {
            return true;
        } else {
            final Node parentNode = element.getParentNode();
            if (parentNode instanceof Element) {
                return withinTemplate((Element) parentNode);
            } else {
                return false;
            }
        }
    }
}
