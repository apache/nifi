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

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.serialization.FlowFromDOMFactory;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.util.DomUtils;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class BundleCompatibilityCheck implements FlowInheritabilityCheck {

    @Override
    public FlowInheritability checkInheritability(final DataFlow existingFlow, final DataFlow proposedFlow, final FlowController flowController) {
        final Document configuration = proposedFlow.getFlowDocument();

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
