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
package org.apache.nifi.controller.service;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.serialization.FlowEncodingVersion;
import org.apache.nifi.controller.serialization.FlowFromDOMFactory;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class ControllerServiceLoader {

    private static final Logger logger = LoggerFactory.getLogger(ControllerServiceLoader.class);

    public static Map<ControllerServiceNode, Element> loadControllerServices(final List<Element> serviceElements, final FlowController controller,
                                                                             final ProcessGroup parentGroup, final PropertyEncryptor encryptor, final FlowEncodingVersion encodingVersion) {

        final Map<ControllerServiceNode, Element> nodeMap = new HashMap<>();
        for (final Element serviceElement : serviceElements) {
            final ControllerServiceNode serviceNode = createControllerService(controller, serviceElement, encryptor, encodingVersion);
            if (parentGroup == null) {
                controller.getFlowManager().addRootControllerService(serviceNode);
            } else {
                parentGroup.addControllerService(serviceNode);
            }

            // We need to clone the node because it will be used in a separate thread below, and
            // Element is not thread-safe.
            nodeMap.put(serviceNode, (Element) serviceElement.cloneNode(true));
        }
        for (final Map.Entry<ControllerServiceNode, Element> entry : nodeMap.entrySet()) {
            configureControllerService(entry.getKey(), entry.getValue(), encryptor, encodingVersion);
        }

        return nodeMap;
    }

    public static void enableControllerServices(final Map<ControllerServiceNode, Element> nodeMap, final FlowController controller,
                                                final PropertyEncryptor encryptor, final boolean autoResumeState, final FlowEncodingVersion encodingVersion) {
        // Start services
        if (autoResumeState) {
            final Set<ControllerServiceNode> nodesToEnable = new HashSet<>();

            for (final ControllerServiceNode node : nodeMap.keySet()) {
                final Element controllerServiceElement = nodeMap.get(node);

                final ControllerServiceDTO dto;
                synchronized (controllerServiceElement.getOwnerDocument()) {
                    dto = FlowFromDOMFactory.getControllerService(controllerServiceElement, encryptor, encodingVersion);
                }

                final ControllerServiceState state = ControllerServiceState.valueOf(dto.getState());
                if (state == ControllerServiceState.ENABLED) {
                    nodesToEnable.add(node);
                    logger.debug("Will enable Controller Service {}", node);
                } else {
                    logger.debug("Will not enable Controller Service {} because its state is set to {}", node, state);
                }
            }

            enableControllerServices(nodesToEnable, controller, autoResumeState);
        } else {
            logger.debug("Will not enable the following Controller Services because 'auto-resume state' flag is false: {}", nodeMap.keySet());
        }
    }

    public static void enableControllerServices(final Collection<ControllerServiceNode> nodesToEnable, final FlowController controller, final boolean autoResumeState) {
        // Start services
        if (autoResumeState) {
            logger.debug("Enabling Controller Services {}", nodesToEnable);
            nodesToEnable.forEach(ControllerServiceNode::performValidation); // validate services before attempting to enable them

            controller.getControllerServiceProvider().enableControllerServices(nodesToEnable);
        } else {
            logger.debug("Will not enable the following Controller Services because 'auto-resume state' flag is false: {}", nodesToEnable);
        }
    }

    public static ControllerServiceNode cloneControllerService(final FlowController flowController, final ControllerServiceNode controllerService) {
        // create a new id for the clone seeded from the original id so that it is consistent in a cluster
        final UUID id = UUID.nameUUIDFromBytes(controllerService.getIdentifier().getBytes(StandardCharsets.UTF_8));

        final ControllerServiceNode clone = flowController.getFlowManager().createControllerService(controllerService.getCanonicalClassName(), id.toString(),
                controllerService.getBundleCoordinate(), Collections.emptySet(), false, true, null);
        clone.setName(controllerService.getName());
        clone.setComments(controllerService.getComments());
        clone.setBulletinLevel(controllerService.getBulletinLevel());

        if (controllerService.getProperties() != null) {
            Map<String,String> properties = new HashMap<>();
            for (Map.Entry<PropertyDescriptor, String> propEntry : controllerService.getRawPropertyValues().entrySet()) {
                properties.put(propEntry.getKey().getName(), propEntry.getValue());
            }
            clone.setProperties(properties);
        }

        return clone;
    }

    private static ControllerServiceNode createControllerService(final FlowController flowController, final Element controllerServiceElement, final PropertyEncryptor encryptor,
                                                                 final FlowEncodingVersion encodingVersion) {
        final ControllerServiceDTO dto = FlowFromDOMFactory.getControllerService(controllerServiceElement, encryptor, encodingVersion);

        BundleCoordinate coordinate;
        try {
            coordinate = BundleUtils.getCompatibleBundle(flowController.getExtensionManager(), dto.getType(), dto.getBundle());
        } catch (final IllegalStateException e) {
            final BundleDTO bundleDTO = dto.getBundle();
            if (bundleDTO == null) {
                coordinate = BundleCoordinate.UNKNOWN_COORDINATE;
            } else {
                coordinate = new BundleCoordinate(bundleDTO.getGroup(), bundleDTO.getArtifact(), bundleDTO.getVersion());
            }
        }

        final ControllerServiceNode node = flowController.getFlowManager().createControllerService(dto.getType(), dto.getId(), coordinate, Collections.emptySet(), false, true, null);
        node.setName(dto.getName());
        node.setComments(dto.getComments());

        if (dto.getBulletinLevel() != null) {
            node.setBulletinLevel(LogLevel.valueOf(dto.getBulletinLevel()));
        } else {
            // this situation exists for backward compatibility with nifi 1.16 and earlier where controller services do not have bulletinLevels set in flow.xml/flow.json
            // and bulletinLevels are at the WARN level by default
            node.setBulletinLevel(LogLevel.WARN);
        }

        node.setVersionedComponentId(dto.getVersionedComponentId());
        return node;
    }

    private static void configureControllerService(final ControllerServiceNode node, final Element controllerServiceElement, final PropertyEncryptor encryptor,
                                                   final FlowEncodingVersion encodingVersion) {
        final ControllerServiceDTO dto = FlowFromDOMFactory.getControllerService(controllerServiceElement, encryptor, encodingVersion);
        node.pauseValidationTrigger();
        try {
            node.setAnnotationData(dto.getAnnotationData());
            final Set<String> sensitiveDynamicPropertyNames = getSensitiveDynamicPropertyNames(dto.getSensitiveDynamicPropertyNames(), node);
            node.setProperties(dto.getProperties(), false, sensitiveDynamicPropertyNames);
        } finally {
            node.resumeValidationTrigger();
        }
    }

    private static Set<String> getSensitiveDynamicPropertyNames(final Set<String> parsedSensitivePropertyNames, final ControllerServiceNode controllerServiceNode) {
        final Set<String> sensitivePropertyNames = parsedSensitivePropertyNames == null ? Collections.emptySet() : parsedSensitivePropertyNames;
        return sensitivePropertyNames.stream().filter(
                propertyName -> {
                    final PropertyDescriptor propertyDescriptor = controllerServiceNode.getPropertyDescriptor(propertyName);
                    return propertyDescriptor.isDynamic();
                }
        ).collect(Collectors.toSet());
    }
}
