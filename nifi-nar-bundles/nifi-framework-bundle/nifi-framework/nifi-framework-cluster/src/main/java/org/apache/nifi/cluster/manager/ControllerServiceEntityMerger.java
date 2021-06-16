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
package org.apache.nifi.cluster.manager;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.ControllerServiceReferencingComponentDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity;

public class ControllerServiceEntityMerger implements ComponentEntityMerger<ControllerServiceEntity> {

    /**
     * Merges the ControllerServiceEntity responses.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap    all node responses
     */
    @Override
    public void mergeComponents(final ControllerServiceEntity clientEntity, final Map<NodeIdentifier, ControllerServiceEntity> entityMap) {
        final ControllerServiceDTO clientDto = clientEntity.getComponent();
        final Map<NodeIdentifier, ControllerServiceDTO> dtoMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, ControllerServiceEntity> entry : entityMap.entrySet()) {
            final ControllerServiceEntity nodeControllerServiceEntity = entry.getValue();
            final ControllerServiceDTO nodeControllerServiceDto = nodeControllerServiceEntity.getComponent();
            dtoMap.put(entry.getKey(), nodeControllerServiceDto);
        }

        mergeDtos(clientDto, dtoMap);
    }

    private static void mergeDtos(final ControllerServiceDTO clientDto, final Map<NodeIdentifier, ControllerServiceDTO> dtoMap) {
        // if unauthorized for the client dto, simple return
        if (clientDto == null) {
            return;
        }

        final Map<String, Set<NodeIdentifier>> validationErrorMap = new HashMap<>();
        final Set<ControllerServiceReferencingComponentEntity> referencingComponents = clientDto.getReferencingComponents();
        final Map<NodeIdentifier, Set<ControllerServiceReferencingComponentEntity>> nodeReferencingComponentsMap = new HashMap<>();
        final Map<String, Map<NodeIdentifier, PropertyDescriptorDTO>> propertyDescriptorMap = new HashMap<>();

        String state = null;
        for (final Map.Entry<NodeIdentifier, ControllerServiceDTO> nodeEntry : dtoMap.entrySet()) {
            final ControllerServiceDTO nodeControllerService = nodeEntry.getValue();

            // consider the node controller service if authorized
            if (nodeControllerService != null) {
                final NodeIdentifier nodeId = nodeEntry.getKey();

                if (state == null) {
                    if (ControllerServiceState.DISABLING.name().equals(nodeControllerService.getState())) {
                        state = ControllerServiceState.DISABLING.name();
                    } else if (ControllerServiceState.ENABLING.name().equals(nodeControllerService.getState())) {
                        state = ControllerServiceState.ENABLING.name();
                    }
                }

                nodeReferencingComponentsMap.put(nodeId, nodeControllerService.getReferencingComponents());

                // merge the validation errors
                ErrorMerger.mergeErrors(validationErrorMap, nodeId, nodeControllerService.getValidationErrors());

                // aggregate the property descriptors
                final Map<String, PropertyDescriptorDTO> descriptors = nodeControllerService.getDescriptors();
                if (descriptors != null) {
                    descriptors.values().stream().forEach(propertyDescriptor -> {
                        propertyDescriptorMap.computeIfAbsent(propertyDescriptor.getName(), nodeIdToPropertyDescriptor -> new HashMap<>()).put(nodeId, propertyDescriptor);
                    });
                }
            }
        }

        // merge property descriptors
        for (Map<NodeIdentifier, PropertyDescriptorDTO> propertyDescriptorByNodeId : propertyDescriptorMap.values()) {
            final Collection<PropertyDescriptorDTO> nodePropertyDescriptors = propertyDescriptorByNodeId.values();
            if (!nodePropertyDescriptors.isEmpty()) {
                // get the name of the property descriptor and find that descriptor being returned to the client
                final PropertyDescriptorDTO propertyDescriptor = nodePropertyDescriptors.iterator().next();
                final PropertyDescriptorDTO clientPropertyDescriptor = clientDto.getDescriptors().get(propertyDescriptor.getName());
                PropertyDescriptorDtoMerger.merge(clientPropertyDescriptor, propertyDescriptorByNodeId);
            }
        }

        // merge the referencing components
        mergeControllerServiceReferences(referencingComponents, nodeReferencingComponentsMap);

        // store the 'transition' state is applicable
        if (state != null) {
            clientDto.setState(state);
        }

        final Set<String> statuses = dtoMap.values().stream()
            .map(ControllerServiceDTO::getValidationStatus)
            .collect(Collectors.toSet());

        clientDto.setValidationStatus(ErrorMerger.mergeValidationStatus(statuses));

        // set the merged the validation errors
        clientDto.setValidationErrors(ErrorMerger.normalizedMergedErrors(validationErrorMap, dtoMap.size()));
    }

    public static void mergeControllerServiceReferences(final Set<ControllerServiceReferencingComponentEntity> referencingComponents,
                                                        final Map<NodeIdentifier, Set<ControllerServiceReferencingComponentEntity>> referencingComponentMap) {

        final Map<String, Integer> activeThreadCounts = new HashMap<>();
        final Map<String, String> states = new HashMap<>();
        final Map<String, PermissionsDTO> permissionsHolder = new HashMap<>();
        final Map<String, PermissionsDTO> operatePermissionsHolder = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, Set<ControllerServiceReferencingComponentEntity>> nodeEntry : referencingComponentMap.entrySet()) {
            final Set<ControllerServiceReferencingComponentEntity> nodeReferencingComponents = nodeEntry.getValue();

            // go through all the nodes referencing components
            if (nodeReferencingComponents != null) {
                for (final ControllerServiceReferencingComponentEntity nodeReferencingComponentEntity : nodeReferencingComponents) {
                    final ControllerServiceReferencingComponentDTO nodeReferencingComponent = nodeReferencingComponentEntity.getComponent();

                    if (nodeReferencingComponentEntity.getPermissions().getCanRead()) {
                        // handle active thread counts
                        if (nodeReferencingComponent.getActiveThreadCount() != null && nodeReferencingComponent.getActiveThreadCount() > 0) {
                            final Integer current = activeThreadCounts.get(nodeReferencingComponent.getId());
                            if (current == null) {
                                activeThreadCounts.put(nodeReferencingComponent.getId(), nodeReferencingComponent.getActiveThreadCount());
                            } else {
                                activeThreadCounts.put(nodeReferencingComponent.getId(), nodeReferencingComponent.getActiveThreadCount() + current);
                            }
                        }

                        // handle controller service state
                        final String state = states.get(nodeReferencingComponent.getId());
                        if (state == null) {
                            if (ControllerServiceState.DISABLING.name().equals(nodeReferencingComponent.getState())) {
                                states.put(nodeReferencingComponent.getId(), ControllerServiceState.DISABLING.name());
                            } else if (ControllerServiceState.ENABLING.name().equals(nodeReferencingComponent.getState())) {
                                states.put(nodeReferencingComponent.getId(), ControllerServiceState.ENABLING.name());
                            }
                        }
                    }

                    // handle read permissions
                    mergePermissions(permissionsHolder, nodeReferencingComponentEntity, nodeReferencingComponentEntity.getPermissions());
                    mergePermissions(operatePermissionsHolder, nodeReferencingComponentEntity, nodeReferencingComponentEntity.getOperatePermissions());
                }
            }
        }

        // go through each referencing components
        if (referencingComponents != null) {
            for (final ControllerServiceReferencingComponentEntity referencingComponent : referencingComponents) {
                final PermissionsDTO permissions = permissionsHolder.get(referencingComponent.getId());
                final PermissionsDTO operatePermissions = operatePermissionsHolder.get(referencingComponent.getId());
                if (permissions != null && permissions.getCanRead() != null && permissions.getCanRead()) {
                    final Integer activeThreadCount = activeThreadCounts.get(referencingComponent.getId());
                    if (activeThreadCount != null) {
                        referencingComponent.getComponent().setActiveThreadCount(activeThreadCount);
                    }

                    final String state = states.get(referencingComponent.getId());
                    if (state != null) {
                        referencingComponent.getComponent().setState(state);
                    }

                    final Map<NodeIdentifier, ControllerServiceReferencingComponentEntity> nodeEntities = new HashMap<>();
                    referencingComponentMap.entrySet().forEach(entry -> {
                        final NodeIdentifier nodeIdentifier = entry.getKey();
                        final Set<ControllerServiceReferencingComponentEntity> nodeControllerServiceReferencingComponents = entry.getValue();

                        nodeControllerServiceReferencingComponents.forEach(nodeControllerServiceReferencingComponent -> {
                            if (referencingComponent.getId() != null && referencingComponent.getId().equals(nodeControllerServiceReferencingComponent.getId())) {
                                nodeEntities.put(nodeIdentifier, nodeControllerServiceReferencingComponent);
                            }
                        });
                    });

                    mergeControllerServiceReferencingComponent(referencingComponent, nodeEntities);
                } else {
                    referencingComponent.setPermissions(permissions);
                    referencingComponent.setOperatePermissions(operatePermissions);
                    referencingComponent.setComponent(null);
                }
            }
        }
    }

    private static void mergePermissions(Map<String, PermissionsDTO> permissionsHolder, ControllerServiceReferencingComponentEntity nodeReferencingComponentEntity, PermissionsDTO permissions) {
        final PermissionsDTO mergedPermissions = permissionsHolder.get(nodeReferencingComponentEntity.getId());
        if (permissions != null) {
            if (mergedPermissions == null) {
                permissionsHolder.put(nodeReferencingComponentEntity.getId(), permissions);
            } else {
                PermissionsDtoMerger.mergePermissions(mergedPermissions, permissions);
            }
        }
    }

    private static void mergeControllerServiceReferencingComponent(ControllerServiceReferencingComponentEntity clientEntity, Map<NodeIdentifier,
            ControllerServiceReferencingComponentEntity> nodeEntities) {
        final Map<String, Map<NodeIdentifier, PropertyDescriptorDTO>> propertyDescriptorMap = new HashMap<>();

        final Map<NodeIdentifier, Set<ControllerServiceReferencingComponentEntity>> nodeReferencingComponentsMap = new HashMap<>();

        // aggregate the property descriptors
        for (Map.Entry<NodeIdentifier, ControllerServiceReferencingComponentEntity> entry : nodeEntities.entrySet()) {
            final NodeIdentifier nodeIdentifier = entry.getKey();
            final ControllerServiceReferencingComponentEntity nodeEntity = entry.getValue();
            nodeEntity.getComponent().getDescriptors().values().stream().forEach(propertyDescriptor -> {
                propertyDescriptorMap.computeIfAbsent(propertyDescriptor.getName(), nodeIdToPropertyDescriptor -> new HashMap<>()).put(nodeIdentifier, propertyDescriptor);
            });
            nodeReferencingComponentsMap.put(nodeIdentifier, nodeEntity.getComponent().getReferencingComponents());
        }

        // merge property descriptors
        for (Map<NodeIdentifier, PropertyDescriptorDTO> propertyDescriptorByNodeId : propertyDescriptorMap.values()) {
            final Collection<PropertyDescriptorDTO> nodePropertyDescriptors = propertyDescriptorByNodeId.values();
            if (!nodePropertyDescriptors.isEmpty()) {
                // get the name of the property descriptor and find that descriptor being returned to the client
                final PropertyDescriptorDTO propertyDescriptor = nodePropertyDescriptors.iterator().next();
                final PropertyDescriptorDTO clientPropertyDescriptor = clientEntity.getComponent().getDescriptors().get(propertyDescriptor.getName());
                PropertyDescriptorDtoMerger.merge(clientPropertyDescriptor, propertyDescriptorByNodeId);
            }
        }

        final Set<ControllerServiceReferencingComponentEntity> referencingComponents = clientEntity.getComponent().getReferencingComponents();
        mergeControllerServiceReferences(referencingComponents, nodeReferencingComponentsMap);
    }
}
