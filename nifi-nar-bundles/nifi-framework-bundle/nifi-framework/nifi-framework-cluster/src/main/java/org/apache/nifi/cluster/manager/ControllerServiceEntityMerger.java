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

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.ControllerServiceReferencingComponentDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ControllerServiceEntityMerger {

    /**
     * Merges the ControllerServiceEntity responses.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap all node responses
     */
    public static void mergeControllerServices(final ControllerServiceEntity clientEntity, final Map<NodeIdentifier, ControllerServiceEntity> entityMap) {
        final ControllerServiceDTO clientDto = clientEntity.getComponent();
        final Map<NodeIdentifier, ControllerServiceDTO> dtoMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, ControllerServiceEntity> entry : entityMap.entrySet()) {
            final ControllerServiceEntity nodeControllerServiceEntity = entry.getValue();
            final ControllerServiceDTO nodeControllerServiceDto = nodeControllerServiceEntity.getComponent();
            dtoMap.put(entry.getKey(), nodeControllerServiceDto);
        }

        ComponentEntityMerger.mergeComponents(clientEntity, entityMap);

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
            }
        }

        // merge the referencing components
        mergeControllerServiceReferences(referencingComponents, nodeReferencingComponentsMap);

        // store the 'transition' state is applicable
        if (state != null) {
            clientDto.setState(state);
        }

        // set the merged the validation errors
        clientDto.setValidationErrors(ErrorMerger.normalizedMergedErrors(validationErrorMap, dtoMap.size()));
    }

    public static void mergeControllerServiceReferences(final Set<ControllerServiceReferencingComponentEntity> referencingComponents,
                                                        final Map<NodeIdentifier, Set<ControllerServiceReferencingComponentEntity>> referencingComponentMap) {

        final Map<String, Integer> activeThreadCounts = new HashMap<>();
        final Map<String, String> states = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, Set<ControllerServiceReferencingComponentEntity>> nodeEntry : referencingComponentMap.entrySet()) {
            final Set<ControllerServiceReferencingComponentEntity> nodeReferencingComponents = nodeEntry.getValue();

            // go through all the nodes referencing components
            if (nodeReferencingComponents != null) {
                for (final ControllerServiceReferencingComponentEntity nodeReferencingComponentEntity : nodeReferencingComponents) {
                    final ControllerServiceReferencingComponentDTO nodeReferencingComponent = nodeReferencingComponentEntity.getComponent();

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
            }
        }

        // go through each referencing components
        if (referencingComponents != null) {
            for (final ControllerServiceReferencingComponentEntity referencingComponent : referencingComponents) {
                final Integer activeThreadCount = activeThreadCounts.get(referencingComponent.getId());
                if (activeThreadCount != null) {
                    referencingComponent.getComponent().setActiveThreadCount(activeThreadCount);
                }

                final String state = states.get(referencingComponent.getId());
                if (state != null) {
                    referencingComponent.getComponent().setState(state);
                }
            }
        }
    }
}
