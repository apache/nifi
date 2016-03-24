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

package org.apache.nifi.cluster.coordination.http.endpoints;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.ControllerServiceReferencingComponentDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity;

public class ControllerServiceEndpointMerger extends AbstractSingleEntityEndpoint<ControllerServiceEntity, ControllerServiceDTO> {
    public static final String CONTROLLER_SERVICES_URI = "/nifi-api/controller-services/node";
    public static final Pattern CONTROLLER_SERVICE_URI_PATTERN = Pattern.compile("/nifi-api/controller-services/node/[a-f0-9\\-]{36}");

    @Override
    public boolean canHandle(URI uri, String method) {
        if (("GET".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) && CONTROLLER_SERVICE_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        } else if ("POST".equalsIgnoreCase(method) && CONTROLLER_SERVICES_URI.equals(uri.getPath())) {
            return true;
        }

        return false;
    }

    @Override
    protected Class<ControllerServiceEntity> getEntityClass() {
        return ControllerServiceEntity.class;
    }

    @Override
    protected ControllerServiceDTO getDto(ControllerServiceEntity entity) {
        return entity.getControllerService();
    }

    @Override
    protected void mergeResponses(ControllerServiceDTO clientDto, Map<NodeIdentifier, ControllerServiceDTO> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        final Map<String, Set<NodeIdentifier>> validationErrorMap = new HashMap<>();
        final Set<ControllerServiceReferencingComponentEntity> referencingComponents = clientDto.getReferencingComponents();
        final Map<NodeIdentifier, Set<ControllerServiceReferencingComponentEntity>> nodeReferencingComponentsMap = new HashMap<>();

        String state = null;
        for (final Map.Entry<NodeIdentifier, ControllerServiceDTO> nodeEntry : dtoMap.entrySet()) {
            final NodeIdentifier nodeId = nodeEntry.getKey();
            final ControllerServiceDTO nodeControllerService = nodeEntry.getValue();

            if (state == null) {
                if (ControllerServiceState.DISABLING.name().equals(nodeControllerService.getState())) {
                    state = ControllerServiceState.DISABLING.name();
                } else if (ControllerServiceState.ENABLING.name().equals(nodeControllerService.getState())) {
                    state = ControllerServiceState.ENABLING.name();
                }
            }

            nodeReferencingComponentsMap.put(nodeId, nodeControllerService.getReferencingComponents());

            // merge the validation errors
            mergeValidationErrors(validationErrorMap, nodeId, nodeControllerService.getValidationErrors());
        }

        // merge the referencing components
        mergeControllerServiceReferences(referencingComponents, nodeReferencingComponentsMap);

        // store the 'transition' state is applicable
        if (state != null) {
            clientDto.setState(state);
        }

        // set the merged the validation errors
        clientDto.setValidationErrors(normalizedMergedValidationErrors(validationErrorMap, dtoMap.size()));
    }

    public static void mergeControllerServiceReferences(Set<ControllerServiceReferencingComponentEntity> referencingComponents,
        Map<NodeIdentifier, Set<ControllerServiceReferencingComponentEntity>> referencingComponentMap) {

        final Map<String, Integer> activeThreadCounts = new HashMap<>();
        final Map<String, String> states = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, Set<ControllerServiceReferencingComponentEntity>> nodeEntry : referencingComponentMap.entrySet()) {
            final Set<ControllerServiceReferencingComponentEntity> nodeReferencingComponents = nodeEntry.getValue();

            // go through all the nodes referencing components
            if (nodeReferencingComponents != null) {
                for (final ControllerServiceReferencingComponentEntity nodeReferencingComponentEntity : nodeReferencingComponents) {
                    final ControllerServiceReferencingComponentDTO nodeReferencingComponent = nodeReferencingComponentEntity.getControllerServiceReferencingComponent();

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
        for (final ControllerServiceReferencingComponentEntity referencingComponent : referencingComponents) {
            final Integer activeThreadCount = activeThreadCounts.get(referencingComponent.getId());
            if (activeThreadCount != null) {
                referencingComponent.getControllerServiceReferencingComponent().setActiveThreadCount(activeThreadCount);
            }

            final String state = states.get(referencingComponent.getId());
            if (state != null) {
                referencingComponent.getControllerServiceReferencingComponent().setState(state);
            }
        }
    }

}
