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
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AffectedComponentEntityMerger {

    public void mergeAffectedComponents(final Set<AffectedComponentEntity> affectedComponents, final Map<NodeIdentifier, Set<AffectedComponentEntity>> affectedComponentMap) {

        final Map<String, Integer> activeThreadCounts = new HashMap<>();
        final Map<String, String> states = new HashMap<>();
        final Map<String, PermissionsDTO> canReads = new HashMap<>();

        for (final Map.Entry<NodeIdentifier, Set<AffectedComponentEntity>> nodeEntry : affectedComponentMap.entrySet()) {
            final Set<AffectedComponentEntity> nodeAffectedComponents = nodeEntry.getValue();

            // go through all the nodes referencing components
            if (nodeAffectedComponents != null) {
                for (final AffectedComponentEntity nodeAffectedComponentEntity : nodeAffectedComponents) {
                    final AffectedComponentDTO nodeAffectedComponent = nodeAffectedComponentEntity.getComponent();

                    if (nodeAffectedComponentEntity.getPermissions().getCanRead()) {
                        // handle active thread counts
                        if (nodeAffectedComponent.getActiveThreadCount() != null && nodeAffectedComponent.getActiveThreadCount() > 0) {
                            final Integer current = activeThreadCounts.get(nodeAffectedComponent.getId());
                            if (current == null) {
                                activeThreadCounts.put(nodeAffectedComponent.getId(), nodeAffectedComponent.getActiveThreadCount());
                            } else {
                                activeThreadCounts.put(nodeAffectedComponent.getId(), nodeAffectedComponent.getActiveThreadCount() + current);
                            }
                        }

                        // handle controller service state
                        final String state = states.get(nodeAffectedComponent.getId());
                        if (state == null) {
                            if (ControllerServiceState.DISABLING.name().equals(nodeAffectedComponent.getState())) {
                                states.put(nodeAffectedComponent.getId(), ControllerServiceState.DISABLING.name());
                            } else if (ControllerServiceState.ENABLING.name().equals(nodeAffectedComponent.getState())) {
                                states.put(nodeAffectedComponent.getId(), ControllerServiceState.ENABLING.name());
                            }
                        }
                    }

                    // handle read permissions
                    final PermissionsDTO mergedPermissions = canReads.get(nodeAffectedComponentEntity.getId());
                    final PermissionsDTO permissions = nodeAffectedComponentEntity.getPermissions();
                    if (permissions != null) {
                        if (mergedPermissions == null) {
                            canReads.put(nodeAffectedComponentEntity.getId(), permissions);
                        } else {
                            PermissionsDtoMerger.mergePermissions(mergedPermissions, permissions);
                        }
                    }
                }
            }
        }

        // go through each affected components
        if (affectedComponents != null) {
            for (final AffectedComponentEntity affectedComponent : affectedComponents) {
                final PermissionsDTO permissions = canReads.get(affectedComponent.getId());
                if (permissions != null && permissions.getCanRead() != null && permissions.getCanRead()) {
                    final Integer activeThreadCount = activeThreadCounts.get(affectedComponent.getId());
                    if (activeThreadCount != null) {
                        affectedComponent.getComponent().setActiveThreadCount(activeThreadCount);
                    }

                    final String state = states.get(affectedComponent.getId());
                    if (state != null) {
                        affectedComponent.getComponent().setState(state);
                    }
                } else {
                    affectedComponent.setPermissions(permissions);
                    affectedComponent.setComponent(null);
                }
            }
        }
    }
}
