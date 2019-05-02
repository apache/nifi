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

import org.apache.nifi.cluster.manager.PermissionsDtoMerger;
import org.apache.nifi.cluster.manager.StatusMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ParameterContextMerger {

    public static void merge(final ParameterContextEntity target, final Map<NodeIdentifier, ParameterContextEntity> entityMap) {
        final Map<NodeIdentifier, ParameterContextDTO> dtoMap = new HashMap<>();

        for (final Map.Entry<NodeIdentifier, ParameterContextEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ParameterContextEntity entity = entry.getValue();

            PermissionsDtoMerger.mergePermissions(entity.getPermissions(), entity.getPermissions());

            if (entity.getComponent() == null) {
                target.setComponent(null);
                continue;
            }

            dtoMap.put(nodeId, entity.getComponent());
        }

        if (target.getComponent() != null) {
            merge(target.getComponent(), dtoMap);
        }
    }

    public static void merge(final ParameterContextDTO target, final Map<NodeIdentifier, ParameterContextDTO> entityMap) {
        final Map<String, ProcessGroupEntity> mergedBoundGroups = new HashMap<>();
        final Map<String, Map<String, AffectedComponentEntity>> affectedComponentsByParametrName = new HashMap<>();

        for (final Map.Entry<NodeIdentifier, ParameterContextDTO> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ParameterContextDTO contextDto = entry.getValue();
            if (contextDto == null) {
                continue;
            }

            Set<ProcessGroupEntity> boundGroupEntities = contextDto.getBoundProcessGroups();
            if (boundGroupEntities != null) {
                for (final ProcessGroupEntity groupEntity : boundGroupEntities) {
                    ProcessGroupEntity merged = mergedBoundGroups.get(groupEntity.getId());
                    if (merged == null) {
                        mergedBoundGroups.put(groupEntity.getId(), groupEntity);
                        continue;
                    }

                    merge(merged, nodeId, groupEntity);
                }
            }

            if (contextDto.getParameters() != null) {
                for (final ParameterDTO parameterDto : contextDto.getParameters()) {
                    final Map<String, AffectedComponentEntity> affectedComponentsById = affectedComponentsByParametrName.computeIfAbsent(parameterDto.getName(), key -> new HashMap<>());

                    for (final AffectedComponentEntity referencingComponent : parameterDto.getReferencingComponents()) {
                        AffectedComponentEntity mergedAffectedComponent = affectedComponentsById.get(referencingComponent.getId());
                        if (mergedAffectedComponent == null) {
                            affectedComponentsById.put(referencingComponent.getId(), referencingComponent);
                            continue;
                        }

                        merge(mergedAffectedComponent, referencingComponent);
                    }
                }
            }
        }

        target.setBoundProcessGroups(new HashSet<>(mergedBoundGroups.values()));

        // Set the merged parameter dto's
        for (final ParameterDTO parameterDto : target.getParameters()) {
            final Map<String, AffectedComponentEntity> componentMap = affectedComponentsByParametrName.get(parameterDto.getName());
            parameterDto.setReferencingComponents(new HashSet<>(componentMap.values()));
        }
    }


    private static void merge(final ProcessGroupEntity merged, final NodeIdentifier additionalNodeId, final ProcessGroupEntity additional) {
        PermissionsDtoMerger.mergePermissions(merged.getPermissions(), additional.getPermissions());

        if (!Boolean.TRUE.equals(merged.getPermissions().getCanRead()) || additional.getComponent() == null) {
            merged.setComponent(null);
        }

        StatusMerger.merge(merged.getStatus(), merged.getPermissions().getCanRead(), additional.getStatus(), additional.getPermissions().getCanRead(), additionalNodeId.getId(),
            additionalNodeId.getApiAddress(), additionalNodeId.getApiPort());
    }

    private static void merge(final AffectedComponentEntity merged, final AffectedComponentEntity additional) {
        PermissionsDtoMerger.mergePermissions(merged.getPermissions(), additional.getPermissions());

        if (!Boolean.TRUE.equals(merged.getPermissions().getCanRead()) || additional.getComponent() == null) {
            merged.setComponent(null);
            return;
        }

        final AffectedComponentDTO mergedComponent = merged.getComponent();
        final AffectedComponentDTO additionalComponent = additional.getComponent();
        mergedComponent.setActiveThreadCount(mergedComponent.getActiveThreadCount() + additionalComponent.getActiveThreadCount());

        if (mergedComponent.getValidationErrors() == null) {
            mergedComponent.setValidationErrors(new ArrayList<>());
        }

        if (additionalComponent.getValidationErrors() != null) {
            mergedComponent.getValidationErrors().addAll(additionalComponent.getValidationErrors());
        }
    }
}
