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
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleEntity;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FlowAnalysisRuleEntityMerger implements ComponentEntityMerger<FlowAnalysisRuleEntity> {

    @Override
    public void merge(FlowAnalysisRuleEntity clientEntity, Map<NodeIdentifier, FlowAnalysisRuleEntity> entityMap) {
        ComponentEntityMerger.super.merge(clientEntity, entityMap);
        for (Map.Entry<NodeIdentifier, FlowAnalysisRuleEntity> entry : entityMap.entrySet()) {
            final FlowAnalysisRuleEntity entityStatus = entry.getValue();
            if (clientEntity != entityStatus) {
                StatusMerger.merge(clientEntity.getStatus(), entityStatus.getStatus());
            }
        }
    }

    /**
     * Merges the FlowAnalysisRuleEntity responses.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap all node responses
     */
    @Override
    public void mergeComponents(final FlowAnalysisRuleEntity clientEntity, final Map<NodeIdentifier, FlowAnalysisRuleEntity> entityMap) {
        final FlowAnalysisRuleDTO clientDto = clientEntity.getComponent();
        final Map<NodeIdentifier, FlowAnalysisRuleDTO> dtoMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, FlowAnalysisRuleEntity> entry : entityMap.entrySet()) {
            final FlowAnalysisRuleEntity nodeFlowAnalysisRuleEntity = entry.getValue();
            final FlowAnalysisRuleDTO nodeFlowAnalysisRuleDto = nodeFlowAnalysisRuleEntity.getComponent();
            dtoMap.put(entry.getKey(), nodeFlowAnalysisRuleDto);
        }

        mergeDtos(clientDto, dtoMap);
    }

    private static void mergeDtos(final FlowAnalysisRuleDTO clientDto, final Map<NodeIdentifier, FlowAnalysisRuleDTO> dtoMap) {
        // if unauthorized for the client dto, simple return
        if (clientDto == null) {
            return;
        }

        final Map<String, Set<NodeIdentifier>> validationErrorMap = new HashMap<>();
        final Map<String, Map<NodeIdentifier, PropertyDescriptorDTO>> propertyDescriptorMap = new HashMap<>();

        for (final Map.Entry<NodeIdentifier, FlowAnalysisRuleDTO> nodeEntry : dtoMap.entrySet()) {
            final FlowAnalysisRuleDTO nodeFlowAnalysisRule = nodeEntry.getValue();

            // consider the node flow analysis rule if authorized
            if (nodeFlowAnalysisRule != null) {
                final NodeIdentifier nodeId = nodeEntry.getKey();

                // merge the validation errors
                ErrorMerger.mergeErrors(validationErrorMap, nodeId, nodeFlowAnalysisRule.getValidationErrors());

                // aggregate the property descriptors
                if (nodeFlowAnalysisRule.getDescriptors() != null) {
                    nodeFlowAnalysisRule.getDescriptors().values().stream().forEach(propertyDescriptor -> {
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

        final Set<String> validationStatuses = dtoMap.values().stream()
            .map(FlowAnalysisRuleDTO::getValidationStatus)
            .collect(Collectors.toSet());
        clientDto.setValidationStatus(ErrorMerger.mergeValidationStatus(validationStatuses));

        // set the merged the validation errors
        clientDto.setValidationErrors(ErrorMerger.normalizedMergedErrors(validationErrorMap, dtoMap.size()));
    }
}
