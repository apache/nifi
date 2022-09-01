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
import org.apache.nifi.web.api.dto.FlowRegistryClientDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FlowRegistryClientEntityMerger implements ComponentEntityMerger<FlowRegistryClientEntity> {

    @Override
    public void mergeComponents(FlowRegistryClientEntity clientEntity, Map<NodeIdentifier, FlowRegistryClientEntity> entityMap) {
        final FlowRegistryClientDTO clientDTO = clientEntity.getComponent();
        final Map<NodeIdentifier, FlowRegistryClientDTO> dtoMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, FlowRegistryClientEntity> entry : entityMap.entrySet()) {
            final FlowRegistryClientEntity flowRegistryClientEntity = entry.getValue();
            final FlowRegistryClientDTO flowRegistryClientDTO = flowRegistryClientEntity.getComponent();
            dtoMap.put(entry.getKey(), flowRegistryClientDTO);
        }

        mergeDtos(clientDTO, dtoMap);
    }

    private void mergeDtos(final FlowRegistryClientDTO clientDto, final Map<NodeIdentifier, FlowRegistryClientDTO> dtoMap) {
        if (clientDto == null) {
            return;
        }

        final Map<String, Set<NodeIdentifier>> validationErrorMap = new HashMap<>();
        final Map<String, Map<NodeIdentifier, PropertyDescriptorDTO>> propertyDescriptorMap = new HashMap<>();

        for (final Map.Entry<NodeIdentifier, FlowRegistryClientDTO> nodeEntry : dtoMap.entrySet()) {
            final FlowRegistryClientDTO nodEntryClient = nodeEntry.getValue();

            if (nodEntryClient != null) {
                final NodeIdentifier nodeId = nodeEntry.getKey();

                // merge the validation errors
                ErrorMerger.mergeErrors(validationErrorMap, nodeId, nodEntryClient.getValidationErrors());

                // aggregate the property descriptors
                if (nodEntryClient.getDescriptors() != null) {
                    nodEntryClient.getDescriptors().values().forEach(propertyDescriptor -> {
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
                .map(FlowRegistryClientDTO::getValidationStatus)
                .collect(Collectors.toSet());
        clientDto.setValidationStatus(ErrorMerger.mergeValidationStatus(validationStatuses));

        // set the merged the validation errors
        clientDto.setValidationErrors(ErrorMerger.normalizedMergedErrors(validationErrorMap, dtoMap.size()));
    }
}
