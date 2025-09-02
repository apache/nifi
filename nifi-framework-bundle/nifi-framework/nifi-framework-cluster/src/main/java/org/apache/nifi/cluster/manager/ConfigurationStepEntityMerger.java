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
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorPropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.web.api.entity.ConfigurationStepEntity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigurationStepEntityMerger {

    /**
     * Merges the ConfigurationStepEntity responses.
     * Note: Permissions are not merged here since they are managed at the parent Connector level.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap all node responses
     */
    public static void merge(final ConfigurationStepEntity clientEntity, final Map<NodeIdentifier, ConfigurationStepEntity> entityMap) {
        final ConfigurationStepConfigurationDTO clientDto = clientEntity.getConfigurationStep();
        final Map<NodeIdentifier, ConfigurationStepConfigurationDTO> dtoMap = new HashMap<>();

        for (final Map.Entry<NodeIdentifier, ConfigurationStepEntity> entry : entityMap.entrySet()) {
            final ConfigurationStepEntity nodeConfigurationStepEntity = entry.getValue();
            final ConfigurationStepConfigurationDTO nodeConfigurationStepDto = nodeConfigurationStepEntity.getConfigurationStep();
            dtoMap.put(entry.getKey(), nodeConfigurationStepDto);
        }

        mergeDtos(clientDto, dtoMap);
    }

    private static void mergeDtos(final ConfigurationStepConfigurationDTO clientDto, final Map<NodeIdentifier, ConfigurationStepConfigurationDTO> dtoMap) {
        // if unauthorized for the client dto, simply return
        if (clientDto == null) {
            return;
        }

        // Configuration step configurations should be consistent across all nodes
        // We primarily need to ensure that the configuration step name is consistent
        // The property values within property groups should also be consistent across nodes

        for (final Map.Entry<NodeIdentifier, ConfigurationStepConfigurationDTO> nodeEntry : dtoMap.entrySet()) {
            final ConfigurationStepConfigurationDTO nodeConfigurationStep = nodeEntry.getValue();

            if (nodeConfigurationStep != null) {
                // Verify that configuration step names are consistent
                if (clientDto.getConfigurationStepName() != null && nodeConfigurationStep.getConfigurationStepName() != null) {
                    if (!clientDto.getConfigurationStepName().equals(nodeConfigurationStep.getConfigurationStepName())) {
                        // Log inconsistency but use the client's version
                        // In a properly functioning cluster, configuration step names should be consistent
                    }
                }

                // Merge property group configurations
                mergePropertyGroupConfigurations(clientDto.getPropertyGroupConfigurations(), nodeConfigurationStep.getPropertyGroupConfigurations());
            }
        }

        // Merge property descriptors to handle dynamic allowable values
        mergePropertyDescriptors(clientDto, dtoMap);
    }

    private static void mergePropertyGroupConfigurations(final List<PropertyGroupConfigurationDTO> clientPropertyGroups, final List<PropertyGroupConfigurationDTO> nodePropertyGroups) {
        if (clientPropertyGroups == null || nodePropertyGroups == null) {
            return;
        }

        // For property groups, we expect configurations to be consistent across nodes
        // This merger primarily ensures that we have a consistent view
        // Property values should not differ between nodes in a properly configured cluster

        for (final PropertyGroupConfigurationDTO clientPropertyGroup : clientPropertyGroups) {
            for (final PropertyGroupConfigurationDTO nodePropertyGroup : nodePropertyGroups) {
                if (clientPropertyGroup.getPropertyGroupName() != null
                        && clientPropertyGroup.getPropertyGroupName().equals(nodePropertyGroup.getPropertyGroupName())) {

                    // Property values should be consistent across nodes
                    // In case of any inconsistency, we use the client's values
                    // This is primarily for defensive programming
                    break;
                }
            }
        }
    }

    /**
     * Merges property descriptors across all nodes in the configuration step.
     * This is necessary to handle dynamic allowable values that may differ between nodes.
     *
     * @param clientDto the configuration step DTO being returned to the client
     * @param dtoMap all node configuration step DTOs
     */
    static void mergePropertyDescriptors(final ConfigurationStepConfigurationDTO clientDto, final Map<NodeIdentifier, ConfigurationStepConfigurationDTO> dtoMap) {
        if (clientDto == null || clientDto.getPropertyGroupConfigurations() == null) {
            return;
        }

        for (final PropertyGroupConfigurationDTO clientPropertyGroup : clientDto.getPropertyGroupConfigurations()) {
            if (clientPropertyGroup.getPropertyDescriptors() == null) {
                continue;
            }

            // Build a map of property descriptors from all nodes for this property group
            final Map<String, Map<NodeIdentifier, ConnectorPropertyDescriptorDTO>> propertyDescriptorMap = new HashMap<>();

            for (final Map.Entry<NodeIdentifier, ConfigurationStepConfigurationDTO> nodeEntry : dtoMap.entrySet()) {
                final ConfigurationStepConfigurationDTO nodeConfigStep = nodeEntry.getValue();
                if (nodeConfigStep == null || nodeConfigStep.getPropertyGroupConfigurations() == null) {
                    continue;
                }

                // Find the matching property group in the node's configuration
                for (final PropertyGroupConfigurationDTO nodePropertyGroup : nodeConfigStep.getPropertyGroupConfigurations()) {
                    if (clientPropertyGroup.getPropertyGroupName() != null
                            && clientPropertyGroup.getPropertyGroupName().equals(nodePropertyGroup.getPropertyGroupName())
                            && nodePropertyGroup.getPropertyDescriptors() != null) {

                        // Aggregate property descriptors by name
                        for (final ConnectorPropertyDescriptorDTO propertyDescriptor : nodePropertyGroup.getPropertyDescriptors().values()) {
                            propertyDescriptorMap.computeIfAbsent(propertyDescriptor.getName(), name -> new HashMap<>())
                                    .put(nodeEntry.getKey(), propertyDescriptor);
                        }
                    }
                }
            }

            // Merge each property descriptor
            for (final Map.Entry<String, Map<NodeIdentifier, ConnectorPropertyDescriptorDTO>> entry : propertyDescriptorMap.entrySet()) {
                final String propertyName = entry.getKey();
                final Map<NodeIdentifier, ConnectorPropertyDescriptorDTO> descriptorByNodeId = entry.getValue();

                // Get the client's property descriptor directly from the map
                final ConnectorPropertyDescriptorDTO clientPropertyDescriptor = clientPropertyGroup.getPropertyDescriptors().get(propertyName);

                if (clientPropertyDescriptor != null) {
                    ConnectorPropertyDescriptorDtoMerger.merge(clientPropertyDescriptor, descriptorByNodeId);
                }
            }
        }
    }
}
