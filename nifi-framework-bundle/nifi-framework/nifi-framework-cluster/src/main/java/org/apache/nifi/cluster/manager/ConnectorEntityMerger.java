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
import org.apache.nifi.web.api.dto.ConnectorConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;

import java.util.HashMap;
import java.util.Map;

public class ConnectorEntityMerger {

    /**
     * Merges the ConnectorEntity responses.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap all node responses
     */
    public static void merge(final ConnectorEntity clientEntity, final Map<NodeIdentifier, ConnectorEntity> entityMap) {
        final ConnectorDTO clientDto = clientEntity.getComponent();
        final Map<NodeIdentifier, ConnectorDTO> dtoMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, ConnectorEntity> entry : entityMap.entrySet()) {
            final ConnectorEntity nodeConnectorEntity = entry.getValue();
            final ConnectorDTO nodeConnectorDto = nodeConnectorEntity.getComponent();
            dtoMap.put(entry.getKey(), nodeConnectorDto);
        }

        mergeDtos(clientDto, dtoMap);
    }

    private static void mergeDtos(final ConnectorDTO clientDto, final Map<NodeIdentifier, ConnectorDTO> dtoMap) {
        // if unauthorized for the client dto, simple return
        if (clientDto == null) {
            return;
        }

        // Basic connector state consistency check - all nodes should have same state
        for (final Map.Entry<NodeIdentifier, ConnectorDTO> nodeEntry : dtoMap.entrySet()) {
            final ConnectorDTO nodeConnector = nodeEntry.getValue();

            if (nodeConnector != null) {
                // Ensure state consistency across cluster
                if (clientDto.getState() == null && nodeConnector.getState() != null) {
                    clientDto.setState(nodeConnector.getState());
                }
            }
        }

        // Merge configuration steps to handle dynamic property descriptors
        mergeActiveConfiguration(clientDto, dtoMap);
        mergeWorkingConfiguration(clientDto, dtoMap);
    }

    private static void mergeActiveConfiguration(final ConnectorDTO clientDto, final Map<NodeIdentifier, ConnectorDTO> dtoMap) {
        final ConnectorConfigurationDTO clientConfig = clientDto.getActiveConfiguration();
        if (clientConfig == null || clientConfig.getConfigurationStepConfigurations() == null) {
            return;
        }

        // For each configuration step in the client's active configuration, merge it with the corresponding steps from all nodes
        for (final ConfigurationStepConfigurationDTO clientStep : clientConfig.getConfigurationStepConfigurations()) {
            final Map<NodeIdentifier, ConfigurationStepConfigurationDTO> stepDtoMap = new HashMap<>();

            for (final Map.Entry<NodeIdentifier, ConnectorDTO> nodeEntry : dtoMap.entrySet()) {
                final ConnectorDTO nodeDto = nodeEntry.getValue();
                if (nodeDto == null || nodeDto.getActiveConfiguration() == null || nodeDto.getActiveConfiguration().getConfigurationStepConfigurations() == null) {
                    continue;
                }

                // Find the matching configuration step in the node's active configuration
                nodeDto.getActiveConfiguration().getConfigurationStepConfigurations().stream()
                        .filter(step -> clientStep.getConfigurationStepName() != null
                                && clientStep.getConfigurationStepName().equals(step.getConfigurationStepName()))
                        .findFirst()
                        .ifPresent(step -> stepDtoMap.put(nodeEntry.getKey(), step));
            }

            // Merge property descriptors for this configuration step
            ConfigurationStepEntityMerger.mergePropertyDescriptors(clientStep, stepDtoMap);
        }
    }

    private static void mergeWorkingConfiguration(final ConnectorDTO clientDto, final Map<NodeIdentifier, ConnectorDTO> dtoMap) {
        final ConnectorConfigurationDTO clientConfig = clientDto.getWorkingConfiguration();
        if (clientConfig == null || clientConfig.getConfigurationStepConfigurations() == null) {
            return;
        }

        // For each configuration step in the client's working configuration, merge it with the corresponding steps from all nodes
        for (final ConfigurationStepConfigurationDTO clientStep : clientConfig.getConfigurationStepConfigurations()) {
            final Map<NodeIdentifier, ConfigurationStepConfigurationDTO> stepDtoMap = new HashMap<>();

            for (final Map.Entry<NodeIdentifier, ConnectorDTO> nodeEntry : dtoMap.entrySet()) {
                final ConnectorDTO nodeDto = nodeEntry.getValue();
                if (nodeDto == null || nodeDto.getWorkingConfiguration() == null || nodeDto.getWorkingConfiguration().getConfigurationStepConfigurations() == null) {
                    continue;
                }

                // Find the matching configuration step in the node's working configuration
                nodeDto.getWorkingConfiguration().getConfigurationStepConfigurations().stream()
                        .filter(step -> clientStep.getConfigurationStepName() != null
                                && clientStep.getConfigurationStepName().equals(step.getConfigurationStepName()))
                        .findFirst()
                        .ifPresent(step -> stepDtoMap.put(nodeEntry.getKey(), step));
            }

            // Merge property descriptors for this configuration step
            ConfigurationStepEntityMerger.mergePropertyDescriptors(clientStep, stepDtoMap);
        }
    }
}
