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
import org.apache.nifi.web.api.dto.AllowableValueDTO;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorPropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.web.api.entity.AllowableValueEntity;
import org.apache.nifi.web.api.entity.ConfigurationStepEntity;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ConfigurationStepEntityMergerTest {

    @Test
    void testMergePropertyDescriptorsAcrossNodes() {
        final ConfigurationStepEntity clientEntity = createConfigurationStepEntity("step1",
            Map.of("hostname", List.of("localhost", "server1", "server2")));

        final ConfigurationStepEntity node1Entity = createConfigurationStepEntity("step1",
            Map.of("hostname", List.of("localhost", "server1")));

        final ConfigurationStepEntity node2Entity = createConfigurationStepEntity("step1",
            Map.of("hostname", List.of("localhost", "server1", "server2")));

        final Map<NodeIdentifier, ConfigurationStepEntity> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("client", 8000), clientEntity);
        entityMap.put(getNodeIdentifier("node1", 8001), node1Entity);
        entityMap.put(getNodeIdentifier("node2", 8002), node2Entity);

        ConfigurationStepEntityMerger.merge(clientEntity, entityMap);

        // Should only include allowable values present on all nodes
        final PropertyGroupConfigurationDTO propertyGroup = clientEntity.getConfigurationStep().getPropertyGroupConfigurations().get(0);
        final ConnectorPropertyDescriptorDTO descriptor = propertyGroup.getPropertyDescriptors().get("hostname");

        assertNotNull(descriptor.getAllowableValues());
        assertEquals(2, descriptor.getAllowableValues().size());

        final List<String> values = descriptor.getAllowableValues().stream()
            .map(AllowableValueEntity::getAllowableValue)
            .map(AllowableValueDTO::getValue)
            .sorted()
            .toList();

        assertEquals(List.of("localhost", "server1"), values);
    }

    @Test
    void testMergeWithNoOverlappingAllowableValues() {
        final ConfigurationStepEntity clientEntity = createConfigurationStepEntity("step1",
            Map.of("region", List.of("us-east-1", "us-west-1")));

        final ConfigurationStepEntity node1Entity = createConfigurationStepEntity("step1",
            Map.of("region", List.of("eu-west-1")));

        final ConfigurationStepEntity node2Entity = createConfigurationStepEntity("step1",
            Map.of("region", List.of("ap-south-1")));

        final Map<NodeIdentifier, ConfigurationStepEntity> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("client", 8000), clientEntity);
        entityMap.put(getNodeIdentifier("node1", 8001), node1Entity);
        entityMap.put(getNodeIdentifier("node2", 8002), node2Entity);

        ConfigurationStepEntityMerger.merge(clientEntity, entityMap);

        final PropertyGroupConfigurationDTO propertyGroup = clientEntity.getConfigurationStep().getPropertyGroupConfigurations().get(0);
        final ConnectorPropertyDescriptorDTO descriptor = propertyGroup.getPropertyDescriptors().get("region");

        assertNotNull(descriptor.getAllowableValues());
        assertEquals(0, descriptor.getAllowableValues().size());
    }

    @Test
    void testMergeMultiplePropertyGroups() {
        final ConfigurationStepConfigurationDTO clientStep = new ConfigurationStepConfigurationDTO();
        clientStep.setConfigurationStepName("database-config");

        final PropertyGroupConfigurationDTO group1 = createPropertyGroup("connection",
            Map.of("hostname", List.of("localhost", "server1")));
        final PropertyGroupConfigurationDTO group2 = createPropertyGroup("credentials",
            Map.of("auth-type", List.of("password", "certificate")));

        clientStep.setPropertyGroupConfigurations(List.of(group1, group2));

        final ConfigurationStepConfigurationDTO node1Step = new ConfigurationStepConfigurationDTO();
        node1Step.setConfigurationStepName("database-config");
        final PropertyGroupConfigurationDTO node1Group1 = createPropertyGroup("connection",
            Map.of("hostname", List.of("localhost", "server1")));
        final PropertyGroupConfigurationDTO node1Group2 = createPropertyGroup("credentials",
            Map.of("auth-type", List.of("password")));
        node1Step.setPropertyGroupConfigurations(List.of(node1Group1, node1Group2));

        final Map<NodeIdentifier, ConfigurationStepConfigurationDTO> dtoMap = new HashMap<>();
        dtoMap.put(getNodeIdentifier("client", 8000), clientStep);
        dtoMap.put(getNodeIdentifier("node1", 8001), node1Step);

        ConfigurationStepEntityMerger.mergePropertyDescriptors(clientStep, dtoMap);

        // Verify first property group
        final ConnectorPropertyDescriptorDTO hostnameDescriptor = group1.getPropertyDescriptors().get("hostname");
        assertEquals(2, hostnameDescriptor.getAllowableValues().size());

        // Verify second property group - should only have "password" since node1 doesn't have "certificate"
        final ConnectorPropertyDescriptorDTO authDescriptor = group2.getPropertyDescriptors().get("auth-type");
        assertEquals(1, authDescriptor.getAllowableValues().size());
        assertEquals("password", authDescriptor.getAllowableValues().get(0).getAllowableValue().getValue());
    }

    private NodeIdentifier getNodeIdentifier(final String id, final int port) {
        return new NodeIdentifier(id, "localhost", port, "localhost", port + 1, "localhost", port + 2, port + 3, true);
    }

    private ConfigurationStepEntity createConfigurationStepEntity(final String stepName, final Map<String, List<String>> propertyAllowableValues) {
        final ConfigurationStepConfigurationDTO stepDto = new ConfigurationStepConfigurationDTO();
        stepDto.setConfigurationStepName(stepName);
        stepDto.setConfigurationStepDescription("Test configuration step");

        final PropertyGroupConfigurationDTO propertyGroup = createPropertyGroup("default", propertyAllowableValues);
        stepDto.setPropertyGroupConfigurations(List.of(propertyGroup));

        final ConfigurationStepEntity entity = new ConfigurationStepEntity();
        entity.setConfigurationStep(stepDto);

        return entity;
    }

    private PropertyGroupConfigurationDTO createPropertyGroup(final String groupName, final Map<String, List<String>> propertyAllowableValues) {
        final PropertyGroupConfigurationDTO propertyGroup = new PropertyGroupConfigurationDTO();
        propertyGroup.setPropertyGroupName(groupName);

        final Map<String, ConnectorPropertyDescriptorDTO> descriptors = new HashMap<>();
        for (final Map.Entry<String, List<String>> entry : propertyAllowableValues.entrySet()) {
            final ConnectorPropertyDescriptorDTO descriptor = new ConnectorPropertyDescriptorDTO();
            descriptor.setName(entry.getKey());
            descriptor.setDescription("Test property");
            descriptor.setType("STRING");
            descriptor.setRequired(false);

            final List<AllowableValueEntity> allowableValues = entry.getValue().stream()
                .map(value -> {
                    final AllowableValueDTO allowableValueDto = new AllowableValueDTO();
                    allowableValueDto.setValue(value);
                    allowableValueDto.setDisplayName(value);

                    final AllowableValueEntity entity = new AllowableValueEntity();
                    entity.setAllowableValue(allowableValueDto);
                    entity.setCanRead(true);
                    return entity;
                })
                .toList();

            descriptor.setAllowableValues(allowableValues);
            descriptors.put(entry.getKey(), descriptor);
        }

        propertyGroup.setPropertyDescriptors(descriptors);
        propertyGroup.setPropertyValues(new HashMap<>());

        return propertyGroup;
    }
}
