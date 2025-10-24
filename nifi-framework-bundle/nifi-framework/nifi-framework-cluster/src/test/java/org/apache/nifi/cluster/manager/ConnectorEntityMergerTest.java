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
import org.apache.nifi.web.api.dto.ConnectorConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.apache.nifi.web.api.dto.ConnectorPropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AllowableValueEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ConnectorEntityMergerTest {

    @Test
    void testMergeConnectorState() {
        final ConnectorEntity clientEntity = createConnectorEntity("connector1", "STOPPED");
        final ConnectorEntity node1Entity = createConnectorEntity("connector1", "STOPPED");
        final ConnectorEntity node2Entity = createConnectorEntity("connector1", "STOPPED");

        final Map<NodeIdentifier, ConnectorEntity> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("client", 8000), clientEntity);
        entityMap.put(getNodeIdentifier("node1", 8001), node1Entity);
        entityMap.put(getNodeIdentifier("node2", 8002), node2Entity);

        ConnectorEntityMerger.merge(clientEntity, entityMap);

        assertEquals("STOPPED", clientEntity.getComponent().getState());
    }

    @Test
    void testMergeConnectorStateWithNull() {
        final ConnectorEntity clientEntity = createConnectorEntity("connector1", null);
        final ConnectorEntity node1Entity = createConnectorEntity("connector1", "RUNNING");
        final ConnectorEntity node2Entity = createConnectorEntity("connector1", "RUNNING");

        final Map<NodeIdentifier, ConnectorEntity> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("client", 8000), clientEntity);
        entityMap.put(getNodeIdentifier("node1", 8001), node1Entity);
        entityMap.put(getNodeIdentifier("node2", 8002), node2Entity);

        ConnectorEntityMerger.merge(clientEntity, entityMap);

        assertEquals("RUNNING", clientEntity.getComponent().getState());
    }

    @Test
    void testMergeConfigurationStepsWithDifferentAllowableValues() {
        final ConnectorEntity clientEntity = createConnectorEntityWithConfig(
            "connector1", "STOPPED",
            Map.of("hostname", List.of("localhost", "server1", "server2"))
        );

        final ConnectorEntity node1Entity = createConnectorEntityWithConfig(
            "connector1", "STOPPED",
            Map.of("hostname", List.of("localhost", "server1"))
        );

        final ConnectorEntity node2Entity = createConnectorEntityWithConfig(
            "connector1", "STOPPED",
            Map.of("hostname", List.of("localhost", "server1", "server2"))
        );

        final Map<NodeIdentifier, ConnectorEntity> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("client", 8000), clientEntity);
        entityMap.put(getNodeIdentifier("node1", 8001), node1Entity);
        entityMap.put(getNodeIdentifier("node2", 8002), node2Entity);

        ConnectorEntityMerger.merge(clientEntity, entityMap);

        // Verify working configuration was merged - should only have values present on all nodes
        final ConnectorConfigurationDTO config = clientEntity.getComponent().getWorkingConfiguration();
        assertNotNull(config);

        final ConfigurationStepConfigurationDTO step = config.getConfigurationStepConfigurations().get(0);
        final PropertyGroupConfigurationDTO propertyGroup = step.getPropertyGroupConfigurations().get(0);
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
    void testMergeMultipleConfigurationSteps() {
        final ConnectorEntity clientEntity = createConnectorEntityWithMultipleSteps();
        final ConnectorEntity node1Entity = createConnectorEntityWithMultipleSteps();

        final Map<NodeIdentifier, ConnectorEntity> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("client", 8000), clientEntity);
        entityMap.put(getNodeIdentifier("node1", 8001), node1Entity);

        ConnectorEntityMerger.merge(clientEntity, entityMap);

        final ConnectorConfigurationDTO config = clientEntity.getComponent().getWorkingConfiguration();
        assertNotNull(config);
        assertEquals(2, config.getConfigurationStepConfigurations().size());
    }

    private NodeIdentifier getNodeIdentifier(final String id, final int port) {
        return new NodeIdentifier(id, "localhost", port, "localhost", port + 1, "localhost", port + 2, port + 3, true);
    }

    private ConnectorEntity createConnectorEntity(final String id, final String state) {
        final ConnectorDTO dto = new ConnectorDTO();
        dto.setId(id);
        dto.setName("Test Connector");
        dto.setType("TestConnector");
        dto.setState(state);

        final PermissionsDTO permissions = new PermissionsDTO();
        permissions.setCanRead(true);
        permissions.setCanWrite(true);

        final ConnectorEntity entity = new ConnectorEntity();
        entity.setComponent(dto);
        entity.setRevision(createNewRevision());
        entity.setPermissions(permissions);

        return entity;
    }

    private ConnectorEntity createConnectorEntityWithConfig(final String id, final String state,
                                                           final Map<String, List<String>> propertyAllowableValues) {
        final ConnectorEntity entity = createConnectorEntity(id, state);

        final ConnectorConfigurationDTO config = new ConnectorConfigurationDTO();
        final ConfigurationStepConfigurationDTO step = createConfigurationStep("step1", propertyAllowableValues);
        config.setConfigurationStepConfigurations(List.of(step));

        entity.getComponent().setActiveConfiguration(config);
        entity.getComponent().setWorkingConfiguration(config);

        return entity;
    }

    private ConnectorEntity createConnectorEntityWithMultipleSteps() {
        final ConnectorEntity entity = createConnectorEntity("connector1", "STOPPED");

        final ConnectorConfigurationDTO config = new ConnectorConfigurationDTO();
        final ConfigurationStepConfigurationDTO step1 = createConfigurationStep("step1",
            Map.of("hostname", List.of("localhost")));
        final ConfigurationStepConfigurationDTO step2 = createConfigurationStep("step2",
            Map.of("port", List.of("8080", "9090")));
        config.setConfigurationStepConfigurations(List.of(step1, step2));

        entity.getComponent().setActiveConfiguration(config);
        entity.getComponent().setWorkingConfiguration(config);

        return entity;
    }

    private ConfigurationStepConfigurationDTO createConfigurationStep(final String stepName,
                                                                      final Map<String, List<String>> propertyAllowableValues) {
        final ConfigurationStepConfigurationDTO step = new ConfigurationStepConfigurationDTO();
        step.setConfigurationStepName(stepName);
        step.setConfigurationStepDescription("Test step");

        final PropertyGroupConfigurationDTO propertyGroup = new PropertyGroupConfigurationDTO();
        propertyGroup.setPropertyGroupName("default");

        final Map<String, ConnectorPropertyDescriptorDTO> descriptors = new HashMap<>();
        for (final Map.Entry<String, List<String>> entry : propertyAllowableValues.entrySet()) {
            final ConnectorPropertyDescriptorDTO descriptor = new ConnectorPropertyDescriptorDTO();
            descriptor.setName(entry.getKey());
            descriptor.setDescription("Test property");
            descriptor.setType("STRING");

            final List<AllowableValueEntity> allowableValues = entry.getValue().stream()
                .map(value -> {
                    final AllowableValueDTO allowableValueDto = new AllowableValueDTO();
                    allowableValueDto.setValue(value);

                    final AllowableValueEntity allowableEntity = new AllowableValueEntity();
                    allowableEntity.setAllowableValue(allowableValueDto);
                    allowableEntity.setCanRead(true);
                    return allowableEntity;
                })
                .toList();

            descriptor.setAllowableValues(allowableValues);
            descriptors.put(entry.getKey(), descriptor);
        }

        propertyGroup.setPropertyDescriptors(descriptors);
        propertyGroup.setPropertyValues(new HashMap<>());

        step.setPropertyGroupConfigurations(List.of(propertyGroup));

        return step;
    }

    private RevisionDTO createNewRevision() {
        final RevisionDTO revisionDto = new RevisionDTO();
        revisionDto.setClientId(getClass().getName());
        revisionDto.setVersion(0L);
        return revisionDto;
    }
}
