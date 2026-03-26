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
import org.apache.nifi.web.api.dto.status.ConnectorStatusDTO;
import org.apache.nifi.web.api.dto.status.ConnectorStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.NodeConnectorStatusSnapshotDTO;
import org.apache.nifi.web.api.entity.AllowableValueEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void testMergeConnectorStatusValidationStatusPriority() {
        final ConnectorEntity clientEntity = createConnectorEntityWithStatus("connector1", "RUNNING", 1, "VALID",
                5, 100L, 10, 200L, 50L, 75L, 10, 1000L, true, 5000L);
        final ConnectorEntity node1Entity = createConnectorEntityWithStatus("connector1", "RUNNING", 1, "VALIDATING",
                7, 150L, 12, 250L, 60L, 80L, 5, 500L, true, 3000L);
        final ConnectorEntity node2Entity = createConnectorEntityWithStatus("connector1", "RUNNING", 1, "VALID",
                3, 200L, 8, 300L, 70L, 85L, 8, 800L, false, null);

        final Map<NodeIdentifier, ConnectorEntity> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("client", 8000), clientEntity);
        entityMap.put(getNodeIdentifier("node1", 8001), node1Entity);
        entityMap.put(getNodeIdentifier("node2", 8002), node2Entity);

        ConnectorEntityMerger.merge(clientEntity, entityMap);

        assertEquals("VALIDATING", clientEntity.getStatus().getValidationStatus());
    }

    @Test
    void testMergeStatusSnapshotsAggregated() {
        final ConnectorEntity clientEntity = createConnectorEntityWithStatus("connector1", "RUNNING", 2, "VALID",
                5, 100L, 10, 200L, 50L, 75L, 10, 1000L, true, 5000L);
        final ConnectorEntity node1Entity = createConnectorEntityWithStatus("connector1", "RUNNING", 3, "VALID",
                7, 150L, 12, 250L, 60L, 80L, 5, 500L, true, 3000L);

        final Map<NodeIdentifier, ConnectorEntity> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("client", 8000), clientEntity);
        entityMap.put(getNodeIdentifier("node1", 8001), node1Entity);

        ConnectorEntityMerger.merge(clientEntity, entityMap);

        final ConnectorStatusDTO mergedStatus = clientEntity.getStatus();
        assertNotNull(mergedStatus);
        assertNotNull(mergedStatus.getAggregateSnapshot());
        assertNotNull(mergedStatus.getNodeSnapshots());

        // Verify node snapshots contain both nodes
        assertEquals(2, mergedStatus.getNodeSnapshots().size());

        // Verify aggregate snapshot has summed values
        final ConnectorStatusSnapshotDTO aggregate = mergedStatus.getAggregateSnapshot();
        assertEquals(Integer.valueOf(12), aggregate.getFlowFilesSent());    // 5 + 7
        assertEquals(Long.valueOf(250L), aggregate.getBytesSent());         // 100 + 150
        assertEquals(Integer.valueOf(22), aggregate.getFlowFilesReceived()); // 10 + 12
        assertEquals(Long.valueOf(450L), aggregate.getBytesReceived());     // 200 + 250
        assertEquals(Long.valueOf(110L), aggregate.getBytesRead());         // 50 + 60
        assertEquals(Long.valueOf(155L), aggregate.getBytesWritten());      // 75 + 80
        assertEquals(Integer.valueOf(15), aggregate.getFlowFilesQueued());  // 10 + 5
        assertEquals(Long.valueOf(1500L), aggregate.getBytesQueued());      // 1000 + 500
        assertEquals(Integer.valueOf(5), aggregate.getActiveThreadCount()); // 2 + 3

        // Both nodes are idle, so aggregate should be idle with min duration
        assertTrue(aggregate.getIdle());
        assertEquals(Long.valueOf(3000L), aggregate.getIdleDurationMillis()); // min(5000, 3000)
    }

    @Test
    void testMergeStatusSnapshotsIdleWhenOneNodeNotIdle() {
        final ConnectorEntity clientEntity = createConnectorEntityWithStatus("connector1", "RUNNING", 2, "VALID",
                5, 100L, 10, 200L, 50L, 75L, 10, 1000L, true, 5000L);
        final ConnectorEntity node1Entity = createConnectorEntityWithStatus("connector1", "RUNNING", 3, "VALID",
                7, 150L, 12, 250L, 60L, 80L, 5, 500L, false, null);

        final Map<NodeIdentifier, ConnectorEntity> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("client", 8000), clientEntity);
        entityMap.put(getNodeIdentifier("node1", 8001), node1Entity);

        ConnectorEntityMerger.merge(clientEntity, entityMap);

        final ConnectorStatusSnapshotDTO aggregate = clientEntity.getStatus().getAggregateSnapshot();

        // One node is not idle, so aggregate should not be idle
        assertFalse(aggregate.getIdle());
        assertNull(aggregate.getIdleDurationMillis());
    }

    @Test
    void testMergeStatusNodeSnapshotsContainCorrectNodeInfo() {
        final ConnectorEntity clientEntity = createConnectorEntityWithStatus("connector1", "RUNNING", 2, "VALID",
                5, 100L, 10, 200L, 50L, 75L, 10, 1000L, false, null);
        final ConnectorEntity node1Entity = createConnectorEntityWithStatus("connector1", "RUNNING", 3, "VALID",
                7, 150L, 12, 250L, 60L, 80L, 5, 500L, false, null);

        final NodeIdentifier clientNodeId = getNodeIdentifier("client", 8000);
        final NodeIdentifier node1NodeId = getNodeIdentifier("node1", 8001);

        final Map<NodeIdentifier, ConnectorEntity> entityMap = new HashMap<>();
        entityMap.put(clientNodeId, clientEntity);
        entityMap.put(node1NodeId, node1Entity);

        ConnectorEntityMerger.merge(clientEntity, entityMap);

        final List<NodeConnectorStatusSnapshotDTO> nodeSnapshots = clientEntity.getStatus().getNodeSnapshots();
        assertEquals(2, nodeSnapshots.size());

        // Find the client node snapshot
        final NodeConnectorStatusSnapshotDTO clientSnapshot = nodeSnapshots.stream()
                .filter(s -> s.getNodeId().equals(clientNodeId.getId()))
                .findFirst()
                .orElse(null);
        assertNotNull(clientSnapshot);
        assertEquals(clientNodeId.getApiAddress(), clientSnapshot.getAddress());
        assertEquals(clientNodeId.getApiPort(), clientSnapshot.getApiPort());
        assertEquals(Long.valueOf(100L), clientSnapshot.getStatusSnapshot().getBytesSent());

        // Find the other node snapshot
        final NodeConnectorStatusSnapshotDTO node1Snapshot = nodeSnapshots.stream()
                .filter(s -> s.getNodeId().equals(node1NodeId.getId()))
                .findFirst()
                .orElse(null);
        assertNotNull(node1Snapshot);
        assertEquals(node1NodeId.getApiAddress(), node1Snapshot.getAddress());
        assertEquals(node1NodeId.getApiPort(), node1Snapshot.getApiPort());
        assertEquals(Long.valueOf(150L), node1Snapshot.getStatusSnapshot().getBytesSent());
    }

    @Test
    void testMergeStatusWithNullStatus() {
        final ConnectorEntity clientEntity = createConnectorEntity("connector1", "STOPPED");
        final ConnectorEntity node1Entity = createConnectorEntity("connector1", "STOPPED");

        final Map<NodeIdentifier, ConnectorEntity> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("client", 8000), clientEntity);
        entityMap.put(getNodeIdentifier("node1", 8001), node1Entity);

        // Both have null status - should not throw
        ConnectorEntityMerger.merge(clientEntity, entityMap);
        assertNull(clientEntity.getStatus());
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

    private ConnectorEntity createConnectorEntityWithStatus(final String id, final String state, final int activeThreadCount, final String validationStatus,
                                                            final Integer flowFilesSent, final Long bytesSent,
                                                            final Integer flowFilesReceived, final Long bytesReceived,
                                                            final Long bytesRead, final Long bytesWritten,
                                                            final Integer flowFilesQueued, final Long bytesQueued,
                                                            final Boolean idle, final Long idleDurationMillis) {
        final ConnectorEntity entity = createConnectorEntity(id, state);

        final ConnectorStatusSnapshotDTO snapshot = new ConnectorStatusSnapshotDTO();
        snapshot.setId(id);
        snapshot.setName("Test Connector");
        snapshot.setType("TestConnector");
        snapshot.setRunStatus(state);
        snapshot.setActiveThreadCount(activeThreadCount);
        snapshot.setFlowFilesSent(flowFilesSent);
        snapshot.setBytesSent(bytesSent);
        snapshot.setFlowFilesReceived(flowFilesReceived);
        snapshot.setBytesReceived(bytesReceived);
        snapshot.setBytesRead(bytesRead);
        snapshot.setBytesWritten(bytesWritten);
        snapshot.setFlowFilesQueued(flowFilesQueued);
        snapshot.setBytesQueued(bytesQueued);
        snapshot.setIdle(idle);
        snapshot.setIdleDurationMillis(idleDurationMillis);

        final ConnectorStatusDTO status = new ConnectorStatusDTO();
        status.setId(id);
        status.setRunStatus(state);
        status.setValidationStatus(validationStatus);
        status.setAggregateSnapshot(snapshot);
        entity.setStatus(status);

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
