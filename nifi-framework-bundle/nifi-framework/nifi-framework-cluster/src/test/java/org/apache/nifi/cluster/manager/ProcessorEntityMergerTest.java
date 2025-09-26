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
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.status.RunStatus;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProcessorEntityMergerTest {

    @Test
    void testMergePhysicalState_StoppingTakePrecedence() {
        // Test that STOPPING state takes precedence over stable states
        final ProcessorEntity clientEntity = createProcessorEntity("client", "STOPPED");
        final ProcessorEntity nodeEntity1 = createProcessorEntity("node1", "STOPPED");
        final ProcessorEntity nodeEntity2 = createProcessorEntity("node2", "STOPPING");
        final ProcessorEntity nodeEntity3 = createProcessorEntity("node3", "STOPPED");

        final Map<NodeIdentifier, ProcessorEntity> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("client", 8000), clientEntity);
        entityMap.put(getNodeIdentifier("node1", 8001), nodeEntity1);
        entityMap.put(getNodeIdentifier("node2", 8002), nodeEntity2);
        entityMap.put(getNodeIdentifier("node3", 8003), nodeEntity3);

        ProcessorEntityMerger merger = new ProcessorEntityMerger();
        merger.merge(clientEntity, entityMap);

        assertEquals("STOPPING", clientEntity.getPhysicalState());
    }

    @Test
    void testMergePhysicalState_StartingTakePrecedence() {
        // Test that STARTING state takes precedence over stable states
        final ProcessorEntity clientEntity = createProcessorEntity("client", "RUNNING");
        final ProcessorEntity nodeEntity1 = createProcessorEntity("node1", "RUNNING");
        final ProcessorEntity nodeEntity2 = createProcessorEntity("node2", "STARTING");
        final ProcessorEntity nodeEntity3 = createProcessorEntity("node3", "RUNNING");

        final Map<NodeIdentifier, ProcessorEntity> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("client", 8000), clientEntity);
        entityMap.put(getNodeIdentifier("node1", 8001), nodeEntity1);
        entityMap.put(getNodeIdentifier("node2", 8002), nodeEntity2);
        entityMap.put(getNodeIdentifier("node3", 8003), nodeEntity3);

        ProcessorEntityMerger merger = new ProcessorEntityMerger();
        merger.merge(clientEntity, entityMap);

        assertEquals("STARTING", clientEntity.getPhysicalState());
    }

    @Test
    void testMergePhysicalState_NullPhysicalStates() {
        // Test that null physical states are handled gracefully
        final ProcessorEntity clientEntity = createProcessorEntity("client", null);
        final ProcessorEntity nodeEntity1 = createProcessorEntity("node1", null);
        final ProcessorEntity nodeEntity2 = createProcessorEntity("node2", "RUNNING");

        final Map<NodeIdentifier, ProcessorEntity> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("client", 8000), clientEntity);
        entityMap.put(getNodeIdentifier("node1", 8001), nodeEntity1);
        entityMap.put(getNodeIdentifier("node2", 8002), nodeEntity2);

        ProcessorEntityMerger merger = new ProcessorEntityMerger();
        merger.merge(clientEntity, entityMap);

        assertEquals("RUNNING", clientEntity.getPhysicalState());
    }

    private NodeIdentifier getNodeIdentifier(final String id, final int port) {
        return new NodeIdentifier(id, "localhost", port, "localhost", port + 1, "localhost", port + 2, port + 3, true);
    }

    private ProcessorEntity createProcessorEntity(final String id, final String physicalState) {
        final ProcessorDTO dto = new ProcessorDTO();
        dto.setId(id);
        dto.setState(ScheduledState.STOPPED.name());
        dto.setPhysicalState(physicalState);
        dto.setValidationStatus(ValidationStatus.VALID.name());

        // Add ProcessorConfigDTO to avoid NPE during merging
        final ProcessorConfigDTO configDto = new ProcessorConfigDTO();
        configDto.setDescriptors(new HashMap<>());
        dto.setConfig(configDto);

        final ProcessorStatusDTO statusDto = new ProcessorStatusDTO();
        statusDto.setRunStatus(RunStatus.Stopped.name());

        final PermissionsDTO permissions = new PermissionsDTO();
        permissions.setCanRead(true);
        permissions.setCanWrite(true);

        final ProcessorEntity entity = new ProcessorEntity();
        entity.setComponent(dto);
        entity.setRevision(createNewRevision());
        entity.setPermissions(permissions);
        entity.setStatus(statusDto);
        entity.setPhysicalState(physicalState);

        return entity;
    }

    public RevisionDTO createNewRevision() {
        final RevisionDTO revisionDto = new RevisionDTO();
        revisionDto.setClientId(getClass().getName());
        revisionDto.setVersion(0L);
        return revisionDto;
    }

}
