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
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.status.ControllerServiceStatusDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ControllerServiceEntityMergerTest {

    @Test
    void testMergeStatusFields() {
        final ControllerServiceEntity nodeOneControllerserviceEntity = getControllerServiceEntity("id1", RunStatus.Stopped.name(), ValidationStatus.VALIDATING.name());
        final ControllerServiceEntity nodeTwoControllerServiceEntity = getControllerServiceEntity("id2", RunStatus.Validating.name(), ValidationStatus.INVALID.name());
        final Map<NodeIdentifier, ControllerServiceEntity> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("node1", 8000), nodeOneControllerserviceEntity);
        entityMap.put(getNodeIdentifier("node2", 8010), nodeTwoControllerServiceEntity);

        ControllerServiceEntityMerger merger = new ControllerServiceEntityMerger();
        merger.merge(nodeOneControllerserviceEntity, entityMap);
        assertEquals("Stopped", nodeOneControllerserviceEntity.getStatus().getRunStatus());
    }

    private NodeIdentifier getNodeIdentifier(final String id, final int port) {
        return new NodeIdentifier(id, "localhost", port, "localhost", port + 1, "localhost", port + 2, port + 3, true);
    }

    private ControllerServiceEntity getControllerServiceEntity(final String id, final String runStatus, final String validationStatus) {
        final ControllerServiceDTO dto = new ControllerServiceDTO();
        dto.setId(id);
        dto.setState(ScheduledState.STOPPED.name());
        dto.setValidationStatus(ValidationStatus.VALIDATING.name());

        final ControllerServiceStatusDTO statusDto = new ControllerServiceStatusDTO();
        statusDto.setRunStatus(RunStatus.Stopped.name());
        statusDto.setActiveThreadCount(1);
        statusDto.setValidationStatus(ValidationStatus.VALIDATING.name());

        final PermissionsDTO permissed = new PermissionsDTO();
        permissed.setCanRead(true);
        permissed.setCanWrite(true);

        final ControllerServiceEntity entity = new ControllerServiceEntity();
        entity.setComponent(dto);
        entity.setRevision(createNewRevision());
        entity.setDisconnectedNodeAcknowledged(true);
        entity.setPermissions(permissed);
        entity.setStatus(statusDto);

        return entity;
    }

    public RevisionDTO createNewRevision() {
        final RevisionDTO revisionDto = new RevisionDTO();
        revisionDto.setClientId(getClass().getName());
        revisionDto.setVersion(0L);
        return revisionDto;
    }
}