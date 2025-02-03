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
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.status.ReportingTaskStatusDTO;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReportingTaskEntityMergerTest {

    @Test
    void testMergeStatusFields() {
        final ReportingTaskEntity nodeOneReportingTaskEntity = getReportingTaskEntity("id1", ReportingTaskStatusDTO.RUNNING, ValidationStatus.VALID.name());
        final ReportingTaskEntity nodeTwoReportingTaskEntity = getReportingTaskEntity("id2", ReportingTaskStatusDTO.RUNNING, ValidationStatus.VALIDATING.name());
        final Map<NodeIdentifier, ReportingTaskEntity> entityMap = new HashMap<>();
        entityMap.put(getNodeIdentifier("node1", 8000), nodeOneReportingTaskEntity);
        entityMap.put(getNodeIdentifier("node2", 8010), nodeTwoReportingTaskEntity);

        ReportingTaskEntityMerger merger = new ReportingTaskEntityMerger();
        merger.merge(nodeOneReportingTaskEntity, entityMap);
        assertEquals(2, nodeOneReportingTaskEntity.getStatus().getActiveThreadCount());
        assertEquals(ReportingTaskStatusDTO.RUNNING, nodeOneReportingTaskEntity.getStatus().getRunStatus());
        assertEquals(ValidationStatus.VALIDATING.name(), nodeOneReportingTaskEntity.getStatus().getValidationStatus());
    }

    private NodeIdentifier getNodeIdentifier(final String id, final int port) {
        return new NodeIdentifier(id, "localhost", port, "localhost", port + 1, "localhost", port + 2, port + 3, true);
    }

    private ReportingTaskEntity getReportingTaskEntity(final String id, final String runStatus, final String validationStatus) {
        final ReportingTaskDTO dto = new ReportingTaskDTO();
        dto.setId(id);
        dto.setState(ScheduledState.STOPPED.name());
        dto.setValidationStatus(validationStatus);

        final ReportingTaskStatusDTO statusDto = new ReportingTaskStatusDTO();
        statusDto.setRunStatus(runStatus);
        statusDto.setActiveThreadCount(1);
        statusDto.setValidationStatus(validationStatus);

        final PermissionsDTO permissed = new PermissionsDTO();
        permissed.setCanRead(true);
        permissed.setCanWrite(true);

        final ReportingTaskEntity entity = new ReportingTaskEntity();
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