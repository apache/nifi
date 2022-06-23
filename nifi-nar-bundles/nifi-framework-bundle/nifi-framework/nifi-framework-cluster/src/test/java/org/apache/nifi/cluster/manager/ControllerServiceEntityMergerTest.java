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
    void TestMergeStatusFields() {
        final ControllerServiceEntity nodeOneControllerserviceEntity = getControllerServiceEntity("id1", RunStatus.Stopped.toString(), ValidationStatus.VALIDATING.toString());
        final ControllerServiceEntity nodeTwoControllerServiceEntity = getControllerServiceEntity("id2", RunStatus.Validating.toString(), ValidationStatus.INVALID.toString());
        final Map<NodeIdentifier, ControllerServiceEntity> entityMap = new HashMap();
        entityMap.put(getNodeIdentifier("node1", 8000), nodeOneControllerserviceEntity);
        entityMap.put(getNodeIdentifier("node2", 8010), nodeTwoControllerServiceEntity);

        ControllerServiceEntityMerger merger = new ControllerServiceEntityMerger();
        merger.merge(nodeOneControllerserviceEntity, entityMap);
        assertEquals("Stopped", nodeOneControllerserviceEntity.getStatus().getRunStatus());
    }

    private NodeIdentifier getNodeIdentifier(final String id, final int port) {
        return new NodeIdentifier(id, "localhost", port, "localhost", port+1, "localhost", port+2, port+3, true);
    }

    private ControllerServiceEntity getControllerServiceEntity(final String id, final String runStatus, final String validationStatus) {
        final ControllerServiceDTO dto = new ControllerServiceDTO();
        dto.setId(id);
        dto.setState(ScheduledState.STOPPED.name());
        dto.setValidationStatus(ValidationStatus.VALIDATING.toString());

        final ControllerServiceStatusDTO statusDto = new ControllerServiceStatusDTO();
        statusDto.setRunStatus(RunStatus.Stopped.toString());
        statusDto.setActiveThreadCount(1);
        statusDto.setValidationStatus(ValidationStatus.VALIDATING.toString());

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