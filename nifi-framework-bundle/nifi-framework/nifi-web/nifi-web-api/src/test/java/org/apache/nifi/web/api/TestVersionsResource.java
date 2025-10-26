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
package org.apache.nifi.web.api;

import jakarta.ws.rs.core.Response;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.CreateFlowBranchRequestEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestVersionsResource {

    @InjectMocks
    private VersionsResource versionsResource = new VersionsResource();

    @Mock
    private NiFiServiceFacade serviceFacade;

    @Mock
    private NiFiProperties properties;

    @BeforeEach
    public void setUp() {
        versionsResource.setProperties(properties);
        lenient().when(properties.isNode()).thenReturn(false);
        lenient().when(properties.isClustered()).thenReturn(false);
        versionsResource.httpServletRequest = new MockHttpServletRequest();
    }

    @Test
    public void testExportFlowVersion() {
        final String groupId = UUID.randomUUID().toString();
        final RegisteredFlowSnapshot versionedFlowSnapshot = mock(RegisteredFlowSnapshot.class);
        final FlowSnapshotContainer snapshotContainer = new FlowSnapshotContainer(versionedFlowSnapshot);
        when(serviceFacade.getVersionedFlowSnapshotByGroupId(groupId)).thenReturn(snapshotContainer);

        final String flowName = "flowname";
        final String flowVersion = "1";
        final VersionedProcessGroup versionedProcessGroup = mock(VersionedProcessGroup.class);
        final RegisteredFlowSnapshotMetadata snapshotMetadata = mock(RegisteredFlowSnapshotMetadata.class);
        when(versionedFlowSnapshot.getFlowContents()).thenReturn(versionedProcessGroup);
        when(versionedProcessGroup.getName()).thenReturn(flowName);
        when(versionedFlowSnapshot.getSnapshotMetadata()).thenReturn(snapshotMetadata);
        when(snapshotMetadata.getVersion()).thenReturn(flowVersion);

        final VersionedProcessGroup innerVersionedProcessGroup = mock(VersionedProcessGroup.class);
        final VersionedProcessGroup innerInnerVersionedProcessGroup = mock(VersionedProcessGroup.class);
        when(versionedProcessGroup.getProcessGroups()).thenReturn(Collections.singleton(innerVersionedProcessGroup));
        when(innerVersionedProcessGroup.getProcessGroups()).thenReturn(Collections.singleton(innerInnerVersionedProcessGroup));

        final Response response = versionsResource.exportFlowVersion(groupId);

        final RegisteredFlowSnapshot resultEntity = (RegisteredFlowSnapshot) response.getEntity();

        assertEquals(200, response.getStatus());
        assertEquals(versionedFlowSnapshot, resultEntity);

        verify(versionedFlowSnapshot).setFlow(null);
        verify(versionedFlowSnapshot).setBucket(null);
        verify(versionedFlowSnapshot).setSnapshotMetadata(null);
        verify(versionedProcessGroup).setVersionedFlowCoordinates(null);
        verify(innerVersionedProcessGroup).setVersionedFlowCoordinates(null);
        verify(innerInnerVersionedProcessGroup).setVersionedFlowCoordinates(null);
    }


    @Test
    public void testCreateFlowBranchRequiresBranchName() {
        final String groupId = UUID.randomUUID().toString();
        final CreateFlowBranchRequestEntity requestEntity = new CreateFlowBranchRequestEntity();
        final RevisionDTO revisionDTO = new RevisionDTO();
        revisionDTO.setClientId("client-id");
        revisionDTO.setVersion(0L);
        requestEntity.setProcessGroupRevision(revisionDTO);

        assertThrows(IllegalArgumentException.class, () -> versionsResource.createFlowBranch(groupId, requestEntity));
    }

    @Test
    public void testCreateFlowBranchInvokesService() {
        final String groupId = UUID.randomUUID().toString();
        final CreateFlowBranchRequestEntity requestEntity = new CreateFlowBranchRequestEntity();
        final RevisionDTO revisionDTO = new RevisionDTO();
        revisionDTO.setClientId("client-id");
        revisionDTO.setVersion(1L);
        requestEntity.setProcessGroupRevision(revisionDTO);
        requestEntity.setBranch("feature");

        versionsResource.httpServletRequest = new MockHttpServletRequest();

        final VersionControlInformationDTO currentDto = new VersionControlInformationDTO();
        currentDto.setGroupId(groupId);
        currentDto.setBranch("main");
        currentDto.setVersion("3");
        currentDto.setState(VersionControlInformationDTO.UP_TO_DATE);
        final VersionControlInformationEntity currentEntity = new VersionControlInformationEntity();
        currentEntity.setVersionControlInformation(currentDto);
        when(serviceFacade.getVersionControlInformation(groupId)).thenReturn(currentEntity);

        final VersionControlInformationEntity expectedEntity = new VersionControlInformationEntity();
        when(serviceFacade.createFlowBranch(any(Revision.class), eq(groupId), eq("feature"), any(), any()))
                .thenReturn(expectedEntity);

        final Response response = versionsResource.createFlowBranch(groupId, requestEntity);
        assertEquals(200, response.getStatus());
        assertEquals(expectedEntity, response.getEntity());

        ArgumentCaptor<Revision> revisionCaptor = ArgumentCaptor.forClass(Revision.class);
        verify(serviceFacade).createFlowBranch(revisionCaptor.capture(), eq(groupId), eq("feature"), isNull(), isNull());

        final Revision capturedRevision = revisionCaptor.getValue();
        assertEquals(1L, capturedRevision.getVersion());
        assertEquals("client-id", capturedRevision.getClientId());
        assertEquals(groupId, capturedRevision.getComponentId());
    }

    @Test
    public void testCreateFlowBranchFailsWhenBranchExists() {
        final String groupId = UUID.randomUUID().toString();
        final CreateFlowBranchRequestEntity requestEntity = new CreateFlowBranchRequestEntity();
        final RevisionDTO revisionDTO = new RevisionDTO();
        revisionDTO.setClientId("client-id");
        revisionDTO.setVersion(1L);
        requestEntity.setProcessGroupRevision(revisionDTO);
        requestEntity.setBranch("main");

        versionsResource.httpServletRequest = new MockHttpServletRequest();

        final VersionControlInformationDTO currentDto = new VersionControlInformationDTO();
        currentDto.setGroupId(groupId);
        currentDto.setBranch("main");
        currentDto.setState(VersionControlInformationDTO.UP_TO_DATE);
        final VersionControlInformationEntity currentEntity = new VersionControlInformationEntity();
        currentEntity.setVersionControlInformation(currentDto);
        when(serviceFacade.getVersionControlInformation(groupId)).thenReturn(currentEntity);

        when(serviceFacade.createFlowBranch(any(Revision.class), eq(groupId), eq("main"), any(), any()))
                .thenThrow(new IllegalArgumentException("Process Group is already tracking branch main"));

        assertThrows(IllegalArgumentException.class, () -> versionsResource.createFlowBranch(groupId, requestEntity));
    }

    @Test
    public void testCreateFlowBranchUnsupported() {
        final String groupId = UUID.randomUUID().toString();
        final CreateFlowBranchRequestEntity requestEntity = new CreateFlowBranchRequestEntity();
        final RevisionDTO revisionDTO = new RevisionDTO();
        revisionDTO.setClientId("client-id");
        revisionDTO.setVersion(1L);
        requestEntity.setProcessGroupRevision(revisionDTO);
        requestEntity.setBranch("feature");

        versionsResource.httpServletRequest = new MockHttpServletRequest();

        final VersionControlInformationDTO currentDto = new VersionControlInformationDTO();
        currentDto.setGroupId(groupId);
        currentDto.setBranch("main");
        currentDto.setVersion("2");
        currentDto.setState(VersionControlInformationDTO.UP_TO_DATE);
        final VersionControlInformationEntity currentEntity = new VersionControlInformationEntity();
        currentEntity.setVersionControlInformation(currentDto);
        when(serviceFacade.getVersionControlInformation(groupId)).thenReturn(currentEntity);

        when(serviceFacade.createFlowBranch(any(Revision.class), eq(groupId), eq("feature"), any(), any()))
                .thenThrow(new IllegalArgumentException("Registry does not support branching"));

        assertThrows(IllegalArgumentException.class, () -> versionsResource.createFlowBranch(groupId, requestEntity));
    }

    @Test
    public void testCreateFlowBranchAllowedWhenLocallyModified() {
        final String groupId = UUID.randomUUID().toString();
        final CreateFlowBranchRequestEntity requestEntity = new CreateFlowBranchRequestEntity();
        final RevisionDTO revisionDTO = new RevisionDTO();
        revisionDTO.setClientId("client-id");
        revisionDTO.setVersion(1L);
        requestEntity.setProcessGroupRevision(revisionDTO);
        requestEntity.setBranch("feature");

        versionsResource.httpServletRequest = new MockHttpServletRequest();

        final VersionControlInformationDTO currentDto = new VersionControlInformationDTO();
        currentDto.setGroupId(groupId);
        currentDto.setBranch("main");
        currentDto.setVersion("1");
        currentDto.setState(VersionControlInformationDTO.LOCALLY_MODIFIED);
        final VersionControlInformationEntity currentEntity = new VersionControlInformationEntity();
        currentEntity.setVersionControlInformation(currentDto);
        when(serviceFacade.getVersionControlInformation(groupId)).thenReturn(currentEntity);

        final VersionControlInformationEntity expectedEntity = new VersionControlInformationEntity();
        when(serviceFacade.createFlowBranch(any(Revision.class), eq(groupId), eq("feature"), any(), any()))
                .thenReturn(expectedEntity);

        final Response response = versionsResource.createFlowBranch(groupId, requestEntity);
        assertEquals(200, response.getStatus());
        assertEquals(expectedEntity, response.getEntity());
    }

    @Test
    public void testCreateFlowBranchBlockedWhenSyncFailure() {
        final String groupId = UUID.randomUUID().toString();
        final CreateFlowBranchRequestEntity requestEntity = new CreateFlowBranchRequestEntity();
        final RevisionDTO revisionDTO = new RevisionDTO();
        revisionDTO.setClientId("client-id");
        revisionDTO.setVersion(1L);
        requestEntity.setProcessGroupRevision(revisionDTO);
        requestEntity.setBranch("feature");

        versionsResource.httpServletRequest = new MockHttpServletRequest();

        final VersionControlInformationDTO currentDto = new VersionControlInformationDTO();
        currentDto.setGroupId(groupId);
        currentDto.setBranch("main");
        currentDto.setVersion("1");
        currentDto.setState(VersionControlInformationDTO.SYNC_FAILURE);
        final VersionControlInformationEntity currentEntity = new VersionControlInformationEntity();
        currentEntity.setVersionControlInformation(currentDto);
        when(serviceFacade.getVersionControlInformation(groupId)).thenReturn(currentEntity);

        assertThrows(IllegalStateException.class, () -> versionsResource.createFlowBranch(groupId, requestEntity));

        verify(serviceFacade, never()).createFlowBranch(any(), anyString(), anyString(), any(), any());
    }

    @Test
    public void testCreateFlowBranchAllowedWhenLocallyModifiedAndStale() {
        final String groupId = UUID.randomUUID().toString();
        final CreateFlowBranchRequestEntity requestEntity = new CreateFlowBranchRequestEntity();
        final RevisionDTO revisionDTO = new RevisionDTO();
        revisionDTO.setClientId("client-id");
        revisionDTO.setVersion(1L);
        requestEntity.setProcessGroupRevision(revisionDTO);
        requestEntity.setBranch("feature");

        versionsResource.httpServletRequest = new MockHttpServletRequest();

        final VersionControlInformationDTO currentDto = new VersionControlInformationDTO();
        currentDto.setGroupId(groupId);
        currentDto.setBranch("main");
        currentDto.setVersion("1");
        currentDto.setState(VersionControlInformationDTO.LOCALLY_MODIFIED_AND_STALE);
        final VersionControlInformationEntity currentEntity = new VersionControlInformationEntity();
        currentEntity.setVersionControlInformation(currentDto);
        when(serviceFacade.getVersionControlInformation(groupId)).thenReturn(currentEntity);

        final VersionControlInformationEntity expectedEntity = new VersionControlInformationEntity();
        when(serviceFacade.createFlowBranch(any(Revision.class), eq(groupId), eq("feature"), any(), any()))
                .thenReturn(expectedEntity);

        final Response response = versionsResource.createFlowBranch(groupId, requestEntity);
        assertEquals(200, response.getStatus());
        assertEquals(expectedEntity, response.getEntity());
    }

    @Test
    public void testCreateFlowBranchBranchAlreadyExists() {
        final String groupId = UUID.randomUUID().toString();
        final CreateFlowBranchRequestEntity requestEntity = new CreateFlowBranchRequestEntity();
        final RevisionDTO revisionDTO = new RevisionDTO();
        revisionDTO.setClientId("client-id");
        revisionDTO.setVersion(1L);
        requestEntity.setProcessGroupRevision(revisionDTO);
        requestEntity.setBranch("feature");

        versionsResource.httpServletRequest = new MockHttpServletRequest();

        final VersionControlInformationDTO currentDto = new VersionControlInformationDTO();
        currentDto.setGroupId(groupId);
        currentDto.setBranch("main");
        currentDto.setVersion("3");
        currentDto.setState(VersionControlInformationDTO.UP_TO_DATE);
        final VersionControlInformationEntity currentEntity = new VersionControlInformationEntity();
        currentEntity.setVersionControlInformation(currentDto);
        when(serviceFacade.getVersionControlInformation(groupId)).thenReturn(currentEntity);

        when(serviceFacade.createFlowBranch(any(Revision.class), eq(groupId), eq("feature"), any(), any()))
                .thenThrow(new NiFiCoreException("Unable to create branch [feature] in registry with ID registry-1: Branch [feature] already exists"));

        assertThrows(NiFiCoreException.class, () -> versionsResource.createFlowBranch(groupId, requestEntity));
    }

    @Test
    public void testCreateFlowBranchNotVersionControlled() {
        final String groupId = UUID.randomUUID().toString();
        final CreateFlowBranchRequestEntity requestEntity = new CreateFlowBranchRequestEntity();
        final RevisionDTO revisionDTO = new RevisionDTO();
        revisionDTO.setClientId("client-id");
        revisionDTO.setVersion(1L);
        requestEntity.setProcessGroupRevision(revisionDTO);
        requestEntity.setBranch("feature");

        versionsResource.httpServletRequest = new MockHttpServletRequest();

        final VersionControlInformationEntity currentEntity = new VersionControlInformationEntity();
        currentEntity.setVersionControlInformation(null);
        when(serviceFacade.getVersionControlInformation(groupId)).thenReturn(currentEntity);

        assertThrows(IllegalStateException.class, () -> versionsResource.createFlowBranch(groupId, requestEntity));
    }
}
