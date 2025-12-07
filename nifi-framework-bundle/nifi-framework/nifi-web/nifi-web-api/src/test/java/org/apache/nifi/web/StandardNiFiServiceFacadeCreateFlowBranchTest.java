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
package org.apache.nifi.web;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.registry.flow.FlowRegistryClientUserContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.FlowVersionLocation;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.StandardVersionControlInformation;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.registry.flow.VersionedFlowStatus;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.EntityFactory;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.controller.ControllerFacade;
import org.apache.nifi.web.dao.FlowRegistryDAO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.nifi.web.revision.RevisionClaim;
import org.apache.nifi.web.revision.RevisionManager;
import org.apache.nifi.web.revision.UpdateRevisionTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardNiFiServiceFacadeCreateFlowBranchTest {

    private StandardNiFiServiceFacade serviceFacade;

    @Mock
    private ProcessGroupDAO processGroupDAO;

    @Mock
    private FlowRegistryDAO flowRegistryDAO;

    @Mock
    private DtoFactory dtoFactory;

    @Mock
    private EntityFactory entityFactory;

    @Mock
    private ControllerFacade controllerFacade;

    @Mock
    private RevisionManager revisionManager;

    @Mock
    private FlowManager flowManager;

    private static final String PROCESS_GROUP_ID = "pg-1";

    @BeforeEach
    void setUp() {
        serviceFacade = new StandardNiFiServiceFacade();
        serviceFacade.setProcessGroupDAO(processGroupDAO);
        serviceFacade.setFlowRegistryDAO(flowRegistryDAO);
        serviceFacade.setDtoFactory(dtoFactory);
        serviceFacade.setEntityFactory(entityFactory);
        serviceFacade.setControllerFacade(controllerFacade);
        serviceFacade.setRevisionManager(revisionManager);

        lenient().when(controllerFacade.getFlowManager()).thenReturn(flowManager);
        lenient().when(flowManager.getFlowRegistryClient(anyString())).thenReturn(null);

        lenient().when(revisionManager.updateRevision(any(RevisionClaim.class), any(NiFiUser.class), any(UpdateRevisionTask.class)))
                .thenAnswer(invocation -> {
                    final UpdateRevisionTask<?> task = invocation.getArgument(2);
                    return task.update();
                });
        lenient().when(revisionManager.getRevision(anyString())).thenAnswer(invocation -> {
            final String componentId = invocation.getArgument(0, String.class);
            return new Revision(1L, "client-1", componentId);
        });

        final NiFiUser user = new StandardNiFiUser.Builder().identity("unit-test").build();
        final TestingAuthenticationToken authenticationToken = new TestingAuthenticationToken(new NiFiUserDetails(user), null);
        SecurityContextHolder.getContext().setAuthentication(authenticationToken);
    }

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void testCreateFlowBranchSuccess() throws IOException, FlowRegistryException {
        final Revision revision = new Revision(1L, "client-1", PROCESS_GROUP_ID);

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroupDAO.getProcessGroup(PROCESS_GROUP_ID)).thenReturn(processGroup);

        final VersionControlInformation versionControlInformation = mock(VersionControlInformation.class);
        when(processGroup.getVersionControlInformation()).thenReturn(versionControlInformation);
        when(versionControlInformation.getRegistryIdentifier()).thenReturn("registry-1");
        when(versionControlInformation.getBranch()).thenReturn("main");
        when(versionControlInformation.getBucketIdentifier()).thenReturn("bucket-1");
        when(versionControlInformation.getFlowIdentifier()).thenReturn("flow-1");
        when(versionControlInformation.getFlowDescription()).thenReturn("desc");
        when(versionControlInformation.getFlowName()).thenReturn("name");
        when(versionControlInformation.getStorageLocation()).thenReturn("loc");
        when(versionControlInformation.getVersion()).thenReturn("1");

        final VersionedFlowStatus flowStatus = mock(VersionedFlowStatus.class);
        when(versionControlInformation.getStatus()).thenReturn(flowStatus);
        when(flowStatus.getState()).thenReturn(VersionedFlowState.LOCALLY_MODIFIED_AND_STALE);
        when(flowStatus.getStateExplanation()).thenReturn("Up to date");

        final ProcessGroup updatedGroup = processGroup;
        when(processGroupDAO.updateVersionControlInformation(any(VersionControlInformationDTO.class), eq(Collections.emptyMap())))
                .thenReturn(updatedGroup);

        final VersionControlInformationDTO updatedDto = new VersionControlInformationDTO();
        updatedDto.setBranch("feature");
        updatedDto.setRegistryId("registry-1");

        final VersionControlInformationDTO refreshedDto = new VersionControlInformationDTO();
        refreshedDto.setBranch("feature");
        refreshedDto.setRegistryId("registry-1");
        refreshedDto.setState(VersionControlInformationDTO.LOCALLY_MODIFIED);
        refreshedDto.setStateExplanation("Process Group has local modifications");

        when(dtoFactory.createVersionControlInformationDto(updatedGroup)).thenReturn(updatedDto, refreshedDto);

        final VersionControlInformationEntity resultEntity = new VersionControlInformationEntity();
        resultEntity.setVersionControlInformation(refreshedDto);
        when(entityFactory.createVersionControlInformationEntity(eq(refreshedDto), any(RevisionDTO.class))).thenReturn(resultEntity);

        when(dtoFactory.createRevisionDTO(any(FlowModification.class))).thenReturn(new RevisionDTO());

        final FlowRegistryClientNode registryClient = mock(FlowRegistryClientNode.class);
        when(flowManager.getFlowRegistryClient("registry-1")).thenReturn(registryClient);
        final VersionedProcessGroup registrySnapshot = new VersionedProcessGroup();
        final RegisteredFlowSnapshot registeredFlowSnapshot = new RegisteredFlowSnapshot();
        registeredFlowSnapshot.setFlowContents(registrySnapshot);
        final FlowSnapshotContainer snapshotContainer = new FlowSnapshotContainer(registeredFlowSnapshot);
        when(registryClient.getFlowContents(any(), any(FlowVersionLocation.class), eq(false))).thenReturn(snapshotContainer);

        final VersionControlInformationEntity response = serviceFacade.createFlowBranch(revision, PROCESS_GROUP_ID, " feature ", null, null);
        assertEquals(resultEntity, response);

        ArgumentCaptor<FlowVersionLocation> locationCaptor = ArgumentCaptor.forClass(FlowVersionLocation.class);
        verify(flowRegistryDAO).createBranchForUser(any(FlowRegistryClientUserContext.class), eq("registry-1"),
                locationCaptor.capture(), eq("feature"));

        final FlowVersionLocation capturedLocation = locationCaptor.getValue();
        assertEquals("main", capturedLocation.getBranch());
        assertEquals("bucket-1", capturedLocation.getBucketId());
        assertEquals("flow-1", capturedLocation.getFlowId());
        assertEquals("1", capturedLocation.getVersion());

        final ArgumentCaptor<FlowModification> modificationCaptor = ArgumentCaptor.forClass(FlowModification.class);
        verify(dtoFactory).createRevisionDTO(modificationCaptor.capture());
        assertEquals("unit-test", modificationCaptor.getValue().getLastModifier());

        verify(registryClient).getFlowContents(any(), any(FlowVersionLocation.class), eq(false));
        verify(processGroup).setVersionControlInformation(argThat(vci -> vci instanceof StandardVersionControlInformation
                && ((StandardVersionControlInformation) vci).getFlowSnapshot() == registrySnapshot), eq(Collections.emptyMap()));
        verify(processGroup).synchronizeWithFlowRegistry(flowManager);
        verify(entityFactory).createVersionControlInformationEntity(eq(refreshedDto), any(RevisionDTO.class));
        assertEquals(refreshedDto, resultEntity.getVersionControlInformation());
    }

    @Test
    void testCreateFlowBranchSameBranchRejected() {
        final Revision revision = new Revision(1L, "client-1", PROCESS_GROUP_ID);
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroupDAO.getProcessGroup(PROCESS_GROUP_ID)).thenReturn(processGroup);

        final VersionControlInformation versionControlInformation = mock(VersionControlInformation.class);
        when(processGroup.getVersionControlInformation()).thenReturn(versionControlInformation);
        when(versionControlInformation.getBranch()).thenReturn("main");

        assertThrows(IllegalArgumentException.class,
                () -> serviceFacade.createFlowBranch(revision, PROCESS_GROUP_ID, "main", null, null));

        verify(flowRegistryDAO, never()).createBranchForUser(any(), any(), any(), any());
        verify(processGroup, never()).synchronizeWithFlowRegistry(any(FlowManager.class));
    }

    @Test
    void testCreateFlowBranchUnsupportedRegistry() throws IOException, FlowRegistryException {
        final Revision revision = new Revision(1L, "client-1", PROCESS_GROUP_ID);
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroupDAO.getProcessGroup(PROCESS_GROUP_ID)).thenReturn(processGroup);

        final VersionControlInformation versionControlInformation = mock(VersionControlInformation.class);
        when(processGroup.getVersionControlInformation()).thenReturn(versionControlInformation);
        when(versionControlInformation.getRegistryIdentifier()).thenReturn("registry-1");
        when(versionControlInformation.getBranch()).thenReturn("main");
        when(versionControlInformation.getBucketIdentifier()).thenReturn("bucket-1");
        when(versionControlInformation.getFlowIdentifier()).thenReturn("flow-1");
        when(versionControlInformation.getVersion()).thenReturn("1");

        doThrow(new UnsupportedOperationException("not supported"))
                .when(flowRegistryDAO)
                .createBranchForUser(any(FlowRegistryClientUserContext.class), eq("registry-1"), any(FlowVersionLocation.class), eq("feature"));

        assertThrows(IllegalArgumentException.class,
                () -> serviceFacade.createFlowBranch(revision, PROCESS_GROUP_ID, "feature", null, null));

        verify(processGroup, never()).synchronizeWithFlowRegistry(any(FlowManager.class));
    }

    @Test
    void testCreateFlowBranchNotVersionControlled() {
        final Revision revision = new Revision(1L, "client-1", PROCESS_GROUP_ID);
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroupDAO.getProcessGroup(PROCESS_GROUP_ID)).thenReturn(processGroup);
        when(processGroup.getVersionControlInformation()).thenReturn(null);

        assertThrows(IllegalStateException.class,
                () -> serviceFacade.createFlowBranch(revision, PROCESS_GROUP_ID, "feature", null, null));

        verify(flowRegistryDAO, never()).createBranchForUser(any(), any(), any(), any());
        verify(processGroup, never()).synchronizeWithFlowRegistry(any(FlowManager.class));
    }

    @Test
    void testCreateFlowBranchPropagatesRegistryErrors() throws IOException, FlowRegistryException {
        final Revision revision = new Revision(1L, "client-1", PROCESS_GROUP_ID);
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroupDAO.getProcessGroup(PROCESS_GROUP_ID)).thenReturn(processGroup);

        final VersionControlInformation versionControlInformation = mock(VersionControlInformation.class);
        when(processGroup.getVersionControlInformation()).thenReturn(versionControlInformation);
        when(versionControlInformation.getRegistryIdentifier()).thenReturn("registry-1");
        when(versionControlInformation.getBranch()).thenReturn("main");
        when(versionControlInformation.getBucketIdentifier()).thenReturn("bucket-1");
        when(versionControlInformation.getFlowIdentifier()).thenReturn("flow-1");
        when(versionControlInformation.getVersion()).thenReturn("1");

        final FlowRegistryException cause = new FlowRegistryException("Branch [feature] already exists");
        doThrow(new NiFiCoreException("Unable to create branch [feature] in registry with ID registry-1", cause))
                .when(flowRegistryDAO)
                .createBranchForUser(any(FlowRegistryClientUserContext.class), eq("registry-1"), any(FlowVersionLocation.class), eq("feature"));

        final NiFiCoreException exception = assertThrows(NiFiCoreException.class,
                () -> serviceFacade.createFlowBranch(revision, PROCESS_GROUP_ID, "feature", null, null));

        assertTrue(exception.getMessage().contains("registry-1"));
        assertTrue(exception.getMessage().contains("[feature]"));
        assertEquals(cause, exception.getCause());

        verify(processGroup, never()).synchronizeWithFlowRegistry(any(FlowManager.class));
    }
}
