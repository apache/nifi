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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser.Builder;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.flow.ExternalControllerServiceReference;
import org.apache.nifi.registry.flow.RestBasedFlowRegistry;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedParameterContext;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedProcessGroup;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.EntityFactory;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.action.HistoryDTO;
import org.apache.nifi.web.api.dto.action.HistoryQueryDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.entity.ActionEntity;
import org.apache.nifi.web.api.entity.StatusHistoryEntity;
import org.apache.nifi.web.controller.ControllerFacade;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.nifi.web.dao.RemoteProcessGroupDAO;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.Arrays;
import java.util.List;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StandardNiFiServiceFacadeTest {

    private static final String USER_1 = "user-1";
    private static final String USER_2 = "user-2";

    private static final Integer UNKNOWN_ACTION_ID = 0;

    private static final Integer ACTION_ID_1 = 1;
    private static final String PROCESSOR_ID_1 = "processor-1";

    private static final Integer ACTION_ID_2 = 2;
    private static final String PROCESSOR_ID_2 = "processor-2";

    private StandardNiFiServiceFacade serviceFacade;
    private Authorizer authorizer;
    private FlowController flowController;
    private ProcessGroupDAO processGroupDAO;

    @Before
    public void setUp() throws Exception {
        // audit service
        final AuditService auditService = mock(AuditService.class);
        when(auditService.getAction(anyInt())).then(invocation -> {
            final Integer actionId = invocation.getArgument(0);

            FlowChangeAction action = null;
            if (ACTION_ID_1.equals(actionId)) {
                action = getAction(actionId, PROCESSOR_ID_1);
            } else if (ACTION_ID_2.equals(actionId)) {
                action = getAction(actionId, PROCESSOR_ID_2);
            }

            return action;
        });
        when(auditService.getActions(any(HistoryQuery.class))).then(invocation -> {
            final History history = new History();
            history.setActions(Arrays.asList(getAction(ACTION_ID_1, PROCESSOR_ID_1), getAction(ACTION_ID_2, PROCESSOR_ID_2)));
            return history;
        });


        // authorizable lookup
        final AuthorizableLookup authorizableLookup = mock(AuthorizableLookup.class);
        when(authorizableLookup.getProcessor(Mockito.anyString())).then(getProcessorInvocation -> {
            final String processorId = getProcessorInvocation.getArgument(0);

            // processor-2 is no longer part of the flow
            if (processorId.equals(PROCESSOR_ID_2)) {
                throw new ResourceNotFoundException("");
            }

            // component authorizable
            final ComponentAuthorizable componentAuthorizable = mock(ComponentAuthorizable.class);
            when(componentAuthorizable.getAuthorizable()).then(getAuthorizableInvocation -> {

                // authorizable
                final Authorizable authorizable = new Authorizable() {
                    @Override
                    public Authorizable getParentAuthorizable() {
                        return null;
                    }

                    @Override
                    public Resource getResource() {
                        return ResourceFactory.getComponentResource(ResourceType.Processor, processorId, processorId);
                    }
                };

                return authorizable;
            });

            return componentAuthorizable;
        });

        // authorizer
        authorizer = mock(Authorizer.class);
        when(authorizer.authorize(any(AuthorizationRequest.class))).then(invocation -> {
            final AuthorizationRequest request = invocation.getArgument(0);

            AuthorizationResult result = AuthorizationResult.denied();
            if (request.getResource().getIdentifier().endsWith(PROCESSOR_ID_1)) {
                if (USER_1.equals(request.getIdentity())) {
                    result = AuthorizationResult.approved();
                }
            } else if (request.getResource().equals(ResourceFactory.getControllerResource())) {
                if (USER_2.equals(request.getIdentity())) {
                    result = AuthorizationResult.approved();
                }
            }

            return result;
        });

        // flow controller
        flowController = mock(FlowController.class);
        when(flowController.getResource()).thenCallRealMethod();
        when(flowController.getParentAuthorizable()).thenCallRealMethod();

        // controller facade
        final ControllerFacade controllerFacade = new ControllerFacade();
        controllerFacade.setFlowController(flowController);

        processGroupDAO = mock(ProcessGroupDAO.class, Answers.RETURNS_DEEP_STUBS);

        serviceFacade = new StandardNiFiServiceFacade();
        serviceFacade.setAuditService(auditService);
        serviceFacade.setAuthorizableLookup(authorizableLookup);
        serviceFacade.setAuthorizer(authorizer);
        serviceFacade.setEntityFactory(new EntityFactory());
        serviceFacade.setDtoFactory(new DtoFactory());
        serviceFacade.setControllerFacade(controllerFacade);
        serviceFacade.setProcessGroupDAO(processGroupDAO);

    }

    private FlowChangeAction getAction(final Integer actionId, final String processorId) {
        final FlowChangeAction action = new FlowChangeAction();
        action.setId(actionId);
        action.setSourceId(processorId);
        action.setSourceType(Component.Processor);
        action.setOperation(Operation.Add);
        return action;
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testGetUnknownAction() throws Exception {
        serviceFacade.getAction(UNKNOWN_ACTION_ID);
    }

    @Test
    public void testGetActionApprovedThroughAction() throws Exception {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_1).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        // get the action
        final ActionEntity entity = serviceFacade.getAction(ACTION_ID_1);

        // verify
        assertEquals(ACTION_ID_1, entity.getId());
        assertTrue(entity.getCanRead());

        // resource exists and is approved, no need to check the controller
        verify(authorizer, times(1)).authorize(argThat(o -> o.getResource().getIdentifier().endsWith(PROCESSOR_ID_1)));
        verify(authorizer, times(0)).authorize(argThat(o -> o.getResource().equals(ResourceFactory.getControllerResource())));
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetActionDeniedDespiteControllerAccess() throws Exception {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_2).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        try {
            // get the action
            serviceFacade.getAction(ACTION_ID_1);
            fail();
        } finally {
            // resource exists, but should trigger access denied and will not check the controller
            verify(authorizer, times(1)).authorize(argThat(o -> o.getResource().getIdentifier().endsWith(PROCESSOR_ID_1)));
            verify(authorizer, times(0)).authorize(argThat(o -> o.getResource().equals(ResourceFactory.getControllerResource())));
        }
    }

    @Test
    public void testGetStatusHistory() {
        // given
        final Date generated = new Date();
        final StatusHistoryDTO dto = new StatusHistoryDTO();
        dto.setGenerated(generated);
        final ControllerFacade controllerFacade = mock(ControllerFacade.class);
        Mockito.when(controllerFacade.getNodeStatusHistory()).thenReturn(dto);
        serviceFacade.setControllerFacade(controllerFacade);

        // when
        final StatusHistoryEntity result = serviceFacade.getNodeStatusHistory();

        // then
        Mockito.verify(controllerFacade).getNodeStatusHistory();
        Assert.assertNotNull(result);
        Assert.assertEquals(generated, result.getStatusHistory().getGenerated());
    }

    @Test
    public void testGetActionApprovedThroughController() throws Exception {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_2).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        // get the action
        final ActionEntity entity = serviceFacade.getAction(ACTION_ID_2);

        // verify
        assertEquals(ACTION_ID_2, entity.getId());
        assertTrue(entity.getCanRead());

        // component does not exists, so only checks against the controller
        verify(authorizer, times(0)).authorize(argThat(o -> o.getResource().getIdentifier().endsWith(PROCESSOR_ID_2)));
        verify(authorizer, times(1)).authorize(argThat(o -> o.getResource().equals(ResourceFactory.getControllerResource())));
    }

    @Test
    public void testGetActionsForUser1() throws Exception {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_1).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        final HistoryDTO dto = serviceFacade.getActions(new HistoryQueryDTO());

        // verify user 1 only has access to actions for processor 1
        dto.getActions().forEach(action -> {
            if (PROCESSOR_ID_1.equals(action.getSourceId())) {
                assertTrue(action.getCanRead());
            } else if (PROCESSOR_ID_2.equals(action.getSourceId())) {
                assertFalse(action.getCanRead());
                assertNull(action.getAction());
            }
        });
    }

    @Test
    public void testGetActionsForUser2() throws Exception {
        // set the user
        final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(USER_2).build()));
        SecurityContextHolder.getContext().setAuthentication(authentication);

        final HistoryDTO  dto = serviceFacade.getActions(new HistoryQueryDTO());

        // verify user 2 only has access to actions for processor 2
        dto.getActions().forEach(action -> {
            if (PROCESSOR_ID_1.equals(action.getSourceId())) {
                assertFalse(action.getCanRead());
                assertNull(action.getAction());
            } else if (PROCESSOR_ID_2.equals(action.getSourceId())) {
                assertTrue(action.getCanRead());
            }
        });
    }

    @Test
    public void testGetCurrentFlowSnapshotByGroupId() {
        final String groupId = UUID.randomUUID().toString();
        final ProcessGroup processGroup = mock(ProcessGroup.class);

        when(processGroupDAO.getProcessGroup(groupId)).thenReturn(processGroup);

        final FlowManager flowManager = mock(FlowManager.class);
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        when(flowController.getFlowManager()).thenReturn(flowManager);
        when(flowController.getExtensionManager()).thenReturn(extensionManager);

        final ControllerServiceProvider controllerServiceProvider = mock(ControllerServiceProvider.class);
        when(flowController.getControllerServiceProvider()).thenReturn(controllerServiceProvider);

        final VersionControlInformation versionControlInformation = mock(VersionControlInformation.class);
        when(processGroup.getVersionControlInformation()).thenReturn(versionControlInformation);

        // use spy to mock the make() method for generating a new flow mapper to make this testable
        final StandardNiFiServiceFacade serviceFacadeSpy = spy(serviceFacade);
        final NiFiRegistryFlowMapper flowMapper = mock(NiFiRegistryFlowMapper.class);
        when(serviceFacadeSpy.makeNiFiRegistryFlowMapper(extensionManager)).thenReturn(flowMapper);

        final InstantiatedVersionedProcessGroup nonVersionedProcessGroup = mock(InstantiatedVersionedProcessGroup.class);
        when(flowMapper.mapNonVersionedProcessGroup(processGroup, controllerServiceProvider)).thenReturn(nonVersionedProcessGroup);

        final String parameterName = "foo";
        final VersionedParameterContext versionedParameterContext = mock(VersionedParameterContext.class);
        when(versionedParameterContext.getName()).thenReturn(parameterName);
        final Map<String, VersionedParameterContext> parameterContexts = Maps.newHashMap();
        parameterContexts.put(parameterName, versionedParameterContext);
        when(flowMapper.mapParameterContexts(processGroup, true)).thenReturn(parameterContexts);

        final ExternalControllerServiceReference externalControllerServiceReference = mock(ExternalControllerServiceReference.class);
        final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences = Maps.newHashMap();
        externalControllerServiceReferences.put("test", externalControllerServiceReference);
        when(nonVersionedProcessGroup.getExternalControllerServiceReferences()).thenReturn(externalControllerServiceReferences);

        final VersionedFlowSnapshot versionedFlowSnapshot = serviceFacadeSpy.getCurrentFlowSnapshotByGroupId(groupId);

        assertEquals(nonVersionedProcessGroup, versionedFlowSnapshot.getFlowContents());
        assertEquals(1, versionedFlowSnapshot.getParameterContexts().size());
        assertEquals(versionedParameterContext, versionedFlowSnapshot.getParameterContexts().get(parameterName));
        assertEquals(externalControllerServiceReferences, versionedFlowSnapshot.getExternalControllerServices());
        assertEquals(RestBasedFlowRegistry.FLOW_ENCODING_VERSION, versionedFlowSnapshot.getFlowEncodingVersion());
        assertNull(versionedFlowSnapshot.getFlow());
        assertNull(versionedFlowSnapshot.getBucket());
        assertNull(versionedFlowSnapshot.getSnapshotMetadata());
    }

    @Test
    public void testIsAnyProcessGroupUnderVersionControl_None() {
        final String groupId = UUID.randomUUID().toString();
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final ProcessGroup childProcessGroup = mock(ProcessGroup.class);

        when(processGroupDAO.getProcessGroup(groupId)).thenReturn(processGroup);

        when(processGroup.getVersionControlInformation()).thenReturn(null);
        when(processGroup.getProcessGroups()).thenReturn(Sets.newHashSet(childProcessGroup));
        when(childProcessGroup.getVersionControlInformation()).thenReturn(null);

        assertFalse(serviceFacade.isAnyProcessGroupUnderVersionControl(groupId));
    }

    @Test
    public void testIsAnyProcessGroupUnderVersionControl_PrimaryGroup() {
        final String groupId = UUID.randomUUID().toString();
        final ProcessGroup processGroup = mock(ProcessGroup.class);

        when(processGroupDAO.getProcessGroup(groupId)).thenReturn(processGroup);

        final VersionControlInformation vci = mock(VersionControlInformation.class);
        when(processGroup.getVersionControlInformation()).thenReturn(vci);
        when(processGroup.getProcessGroups()).thenReturn(Sets.newHashSet());

        assertTrue(serviceFacade.isAnyProcessGroupUnderVersionControl(groupId));
    }

    @Test
    public void testIsAnyProcessGroupUnderVersionControl_ChildGroup() {
        final String groupId = UUID.randomUUID().toString();
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final ProcessGroup childProcessGroup = mock(ProcessGroup.class);

        when(processGroupDAO.getProcessGroup(groupId)).thenReturn(processGroup);

        final VersionControlInformation vci = mock(VersionControlInformation.class);
        when(processGroup.getVersionControlInformation()).thenReturn(null);
        when(processGroup.getProcessGroups()).thenReturn(Sets.newHashSet(childProcessGroup));
        when(childProcessGroup.getVersionControlInformation()).thenReturn(vci);

        assertTrue(serviceFacade.isAnyProcessGroupUnderVersionControl(groupId));
    }

    @Test
    public void testVerifyUpdateRemoteProcessGroups() throws Exception {
        // GIVEN
        RemoteProcessGroupDAO remoteProcessGroupDAO = mock(RemoteProcessGroupDAO.class);
        serviceFacade.setRemoteProcessGroupDAO(remoteProcessGroupDAO);

        String groupId = "groupId";
        boolean shouldTransmit = true;

        String remoteProcessGroupId1 = "remoteProcessGroupId1";
        String remoteProcessGroupId2 = "remoteProcessGroupId2";

        List<RemoteProcessGroup> remoteProcessGroups = Arrays.asList(
            // Current 'transmitting' status should not influence the verification, which should be solely based on the 'shouldTransmitting' value
            mockRemoteProcessGroup(remoteProcessGroupId1, true),
            mockRemoteProcessGroup(remoteProcessGroupId2, false)
        );

        List<RemoteProcessGroupDTO> expected = Arrays.asList(
            createRemoteProcessGroupDTO(remoteProcessGroupId1, shouldTransmit),
            createRemoteProcessGroupDTO(remoteProcessGroupId2, shouldTransmit)
        );

        when(processGroupDAO.getProcessGroup(groupId).findAllRemoteProcessGroups()).thenReturn(remoteProcessGroups);
        expected.stream()
            .map(RemoteProcessGroupDTO::getId)
            .forEach(remoteProcessGroupId -> when(remoteProcessGroupDAO.hasRemoteProcessGroup(remoteProcessGroupId)).thenReturn(true));


        // WHEN
        serviceFacade.verifyUpdateRemoteProcessGroups(groupId, shouldTransmit);

        // THEN
        ArgumentCaptor<RemoteProcessGroupDTO> remoteProcessGroupDTOArgumentCaptor = ArgumentCaptor.forClass(RemoteProcessGroupDTO.class);

        verify(remoteProcessGroupDAO, times(remoteProcessGroups.size())).verifyUpdate(remoteProcessGroupDTOArgumentCaptor.capture());

        List<RemoteProcessGroupDTO> actual = remoteProcessGroupDTOArgumentCaptor.getAllValues();

        assertEquals(toMap(expected), toMap(actual));
    }

    private Map<String, Boolean> toMap(List<RemoteProcessGroupDTO> list) {
        return list.stream().collect(Collectors.toMap(RemoteProcessGroupDTO::getId, RemoteProcessGroupDTO::isTransmitting));
    }

    private RemoteProcessGroup mockRemoteProcessGroup(String identifier, boolean transmitting) {
        RemoteProcessGroup remoteProcessGroup = mock(RemoteProcessGroup.class);

        when(remoteProcessGroup.getIdentifier()).thenReturn(identifier);
        when(remoteProcessGroup.isTransmitting()).thenReturn(transmitting);

        return remoteProcessGroup;
    }

    private RemoteProcessGroupDTO createRemoteProcessGroupDTO(String id, boolean transmitting) {
        RemoteProcessGroupDTO remoteProcessGroup = new RemoteProcessGroupDTO();

        remoteProcessGroup.setId(id);
        remoteProcessGroup.setTransmitting(transmitting);

        return remoteProcessGroup;
    }
}