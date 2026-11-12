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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizeAccess;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.ProcessGroupAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.manager.exception.IllegalClusterStateException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ProcessGroupDrainRequestEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupRecursivity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestProcessGroupResource {

    @InjectMocks
    private ProcessGroupResource processGroupResource;

    @Mock
    private NiFiServiceFacade serviceFacade;

    @Test
    public void testExportProcessGroup(@Mock RegisteredFlowSnapshot versionedFlowSnapshot, @Mock VersionedProcessGroup versionedProcessGroup) {
        final String groupId = UUID.randomUUID().toString();
        when(serviceFacade.getCurrentFlowSnapshotByGroupId(groupId)).thenReturn(versionedFlowSnapshot);
        when(versionedFlowSnapshot.getFlowContents()).thenReturn(versionedProcessGroup);
        when(versionedProcessGroup.getName()).thenReturn("flowname");

        try (Response response = processGroupResource.exportProcessGroup(groupId, false, false)) {
            assertEquals(200, response.getStatus());
            assertEquals(versionedFlowSnapshot, response.getEntity());
        }
    }

    @Test
    public void testUpdateProcessGroupNotExecuted_WhenUserNotAuthorized(@Mock HttpServletRequest httpServletRequest, @Mock NiFiProperties properties) {
        when(httpServletRequest.getHeader(any())).thenReturn(null);
        when(properties.isNode()).thenReturn(Boolean.FALSE);

        processGroupResource.properties = properties;
        processGroupResource.serviceFacade = serviceFacade;
        processGroupResource.httpServletRequest = httpServletRequest;

        final ProcessGroupEntity processGroupEntity = new ProcessGroupEntity();
        final ProcessGroupDTO groupDTO = new ProcessGroupDTO();
        groupDTO.setId("id");
        groupDTO.setName("name");
        final RevisionDTO revisionDTO = new RevisionDTO();
        revisionDTO.setClientId("clientId");
        revisionDTO.setVersion(1L);

        processGroupEntity.setRevision(revisionDTO);
        processGroupEntity.setProcessGroupUpdateStrategy(ProcessGroupRecursivity.DIRECT_CHILDREN.name());
        processGroupEntity.setComponent(groupDTO);

        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () ->
                processGroupResource.updateProcessGroup("id", processGroupEntity));

        verify(serviceFacade, never()).verifyUpdateProcessGroup(any());
        verify(serviceFacade, never()).updateProcessGroup(any(), any());
    }

    @Test
    public void testProcessGroupDrainRequests(@Mock HttpServletRequest httpServletRequest, @Mock NiFiProperties properties, @Mock ClusterCoordinator clusterCoordinator,
                                              @Mock NodeIdentifier disconnectedNode) {
        when(httpServletRequest.getHeader(any())).thenReturn(null);
        when(properties.isNode()).thenReturn(Boolean.FALSE);
        when(clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTING, NodeConnectionState.DISCONNECTED, NodeConnectionState.DISCONNECTING))
                .thenReturn(Set.of(disconnectedNode));

        processGroupResource.properties = properties;
        processGroupResource.serviceFacade = serviceFacade;
        processGroupResource.httpServletRequest = httpServletRequest;
        processGroupResource.setClusterCoordinator(clusterCoordinator);

        try {
            processGroupResource.createProcessGroupDrainRequest("id");
            fail();
        } catch (final IllegalClusterStateException expected) {
        }

        verify(serviceFacade).verifyBeginProcessGroupDrain("id");
        verify(serviceFacade, never()).beginProcessGroupDrain(any(), any());

        // Beginning a drain requires WRITE on each referenced Controller Service that the drain enables and READ on each one that is already enabled
        final ArgumentCaptor<AuthorizeAccess> authorizeCaptor = ArgumentCaptor.forClass(AuthorizeAccess.class);
        verify(serviceFacade).authorizeAccess(authorizeCaptor.capture());
        final AuthorizeAccess authorizeDrain = authorizeCaptor.getValue();

        final ProcessGroup group = mock(ProcessGroup.class);
        final ProcessGroupAuthorizable groupAuthorizable = mock(ProcessGroupAuthorizable.class);
        when(groupAuthorizable.getAuthorizable()).thenReturn(mock(Authorizable.class));
        when(groupAuthorizable.getProcessGroup()).thenReturn(group);
        final AuthorizableLookup lookup = mock(AuthorizableLookup.class);
        when(lookup.getProcessGroup("id")).thenReturn(groupAuthorizable);

        final Authorizable enabledServiceAuthorizable = mock(Authorizable.class);
        final ControllerServiceNode enabledService = createService("enabled-service", true, enabledServiceAuthorizable, lookup);
        when(group.findControllerServicesForDrain()).thenReturn(Set.of(enabledService));
        authorizeDrain.authorize(lookup);
        verify(enabledServiceAuthorizable).authorize(any(), eq(RequestAction.READ), any());
        verify(enabledServiceAuthorizable, never()).authorize(any(), eq(RequestAction.WRITE), any());

        final Authorizable disabledServiceAuthorizable = mock(Authorizable.class);
        doThrow(AccessDeniedException.class).when(disabledServiceAuthorizable).authorize(any(), eq(RequestAction.WRITE), any());
        final ControllerServiceNode disabledService = createService("disabled-service", false, disabledServiceAuthorizable, lookup);
        when(group.findControllerServicesForDrain()).thenReturn(Set.of(enabledService, disabledService));
        try {
            authorizeDrain.authorize(lookup);
            fail();
        } catch (final AccessDeniedException expected) {
        }

        // A drain request that no longer exists is reported as an empty entity so that the responses of cluster nodes can be merged
        when(serviceFacade.getProcessGroupDrainRequest("id", "drain-1")).thenReturn(Optional.empty());
        try (Response response = processGroupResource.getProcessGroupDrainRequest("id", "drain-1")) {
            assertEquals(200, response.getStatus());
            assertNull(((ProcessGroupDrainRequestEntity) response.getEntity()).getDrainRequest());
        }

        when(serviceFacade.endProcessGroupDrain("id", "drain-1")).thenReturn(Optional.empty());
        try (Response response = processGroupResource.removeProcessGroupDrainRequest("id", "drain-1")) {
            assertEquals(200, response.getStatus());
            assertNull(((ProcessGroupDrainRequestEntity) response.getEntity()).getDrainRequest());
        }
    }

    private static ControllerServiceNode createService(final String id, final boolean active, final Authorizable authorizable, final AuthorizableLookup lookup) {
        final ControllerServiceNode service = mock(ControllerServiceNode.class);
        when(service.getIdentifier()).thenReturn(id);
        when(service.isActive()).thenReturn(active);
        final ComponentAuthorizable serviceComponent = mock(ComponentAuthorizable.class);
        when(serviceComponent.getAuthorizable()).thenReturn(authorizable);
        when(lookup.getControllerService(id)).thenReturn(serviceComponent);
        return service;
    }
}
