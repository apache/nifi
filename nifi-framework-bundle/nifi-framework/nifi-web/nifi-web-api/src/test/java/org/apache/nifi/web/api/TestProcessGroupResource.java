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

import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizeAccess;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupRecursivity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Response;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
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

        try(Response response = processGroupResource.exportProcessGroup(groupId, false)) {
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
}