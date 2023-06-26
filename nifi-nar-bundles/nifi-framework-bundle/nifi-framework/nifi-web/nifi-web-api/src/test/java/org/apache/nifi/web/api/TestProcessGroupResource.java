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
import org.apache.nifi.web.api.entity.ProcessGroupUpdateStrategy;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.entity.TemplateEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.core.UriInfo;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestProcessGroupResource {

    @InjectMocks
    private ProcessGroupResource processGroupResource;

    @Mock
    private NiFiServiceFacade serviceFacade;

    @Mock
    private NiFiProperties properties;

    @Mock
    private HttpServletRequest httpServletRequest;

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

    /** This test creates a malformed template upload request to exercise error handling and sanitization */
    @Test
    public void testUploadShouldHandleMalformedTemplate(@Mock HttpServletRequest request, @Mock UriInfo uriInfo) throws Exception {
        final String templateWithXssPlain = "<?xml version=\"1.0\" encoding='><script xmlns=\"http://www.w3.org/1999/xhtml\">alert(JSON.stringify(localstorage));</script><errorResponse test='?>";
        Response response = processGroupResource.uploadTemplate(request, uriInfo, "1",
                false, new ByteArrayInputStream(templateWithXssPlain.getBytes()));

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        assertFalse(Pattern.compile("<script.*>").matcher(response.getEntity().toString()).find());
    }

    /** This test creates a malformed template import request to exercise error handling and sanitization */
    @Test
    public void testImportShouldHandleMalformedTemplate(@Mock NiFiProperties niFiProperties, @Mock TemplateResource templateResource,
                                                        @Mock TemplateDTO mockIAETemplate, @Mock TemplateDTO mockExceptionTemplate,
                                                        @Mock TemplateEntity mockIAETemplateEntity, @Mock TemplateEntity mockExceptionTemplateEntity,
                                                        @Mock HttpServletRequest mockRequest) {
        when(niFiProperties.isNode()).thenReturn(false);
        when(serviceFacade.importTemplate(any(TemplateDTO.class), anyString(), any())).thenAnswer((Answer<TemplateDTO>) invocationOnMock -> invocationOnMock.getArgument(0));
        when(mockIAETemplate.getName()).thenReturn("mockIAETemplate");
        when(mockIAETemplate.getUri()).thenThrow(new IllegalArgumentException("Expected exception with <script> element"));
        when(mockIAETemplate.getSnippet()).thenReturn(new FlowSnippetDTO());
        when(mockExceptionTemplate.getName()).thenReturn("mockExceptionTemplate");
        when(mockExceptionTemplate.getUri()).thenThrow(new RuntimeException("Expected exception with <script> element"));
        when(mockExceptionTemplate.getSnippet()).thenReturn(new FlowSnippetDTO());
        when(mockIAETemplateEntity.getTemplate()).thenReturn(mockIAETemplate);
        when(mockExceptionTemplateEntity.getTemplate()).thenReturn(mockExceptionTemplate);

        processGroupResource.properties = niFiProperties;
        processGroupResource.serviceFacade = serviceFacade;
        processGroupResource.setTemplateResource(templateResource);
        processGroupResource.httpServletRequest = mockRequest;

        List<Response> responses = Stream.of(mockIAETemplateEntity, mockExceptionTemplateEntity)
                .map(templateEntity -> processGroupResource.importTemplate(mockRequest, "1", templateEntity))
                .collect(Collectors.toList());

        responses.forEach(response -> {
            assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
            assertFalse(Pattern.compile("<script.*>").matcher(response.getEntity().toString()).find());
        });
    }
    @Test
    public void testUpdateProcessGroupNotExecuted_WhenUserNotAuthorized() {
        when(httpServletRequest.getHeader(any())).thenReturn(null);
        when(properties.isNode()).thenReturn(Boolean.FALSE);

        final ProcessGroupEntity processGroupEntity = new ProcessGroupEntity();
        final ProcessGroupDTO groupDTO = new ProcessGroupDTO();
        groupDTO.setId("id");
        groupDTO.setName("name");
        final RevisionDTO revisionDTO = new RevisionDTO();
        revisionDTO.setClientId("clientId");
        revisionDTO.setVersion(1L);

        processGroupEntity.setRevision(revisionDTO);
        processGroupEntity.setProcessGroupUpdateStrategy(ProcessGroupUpdateStrategy.UPDATE_PROCESS_GROUP_ONLY.name());
        processGroupEntity.setComponent(groupDTO);

        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () ->
                processGroupResource.updateProcessGroup(httpServletRequest, "id", processGroupEntity));

        verify(serviceFacade, never()).verifyUpdateProcessGroup(any());
        verify(serviceFacade, never()).updateProcessGroup(any(), any());
    }
}