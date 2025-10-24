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

import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizeAccess;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.AllowableValueDTO;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AllowableValueEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ConnectorPropertyAllowableValuesEntity;
import org.apache.nifi.web.api.entity.ConnectorRunStatusEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestConnectorResource {

    @InjectMocks
    private ConnectorResource connectorResource;

    @Mock
    private NiFiServiceFacade serviceFacade;

    @Mock
    private Authorizer authorizer;

    @Mock
    private HttpServletRequest httpServletRequest;

    @Mock
    private ServletContext servletContext;

    @Mock
    private NiFiProperties properties;

    @Mock
    private UriInfo uriInfo;

    @Mock
    private UriBuilder uriBuilder;

    private static final String CONNECTOR_ID = "test-connector-id";
    private static final String CONNECTOR_NAME = "Test Connector";
    private static final String CONNECTOR_TYPE = "TestConnectorType";
    private static final String CONFIGURATION_STEP_NAME = "test-step";
    private static final String PROPERTY_GROUP_NAME = "test-group";
    private static final String PROPERTY_NAME = "test-property";

    @BeforeEach
    public void setUp() throws Exception {
        lenient().when(httpServletRequest.getHeader(any())).thenReturn(null);
        lenient().when(httpServletRequest.getServletContext()).thenReturn(servletContext);
        lenient().when(httpServletRequest.getContextPath()).thenReturn("/nifi-api");
        lenient().when(httpServletRequest.getScheme()).thenReturn("http");
        lenient().when(httpServletRequest.getServerName()).thenReturn("localhost");
        lenient().when(httpServletRequest.getServerPort()).thenReturn(8080);
        lenient().when(servletContext.getInitParameter(any())).thenReturn(null);
        lenient().when(properties.isNode()).thenReturn(Boolean.FALSE);

        lenient().when(uriInfo.getBaseUriBuilder()).thenReturn(uriBuilder);
        lenient().when(uriBuilder.segment(any(String[].class))).thenReturn(uriBuilder);
        lenient().when(uriBuilder.build()).thenReturn(new URI("http://localhost:8080/nifi-api/connectors/" + CONNECTOR_ID));

        connectorResource.setServiceFacade(serviceFacade);
        connectorResource.httpServletRequest = httpServletRequest;
        connectorResource.properties = properties;
        connectorResource.uriInfo = uriInfo;
    }

    @Test
    public void testGetConnector() {
        final ConnectorEntity connectorEntity = createConnectorEntity();

        when(serviceFacade.getConnector(CONNECTOR_ID)).thenReturn(connectorEntity);

        try (Response response = connectorResource.getConnector(CONNECTOR_ID)) {
            assertEquals(200, response.getStatus());
            assertEquals(connectorEntity, response.getEntity());
        }

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade).getConnector(CONNECTOR_ID);
    }

    @Test
    public void testGetConnectorNotAuthorized() {
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () -> connectorResource.getConnector(CONNECTOR_ID));

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade, never()).getConnector(anyString());
    }

    @Test
    public void testUpdateConnector() {
        final ConnectorEntity requestEntity = createConnectorEntity();
        final ConnectorEntity responseEntity = createConnectorEntity();

        when(serviceFacade.updateConnector(any(Revision.class), any(ConnectorDTO.class))).thenReturn(responseEntity);

        try (Response response = connectorResource.updateConnector(CONNECTOR_ID, requestEntity)) {
            assertEquals(200, response.getStatus());
            assertEquals(responseEntity, response.getEntity());
        }

        verify(serviceFacade).verifyUpdateConnector(any(ConnectorDTO.class));
        verify(serviceFacade).updateConnector(any(Revision.class), any(ConnectorDTO.class));
    }

    @Test
    public void testUpdateConnectorNotAuthorized() {
        final ConnectorEntity requestEntity = createConnectorEntity();

        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () -> connectorResource.updateConnector(CONNECTOR_ID, requestEntity));

        verify(serviceFacade, never()).verifyUpdateConnector(any(ConnectorDTO.class));
        verify(serviceFacade, never()).updateConnector(any(Revision.class), any(ConnectorDTO.class));
    }

    @Test
    public void testUpdateConnectorWithMismatchedId() {
        final ConnectorEntity requestEntity = createConnectorEntity();
        requestEntity.getComponent().setId("different-id");

        assertThrows(IllegalArgumentException.class, () ->
            connectorResource.updateConnector(CONNECTOR_ID, requestEntity));

        verify(serviceFacade, never()).updateConnector(any(Revision.class), any(ConnectorDTO.class));
    }

    @Test
    public void testUpdateConnectorWithNullEntity() {
        assertThrows(IllegalArgumentException.class, () ->
            connectorResource.updateConnector(CONNECTOR_ID, null));

        verify(serviceFacade, never()).updateConnector(any(Revision.class), any(ConnectorDTO.class));
    }

    @Test
    public void testUpdateConnectorWithNullComponent() {
        final ConnectorEntity requestEntity = new ConnectorEntity();
        requestEntity.setComponent(null);

        assertThrows(IllegalArgumentException.class, () ->
            connectorResource.updateConnector(CONNECTOR_ID, requestEntity));

        verify(serviceFacade, never()).updateConnector(any(Revision.class), any(ConnectorDTO.class));
    }

    @Test
    public void testUpdateConnectorWithNullRevision() {
        final ConnectorEntity requestEntity = createConnectorEntity();
        requestEntity.setRevision(null);

        assertThrows(IllegalArgumentException.class, () ->
            connectorResource.updateConnector(CONNECTOR_ID, requestEntity));

        verify(serviceFacade, never()).updateConnector(any(Revision.class), any(ConnectorDTO.class));
    }

    @Test
    public void testDeleteConnector() {
        final ConnectorEntity responseEntity = createConnectorEntity();

        when(serviceFacade.deleteConnector(any(Revision.class), eq(CONNECTOR_ID))).thenReturn(responseEntity);

        try (Response response = connectorResource.deleteConnector(new LongParameter("1"), new ClientIdParameter("client-id"), false, CONNECTOR_ID)) {
            assertEquals(200, response.getStatus());
            assertEquals(responseEntity, response.getEntity());
        }

        verify(serviceFacade).verifyDeleteConnector(CONNECTOR_ID);
        verify(serviceFacade).deleteConnector(any(Revision.class), eq(CONNECTOR_ID));
    }

    @Test
    public void testDeleteConnectorNotAuthorized() {
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () ->
            connectorResource.deleteConnector(new LongParameter("1"), new ClientIdParameter("client-id"), false, CONNECTOR_ID));

        verify(serviceFacade, never()).verifyDeleteConnector(anyString());
        verify(serviceFacade, never()).deleteConnector(any(Revision.class), anyString());
    }

    @Test
    public void testUpdateRunStatus() {
        final ConnectorRunStatusEntity requestEntity = createConnectorRunStatusEntity();
        final ConnectorEntity responseEntity = createConnectorEntity();

        when(serviceFacade.scheduleConnector(any(Revision.class), eq(CONNECTOR_ID), eq(ScheduledState.RUNNING)))
            .thenReturn(responseEntity);

        try (Response response = connectorResource.updateRunStatus(CONNECTOR_ID, requestEntity)) {
            assertEquals(200, response.getStatus());
            assertEquals(responseEntity, response.getEntity());
        }

        verify(serviceFacade).verifyUpdateConnector(any(ConnectorDTO.class));
        verify(serviceFacade).scheduleConnector(any(Revision.class), eq(CONNECTOR_ID), eq(ScheduledState.RUNNING));
    }

    @Test
    public void testUpdateRunStatusNotAuthorized() {
        final ConnectorRunStatusEntity requestEntity = createConnectorRunStatusEntity();

        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () ->
            connectorResource.updateRunStatus(CONNECTOR_ID, requestEntity));

        verify(serviceFacade, never()).verifyUpdateConnector(any(ConnectorDTO.class));
        verify(serviceFacade, never()).scheduleConnector(any(Revision.class), anyString(), any(ScheduledState.class));
    }

    @Test
    public void testUpdateRunStatusWithNullEntity() {
        assertThrows(IllegalArgumentException.class, () ->
            connectorResource.updateRunStatus(CONNECTOR_ID, null));

        verify(serviceFacade, never()).scheduleConnector(any(Revision.class), anyString(), any(ScheduledState.class));
    }

    @Test
    public void testUpdateRunStatusWithNullRevision() {
        final ConnectorRunStatusEntity requestEntity = createConnectorRunStatusEntity();
        requestEntity.setRevision(null);

        assertThrows(IllegalArgumentException.class, () ->
            connectorResource.updateRunStatus(CONNECTOR_ID, requestEntity));

        verify(serviceFacade, never()).scheduleConnector(any(Revision.class), anyString(), any(ScheduledState.class));
    }

    @Test
    public void testGetConnectorPropertyAllowableValues() {
        final ConnectorPropertyAllowableValuesEntity responseEntity = createConnectorPropertyAllowableValuesEntity();

        when(serviceFacade.getConnectorPropertyAllowableValues(CONNECTOR_ID, CONFIGURATION_STEP_NAME, PROPERTY_GROUP_NAME, PROPERTY_NAME, null))
            .thenReturn(responseEntity);

        try (Response response = connectorResource.getConnectorPropertyAllowableValues(CONNECTOR_ID, CONFIGURATION_STEP_NAME, PROPERTY_GROUP_NAME, PROPERTY_NAME, null)) {
            assertEquals(200, response.getStatus());
            assertEquals(responseEntity, response.getEntity());
        }

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade).getConnectorPropertyAllowableValues(CONNECTOR_ID, CONFIGURATION_STEP_NAME, PROPERTY_GROUP_NAME, PROPERTY_NAME, null);
    }

    @Test
    public void testGetConnectorPropertyAllowableValuesWithFilter() {
        final String filter = "test-filter";
        final ConnectorPropertyAllowableValuesEntity responseEntity = createConnectorPropertyAllowableValuesEntity();

        when(serviceFacade.getConnectorPropertyAllowableValues(CONNECTOR_ID, CONFIGURATION_STEP_NAME, PROPERTY_GROUP_NAME, PROPERTY_NAME, filter))
            .thenReturn(responseEntity);

        try (Response response = connectorResource.getConnectorPropertyAllowableValues(CONNECTOR_ID, CONFIGURATION_STEP_NAME, PROPERTY_GROUP_NAME, PROPERTY_NAME, filter)) {
            assertEquals(200, response.getStatus());
            assertEquals(responseEntity, response.getEntity());
        }

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade).getConnectorPropertyAllowableValues(CONNECTOR_ID, CONFIGURATION_STEP_NAME, PROPERTY_GROUP_NAME, PROPERTY_NAME, filter);
    }

    @Test
    public void testGetConnectorPropertyAllowableValuesNotAuthorized() {
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () ->
            connectorResource.getConnectorPropertyAllowableValues(CONNECTOR_ID, CONFIGURATION_STEP_NAME, PROPERTY_GROUP_NAME, PROPERTY_NAME, null));

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade, never()).getConnectorPropertyAllowableValues(anyString(), anyString(), anyString(), anyString(), any());
    }

    private ConnectorEntity createConnectorEntity() {
        final ConnectorEntity entity = new ConnectorEntity();

        final ConnectorDTO dto = new ConnectorDTO();
        dto.setId(CONNECTOR_ID);
        dto.setName(CONNECTOR_NAME);
        dto.setType(CONNECTOR_TYPE);
        dto.setState("STOPPED");
        entity.setComponent(dto);

        final RevisionDTO revision = new RevisionDTO();
        revision.setVersion(1L);
        revision.setClientId("client-id");
        entity.setRevision(revision);

        return entity;
    }

    private ConnectorRunStatusEntity createConnectorRunStatusEntity() {
        final ConnectorRunStatusEntity entity = new ConnectorRunStatusEntity();
        entity.setState("RUNNING");

        final RevisionDTO revision = new RevisionDTO();
        revision.setVersion(1L);
        revision.setClientId("client-id");
        entity.setRevision(revision);

        return entity;
    }

    private ConnectorPropertyAllowableValuesEntity createConnectorPropertyAllowableValuesEntity() {
        final ConnectorPropertyAllowableValuesEntity entity = new ConnectorPropertyAllowableValuesEntity();
        entity.setConfigurationStepName(CONFIGURATION_STEP_NAME);
        entity.setPropertyGroupName(PROPERTY_GROUP_NAME);
        entity.setPropertyName(PROPERTY_NAME);

        final AllowableValueDTO allowableValueDto1 = new AllowableValueDTO();
        allowableValueDto1.setValue("value1");
        allowableValueDto1.setDisplayName("Value 1");
        allowableValueDto1.setDescription("First allowable value");

        final AllowableValueEntity allowableValueEntity1 = new AllowableValueEntity();
        allowableValueEntity1.setAllowableValue(allowableValueDto1);
        allowableValueEntity1.setCanRead(true);

        final AllowableValueDTO allowableValueDto2 = new AllowableValueDTO();
        allowableValueDto2.setValue("value2");
        allowableValueDto2.setDisplayName("Value 2");
        allowableValueDto2.setDescription("Second allowable value");

        final AllowableValueEntity allowableValueEntity2 = new AllowableValueEntity();
        allowableValueEntity2.setAllowableValue(allowableValueDto2);
        allowableValueEntity2.setCanRead(true);

        entity.setAllowableValues(List.of(allowableValueEntity1, allowableValueEntity2));

        return entity;
    }
}
