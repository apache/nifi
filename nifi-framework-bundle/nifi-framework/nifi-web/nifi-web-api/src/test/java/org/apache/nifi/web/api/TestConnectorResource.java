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
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.entity.AllowableValueEntity;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ConnectorPropertyAllowableValuesEntity;
import org.apache.nifi.web.api.entity.ConnectorRunStatusEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.SecretsEntity;
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
import java.util.Set;

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

    @Mock
    private FlowResource flowResource;

    @Mock
    private ControllerServiceResource controllerServiceResource;

    private static final String CONNECTOR_ID = "test-connector-id";
    private static final String CONNECTOR_NAME = "Test Connector";
    private static final String CONNECTOR_TYPE = "TestConnectorType";
    private static final String CONFIGURATION_STEP_NAME = "test-step";
    private static final String PROPERTY_GROUP_NAME = "test-group";
    private static final String PROPERTY_NAME = "test-property";
    private static final String PROCESS_GROUP_ID = "test-process-group-id";
    private static final String PROCESSOR_ID = "test-processor-id";
    private static final String CONTROLLER_SERVICE_ID = "test-controller-service-id";

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
        connectorResource.setFlowResource(flowResource);
        connectorResource.setControllerServiceResource(controllerServiceResource);
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

    @Test
    public void testGetSecrets() {
        final SecretsEntity responseEntity = new SecretsEntity();
        responseEntity.setSecrets(List.of());

        when(serviceFacade.getSecrets()).thenReturn(responseEntity);

        try (Response response = connectorResource.getSecrets(CONNECTOR_ID)) {
            assertEquals(200, response.getStatus());
            assertEquals(responseEntity, response.getEntity());
        }

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade).getSecrets();
    }

    @Test
    public void testGetSecretsNotAuthorized() {
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () -> connectorResource.getSecrets(CONNECTOR_ID));

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade, never()).getSecrets();
    }

    @Test
    public void testGetFlow() {
        final ProcessGroupFlowEntity responseEntity = createProcessGroupFlowEntity();
        when(serviceFacade.getConnectorFlow(CONNECTOR_ID, PROCESS_GROUP_ID, false)).thenReturn(responseEntity);

        try (Response response = connectorResource.getFlow(CONNECTOR_ID, PROCESS_GROUP_ID, false)) {
            assertEquals(200, response.getStatus());
            assertEquals(responseEntity, response.getEntity());
        }

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade).getConnectorFlow(CONNECTOR_ID, PROCESS_GROUP_ID, false);
    }

    @Test
    public void testGetFlowNotAuthorized() {
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () -> connectorResource.getFlow(CONNECTOR_ID, PROCESS_GROUP_ID, false));

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade, never()).getConnectorFlow(anyString(), anyString(), eq(false));
    }

    @Test
    public void testGetControllerServicesFromConnectorProcessGroup() {
        final Set<ControllerServiceEntity> controllerServices = Set.of();
        when(serviceFacade.getConnectorControllerServices(CONNECTOR_ID, PROCESS_GROUP_ID, true, false, true)).thenReturn(controllerServices);

        try (Response response = connectorResource.getControllerServicesFromConnectorProcessGroup(CONNECTOR_ID, PROCESS_GROUP_ID, true, false, true)) {
            assertEquals(200, response.getStatus());
            final ControllerServicesEntity entity = (ControllerServicesEntity) response.getEntity();
            assertEquals(controllerServices, entity.getControllerServices());
        }

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade).getConnectorControllerServices(CONNECTOR_ID, PROCESS_GROUP_ID, true, false, true);
        verify(controllerServiceResource).populateRemainingControllerServiceEntitiesContent(controllerServices);
    }

    @Test
    public void testGetControllerServicesFromConnectorProcessGroupNotAuthorized() {
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () ->
            connectorResource.getControllerServicesFromConnectorProcessGroup(CONNECTOR_ID, PROCESS_GROUP_ID, true, false, true));

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade, never()).getConnectorControllerServices(anyString(), anyString(), eq(true), eq(false), eq(true));
    }

    @Test
    public void testInitiateDrain() {
        final ConnectorEntity requestEntity = createConnectorEntity();
        final ConnectorEntity responseEntity = createConnectorEntity();
        responseEntity.getComponent().setState("DRAINING");

        when(serviceFacade.drainConnector(any(Revision.class), eq(CONNECTOR_ID))).thenReturn(responseEntity);

        try (Response response = connectorResource.initiateDrain(CONNECTOR_ID, requestEntity)) {
            assertEquals(200, response.getStatus());
            assertEquals(responseEntity, response.getEntity());
        }

        verify(serviceFacade).verifyDrainConnector(CONNECTOR_ID);
        verify(serviceFacade).drainConnector(any(Revision.class), eq(CONNECTOR_ID));
    }

    @Test
    public void testInitiateDrainNotAuthorized() {
        final ConnectorEntity requestEntity = createConnectorEntity();

        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () ->
            connectorResource.initiateDrain(CONNECTOR_ID, requestEntity));

        verify(serviceFacade, never()).verifyDrainConnector(anyString());
        verify(serviceFacade, never()).drainConnector(any(Revision.class), anyString());
    }

    @Test
    public void testInitiateDrainWithNullEntity() {
        assertThrows(IllegalArgumentException.class, () ->
            connectorResource.initiateDrain(CONNECTOR_ID, null));

        verify(serviceFacade, never()).drainConnector(any(Revision.class), anyString());
    }

    @Test
    public void testInitiateDrainWithNullRevision() {
        final ConnectorEntity requestEntity = createConnectorEntity();
        requestEntity.setRevision(null);

        assertThrows(IllegalArgumentException.class, () ->
            connectorResource.initiateDrain(CONNECTOR_ID, requestEntity));

        verify(serviceFacade, never()).drainConnector(any(Revision.class), anyString());
    }

    @Test
    public void testInitiateDrainWithMismatchedId() {
        final ConnectorEntity requestEntity = createConnectorEntity();
        requestEntity.setId("different-id");

        assertThrows(IllegalArgumentException.class, () ->
            connectorResource.initiateDrain(CONNECTOR_ID, requestEntity));

        verify(serviceFacade, never()).drainConnector(any(Revision.class), anyString());
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

    private ProcessGroupFlowEntity createProcessGroupFlowEntity() {
        final ProcessGroupFlowEntity entity = new ProcessGroupFlowEntity();
        final ProcessGroupFlowDTO flowDTO = new ProcessGroupFlowDTO();
        flowDTO.setId("root-process-group-id");
        entity.setProcessGroupFlow(flowDTO);
        return entity;
    }

    @Test
    public void testCreateConnectorWithValidClientSpecifiedUuid() {
        final String uppercaseUuid = "A1B2C3D4-E5F6-7890-ABCD-EF1234567890";
        final String normalizedUuid = uppercaseUuid.toLowerCase();

        final ConnectorEntity requestEntity = createConnectorEntity();
        requestEntity.getComponent().setId(uppercaseUuid);
        requestEntity.getComponent().setType(CONNECTOR_TYPE);
        requestEntity.getRevision().setVersion(0L);

        final ConnectorEntity responseEntity = createConnectorEntity();
        responseEntity.getComponent().setId(normalizedUuid);

        when(serviceFacade.createConnector(any(Revision.class), any(ConnectorDTO.class))).thenReturn(responseEntity);

        try (Response response = connectorResource.createConnector(requestEntity)) {
            assertEquals(201, response.getStatus());
            final ConnectorEntity entity = (ConnectorEntity) response.getEntity();
            assertEquals(normalizedUuid, entity.getComponent().getId());
        }

        verify(serviceFacade).verifyCreateConnector(any(ConnectorDTO.class));
        verify(serviceFacade).createConnector(any(Revision.class), any(ConnectorDTO.class));
    }

    @Test
    public void testCreateConnectorWithInvalidClientSpecifiedUuid() {
        final String invalidId = "not-a-valid-uuid";

        final ConnectorEntity requestEntity = createConnectorEntity();
        requestEntity.getComponent().setId(invalidId);
        requestEntity.getComponent().setType(CONNECTOR_TYPE);
        requestEntity.getRevision().setVersion(0L);

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            connectorResource.createConnector(requestEntity));

        assertEquals("ID [" + invalidId + "] is not a valid UUID.", exception.getMessage());

        verify(serviceFacade, never()).createConnector(any(Revision.class), any(ConnectorDTO.class));
    }

    @Test
    public void testCreateConnectorWithNullEntity() {
        assertThrows(IllegalArgumentException.class, () ->
            connectorResource.createConnector(null));

        verify(serviceFacade, never()).createConnector(any(Revision.class), any(ConnectorDTO.class));
    }

    @Test
    public void testCreateConnectorWithNullComponent() {
        final ConnectorEntity requestEntity = new ConnectorEntity();
        requestEntity.setComponent(null);

        assertThrows(IllegalArgumentException.class, () ->
            connectorResource.createConnector(requestEntity));

        verify(serviceFacade, never()).createConnector(any(Revision.class), any(ConnectorDTO.class));
    }

    @Test
    public void testCreateConnectorWithInvalidRevision() {
        final ConnectorEntity requestEntity = createConnectorEntity();
        requestEntity.getComponent().setType(CONNECTOR_TYPE);
        requestEntity.getRevision().setVersion(1L);

        assertThrows(IllegalArgumentException.class, () ->
            connectorResource.createConnector(requestEntity));

        verify(serviceFacade, never()).createConnector(any(Revision.class), any(ConnectorDTO.class));
    }

    @Test
    public void testCreateConnectorWithBlankType() {
        final ConnectorEntity requestEntity = createConnectorEntity();
        requestEntity.getComponent().setType("");
        requestEntity.getRevision().setVersion(0L);

        assertThrows(IllegalArgumentException.class, () ->
            connectorResource.createConnector(requestEntity));

        verify(serviceFacade, never()).createConnector(any(Revision.class), any(ConnectorDTO.class));
    }

    @Test
    public void testGetConnectorProcessorState() {
        final ComponentStateDTO stateDTO = new ComponentStateDTO();
        stateDTO.setComponentId(PROCESSOR_ID);
        when(serviceFacade.getConnectorProcessorState(CONNECTOR_ID, PROCESSOR_ID)).thenReturn(stateDTO);

        try (Response response = connectorResource.getConnectorProcessorState(CONNECTOR_ID, PROCESSOR_ID)) {
            assertEquals(200, response.getStatus());
            final ComponentStateEntity entity = (ComponentStateEntity) response.getEntity();
            assertEquals(PROCESSOR_ID, entity.getComponentState().getComponentId());
        }

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade).getConnectorProcessorState(CONNECTOR_ID, PROCESSOR_ID);
    }

    @Test
    public void testGetConnectorProcessorStateNotAuthorized() {
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () ->
            connectorResource.getConnectorProcessorState(CONNECTOR_ID, PROCESSOR_ID));

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade, never()).getConnectorProcessorState(anyString(), anyString());
    }

    @Test
    public void testClearConnectorProcessorState() {
        final ComponentStateDTO stateDTO = new ComponentStateDTO();
        stateDTO.setComponentId(PROCESSOR_ID);
        when(serviceFacade.clearConnectorProcessorState(eq(CONNECTOR_ID), eq(PROCESSOR_ID), any())).thenReturn(stateDTO);

        try (Response response = connectorResource.clearConnectorProcessorState(CONNECTOR_ID, PROCESSOR_ID, null)) {
            assertEquals(200, response.getStatus());
            final ComponentStateEntity entity = (ComponentStateEntity) response.getEntity();
            assertEquals(PROCESSOR_ID, entity.getComponentState().getComponentId());
        }

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade).verifyCanClearConnectorProcessorState(CONNECTOR_ID, PROCESSOR_ID);
        verify(serviceFacade).clearConnectorProcessorState(eq(CONNECTOR_ID), eq(PROCESSOR_ID), any());
    }

    @Test
    public void testClearConnectorProcessorStateNotAuthorized() {
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () ->
            connectorResource.clearConnectorProcessorState(CONNECTOR_ID, PROCESSOR_ID, null));

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade, never()).verifyCanClearConnectorProcessorState(anyString(), anyString());
        verify(serviceFacade, never()).clearConnectorProcessorState(anyString(), anyString(), any());
    }

    @Test
    public void testGetConnectorControllerServiceState() {
        final ComponentStateDTO stateDTO = new ComponentStateDTO();
        stateDTO.setComponentId(CONTROLLER_SERVICE_ID);
        when(serviceFacade.getConnectorControllerServiceState(CONNECTOR_ID, CONTROLLER_SERVICE_ID)).thenReturn(stateDTO);

        try (Response response = connectorResource.getConnectorControllerServiceState(CONNECTOR_ID, CONTROLLER_SERVICE_ID)) {
            assertEquals(200, response.getStatus());
            final ComponentStateEntity entity = (ComponentStateEntity) response.getEntity();
            assertEquals(CONTROLLER_SERVICE_ID, entity.getComponentState().getComponentId());
        }

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade).getConnectorControllerServiceState(CONNECTOR_ID, CONTROLLER_SERVICE_ID);
    }

    @Test
    public void testGetConnectorControllerServiceStateNotAuthorized() {
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () ->
            connectorResource.getConnectorControllerServiceState(CONNECTOR_ID, CONTROLLER_SERVICE_ID));

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade, never()).getConnectorControllerServiceState(anyString(), anyString());
    }

    @Test
    public void testClearConnectorControllerServiceState() {
        final ComponentStateDTO stateDTO = new ComponentStateDTO();
        stateDTO.setComponentId(CONTROLLER_SERVICE_ID);
        when(serviceFacade.clearConnectorControllerServiceState(eq(CONNECTOR_ID), eq(CONTROLLER_SERVICE_ID), any())).thenReturn(stateDTO);

        try (Response response = connectorResource.clearConnectorControllerServiceState(CONNECTOR_ID, CONTROLLER_SERVICE_ID, null)) {
            assertEquals(200, response.getStatus());
            final ComponentStateEntity entity = (ComponentStateEntity) response.getEntity();
            assertEquals(CONTROLLER_SERVICE_ID, entity.getComponentState().getComponentId());
        }

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade).verifyCanClearConnectorControllerServiceState(CONNECTOR_ID, CONTROLLER_SERVICE_ID);
        verify(serviceFacade).clearConnectorControllerServiceState(eq(CONNECTOR_ID), eq(CONTROLLER_SERVICE_ID), any());
    }

    @Test
    public void testClearConnectorControllerServiceStateNotAuthorized() {
        doThrow(AccessDeniedException.class).when(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));

        assertThrows(AccessDeniedException.class, () ->
            connectorResource.clearConnectorControllerServiceState(CONNECTOR_ID, CONTROLLER_SERVICE_ID, null));

        verify(serviceFacade).authorizeAccess(any(AuthorizeAccess.class));
        verify(serviceFacade, never()).verifyCanClearConnectorControllerServiceState(anyString(), anyString());
        verify(serviceFacade, never()).clearConnectorControllerServiceState(anyString(), anyString(), any());
    }
}
