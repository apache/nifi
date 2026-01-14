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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.connector.ConnectorAssetRepository;
import org.apache.nifi.components.connector.ConnectorConfiguration;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorRepository;
import org.apache.nifi.components.connector.ConnectorUpdateContext;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.FrameworkFlowContext;
import org.apache.nifi.components.connector.MutableConnectorConfigurationContext;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class StandardConnectorDAOTest {

    private StandardConnectorDAO connectorDAO;

    @Mock
    private FlowController flowController;

    @Mock
    private ConnectorRepository connectorRepository;

    @Mock
    private ConnectorNode connectorNode;

    @Mock
    private ConnectorUpdateContext connectorUpdateContext;

    @Mock
    private ConnectorAssetRepository connectorAssetRepository;

    @Mock
    private FrameworkFlowContext frameworkFlowContext;

    @Mock
    private MutableConnectorConfigurationContext configurationContext;

    @Mock
    private ConnectorConfiguration connectorConfiguration;

    private static final String CONNECTOR_ID = "test-connector-id";
    private static final String STEP_NAME = "test-step";
    private static final String PROPERTY_NAME = "test-property";

    @BeforeEach
    void setUp() {
        connectorDAO = new StandardConnectorDAO();
        connectorDAO.setFlowController(flowController);

        when(flowController.getConnectorRepository()).thenReturn(connectorRepository);
        when(connectorRepository.getAssetRepository()).thenReturn(connectorAssetRepository);

        final MutableConnectorConfigurationContext configContext = mock(MutableConnectorConfigurationContext.class);
        when(configContext.toConnectorConfiguration()).thenReturn(mock(ConnectorConfiguration.class));
        final FrameworkFlowContext activeContext = mock(FrameworkFlowContext.class);
        when(activeContext.getConfigurationContext()).thenReturn(configContext);
        when(connectorNode.getActiveFlowContext()).thenReturn(activeContext);
    }

    @Test
    void testApplyConnectorUpdate() throws Exception {
        when(connectorRepository.getConnector(CONNECTOR_ID)).thenReturn(connectorNode);
        when(connectorRepository.getAssetRepository()).thenReturn(connectorAssetRepository);
        when(connectorNode.getActiveFlowContext()).thenReturn(frameworkFlowContext);
        when(frameworkFlowContext.getConfigurationContext()).thenReturn(configurationContext);
        when(configurationContext.toConnectorConfiguration()).thenReturn(connectorConfiguration);
        when(connectorConfiguration.getNamedStepConfigurations()).thenReturn(Collections.emptySet());
        when(connectorNode.getIdentifier()).thenReturn(CONNECTOR_ID);
        when(connectorAssetRepository.getAssets(CONNECTOR_ID)).thenReturn(Collections.emptyList());

        connectorDAO.applyConnectorUpdate(CONNECTOR_ID, connectorUpdateContext);

        verify(connectorRepository).getConnector(CONNECTOR_ID);
        verify(connectorRepository).applyUpdate(connectorNode, connectorUpdateContext);
    }

    @Test
    void testApplyConnectorUpdateWithNonExistentConnector() throws Exception {
        when(connectorRepository.getConnector(CONNECTOR_ID)).thenReturn(null);

        final ResourceNotFoundException exception = assertThrows(ResourceNotFoundException.class, () ->
            connectorDAO.applyConnectorUpdate(CONNECTOR_ID, connectorUpdateContext)
        );

        assertEquals("Could not find Connector with ID " + CONNECTOR_ID, exception.getMessage());
        verify(connectorRepository).getConnector(CONNECTOR_ID);
        verify(connectorRepository, never()).applyUpdate(any(ConnectorNode.class), any(ConnectorUpdateContext.class));
    }

    @Test
    void testApplyConnectorUpdateWithFlowUpdateException() throws Exception {
        when(connectorRepository.getConnector(CONNECTOR_ID)).thenReturn(connectorNode);
        doThrow(new FlowUpdateException("Flow update failed")).when(connectorRepository).applyUpdate(connectorNode, connectorUpdateContext);

        final NiFiCoreException exception = assertThrows(NiFiCoreException.class, () ->
            connectorDAO.applyConnectorUpdate(CONNECTOR_ID, connectorUpdateContext)
        );

        assertEquals("Failed to apply connector update: org.apache.nifi.components.connector.FlowUpdateException: Flow update failed", exception.getMessage());
        verify(connectorRepository).getConnector(CONNECTOR_ID);
        verify(connectorRepository).applyUpdate(connectorNode, connectorUpdateContext);
    }

    @Test
    void testApplyConnectorUpdateWithRuntimeException() throws Exception {
        when(connectorRepository.getConnector(CONNECTOR_ID)).thenReturn(connectorNode);
        doThrow(new RuntimeException("Test exception")).when(connectorRepository).applyUpdate(connectorNode, connectorUpdateContext);

        final NiFiCoreException exception = assertThrows(NiFiCoreException.class, () ->
            connectorDAO.applyConnectorUpdate(CONNECTOR_ID, connectorUpdateContext)
        );

        assertEquals("Failed to apply connector update: java.lang.RuntimeException: Test exception", exception.getMessage());
        verify(connectorRepository).getConnector(CONNECTOR_ID);
        verify(connectorRepository).applyUpdate(connectorNode, connectorUpdateContext);
    }

    @Test
    void testApplyConnectorUpdateWithNullException() throws Exception {
        when(connectorRepository.getConnector(CONNECTOR_ID)).thenReturn(connectorNode);
        doThrow(new RuntimeException()).when(connectorRepository).applyUpdate(connectorNode, connectorUpdateContext);

        final NiFiCoreException exception = assertThrows(NiFiCoreException.class, () ->
            connectorDAO.applyConnectorUpdate(CONNECTOR_ID, connectorUpdateContext)
        );

        assertEquals("Failed to apply connector update: java.lang.RuntimeException", exception.getMessage());
        verify(connectorRepository).getConnector(CONNECTOR_ID);
        verify(connectorRepository).applyUpdate(connectorNode, connectorUpdateContext);
    }

    @Test
    void testGetConnectorWithNonExistentId() {
        when(connectorRepository.getConnector(CONNECTOR_ID)).thenReturn(null);

        assertThrows(ResourceNotFoundException.class, () ->
            connectorDAO.getConnector(CONNECTOR_ID)
        );

        verify(connectorRepository).getConnector(CONNECTOR_ID);
    }

    @Test
    void testGetConnectorSuccess() {
        when(connectorRepository.getConnector(CONNECTOR_ID)).thenReturn(connectorNode);

        final ConnectorNode result = connectorDAO.getConnector(CONNECTOR_ID);

        assertEquals(connectorNode, result);
        verify(connectorRepository).getConnector(CONNECTOR_ID);
    }

    @Test
    void testFetchAllowableValuesWithoutFilter() {
        final List<AllowableValue> expectedValues = List.of(
            new AllowableValue("value1", "Value 1", "First value"),
            new AllowableValue("value2", "Value 2", "Second value")
        );
        when(connectorRepository.getConnector(CONNECTOR_ID)).thenReturn(connectorNode);
        when(connectorNode.fetchAllowableValues(STEP_NAME, PROPERTY_NAME)).thenReturn(expectedValues);

        final List<AllowableValue> result = connectorDAO.fetchAllowableValues(CONNECTOR_ID, STEP_NAME, PROPERTY_NAME, null);

        assertEquals(expectedValues, result);
        verify(connectorNode).fetchAllowableValues(STEP_NAME, PROPERTY_NAME);
        verify(connectorNode, never()).fetchAllowableValues(any(), any(), any());
    }

    @Test
    void testFetchAllowableValuesWithEmptyFilter() {
        final List<AllowableValue> expectedValues = List.of(
            new AllowableValue("value1", "Value 1", "First value")
        );
        when(connectorRepository.getConnector(CONNECTOR_ID)).thenReturn(connectorNode);
        when(connectorNode.fetchAllowableValues(STEP_NAME, PROPERTY_NAME)).thenReturn(expectedValues);

        final List<AllowableValue> result = connectorDAO.fetchAllowableValues(CONNECTOR_ID, STEP_NAME, PROPERTY_NAME, "");

        assertEquals(expectedValues, result);
        verify(connectorNode).fetchAllowableValues(STEP_NAME, PROPERTY_NAME);
        verify(connectorNode, never()).fetchAllowableValues(any(), any(), any());
    }

    @Test
    void testFetchAllowableValuesWithFilter() {
        final String filter = "test-filter";
        final List<AllowableValue> expectedValues = List.of(
            new AllowableValue("filtered-value", "Filtered Value", "Filtered result")
        );
        when(connectorRepository.getConnector(CONNECTOR_ID)).thenReturn(connectorNode);
        when(connectorNode.fetchAllowableValues(STEP_NAME, PROPERTY_NAME, filter)).thenReturn(expectedValues);

        final List<AllowableValue> result = connectorDAO.fetchAllowableValues(CONNECTOR_ID, STEP_NAME, PROPERTY_NAME, filter);

        assertEquals(expectedValues, result);
        verify(connectorNode).fetchAllowableValues(STEP_NAME, PROPERTY_NAME, filter);
        verify(connectorNode, never()).fetchAllowableValues(STEP_NAME, PROPERTY_NAME);
    }

    @Test
    void testFetchAllowableValuesWithNonExistentConnector() {
        when(connectorRepository.getConnector(CONNECTOR_ID)).thenReturn(null);

        assertThrows(ResourceNotFoundException.class, () ->
            connectorDAO.fetchAllowableValues(CONNECTOR_ID, STEP_NAME, PROPERTY_NAME, null)
        );

        verify(connectorRepository).getConnector(CONNECTOR_ID);
    }

    @Test
    void testDeleteConnectorRemovesConnectorAndAssets() {
        when(connectorRepository.getAssetRepository()).thenReturn(connectorAssetRepository);

        connectorDAO.deleteConnector(CONNECTOR_ID);

        verify(connectorRepository).removeConnector(CONNECTOR_ID);
        verify(connectorAssetRepository).deleteAssets(CONNECTOR_ID);
    }

    @Test
    void testDeleteConnectorDoesNotDeleteAssetsWhenRemovalFails() {
        doThrow(new RuntimeException("Removal failed")).when(connectorRepository).removeConnector(CONNECTOR_ID);

        assertThrows(RuntimeException.class, () ->
            connectorDAO.deleteConnector(CONNECTOR_ID)
        );

        verify(connectorRepository).removeConnector(CONNECTOR_ID);
        verify(connectorAssetRepository, never()).deleteAssets(any());
    }

    @Test
    void testVerifyCreateWithExistingConnectorId() {
        final ConnectorDTO connectorDTO = new ConnectorDTO();
        connectorDTO.setId(CONNECTOR_ID);
        connectorDTO.setType("org.apache.nifi.connector.TestConnector");

        when(connectorRepository.getConnector(CONNECTOR_ID)).thenReturn(connectorNode);

        final IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            connectorDAO.verifyCreate(connectorDTO)
        );

        assertEquals("A Connector already exists with ID %s".formatted(CONNECTOR_ID), exception.getMessage());
    }

    @Test
    void testVerifyCreateWithNewId() {
        final ConnectorDTO connectorDTO = new ConnectorDTO();
        connectorDTO.setId(CONNECTOR_ID);

        when(connectorRepository.getConnector(CONNECTOR_ID)).thenReturn(null);

        connectorDAO.verifyCreate(connectorDTO);

        verify(connectorRepository).getConnector(CONNECTOR_ID);
    }

    @Test
    void testVerifyCreateWithNullId() {
        final ConnectorDTO connectorDTO = new ConnectorDTO();
        connectorDTO.setId(null);

        connectorDAO.verifyCreate(connectorDTO);

        verify(connectorRepository, never()).getConnector(any());
    }

}

