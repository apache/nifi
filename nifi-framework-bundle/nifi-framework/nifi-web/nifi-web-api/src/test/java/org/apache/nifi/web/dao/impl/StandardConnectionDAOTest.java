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

import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorRepository;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.components.connector.ConnectorSyncMode;
import org.apache.nifi.components.connector.FrameworkFlowContext;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.ResourceNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class StandardConnectionDAOTest {

    private StandardConnectionDAO connectionDAO;
    private StandardConnectorManagedComponentLookup connectorManagedComponentLookup;

    @Mock
    private FlowController flowController;

    @Mock
    private FlowManager flowManager;

    @Mock
    private ProcessGroup rootGroup;

    @Mock
    private ConnectorRepository connectorRepository;

    @Mock
    private ConnectorNode connectorNode;

    @Mock
    private FrameworkFlowContext frameworkFlowContext;

    @Mock
    private ProcessGroup connectorManagedGroup;

    @Mock
    private Connection rootConnection;

    @Mock
    private Connection connectorConnection;

    private static final String ROOT_CONNECTION_ID = "root-connection-id";
    private static final String CONNECTOR_CONNECTION_ID = "connector-connection-id";
    private static final String NON_EXISTENT_ID = "non-existent-id";

    @BeforeEach
    void setUp() {
        connectionDAO = new StandardConnectionDAO();
        connectionDAO.setFlowController(flowController);

        connectorManagedComponentLookup = new StandardConnectorManagedComponentLookup();
        connectorManagedComponentLookup.setConnectionDAO(connectionDAO);

        when(flowController.getFlowManager()).thenReturn(flowManager);
        when(flowManager.getRootGroup()).thenReturn(rootGroup);
        when(flowController.getConnectorRepository()).thenReturn(connectorRepository);

        // Setup root group connection
        when(rootGroup.findConnection(ROOT_CONNECTION_ID)).thenReturn(rootConnection);
        when(rootGroup.findConnection(CONNECTOR_CONNECTION_ID)).thenReturn(null);
        when(rootGroup.findConnection(NON_EXISTENT_ID)).thenReturn(null);

        when(connectorRepository.getConnectors(ConnectorSyncMode.LOCAL_ONLY)).thenReturn(List.of(connectorNode));
        when(connectorNode.getActiveFlowContext()).thenReturn(frameworkFlowContext);
        when(frameworkFlowContext.getManagedProcessGroup()).thenReturn(connectorManagedGroup);
        when(connectorManagedGroup.findConnection(CONNECTOR_CONNECTION_ID)).thenReturn(connectorConnection);
        when(connectorManagedGroup.findConnection(ROOT_CONNECTION_ID)).thenReturn(null);
        when(connectorManagedGroup.findConnection(NON_EXISTENT_ID)).thenReturn(null);

        // Attach the connector-managed Connection to a ProcessGroup owned by a Connector that is not in
        // Troubleshooting mode so that access checks behave as they would for a real connector-managed component.
        when(connectorConnection.getProcessGroup()).thenReturn(connectorManagedGroup);
        when(connectorManagedGroup.findOwningConnector()).thenReturn(Optional.of(connectorNode));
        when(connectorNode.getCurrentState()).thenReturn(ConnectorState.STOPPED);
    }

    @Test
    void testGetConnectionFromRootGroup() {
        final Connection result = connectionDAO.getConnection(ROOT_CONNECTION_ID);

        assertEquals(rootConnection, result);
    }

    @Test
    void testConnectorManagedLookupReturnsRootConnection() {
        final Connection result = connectorManagedComponentLookup.getConnection(ROOT_CONNECTION_ID);

        assertEquals(rootConnection, result);
    }

    @Test
    void testConnectorManagedLookupReturnsConnectorConnectionRegardlessOfConnectorState() {
        final Connection result = connectorManagedComponentLookup.getConnection(CONNECTOR_CONNECTION_ID);

        assertEquals(connectorConnection, result);
    }

    @Test
    void testGetConnectionOnConnectorManagedConnectionThrowsWhenConnectorNotTroubleshooting() {
        assertThrows(IllegalStateException.class, () ->
            connectionDAO.getConnection(CONNECTOR_CONNECTION_ID)
        );
    }

    @Test
    void testGetConnectionFromConnectorManagedConnectionInTroubleshootingReturnsConnection() {
        when(connectorNode.getCurrentState()).thenReturn(ConnectorState.TROUBLESHOOTING);

        final Connection result = connectionDAO.getConnection(CONNECTOR_CONNECTION_ID);
        assertEquals(connectorConnection, result);
    }

    @Test
    void testGetConnectionWithNonExistentIdThrows() {
        assertThrows(ResourceNotFoundException.class, () ->
            connectorManagedComponentLookup.getConnection(NON_EXISTENT_ID)
        );
    }

    @Test
    void testHasConnectionInRootGroup() {
        assertTrue(connectionDAO.hasConnection(ROOT_CONNECTION_ID));
    }

    @Test
    void testHasConnectionNotInRootGroup() {
        // hasConnection only checks the root group, not connector-managed groups
        assertFalse(connectionDAO.hasConnection(CONNECTOR_CONNECTION_ID));
    }

    @Test
    void testHasConnectionWithNonExistentId() {
        assertFalse(connectionDAO.hasConnection(NON_EXISTENT_ID));
    }

    @Test
    void testGetConnectionFromConnectorWithNullActiveFlowContext() {
        // Simulate connector with no active flow context
        when(connectorNode.getActiveFlowContext()).thenReturn(null);

        assertThrows(ResourceNotFoundException.class, () ->
            connectorManagedComponentLookup.getConnection(CONNECTOR_CONNECTION_ID)
        );
    }

    @Test
    void testGetConnectionWithMultipleConnectors() {
        // Setup a second connector
        final ConnectorNode connectorNode2 = org.mockito.Mockito.mock(ConnectorNode.class);
        final FrameworkFlowContext flowContext2 = org.mockito.Mockito.mock(FrameworkFlowContext.class);
        final ProcessGroup managedGroup2 = org.mockito.Mockito.mock(ProcessGroup.class);
        final Connection connectionInSecondConnector = org.mockito.Mockito.mock(Connection.class);
        final String secondConnectorConnectionId = "second-connector-connection-id";

        when(connectorRepository.getConnectors(ConnectorSyncMode.LOCAL_ONLY)).thenReturn(List.of(connectorNode, connectorNode2));
        when(connectorNode2.getActiveFlowContext()).thenReturn(flowContext2);
        when(flowContext2.getManagedProcessGroup()).thenReturn(managedGroup2);
        when(managedGroup2.findConnection(secondConnectorConnectionId)).thenReturn(connectionInSecondConnector);

        final Connection result = connectorManagedComponentLookup.getConnection(secondConnectorConnectionId);

        assertEquals(connectionInSecondConnector, result);
    }
}
