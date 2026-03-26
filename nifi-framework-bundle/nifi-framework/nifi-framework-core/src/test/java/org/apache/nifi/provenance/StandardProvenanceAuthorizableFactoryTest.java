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
package org.apache.nifi.provenance;

import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.DataAuthorizable;
import org.apache.nifi.authorization.resource.EnforcePolicyPermissionsThroughBaseResource;
import org.apache.nifi.authorization.resource.ProvenanceDataAuthorizable;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.web.ResourceNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class StandardProvenanceAuthorizableFactoryTest {

    private StandardProvenanceAuthorizableFactory factory;

    @Mock
    private FlowController flowController;

    @Mock
    private FlowManager flowManager;

    @Mock
    private ProcessGroup rootGroup;

    @Mock
    private Connection rootConnection;

    @Mock
    private Connection connectorConnection;

    @Mock
    private Connectable connectable;

    @Mock
    private Connectable connectionSource;

    @Mock
    private Connectable connectorConnectionSource;

    @Mock
    private RemoteProcessGroup remoteProcessGroup;

    private static final String ROOT_GROUP_ID = "root-group-id";
    private static final String CONNECTABLE_ID = "connectable-id";
    private static final String ROOT_CONNECTION_ID = "root-connection-id";
    private static final String CONNECTOR_CONNECTION_ID = "connector-connection-id";
    private static final String ROOT_REMOTE_PORT_ID = "root-remote-port-id";
    private static final String CONNECTOR_REMOTE_PORT_ID = "connector-remote-port-id";
    private static final String NON_EXISTENT_ID = "non-existent-id";

    @BeforeEach
    void setUp() {
        factory = new StandardProvenanceAuthorizableFactory(flowController);

        when(flowController.getFlowManager()).thenReturn(flowManager);
        when(flowManager.getRootGroup()).thenReturn(rootGroup);
        when(flowManager.getRootGroupId()).thenReturn(ROOT_GROUP_ID);

        // Connectable lookups (flat map, works for all components)
        when(flowManager.findConnectable(CONNECTABLE_ID)).thenReturn(connectable);
        when(flowManager.findConnectable(ROOT_CONNECTION_ID)).thenReturn(null);
        when(flowManager.findConnectable(CONNECTOR_CONNECTION_ID)).thenReturn(null);
        when(flowManager.findConnectable(NON_EXISTENT_ID)).thenReturn(null);

        // Connection lookups via FlowController (includes connector-managed PGs)
        when(rootConnection.getSource()).thenReturn(connectionSource);
        when(connectorConnection.getSource()).thenReturn(connectorConnectionSource);
        when(flowController.findConnectionIncludingConnectorManaged(ROOT_CONNECTION_ID)).thenReturn(rootConnection);
        when(flowController.findConnectionIncludingConnectorManaged(CONNECTOR_CONNECTION_ID)).thenReturn(connectorConnection);
        when(flowController.findConnectionIncludingConnectorManaged(NON_EXISTENT_ID)).thenReturn(null);

        // Remote group port lookups via FlowController
        when(flowController.findRemoteGroupPortIncludingConnectorManaged(NON_EXISTENT_ID)).thenReturn(null);
    }

    @Nested
    class CreateLocalDataAuthorizable {

        @Test
        void testRootGroupIdReturnsDataAuthorizableForRootGroup() {
            final Authorizable result = factory.createLocalDataAuthorizable(ROOT_GROUP_ID);

            assertInstanceOf(DataAuthorizable.class, result);
            assertEquals(rootGroup, ((EnforcePolicyPermissionsThroughBaseResource) result).getBaseAuthorizable());
        }

        @Test
        void testConnectableReturnsDataAuthorizableForConnectable() {
            final Authorizable result = factory.createLocalDataAuthorizable(CONNECTABLE_ID);

            assertInstanceOf(DataAuthorizable.class, result);
            assertEquals(connectable, ((EnforcePolicyPermissionsThroughBaseResource) result).getBaseAuthorizable());
        }

        @Test
        void testConnectionInRootGroupReturnsDataAuthorizableForSource() {
            final Authorizable result = factory.createLocalDataAuthorizable(ROOT_CONNECTION_ID);

            assertInstanceOf(DataAuthorizable.class, result);
            assertEquals(connectionSource, ((EnforcePolicyPermissionsThroughBaseResource) result).getBaseAuthorizable());
        }

        @Test
        void testConnectionInConnectorManagedGroupReturnsDataAuthorizableForSource() {
            final Authorizable result = factory.createLocalDataAuthorizable(CONNECTOR_CONNECTION_ID);

            assertInstanceOf(DataAuthorizable.class, result);
            assertEquals(connectorConnectionSource, ((EnforcePolicyPermissionsThroughBaseResource) result).getBaseAuthorizable());
        }

        @Test
        void testNonExistentComponentThrowsResourceNotFoundException() {
            assertThrows(ResourceNotFoundException.class, () ->
                factory.createLocalDataAuthorizable(NON_EXISTENT_ID)
            );
        }
    }

    @Nested
    class CreateProvenanceDataAuthorizable {

        @Test
        void testRootGroupIdReturnsProvenanceDataAuthorizableForRootGroup() {
            final Authorizable result = factory.createProvenanceDataAuthorizable(ROOT_GROUP_ID);

            assertInstanceOf(ProvenanceDataAuthorizable.class, result);
            assertEquals(rootGroup, ((EnforcePolicyPermissionsThroughBaseResource) result).getBaseAuthorizable());
        }

        @Test
        void testConnectableReturnsProvenanceDataAuthorizableForConnectable() {
            final Authorizable result = factory.createProvenanceDataAuthorizable(CONNECTABLE_ID);

            assertInstanceOf(ProvenanceDataAuthorizable.class, result);
            assertEquals(connectable, ((EnforcePolicyPermissionsThroughBaseResource) result).getBaseAuthorizable());
        }

        @Test
        void testConnectionInRootGroupReturnsProvenanceDataAuthorizableForSource() {
            final Authorizable result = factory.createProvenanceDataAuthorizable(ROOT_CONNECTION_ID);

            assertInstanceOf(ProvenanceDataAuthorizable.class, result);
            assertEquals(connectionSource, ((EnforcePolicyPermissionsThroughBaseResource) result).getBaseAuthorizable());
        }

        @Test
        void testConnectionInConnectorManagedGroupReturnsProvenanceDataAuthorizableForSource() {
            final Authorizable result = factory.createProvenanceDataAuthorizable(CONNECTOR_CONNECTION_ID);

            assertInstanceOf(ProvenanceDataAuthorizable.class, result);
            assertEquals(connectorConnectionSource, ((EnforcePolicyPermissionsThroughBaseResource) result).getBaseAuthorizable());
        }

        @Test
        void testNonExistentComponentThrowsResourceNotFoundException() {
            assertThrows(ResourceNotFoundException.class, () ->
                factory.createProvenanceDataAuthorizable(NON_EXISTENT_ID)
            );
        }
    }

    @Nested
    class CreateRemoteDataAuthorizable {

        private RemoteGroupPort rootRemoteGroupPort;
        private RemoteGroupPort connectorRemoteGroupPort;

        @Mock
        private RemoteProcessGroup connectorRemoteProcessGroup;

        @BeforeEach
        void setUpRemotePorts() {
            rootRemoteGroupPort = mock(RemoteGroupPort.class);
            connectorRemoteGroupPort = mock(RemoteGroupPort.class);

            when(flowController.findRemoteGroupPortIncludingConnectorManaged(ROOT_REMOTE_PORT_ID)).thenReturn(rootRemoteGroupPort);
            when(rootRemoteGroupPort.getRemoteProcessGroup()).thenReturn(remoteProcessGroup);

            when(flowController.findRemoteGroupPortIncludingConnectorManaged(CONNECTOR_REMOTE_PORT_ID)).thenReturn(connectorRemoteGroupPort);
            when(connectorRemoteGroupPort.getRemoteProcessGroup()).thenReturn(connectorRemoteProcessGroup);
        }

        @Test
        void testRemoteGroupPortInRootGroupReturnsDataAuthorizableForRemoteProcessGroup() {
            final Authorizable result = factory.createRemoteDataAuthorizable(ROOT_REMOTE_PORT_ID);

            assertInstanceOf(DataAuthorizable.class, result);
            assertEquals(remoteProcessGroup, ((EnforcePolicyPermissionsThroughBaseResource) result).getBaseAuthorizable());
        }

        @Test
        void testRemoteGroupPortInConnectorManagedGroupReturnsDataAuthorizableForRemoteProcessGroup() {
            final Authorizable result = factory.createRemoteDataAuthorizable(CONNECTOR_REMOTE_PORT_ID);

            assertInstanceOf(DataAuthorizable.class, result);
            assertEquals(connectorRemoteProcessGroup, ((EnforcePolicyPermissionsThroughBaseResource) result).getBaseAuthorizable());
        }

        @Test
        void testNonExistentRemoteGroupPortThrowsResourceNotFoundException() {
            assertThrows(ResourceNotFoundException.class, () ->
                factory.createRemoteDataAuthorizable(NON_EXISTENT_ID)
            );
        }
    }
}
