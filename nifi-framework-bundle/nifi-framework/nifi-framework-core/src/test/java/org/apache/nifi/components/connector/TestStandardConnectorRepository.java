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

package org.apache.nifi.components.connector;

import org.apache.nifi.nar.ExtensionManager;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStandardConnectorRepository {

    @Test
    public void testAddAndGetConnectors() {
        final StandardConnectorRepository repository = new StandardConnectorRepository();

        final ConnectorNode connector1 = mock(ConnectorNode.class);
        final ConnectorNode connector2 = mock(ConnectorNode.class);
        when(connector1.getIdentifier()).thenReturn("connector-1");
        when(connector2.getIdentifier()).thenReturn("connector-2");

        repository.addConnector(connector1);
        repository.addConnector(connector2);

        final List<ConnectorNode> connectors = repository.getConnectors();
        assertEquals(2, connectors.size());
        assertTrue(connectors.contains(connector1));
        assertTrue(connectors.contains(connector2));
    }

    @Test
    public void testRemoveConnector() {
        final StandardConnectorRepository repository = new StandardConnectorRepository();

        final ConnectorRepositoryInitializationContext initContext = mock(ConnectorRepositoryInitializationContext.class);
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        when(initContext.getExtensionManager()).thenReturn(extensionManager);
        repository.initialize(initContext);

        final Connector mockConnector = mock(Connector.class);
        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");
        when(connector.getConnector()).thenReturn(mockConnector);

        repository.addConnector(connector);
        repository.removeConnector("connector-1");

        assertEquals(0, repository.getConnectors().size());
        assertNull(repository.getConnector("connector-1"));
    }

    @Test
    public void testRestoreConnector() {
        final StandardConnectorRepository repository = new StandardConnectorRepository();

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");

        repository.restoreConnector(connector);
        assertEquals(1, repository.getConnectors().size());
        assertEquals(connector, repository.getConnector("connector-1"));
    }

    @Test
    public void testGetConnectorsReturnsNewListInstances() {
        final StandardConnectorRepository repository = new StandardConnectorRepository();

        final ConnectorNode connector1 = mock(ConnectorNode.class);
        final ConnectorNode connector2 = mock(ConnectorNode.class);
        when(connector1.getIdentifier()).thenReturn("connector-1");
        when(connector2.getIdentifier()).thenReturn("connector-2");

        repository.addConnector(connector1);
        repository.addConnector(connector2);

        final List<ConnectorNode> connectors1 = repository.getConnectors();
        final List<ConnectorNode> connectors2 = repository.getConnectors();

        assertEquals(2, connectors1.size());
        assertEquals(2, connectors2.size());
        assertEquals(connectors1, connectors2);
        assertNotSame(connectors1, connectors2);
    }

    @Test
    public void testAddConnectorWithDuplicateIdReplaces() {
        final StandardConnectorRepository repository = new StandardConnectorRepository();

        final ConnectorNode connector1 = mock(ConnectorNode.class);
        final ConnectorNode connector2 = mock(ConnectorNode.class);
        when(connector1.getIdentifier()).thenReturn("same-id");
        when(connector2.getIdentifier()).thenReturn("same-id");

        repository.addConnector(connector1);
        repository.addConnector(connector2);

        final List<ConnectorNode> connectors = repository.getConnectors();
        assertEquals(1, connectors.size());
        assertEquals(connector2, repository.getConnector("same-id"));
    }
}
