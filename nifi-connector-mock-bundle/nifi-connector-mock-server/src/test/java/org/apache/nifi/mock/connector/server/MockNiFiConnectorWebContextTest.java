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

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.components.connector.Connector;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorRepository;
import org.apache.nifi.components.connector.FrameworkFlowContext;
import org.apache.nifi.web.NiFiConnectorWebContext;
import org.apache.nifi.web.NiFiConnectorWebContext.ConnectorWebContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MockNiFiConnectorWebContextTest {

    private static final String CONNECTOR_ID = "test-connector";

    @Mock
    private ConnectorRepository connectorRepository;

    @Mock
    private ConnectorNode connectorNode;

    @Mock
    private Connector connector;

    @Mock
    private FrameworkFlowContext workingFlowContext;

    @Mock
    private FrameworkFlowContext activeFlowContext;

    @Test
    void testGetConnectorWebContextReturnsConnectorAndFlowContexts() {
        when(connectorRepository.getConnector(CONNECTOR_ID)).thenReturn(connectorNode);
        when(connectorNode.getConnector()).thenReturn(connector);
        when(connectorNode.getWorkingFlowContext()).thenReturn(workingFlowContext);
        when(connectorNode.getActiveFlowContext()).thenReturn(activeFlowContext);

        final NiFiConnectorWebContext context = new MockNiFiConnectorWebContext(connectorRepository);
        final ConnectorWebContext<Connector> result = context.getConnectorWebContext(CONNECTOR_ID);

        assertNotNull(result);
        assertEquals(connector, result.connector());
        assertEquals(workingFlowContext, result.workingFlowContext());
        assertEquals(activeFlowContext, result.activeFlowContext());
    }

    @Test
    void testGetConnectorWebContextThrowsForUnknownConnector() {
        when(connectorRepository.getConnector("unknown-id")).thenReturn(null);

        final NiFiConnectorWebContext context = new MockNiFiConnectorWebContext(connectorRepository);

        assertThrows(IllegalArgumentException.class, () -> context.getConnectorWebContext("unknown-id"));
    }
}
