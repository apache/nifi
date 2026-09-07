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

import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.connectable.FlowFileTransferCounts;
import org.apache.nifi.engine.FlowEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardConnectorMockServerTest {

    @Mock
    private ConnectorNode connectorNode;

    @Mock
    private FlowEngine flowEngine;

    @Mock
    private Future<Void> startFuture;

    @InjectMocks
    private StandardConnectorMockServer server;

    @Test
    void testStartConnectorPropagatesFailureAndRequestsStop() {
        final IllegalStateException startFailure = new IllegalStateException("Connector is not valid");
        when(connectorNode.getFlowFileTransferCounts()).thenReturn(new FlowFileTransferCounts(0, 0, 0, 0));
        when(connectorNode.start(flowEngine)).thenReturn(CompletableFuture.failedFuture(startFailure));

        final IllegalStateException exception = assertThrows(IllegalStateException.class, server::startConnector);

        assertEquals("Failed to start Connector", exception.getMessage());
        assertSame(startFailure, exception.getCause());
        verify(connectorNode).stop(flowEngine);
    }

    @Test
    void testStartConnectorDoesNotWaitForPendingStart() throws Exception {
        when(connectorNode.getFlowFileTransferCounts()).thenReturn(new FlowFileTransferCounts(0, 0, 0, 0));
        when(connectorNode.start(flowEngine)).thenReturn(startFuture);
        when(startFuture.isDone()).thenReturn(false);

        server.startConnector();

        verify(startFuture).isDone();
        verify(startFuture, never()).get();
        verify(startFuture, never()).get(anyLong(), any());
        verify(connectorNode, never()).stop(flowEngine);
    }
}
