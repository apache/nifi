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
package org.apache.nifi.processors.websocket;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.websocket.AbstractWebSocketSession;
import org.apache.nifi.websocket.WebSocketMessage;
import org.apache.nifi.websocket.WebSocketServerService;
import org.apache.nifi.websocket.WebSocketSession;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class TestListenWebSocket {

    private void assertFlowFile(WebSocketSession webSocketSession, String serviceId, String endpointId, MockFlowFile ff, WebSocketMessage.Type messageType) {
        assertEquals(serviceId, ff.getAttribute(ListenWebSocket.ATTR_WS_CS_ID));
        assertEquals(webSocketSession.getSessionId(), ff.getAttribute(ListenWebSocket.ATTR_WS_SESSION_ID));
        assertEquals(endpointId, ff.getAttribute(ListenWebSocket.ATTR_WS_ENDPOINT_ID));
        assertEquals(webSocketSession.getLocalAddress().toString(), ff.getAttribute(ListenWebSocket.ATTR_WS_LOCAL_ADDRESS));
        assertEquals(webSocketSession.getRemoteAddress().toString(), ff.getAttribute(ListenWebSocket.ATTR_WS_REMOTE_ADDRESS));
        assertEquals(messageType != null ? messageType.name() : null, ff.getAttribute(ListenWebSocket.ATTR_WS_MESSAGE_TYPE));
    }

    @Test
    public void testValidationError() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ListenWebSocket.class);
        final WebSocketServerService service = mock(WebSocketServerService.class);

        final String serviceId = "ws-service";
        final String endpointId = "test";
        when(service.getIdentifier()).thenReturn(serviceId);
        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        runner.setProperty(ListenWebSocket.PROP_MAX_QUEUE_SIZE, "10");
        runner.setProperty(ListenWebSocket.PROP_WEBSOCKET_SERVER_SERVICE, serviceId);
        runner.setProperty(ListenWebSocket.PROP_SERVER_URL_PATH, endpointId);

        try {
            runner.run();
            fail("Should fail with validation error.");
        } catch (AssertionError e) {
        }

    }

    @Test
    public void testSuccess() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ListenWebSocket.class);
        final ListenWebSocket processor = (ListenWebSocket)runner.getProcessor();
        final WebSocketServerService service = mock(WebSocketServerService.class);

        final WebSocketSession webSocketSession = spy(AbstractWebSocketSession.class);
        when(webSocketSession.getSessionId()).thenReturn("ws-session-id");
        when(webSocketSession.getLocalAddress()).thenReturn(new InetSocketAddress("localhost", 12345));
        when(webSocketSession.getRemoteAddress()).thenReturn(new InetSocketAddress("example.com", 80));

        final String serviceId = "ws-service";
        final String endpointId = "/test";
        final String textMessageReceived = "message from server.";
        when(service.getIdentifier()).thenReturn(serviceId);
        doAnswer(invocation -> {
            processor.connected(webSocketSession);
            // Two times.
            processor.consume(webSocketSession, textMessageReceived);
            processor.consume(webSocketSession, textMessageReceived);
            // Three times.
            final byte[] binaryMessage = textMessageReceived.getBytes();
            processor.consume(webSocketSession, binaryMessage, 0, binaryMessage.length);
            processor.consume(webSocketSession, binaryMessage, 0, binaryMessage.length);
            processor.consume(webSocketSession, binaryMessage, 0, binaryMessage.length);
            return null;
        }).when(service).registerProcessor(endpointId, processor);
        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        runner.setProperty(ListenWebSocket.PROP_MAX_QUEUE_SIZE, "10");
        runner.setProperty(ListenWebSocket.PROP_WEBSOCKET_SERVER_SERVICE, serviceId);
        runner.setProperty(ListenWebSocket.PROP_SERVER_URL_PATH, endpointId);

        runner.run();

        final List<MockFlowFile> connectedFlowFiles = runner.getFlowFilesForRelationship(ListenWebSocket.REL_CONNECTED);
        assertEquals(1, connectedFlowFiles.size());
        connectedFlowFiles.forEach(ff -> {
            assertFlowFile(webSocketSession, serviceId, endpointId, ff, null);
        });

        final List<MockFlowFile> textFlowFiles = runner.getFlowFilesForRelationship(ListenWebSocket.REL_MESSAGE_TEXT);
        assertEquals(2, textFlowFiles.size());
        textFlowFiles.forEach(ff -> {
            assertFlowFile(webSocketSession, serviceId, endpointId, ff, WebSocketMessage.Type.TEXT);
        });

        final List<MockFlowFile> binaryFlowFiles = runner.getFlowFilesForRelationship(ListenWebSocket.REL_MESSAGE_BINARY);
        assertEquals(3, binaryFlowFiles.size());
        binaryFlowFiles.forEach(ff -> {
            assertFlowFile(webSocketSession, serviceId, endpointId, ff, WebSocketMessage.Type.BINARY);
        });

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(6, provenanceEvents.size());
    }

}
