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
import org.apache.nifi.websocket.WebSocketClientService;
import org.apache.nifi.websocket.WebSocketMessage;
import org.apache.nifi.websocket.WebSocketSession;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class TestConnectWebSocket {

    private void assertFlowFile(WebSocketSession webSocketSession, String serviceId, String endpointId, MockFlowFile ff, WebSocketMessage.Type messageType) {
        assertEquals(serviceId, ff.getAttribute(ConnectWebSocket.ATTR_WS_CS_ID));
        assertEquals(webSocketSession.getSessionId(), ff.getAttribute(ConnectWebSocket.ATTR_WS_SESSION_ID));
        assertEquals(endpointId, ff.getAttribute(ConnectWebSocket.ATTR_WS_ENDPOINT_ID));
        assertEquals(webSocketSession.getLocalAddress().toString(), ff.getAttribute(ConnectWebSocket.ATTR_WS_LOCAL_ADDRESS));
        assertEquals(webSocketSession.getRemoteAddress().toString(), ff.getAttribute(ConnectWebSocket.ATTR_WS_REMOTE_ADDRESS));
        assertEquals(messageType != null ? messageType.name() : null, ff.getAttribute(ConnectWebSocket.ATTR_WS_MESSAGE_TYPE));
    }

    @Test
    public void testSuccess() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ConnectWebSocket.class);
        final ConnectWebSocket processor = (ConnectWebSocket)runner.getProcessor();
        final WebSocketClientService service = mock(WebSocketClientService.class);

        final WebSocketSession webSocketSession = spy(AbstractWebSocketSession.class);
        when(webSocketSession.getSessionId()).thenReturn("ws-session-id");
        when(webSocketSession.getLocalAddress()).thenReturn(new InetSocketAddress("localhost", 12345));
        when(webSocketSession.getRemoteAddress()).thenReturn(new InetSocketAddress("example.com", 80));

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageFromServer = "message from server.";
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getTargetUri()).thenReturn("ws://example.com/web-socket");
        doAnswer(invocation -> {
            processor.connected(webSocketSession);
            // Two times.
            processor.consume(webSocketSession, textMessageFromServer);
            processor.consume(webSocketSession, textMessageFromServer);
            // Three times.
            final byte[] binaryMessage = textMessageFromServer.getBytes();
            processor.consume(webSocketSession, binaryMessage, 0, binaryMessage.length);
            processor.consume(webSocketSession, binaryMessage, 0, binaryMessage.length);
            processor.consume(webSocketSession, binaryMessage, 0, binaryMessage.length);
            return null;
        }).when(service).connect(endpointId);
        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        runner.setProperty(ConnectWebSocket.PROP_MAX_QUEUE_SIZE, "10");
        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_SERVICE, serviceId);
        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_ID, endpointId);

        runner.run();

        final List<MockFlowFile> connectedFlowFiles = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_CONNECTED);
        assertEquals(1, connectedFlowFiles.size());
        connectedFlowFiles.forEach(ff -> {
            assertFlowFile(webSocketSession, serviceId, endpointId, ff, null);
        });

        final List<MockFlowFile> textFlowFiles = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_MESSAGE_TEXT);
        assertEquals(2, textFlowFiles.size());
        textFlowFiles.forEach(ff -> {
            assertFlowFile(webSocketSession, serviceId, endpointId, ff, WebSocketMessage.Type.TEXT);
        });

        final List<MockFlowFile> binaryFlowFiles = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_MESSAGE_BINARY);
        assertEquals(3, binaryFlowFiles.size());
        binaryFlowFiles.forEach(ff -> {
            assertFlowFile(webSocketSession, serviceId, endpointId, ff, WebSocketMessage.Type.BINARY);
        });

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(6, provenanceEvents.size());
    }

    @Test
    public void testExceedQueueMaxSize() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ConnectWebSocket.class);
        final ConnectWebSocket processor = (ConnectWebSocket)runner.getProcessor();
        final WebSocketClientService service = mock(WebSocketClientService.class);

        final WebSocketSession webSocketSession = spy(AbstractWebSocketSession.class);
        when(webSocketSession.getSessionId()).thenReturn("ws-session-id");
        when(webSocketSession.getLocalAddress()).thenReturn(new InetSocketAddress("localhost", 12345));
        when(webSocketSession.getRemoteAddress()).thenReturn(new InetSocketAddress("example.com", 80));

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageReceived = "message from server.";
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getTargetUri()).thenReturn("ws://example.com/web-socket");
        doAnswer(invocation -> {
            processor.connected(webSocketSession);
            processor.consume(webSocketSession, textMessageReceived);
            // Three times.
            final byte[] binaryMessage = textMessageReceived.getBytes();
            processor.consume(webSocketSession, binaryMessage, 0, binaryMessage.length);
            processor.consume(webSocketSession, binaryMessage, 0, binaryMessage.length);
            processor.consume(webSocketSession, binaryMessage, 0, binaryMessage.length);
            return null;
        }).when(service).connect(endpointId);
        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        runner.setProperty(ConnectWebSocket.PROP_MAX_QUEUE_SIZE, "3");
        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_SERVICE, serviceId);
        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_ID, endpointId);

        runner.run();

        final List<MockFlowFile> connectedFlowFiles = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_CONNECTED);
        assertEquals(1, connectedFlowFiles.size());
        connectedFlowFiles.forEach(ff -> {
            assertFlowFile(webSocketSession, serviceId, endpointId, ff, null);
        });

        final List<MockFlowFile> textFlowFiles = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_MESSAGE_TEXT);
        assertEquals(1, textFlowFiles.size());
        textFlowFiles.forEach(ff -> {
            assertFlowFile(webSocketSession, serviceId, endpointId, ff, WebSocketMessage.Type.TEXT);
        });

        final List<MockFlowFile> binaryFlowFiles = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_MESSAGE_BINARY);
        assertEquals(1, binaryFlowFiles.size());
        binaryFlowFiles.forEach(ff -> {
            assertFlowFile(webSocketSession, serviceId, endpointId, ff, WebSocketMessage.Type.BINARY);
        });

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(3, provenanceEvents.size());
    }

    @Test
    public void testChangeQueueSize() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ConnectWebSocket.class);
        final ConnectWebSocket processor = (ConnectWebSocket)runner.getProcessor();
        final WebSocketClientService service = mock(WebSocketClientService.class);

        final WebSocketSession webSocketSession = spy(AbstractWebSocketSession.class);
        when(webSocketSession.getSessionId()).thenReturn("ws-session-id");
        when(webSocketSession.getLocalAddress()).thenReturn(new InetSocketAddress("localhost", 12345));
        when(webSocketSession.getRemoteAddress()).thenReturn(new InetSocketAddress("example.com", 80));

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageReceived = "message from server.";
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getTargetUri()).thenReturn("ws://example.com/web-socket");
        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        runner.setProperty(ConnectWebSocket.PROP_MAX_QUEUE_SIZE, "10");
        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_SERVICE, serviceId);
        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_ID, endpointId);

        runner.run(1, false);

        // Enqueue some messages, before next schedule is executed.
        for(int i = 0; i < 10; i++) {
            processor.consume(webSocketSession, textMessageReceived);
        }

        // Normally the processor has to be stopped to be configured.
        // And when it stops, it transfer all remaining messages in WebSocket message internal queue.
        // So, it's not easy to reproduce this behavior with the real NiFi UI, but if it receives messages while
        // it's being stopped, it may be possible.
        // For testing purpose, this case doesn't stop processor before changing property intentionally.
        runner.setProperty(ConnectWebSocket.PROP_MAX_QUEUE_SIZE, "9");

        try {
            runner.run();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("(9) is smaller than the number of messages pending (10)"));
        }

    }

    @Test
    public void testRemainingMessages() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ConnectWebSocket.class);
        final ConnectWebSocket processor = (ConnectWebSocket)runner.getProcessor();
        final WebSocketClientService service = mock(WebSocketClientService.class);

        final WebSocketSession webSocketSession = spy(AbstractWebSocketSession.class);
        when(webSocketSession.getSessionId()).thenReturn("ws-session-id");
        when(webSocketSession.getLocalAddress()).thenReturn(new InetSocketAddress("localhost", 12345));
        when(webSocketSession.getRemoteAddress()).thenReturn(new InetSocketAddress("example.com", 80));

        final String serviceId = "ws-service";
        final String endpointId = "client-1";
        final String textMessageReceived = "message from server.";
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getTargetUri()).thenReturn("ws://example.com/web-socket");
        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        runner.setProperty(ConnectWebSocket.PROP_MAX_QUEUE_SIZE, "10");
        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_SERVICE, serviceId);
        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_ID, endpointId);

        runner.run(1, false);

        // Enqueue some messages, before next schedule is executed.
        for(int i = 0; i < 10; i++) {
            processor.consume(webSocketSession, textMessageReceived);
        }

        // Then stop. Processor should flush remaining messages.
        processor.onUnscheduled(runner.getProcessContext());
        processor.onStopped(runner.getProcessContext());

        final List<MockFlowFile> connectedFlowFiles = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_CONNECTED);
        assertEquals(0, connectedFlowFiles.size());

        final List<MockFlowFile> textFlowFiles = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_MESSAGE_TEXT);
        assertEquals(10, textFlowFiles.size());
        textFlowFiles.forEach(ff -> {
            assertFlowFile(webSocketSession, serviceId, endpointId, ff, WebSocketMessage.Type.TEXT);
        });

        final List<MockFlowFile> binaryFlowFiles = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_MESSAGE_BINARY);
        assertEquals(0, binaryFlowFiles.size());

    }
}
