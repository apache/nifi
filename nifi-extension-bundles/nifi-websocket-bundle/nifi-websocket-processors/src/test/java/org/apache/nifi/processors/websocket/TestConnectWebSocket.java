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

import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.websocket.AbstractWebSocketSession;
import org.apache.nifi.websocket.WebSocketClientService;
import org.apache.nifi.websocket.WebSocketMessage;
import org.apache.nifi.websocket.WebSocketSession;
import org.apache.nifi.websocket.jetty.JettyWebSocketClient;
import org.apache.nifi.websocket.jetty.JettyWebSocketServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


class TestConnectWebSocket extends TestListenWebSocket {

    @Test
    public void testSuccess() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ConnectWebSocket.class);
        runner.setIncomingConnection(false);

        final ConnectWebSocket processor = (ConnectWebSocket) runner.getProcessor();

        final SharedSessionState sharedSessionState = new SharedSessionState(processor, new AtomicLong(0));
        // Use this custom session factory implementation so that createdSessions can be read from test case,
        // because MockSessionFactory doesn't expose it.
        final Set<MockProcessSession> createdSessions = new HashSet<>();
        final ProcessSessionFactory sessionFactory = () -> {
            final MockProcessSession session = new MockProcessSession(sharedSessionState, processor);
            createdSessions.add(session);
            return session;
        };

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
            processor.disconnected(webSocketSession);
            return null;
        }).when(service).connect(endpointId);
        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_SERVICE, serviceId);
        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_ID, endpointId);

        processor.onTrigger(runner.getProcessContext(), sessionFactory);

        final Map<Relationship, List<MockFlowFile>> transferredFlowFiles = getAllTransferredFlowFiles(createdSessions, processor);

        List<MockFlowFile> connectedFlowFiles = transferredFlowFiles.get(AbstractWebSocketGatewayProcessor.REL_CONNECTED);
        assertEquals(1, connectedFlowFiles.size());
        connectedFlowFiles.forEach(ff -> assertFlowFile(webSocketSession, serviceId, endpointId, ff, null));

        List<MockFlowFile> textFlowFiles = transferredFlowFiles.get(AbstractWebSocketGatewayProcessor.REL_MESSAGE_TEXT);
        assertEquals(2, textFlowFiles.size());
        textFlowFiles.forEach(ff -> assertFlowFile(webSocketSession, serviceId, endpointId, ff, WebSocketMessage.Type.TEXT));

        List<MockFlowFile> binaryFlowFiles = transferredFlowFiles.get(AbstractWebSocketGatewayProcessor.REL_MESSAGE_BINARY);
        assertEquals(3, binaryFlowFiles.size());
        binaryFlowFiles.forEach(ff -> assertFlowFile(webSocketSession, serviceId, endpointId, ff, WebSocketMessage.Type.BINARY));

        final List<ProvenanceEventRecord> provenanceEvents = sharedSessionState.getProvenanceEvents();
        assertEquals(7, provenanceEvents.size());
        assertTrue(provenanceEvents.stream().allMatch(event -> ProvenanceEventType.RECEIVE.equals(event.getEventType())));
    }

    @Test
    void testDynamicUrlsParsedFromFlowFileAndAbleToConnect() throws InitializationException {
        // Start websocket server
        final TestRunner webSocketListener = TestRunners.newTestRunner(ListenWebSocket.class);

        final String serverId = "ws-server-service";
        JettyWebSocketServer server = new JettyWebSocketServer();
        webSocketListener.addControllerService(serverId, server);
        webSocketListener.setProperty(server, JettyWebSocketServer.LISTEN_PORT, "0");
        webSocketListener.enableControllerService(server);

        webSocketListener.setProperty(ListenWebSocket.PROP_WEBSOCKET_SERVER_SERVICE, serverId);
        webSocketListener.setProperty(ListenWebSocket.PROP_SERVER_URL_PATH, "/test");

        webSocketListener.run(1, false);
        final int listeningPort = server.getListeningPort();

        final TestRunner runner = TestRunners.newTestRunner(ConnectWebSocket.class);

        final String clientId = "ws-service";
        final String endpointId = "client-1";

        MockFlowFile flowFile = getFlowFile();
        runner.enqueue(flowFile);

        JettyWebSocketClient client = new JettyWebSocketClient();


        runner.addControllerService(clientId, client);
        runner.setProperty(client, JettyWebSocketClient.WS_URI, String.format("ws://localhost:%s/${dynamicUrlPart}", listeningPort));
        runner.enableControllerService(client);

        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_SERVICE, clientId);
        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_ID, endpointId);

        runner.run(1, false);

        final List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_CONNECTED);
        assertEquals(1, flowFilesForRelationship.size());

        final List<MockFlowFile> flowFilesForSuccess = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_SUCCESS);
        assertEquals(1, flowFilesForSuccess.size());

        runner.stop();
        webSocketListener.stop();
    }

    @Test
    void testDynamicUrlsParsedFromFlowFileButNotAbleToConnect() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ConnectWebSocket.class);

        final String serviceId = "ws-service";
        final String endpointId = "client-1";

        MockFlowFile flowFile = getFlowFile();
        runner.enqueue(flowFile);

        JettyWebSocketClient service = new JettyWebSocketClient();


        runner.addControllerService(serviceId, service);
        runner.setProperty(service, JettyWebSocketClient.WS_URI, "ws://localhost/${dynamicUrlPart}");
        runner.enableControllerService(service);

        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_SERVICE, serviceId);
        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_ID, endpointId);

        runner.run(1, false);

        final List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_CONNECTED);
        assertEquals(0, flowFilesForRelationship.size());

        final List<MockFlowFile> flowFilesForSuccess = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_FAILURE);
        assertEquals(1, flowFilesForSuccess.size());

        runner.stop();
    }

    @Test
    void testDynamicUrlsParsedFromFlowFileAndAbleToConnectAndDisconnect() throws InitializationException {
        // Start websocket server
        final TestRunner webSocketListener = TestRunners.newTestRunner(ListenWebSocket.class);

        final String serverId = "ws-server-service";
        JettyWebSocketServer server = new JettyWebSocketServer();
        webSocketListener.addControllerService(serverId, server);
        webSocketListener.setProperty(server, JettyWebSocketServer.LISTEN_PORT, "0");
        webSocketListener.enableControllerService(server);

        webSocketListener.setProperty(ListenWebSocket.PROP_WEBSOCKET_SERVER_SERVICE, serverId);
        webSocketListener.setProperty(ListenWebSocket.PROP_SERVER_URL_PATH, "/test");

        webSocketListener.run(1, false);
        final int listeningPort = server.getListeningPort();

        final TestRunner runner = TestRunners.newTestRunner(ConnectWebSocket.class);

        final String clientId = "ws-service";
        final String endpointId = "client-1";

        MockFlowFile flowFile = getFlowFile();
        runner.enqueue(flowFile);

        JettyWebSocketClient client = new JettyWebSocketClient();


        runner.addControllerService(clientId, client);
        runner.setProperty(client, JettyWebSocketClient.WS_URI, String.format("ws://localhost:%s/${dynamicUrlPart}", listeningPort));
        runner.enableControllerService(client);

        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_SERVICE, clientId);
        runner.setProperty(ConnectWebSocket.PROP_WEBSOCKET_CLIENT_ID, endpointId);

        runner.run(1, false);

        final List<MockFlowFile> flowFilesForConnectedRelationship = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_CONNECTED);
        assertEquals(1, flowFilesForConnectedRelationship.size());

        final List<MockFlowFile> flowFilesForSuccess = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_SUCCESS);
        assertEquals(1, flowFilesForSuccess.size());

        webSocketListener.disableControllerService(server);

        final List<MockFlowFile> flowFilesForDisconnectedRelationship = runner.getFlowFilesForRelationship(ConnectWebSocket.REL_DISCONNECTED);
        assertEquals(1, flowFilesForDisconnectedRelationship.size());

        runner.stop();
    }

    private MockFlowFile getFlowFile() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("dynamicUrlPart", "test");
        MockFlowFile flowFile = new MockFlowFile(1L);
        flowFile.putAttributes(attributes);
        return flowFile;
    }
}
