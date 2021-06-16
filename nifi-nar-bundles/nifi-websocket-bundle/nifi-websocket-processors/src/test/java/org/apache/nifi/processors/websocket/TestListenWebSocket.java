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
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.websocket.AbstractWebSocketSession;
import org.apache.nifi.websocket.WebSocketMessage;
import org.apache.nifi.websocket.WebSocketServerService;
import org.apache.nifi.websocket.WebSocketSession;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_CS_ID;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_ENDPOINT_ID;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_LOCAL_ADDRESS;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_MESSAGE_TYPE;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_REMOTE_ADDRESS;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_SESSION_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class TestListenWebSocket {

    protected void assertFlowFile(WebSocketSession webSocketSession, String serviceId, String endpointId, MockFlowFile ff, WebSocketMessage.Type messageType) {
        assertEquals(serviceId, ff.getAttribute(ATTR_WS_CS_ID));
        assertEquals(webSocketSession.getSessionId(), ff.getAttribute(ATTR_WS_SESSION_ID));
        assertEquals(endpointId, ff.getAttribute(ATTR_WS_ENDPOINT_ID));
        assertEquals(webSocketSession.getLocalAddress().toString(), ff.getAttribute(ATTR_WS_LOCAL_ADDRESS));
        assertEquals(webSocketSession.getRemoteAddress().toString(), ff.getAttribute(ATTR_WS_REMOTE_ADDRESS));
        assertEquals(messageType != null ? messageType.name() : null, ff.getAttribute(ATTR_WS_MESSAGE_TYPE));
    }

    protected Map<Relationship, List<MockFlowFile>> getAllTransferredFlowFiles(final Collection<MockProcessSession> processSessions, final Processor processor) {
        final Map<Relationship, List<MockFlowFile>> flowFiles = new HashMap<>();

        processSessions.forEach(session -> {
            processor.getRelationships().forEach(rel -> {
                List<MockFlowFile> relFlowFiles = flowFiles.get(rel);
                if (relFlowFiles == null) {
                    relFlowFiles = new ArrayList<>();
                    flowFiles.put(rel, relFlowFiles);
                }
                relFlowFiles.addAll(session.getFlowFilesForRelationship(rel));
            });
        });

        return flowFiles;
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

        runner.setProperty(ListenWebSocket.PROP_WEBSOCKET_SERVER_SERVICE, serviceId);
        runner.setProperty(ListenWebSocket.PROP_SERVER_URL_PATH, endpointId);

        try {
            runner.run();
            fail("Should fail with validation error.");
        } catch (AssertionError e) {
            assertTrue(e.toString().contains("'server-url-path' is invalid because Must starts with"));
        }

    }

    @Test
    public void testSuccess() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ListenWebSocket.class);
        final ListenWebSocket processor = (ListenWebSocket)runner.getProcessor();

        final SharedSessionState sharedSessionState = new SharedSessionState(processor, new AtomicLong(0));
        // Use this custom session factory implementation so that createdSessions can be read from test case,
        // because MockSessionFactory doesn't expose it.
        final Set<MockProcessSession> createdSessions = new HashSet<>();
        final ProcessSessionFactory sessionFactory = () -> {
            final MockProcessSession session = new MockProcessSession(sharedSessionState, processor);
            createdSessions.add(session);
            return session;
        };

        final WebSocketServerService service = mock(WebSocketServerService.class);

        final WebSocketSession webSocketSession = spy(AbstractWebSocketSession.class);
        when(webSocketSession.getSessionId()).thenReturn("ws-session-id");
        when(webSocketSession.getLocalAddress()).thenReturn(new InetSocketAddress("localhost", 12345));
        when(webSocketSession.getRemoteAddress()).thenReturn(new InetSocketAddress("example.com", 80));

        final String serviceId = "ws-service";
        final String endpointId = "/test";
        final String textMessageReceived = "message from server.";
        final AtomicReference<Boolean> registered = new AtomicReference<>(false);
        when(service.getIdentifier()).thenReturn(serviceId);
        doAnswer(invocation -> {
            registered.set(true);
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
        doAnswer(invocation -> registered.get())
                .when(service).isProcessorRegistered(eq(endpointId), eq(processor));
        doAnswer(invocation -> {
            registered.set(false);
            return null;
        }).when(service).deregisterProcessor(eq(endpointId), eq(processor));

        runner.addControllerService(serviceId, service);

        runner.enableControllerService(service);

        runner.setProperty(ListenWebSocket.PROP_WEBSOCKET_SERVER_SERVICE, serviceId);
        runner.setProperty(ListenWebSocket.PROP_SERVER_URL_PATH, endpointId);

        processor.onTrigger(runner.getProcessContext(), sessionFactory);

        Map<Relationship, List<MockFlowFile>> transferredFlowFiles = getAllTransferredFlowFiles(createdSessions, processor);

        List<MockFlowFile> connectedFlowFiles = transferredFlowFiles.get(AbstractWebSocketGatewayProcessor.REL_CONNECTED);
        assertEquals(1, connectedFlowFiles.size());
        connectedFlowFiles.forEach(ff -> {
            assertFlowFile(webSocketSession, serviceId, endpointId, ff, null);
        });

        List<MockFlowFile> textFlowFiles = transferredFlowFiles.get(AbstractWebSocketGatewayProcessor.REL_MESSAGE_TEXT);
        assertEquals(2, textFlowFiles.size());
        textFlowFiles.forEach(ff -> {
            assertFlowFile(webSocketSession, serviceId, endpointId, ff, WebSocketMessage.Type.TEXT);
        });

        List<MockFlowFile> binaryFlowFiles = transferredFlowFiles.get(AbstractWebSocketGatewayProcessor.REL_MESSAGE_BINARY);
        assertEquals(3, binaryFlowFiles.size());
        binaryFlowFiles.forEach(ff -> {
            assertFlowFile(webSocketSession, serviceId, endpointId, ff, WebSocketMessage.Type.BINARY);
        });

        final List<ProvenanceEventRecord> provenanceEvents = sharedSessionState.getProvenanceEvents();
        assertEquals(6, provenanceEvents.size());
        assertTrue(provenanceEvents.stream().allMatch(event -> ProvenanceEventType.RECEIVE.equals(event.getEventType())));

        runner.clearTransferState();
        runner.clearProvenanceEvents();
        createdSessions.clear();
        assertEquals(0, createdSessions.size());

        // Simulate that the processor has started, and it get's triggered again
        processor.onTrigger(runner.getProcessContext(), sessionFactory);
        assertEquals("No session should be created", 0, createdSessions.size());

        // Simulate that the processor is stopped.
        processor.onStopped(runner.getProcessContext());
        assertEquals("No session should be created", 0, createdSessions.size());

        // Simulate that the processor is restarted.
        // And the mock service will emit consume msg events.
        processor.onTrigger(runner.getProcessContext(), sessionFactory);
        assertEquals("Processor should register it with the service again", 6, createdSessions.size());
    }

}
