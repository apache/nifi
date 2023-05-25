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
package org.apache.nifi.websocket.jetty;

import org.apache.nifi.websocket.WebSocketClientService;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ITJettyWebsocketReconnect {

    private ControllerServiceTestContext clientServiceContext;
    private WebSocketClientService clientService;

    @BeforeEach
    public void setup() throws Exception {
        setupClient();
    }

    @AfterEach
    public void teardown() throws Exception {
        clientService.stopClient();
    }

    private void setupClient() throws Exception {
        clientService = new JettyWebSocketTestClient();

        clientServiceContext = new ControllerServiceTestContext(clientService, "JettyWebSocketClient1");
        clientServiceContext.setCustomValue(JettyWebSocketClient.WS_URI, "ws://localhost:0/test");

        clientServiceContext.setCustomValue(JettyWebSocketClient.USER_NAME, "user2");
        clientServiceContext.setCustomValue(JettyWebSocketClient.USER_PASSWORD, "password2");

        clientService.initialize(clientServiceContext.getInitializationContext());
        clientService.startClient(clientServiceContext.getConfigurationContext());
    }

    @Test
    void testClientAttemptsToReconnect() throws Exception {
        final ITJettyWebSocketCommunication.MockWebSocketProcessor clientProcessor = mock(ITJettyWebSocketCommunication.MockWebSocketProcessor.class);
        doReturn("clientProcessor1").when(clientProcessor).getIdentifier();

        final String clientId = "client1";

        clientService.registerProcessor(clientId, clientProcessor);

        assertThrows(IOException.class,
                () -> clientService.connect(clientId)
        );

        JettyWebSocketTestClient testClientService = (JettyWebSocketTestClient) clientService;
        verify(testClientService.getMockSession(), times(3)).get(anyLong(), any(TimeUnit.class));
    }

    private static class JettyWebSocketTestClient extends JettyWebSocketClient {
        private CompletableFuture<Session> mockSession;

        public JettyWebSocketTestClient() throws ExecutionException, InterruptedException, TimeoutException {
            mockSession = mock(CompletableFuture.class);
            when(mockSession.get(anyLong(), any(TimeUnit.class))).thenThrow(new RuntimeException("Test: Connecting timed out."));
        }

        Future<Session> createWebsocketSession(RoutingWebSocketListener listener, ClientUpgradeRequest request) {
            return mockSession;
        }

        public CompletableFuture<Session> getMockSession() {
            return mockSession;
        }
    }

}
