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

import org.apache.nifi.processor.Processor;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestJettyWebSocketServer {
    private static final long TIMEOUT_SECONDS = 5;

    private static final String ROOT_ENDPOINT_ID = "/";

    private static final String IDENTIFIER = JettyWebSocketServer.class.getSimpleName();

    private static final int MAX_PORT = 65535;

    private TestRunner runner;

    @Before
    public void setRunner() {
        final Processor processor = mock(Processor.class);
        runner = TestRunners.newTestRunner(processor);
    }

    @After
    public void shutdown() {
        runner.shutdown();
    }

    @Test
    public void testValidationHashLoginService() throws Exception {
        final JettyWebSocketServer server = new JettyWebSocketServer();
        runner.addControllerService(IDENTIFIER, server);
        runner.setProperty(server, JettyWebSocketServer.LISTEN_PORT, Integer.toString(MAX_PORT));
        runner.setProperty(server, JettyWebSocketServer.LOGIN_SERVICE, JettyWebSocketServer.LOGIN_SERVICE_HASH.getValue());
        runner.setProperty(server, JettyWebSocketServer.BASIC_AUTH, Boolean.TRUE.toString());
        runner.assertNotValid();
    }

    @Test
    public void testValidationSuccess() throws Exception {
        final JettyWebSocketServer server = new JettyWebSocketServer();
        runner.addControllerService(IDENTIFIER, server);
        runner.setProperty(server, JettyWebSocketServer.LISTEN_PORT, Integer.toString(MAX_PORT));
        runner.assertValid(server);
    }

    @Test
    public void testWebSocketConnect() throws Exception {
        final int port = NetworkUtils.availablePort();

        final String identifier = JettyWebSocketServer.class.getSimpleName();
        final JettyWebSocketServer server = new JettyWebSocketServer();
        runner.addControllerService(identifier, server);
        runner.setProperty(server, JettyWebSocketServer.LISTEN_PORT, Integer.toString(port));
        runner.enableControllerService(server);

        server.registerProcessor(ROOT_ENDPOINT_ID, runner.getProcessor());

        final String command = String.class.getName();
        final AtomicBoolean connected = new AtomicBoolean();

        final WebSocketClient client = new WebSocketClient();
        final WebSocketAdapter adapter = new WebSocketAdapter() {
            @Override
            public void onWebSocketConnect(Session session) {
                super.onWebSocketConnect(session);
                connected.set(true);
            }

            @Override
            public void onWebSocketText(final String message) {

            }
        };
        try {
            client.start();

            final URI uri = getWebSocketUri(port);
            final Future<Session> connectSession = client.connect(adapter, uri);
            final Session session = connectSession.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            session.getRemote().sendString(command);
            session.close();

            assertTrue("Connection not found", connected.get());
        } finally {
            client.stop();
            runner.disableControllerService(server);
        }
    }

    private URI getWebSocketUri(final int port) {
        return URI.create(String.format("ws://localhost:%d", port));
    }
}
