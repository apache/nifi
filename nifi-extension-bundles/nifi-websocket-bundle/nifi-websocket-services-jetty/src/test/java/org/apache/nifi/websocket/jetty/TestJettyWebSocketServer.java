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

import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.Session.Listener.AbstractAutoDemanding;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestJettyWebSocketServer {
    private static final long TIMEOUT_SECONDS = 5;

    private static final String ROOT_ENDPOINT_ID = "/";

    private static final String IDENTIFIER = JettyWebSocketServer.class.getSimpleName();

    private static final int MAX_PORT = 65535;

    private TestRunner runner;
    private JettyWebSocketServer server;

    @BeforeEach
    public void setRunner() {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        server = new JettyWebSocketServer();
    }

    @AfterEach
    public void shutdown() {
        runner.shutdown();
    }

    @Test
    public void testValidationHashLoginService() throws Exception {
        runner.addControllerService(IDENTIFIER, server);
        runner.setProperty(server, JettyWebSocketServer.PORT, Integer.toString(MAX_PORT));
        runner.setProperty(server, JettyWebSocketServer.LOGIN_SERVICE, JettyWebSocketServer.LOGIN_SERVICE_HASH.getValue());
        runner.setProperty(server, JettyWebSocketServer.BASIC_AUTH, Boolean.TRUE.toString());
        runner.assertNotValid();
    }

    @Test
    public void testValidationSuccess() throws Exception {
        runner.addControllerService(IDENTIFIER, server);
        runner.setProperty(server, JettyWebSocketServer.PORT, Integer.toString(MAX_PORT));
        runner.assertValid(server);
    }

    @Test
    public void testWebSocketConnect() throws Exception {
        final String identifier = JettyWebSocketServer.class.getSimpleName();
        runner.addControllerService(identifier, server);
        runner.setProperty(server, JettyWebSocketServer.PORT, "0");
        runner.enableControllerService(server);

        server.registerProcessor(ROOT_ENDPOINT_ID, runner.getProcessor());

        final String command = String.class.getName();
        final AtomicBoolean connected = new AtomicBoolean();

        final WebSocketClient client = new WebSocketClient();
        final Session.Listener.AutoDemanding adapter = new TrackedAutoDemandingListener(connected);

        try {
            client.start();

            final URI uri = getWebSocketUri(server.getListeningPort());
            final Future<Session> connectSession = client.connect(adapter, uri);
            final Session session = connectSession.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            session.sendText(command, new Callback.Completable());
            session.close();

            assertTrue(connected.get(), "Connection not found");
        } finally {
            client.stop();
            runner.disableControllerService(server);
        }
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("listening-port", JettyWebSocketServer.PORT.getName()),
                Map.entry("ssl-context-service", JettyWebSocketServer.SSL_CONTEXT_SERVICE.getName()),
                Map.entry("client-authentication", JettyWebSocketServer.CLIENT_AUTH.getName()),
                Map.entry("basic-auth", JettyWebSocketServer.BASIC_AUTH.getName()),
                Map.entry("auth-path-spec", JettyWebSocketServer.AUTH_PATH_SPEC.getName()),
                Map.entry("auth-roles", JettyWebSocketServer.AUTH_ROLES.getName()),
                Map.entry("login-service", JettyWebSocketServer.LOGIN_SERVICE.getName()),
                Map.entry("users-properties-file", JettyWebSocketServer.USERS_PROPERTIES_FILE.getName()),
                Map.entry("input-buffer-size", AbstractJettyWebSocketService.INPUT_BUFFER_SIZE.getName()),
                Map.entry("max-text-message-size", AbstractJettyWebSocketService.MAX_TEXT_MESSAGE_SIZE.getName()),
                Map.entry("max-binary-message-size", AbstractJettyWebSocketService.MAX_BINARY_MESSAGE_SIZE.getName())
        );

        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        server.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);
    }

    private URI getWebSocketUri(final int port) {
        return URI.create(String.format("ws://localhost:%d", port));
    }

    public static class TrackedAutoDemandingListener extends AbstractAutoDemanding {
        private final AtomicBoolean connected;

        public TrackedAutoDemandingListener(final AtomicBoolean connected) {
            this.connected = connected;
        }

        @Override
        public void onWebSocketOpen(Session session) {
            super.onWebSocketOpen(session);
            connected.set(true);
        }

        @Override
        public void onWebSocketText(final String message) {

        }
    }
}
