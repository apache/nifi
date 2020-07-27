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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.websocket.WebSocketClientService;
import org.apache.nifi.websocket.WebSocketConfigurationException;
import org.apache.nifi.websocket.WebSocketMessageRouter;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@Tags({"WebSocket", "Jetty", "client"})
@CapabilityDescription("Implementation of WebSocketClientService." +
        " This service uses Jetty WebSocket client module to provide" +
        " WebSocket session management throughout the application.")
public class JettyWebSocketClient extends AbstractJettyWebSocketService implements WebSocketClientService {

    public static final PropertyDescriptor WS_URI = new PropertyDescriptor.Builder()
            .name("websocket-uri")
            .displayName("WebSocket URI")
            .description("The WebSocket URI this client connects to.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .addValidator((subject, input, context) -> {
                final ValidationResult.Builder result = new ValidationResult.Builder()
                        .valid(input.startsWith("/"))
                        .subject(subject);

                if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                    result.explanation("Expression Language Present").valid(true);
                } else {
                    result.explanation("Protocol should be either 'ws' or 'wss'.")
                        .valid(input.startsWith("ws://") || input.startsWith("wss://"));
                }

                return result.build();
            })
            .build();

    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("connection-timeout")
            .displayName("Connection Timeout")
            .description("The timeout to connect the WebSocket URI.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("3 sec")
            .build();

    public static final PropertyDescriptor SESSION_MAINTENANCE_INTERVAL = new PropertyDescriptor.Builder()
            .name("session-maintenance-interval")
            .displayName("Session Maintenance Interval")
            .description("The interval between session maintenance activities." +
                    " A WebSocket session established with a WebSocket server can be terminated due to different reasons" +
                    " including restarting the WebSocket server or timing out inactive sessions." +
                    " This session maintenance activity is periodically executed in order to reconnect those lost sessions," +
                    " so that a WebSocket client can reuse the same session id transparently after it reconnects successfully. " +
                    " The maintenance activity is executed until corresponding processors or this controller service is stopped.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 sec")
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.addAll(getAbstractPropertyDescriptors());
        props.add(WS_URI);
        props.add(SSL_CONTEXT);
        props.add(CONNECTION_TIMEOUT);
        props.add(SESSION_MAINTENANCE_INTERVAL);

        properties = Collections.unmodifiableList(props);
    }

    private WebSocketClient client;
    private URI webSocketUri;
    private long connectionTimeoutMillis;
    private volatile ScheduledExecutorService sessionMaintenanceScheduler;
    private final ReentrantLock connectionLock = new ReentrantLock();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    @Override
    public void startClient(final ConfigurationContext context) throws Exception{

        final SSLContextService sslService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);
        SslContextFactory sslContextFactory = null;
        if (sslService != null) {
            sslContextFactory = createSslFactory(sslService, false, false);
        }
        client = new WebSocketClient(sslContextFactory);

        configurePolicy(context, client.getPolicy());

        client.start();
        activeSessions.clear();

        webSocketUri = new URI(context.getProperty(WS_URI).getValue());
        connectionTimeoutMillis = context.getProperty(CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);

        final Long sessionMaintenanceInterval = context.getProperty(SESSION_MAINTENANCE_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);

        sessionMaintenanceScheduler = Executors.newSingleThreadScheduledExecutor();
        sessionMaintenanceScheduler.scheduleAtFixedRate(() -> {
            try {
                maintainSessions();
            } catch (final Exception e) {
                getLogger().warn("Failed to maintain sessions due to {}", new Object[]{e}, e);
            }
        }, sessionMaintenanceInterval, sessionMaintenanceInterval, TimeUnit.MILLISECONDS);
    }

    @OnDisabled
    @OnShutdown
    @Override
    public void stopClient() throws Exception {
        activeSessions.clear();

        if (sessionMaintenanceScheduler != null) {
            try {
                sessionMaintenanceScheduler.shutdown();
            } catch (Exception e) {
                getLogger().warn("Failed to shutdown session maintainer due to {}", new Object[]{e}, e);
            }
            sessionMaintenanceScheduler = null;
        }

        if (client == null) {
            return;
        }

        client.stop();
        client = null;
    }

    @Override
    public void connect(final String clientId) throws IOException {
        connect(clientId, null);
    }

    private void connect(final String clientId, String sessionId) throws IOException {

        connectionLock.lock();

        try {
            final WebSocketMessageRouter router;
            try {
                router = routers.getRouterOrFail(clientId);
            } catch (WebSocketConfigurationException e) {
                throw new IllegalStateException("Failed to get router due to: "  + e, e);
            }
            final RoutingWebSocketListener listener = new RoutingWebSocketListener(router);
            listener.setSessionId(sessionId);

            final ClientUpgradeRequest request = new ClientUpgradeRequest();
            final Future<Session> connect = client.connect(listener, webSocketUri, request);
            getLogger().info("Connecting to : {}", new Object[]{webSocketUri});

            final Session session;
            try {
                session = connect.get(connectionTimeoutMillis, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new IOException("Failed to connect " + webSocketUri + " due to: " + e, e);
            }
            getLogger().info("Connected, session={}", new Object[]{session});
            activeSessions.put(clientId, listener.getSessionId());

        } finally {
            connectionLock.unlock();
        }

    }

    private Map<String, String> activeSessions = new ConcurrentHashMap<>();

    void maintainSessions() throws Exception {
        if (client == null) {
            return;
        }

        connectionLock.lock();

        final ComponentLog logger = getLogger();
        try {
            // Loop through existing sessions and reconnect.
            for (String clientId : activeSessions.keySet()) {
                final WebSocketMessageRouter router;
                try {
                    router = routers.getRouterOrFail(clientId);
                } catch (final WebSocketConfigurationException e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("The clientId {} is no longer active. Discarding the clientId.", new Object[]{clientId});
                    }
                    activeSessions.remove(clientId);
                    continue;
                }

                final String sessionId = activeSessions.get(clientId);
                // If this session is still alive, do nothing.
                if (!router.containsSession(sessionId)) {
                    // This session is no longer active, reconnect it.
                    // If it fails, the sessionId will remain in activeSessions, and retries later.
                    // This reconnect attempt is continued until user explicitly stops a processor or this controller service.
                    connect(clientId, sessionId);
                }
            }
        } finally {
            connectionLock.unlock();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Session maintenance completed. activeSessions={}", new Object[]{activeSessions});
        }
    }

    @Override
    public String getTargetUri() {
        return webSocketUri.toString();
    }
}
