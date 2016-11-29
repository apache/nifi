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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.websocket.WebSocketConfigurationException;
import org.apache.nifi.websocket.WebSocketMessageRouter;
import org.apache.nifi.websocket.WebSocketServerService;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketPolicy;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Tags({"WebSocket", "Jetty", "server"})
@CapabilityDescription("Implementation of WebSocketServerService." +
        " This service uses Jetty WebSocket server module to provide" +
        " WebSocket session management throughout the application.")
public class JettyWebSocketServer extends AbstractJettyWebSocketService implements WebSocketServerService {

    /**
     * A global map to refer a controller service instance by requested port number.
     */
    private static final Map<Integer, JettyWebSocketServer> portToControllerService = new ConcurrentHashMap<>();

    // Allowable values for client auth
    public static final AllowableValue CLIENT_NONE = new AllowableValue("no", "No Authentication",
            "Processor will not authenticate clients. Anyone can communicate with this Processor anonymously");
    public static final AllowableValue CLIENT_WANT = new AllowableValue("want", "Want Authentication",
            "Processor will try to verify the client but if unable to verify will allow the client to communicate anonymously");
    public static final AllowableValue CLIENT_NEED = new AllowableValue("need", "Need Authentication",
            "Processor will reject communications from any client unless the client provides a certificate that is trusted by the TrustStore"
                    + "specified in the SSL Context Service");

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("client-authentication")
            .displayName("Client Authentication")
            .description("Specifies whether or not the Processor should authenticate clients. This value is ignored if the <SSL Context Service> "
                    + "Property is not specified or the SSL Context provided uses only a KeyStore and not a TrustStore.")
            .required(true)
            .allowableValues(CLIENT_NONE, CLIENT_WANT, CLIENT_NEED)
            .defaultValue(CLIENT_NONE.getValue())
            .build();

    public static final PropertyDescriptor LISTEN_PORT = new PropertyDescriptor.Builder()
            .name("listen-port")
            .displayName("Listen Port")
            .description("The port number on which this WebSocketServer listens to.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.addAll(getAbstractPropertyDescriptors());
        props.add(LISTEN_PORT);
        props.add(SSL_CONTEXT);
        props.add(CLIENT_AUTH);

        properties = Collections.unmodifiableList(props);
    }

    private WebSocketPolicy configuredPolicy;
    private Server server;
    private Integer listenPort;
    private ServletHandler servletHandler;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }


    public static class JettyWebSocketServlet extends WebSocketServlet implements WebSocketCreator {
        @Override
        public void configure(WebSocketServletFactory webSocketServletFactory) {
            webSocketServletFactory.setCreator(this);
        }

        @Override
        public Object createWebSocket(ServletUpgradeRequest servletUpgradeRequest, ServletUpgradeResponse servletUpgradeResponse) {
            final URI requestURI = servletUpgradeRequest.getRequestURI();
            final int port = requestURI.getPort();
            final JettyWebSocketServer service = portToControllerService.get(port);

            if (service == null) {
                throw new RuntimeException("No controller service is bound with port: " + port);
            }

            final String path = requestURI.getPath();
            final WebSocketMessageRouter router;
            try {
                router = service.routers.getRouterOrFail(path);
            } catch (WebSocketConfigurationException e) {
                throw new IllegalStateException("Failed to get router due to: "  + e, e);
            }

            final RoutingWebSocketListener listener = new RoutingWebSocketListener(router) {
                @Override
                public void onWebSocketConnect(Session session) {
                    final WebSocketPolicy currentPolicy = session.getPolicy();
                    currentPolicy.setInputBufferSize(service.configuredPolicy.getInputBufferSize());
                    currentPolicy.setMaxTextMessageSize(service.configuredPolicy.getMaxTextMessageSize());
                    currentPolicy.setMaxBinaryMessageSize(service.configuredPolicy.getMaxBinaryMessageSize());
                    super.onWebSocketConnect(session);
                }
            };

            return listener;
        }
    }

    @OnEnabled
    @Override
    public void startServer(final ConfigurationContext context) throws Exception {

        if (server != null && server.isRunning()) {
            getLogger().info("A WebSocket server is already running. {}", new Object[]{server});
            return;
        }

        configuredPolicy = WebSocketPolicy.newServerPolicy();
        configurePolicy(context, configuredPolicy);

        server = new Server();

        final ContextHandlerCollection handlerCollection = new ContextHandlerCollection();

        final ServletContextHandler contextHandler = new ServletContextHandler();
        servletHandler = new ServletHandler();
        contextHandler.insertHandler(servletHandler);

        handlerCollection.setHandlers(new Handler[]{contextHandler});

        server.setHandler(handlerCollection);

        listenPort = context.getProperty(LISTEN_PORT).asInteger();
        final SslContextFactory sslContextFactory = createSslFactory(context);

        final ServerConnector serverConnector = createConnector(sslContextFactory, listenPort);

        server.setConnectors(new Connector[] {serverConnector});

        servletHandler.addServletWithMapping(JettyWebSocketServlet.class, "/*");

        getLogger().info("Starting JettyWebSocketServer on port {}.", new Object[]{listenPort});
        server.start();

        portToControllerService.put(listenPort, this);
    }

    private ServerConnector createConnector(final SslContextFactory sslContextFactory, final Integer listenPort) {

        final ServerConnector serverConnector;
        if (sslContextFactory == null) {
            serverConnector = new ServerConnector(server);
        } else {
            final HttpConfiguration httpsConfiguration = new HttpConfiguration();
            httpsConfiguration.setSecureScheme("https");
            httpsConfiguration.addCustomizer(new SecureRequestCustomizer());
            serverConnector = new ServerConnector(server,
                    new SslConnectionFactory(sslContextFactory, "http/1.1"),
                    new HttpConnectionFactory(httpsConfiguration));
        }
        serverConnector.setPort(listenPort);
        return serverConnector;
    }

    private SslContextFactory createSslFactory(final ConfigurationContext context) {
        final SSLContextService sslService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);

        final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
        final boolean need;
        final boolean want;
        if (CLIENT_NEED.equals(clientAuthValue)) {
            need = true;
            want = false;
        } else if (CLIENT_WANT.equals(clientAuthValue)) {
            need = false;
            want = true;
        } else {
            need = false;
            want = false;
        }

        final SslContextFactory sslFactory = (sslService == null) ? null : createSslFactory(sslService, need, want);
        return sslFactory;
    }

    @OnDisabled
    @OnShutdown
    @Override
    public void stopServer() throws Exception {
        if (server == null) {
            return;
        }

        getLogger().info("Stopping JettyWebSocketServer.");
        server.stop();
        if (portToControllerService.containsKey(listenPort)
                && this.getIdentifier().equals(portToControllerService.get(listenPort).getIdentifier())) {
            portToControllerService.remove(listenPort);
        }
    }

}
