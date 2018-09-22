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
package org.apache.nifi.websocket.example;

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
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a WebSocket server example testcase.
 */
@Ignore
public class WebSocketServerExample {

    private static Logger logger = LoggerFactory.getLogger(WebSocketServerExample.class);
    private static Server server;
    private static ServletHandler servletHandler;
    private static ServletHolder servletHolder;
    private static ServerConnector httpConnector;
    private static ServerConnector sslConnector;

    private static final Map<Integer, WebSocketServerExample> portToController = new HashMap<>();

    private Map<String, WebSocketListener> listeners = new HashMap<>();

    public class SocketListener extends WebSocketAdapter {

        public SocketListener() {
            logger.info("New instance is created: {}", this);
        }

        @Override
        public void onWebSocketConnect(Session session) {
            logger.info("Connected, {}, {}", session.getLocalAddress(), session.getRemoteAddress());
            super.onWebSocketConnect(session);

            session.getUpgradeRequest().getRequestURI();
        }

        @Override
        public void onWebSocketText(String message) {
            logger.info("Received: {}", message);

            final String resultMessage;
            if (message.startsWith("add-servlet")) {
                // Is it possible to add servlet mapping??
                final String path = message.split(":")[1].trim();
                servletHandler.addServletWithMapping(servletHolder, path);

                resultMessage = "Deployed new servlet under: " + path;
            } else {
                resultMessage = "Got message: " + message;
            }

            try {
                getSession().getRemote().sendString(resultMessage);
            } catch (IOException e) {
                logger.error("Failed to send a message back to remote.", e);
            }
        }

    }

    public WebSocketServerExample() {
        this.listeners.put("/test", new SocketListener());
        portToController.put(httpConnector.getPort(), this);
        portToController.put(sslConnector.getPort(), this);
    }

    public static class WSServlet extends WebSocketServlet implements WebSocketCreator {
        @Override
        public void configure(WebSocketServletFactory webSocketServletFactory) {
            webSocketServletFactory.setCreator(this);
        }

        @Override
        public Object createWebSocket(ServletUpgradeRequest servletUpgradeRequest, ServletUpgradeResponse servletUpgradeResponse) {
            final WebSocketServerExample testWebSocket = portToController.get(servletUpgradeRequest.getLocalPort());
            return testWebSocket.listeners.get(servletUpgradeRequest.getRequestURI().getPath());
        }
    }

    public static class ConnectionCheckServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            resp.setContentType("text/plain");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("Ok :)");
        }
    }

    @BeforeClass
    public static void setup() throws Exception {
        server = new Server(0);

        final ContextHandlerCollection handlerCollection = new ContextHandlerCollection();

        final ServletContextHandler contextHandler = new ServletContextHandler();
        servletHandler = new ServletHandler();
        contextHandler.insertHandler(servletHandler);

        handlerCollection.setHandlers(new Handler[]{contextHandler});

        server.setHandler(handlerCollection);

        httpConnector = new ServerConnector(server);
        httpConnector.setPort(50010);

        final SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePath("src/test/resources/certs/keystore.jks");
        sslContextFactory.setKeyStorePassword("passwordpassword");
        sslContextFactory.setKeyStoreType("JKS");

        final HttpConfiguration https = new HttpConfiguration();
        https.addCustomizer(new SecureRequestCustomizer());
        sslConnector = new ServerConnector(server,
                new SslConnectionFactory(sslContextFactory, "http/1.1"),
                new HttpConnectionFactory(https));
        sslConnector.setPort(50011);


        server.setConnectors(new Connector[]{httpConnector, sslConnector});

        servletHolder = servletHandler.addServletWithMapping(WSServlet.class, "/test");
        servletHolder = servletHandler.addServletWithMapping(ConnectionCheckServlet.class, "/check");

        server.start();

        logger.info("Starting server on port {} for HTTP, and {} for HTTPS", httpConnector.getLocalPort(), sslConnector.getLocalPort());


    }

    @AfterClass
    public static void teardown() throws Exception {
        logger.info("Stopping server.");
        try {
            server.stop();
        } catch (Exception e) {
            logger.error("Failed to stop Jetty server due to " + e, e);
        }
    }

    @Test
    public void test() throws Exception {
        logger.info("Waiting for a while...");
        Thread.sleep(1000_000);
    }
}
