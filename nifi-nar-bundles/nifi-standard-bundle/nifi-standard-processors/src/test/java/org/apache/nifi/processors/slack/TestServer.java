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
package org.apache.nifi.processors.slack;

import java.util.Map;

import org.apache.nifi.ssl.StandardRestrictedSSLContextService;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * Test server to assist with unit tests that requires a server to be stood up.
 */
public class TestServer {

    public static final String NEED_CLIENT_AUTH = "clientAuth";

    private Server jetty;
    private boolean secure = false;

    /**
     * Creates the test server.
     */
    public TestServer() {
        createServer(null);
    }

    /**
     * Creates the test server.
     *
     * @param sslProperties SSLProps to be used in the secure connection. The keys should should use the StandardSSLContextService properties.
     */
    public TestServer(final Map<String, String> sslProperties) {
        createServer(sslProperties);
    }

    private void createServer(final Map<String, String> sslProperties) {
        jetty = new Server();

        // create the unsecure connector
        createConnector();

        // create the secure connector if sslProperties are specified
        if (sslProperties != null) {
            createSecureConnector(sslProperties);
        }

        jetty.setHandler(new HandlerCollection(true));
    }

    /**
     * Creates the http connection
     */
    private void createConnector() {
        final ServerConnector http = new ServerConnector(jetty);
        http.setPort(0);
        // Severely taxed environments may have significant delays when executing.
        http.setIdleTimeout(30000L);
        jetty.addConnector(http);
    }

    private void createSecureConnector(final Map<String, String> sslProperties) {
        SslContextFactory ssl = new SslContextFactory();

        if (sslProperties.get(StandardRestrictedSSLContextService.KEYSTORE.getName()) != null) {
            ssl.setKeyStorePath(sslProperties.get(StandardRestrictedSSLContextService.KEYSTORE.getName()));
            ssl.setKeyStorePassword(sslProperties.get(StandardRestrictedSSLContextService.KEYSTORE_PASSWORD.getName()));
            ssl.setKeyStoreType(sslProperties.get(StandardRestrictedSSLContextService.KEYSTORE_TYPE.getName()));
        }

        if (sslProperties.get(StandardRestrictedSSLContextService.TRUSTSTORE.getName()) != null) {
            ssl.setTrustStorePath(sslProperties.get(StandardRestrictedSSLContextService.TRUSTSTORE.getName()));
            ssl.setTrustStorePassword(sslProperties.get(StandardRestrictedSSLContextService.TRUSTSTORE_PASSWORD.getName()));
            ssl.setTrustStoreType(sslProperties.get(StandardRestrictedSSLContextService.TRUSTSTORE_TYPE.getName()));
        }

        final String clientAuth = sslProperties.get(NEED_CLIENT_AUTH);
        if (clientAuth == null) {
            ssl.setNeedClientAuth(true);
        } else {
            ssl.setNeedClientAuth(Boolean.parseBoolean(clientAuth));
        }

        // build the connector
        final ServerConnector https = new ServerConnector(jetty, ssl);

        // set host and port
        https.setPort(0);
        // Severely taxed environments may have significant delays when executing.
        https.setIdleTimeout(30000L);

        // add the connector
        jetty.addConnector(https);

        // mark secure as enabled
        secure = true;
    }

    public void clearHandlers() {
        HandlerCollection hc = (HandlerCollection) jetty.getHandler();
        Handler[] ha = hc.getHandlers();
        if (ha != null) {
            for (Handler h : ha) {
                hc.removeHandler(h);
            }
        }
    }

    public void addHandler(Handler handler) {
        ((HandlerCollection) jetty.getHandler()).addHandler(handler);
    }

    public void startServer() throws Exception {
        jetty.start();
    }

    public void shutdownServer() throws Exception {
        jetty.stop();
        jetty.destroy();
    }

    private int getPort() {
        if (!jetty.isStarted()) {
            throw new IllegalStateException("Jetty server not started");
        }
        return ((ServerConnector) jetty.getConnectors()[0]).getLocalPort();
    }

    private int getSecurePort() {
        if (!jetty.isStarted()) {
            throw new IllegalStateException("Jetty server not started");
        }
        return ((ServerConnector) jetty.getConnectors()[1]).getLocalPort();
    }

    public String getUrl() {
        return "http://localhost:" + getPort();
    }

    public String getSecureUrl() {
        String url = null;
        if (secure) {
            url = "https://localhost:" + getSecurePort();
        }
        return url;
    }
}
