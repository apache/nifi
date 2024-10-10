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
package org.apache.nifi.jetty.configuration.connector;

import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http.HttpScheme;
import org.eclipse.jetty.http2.HTTP2Cipher;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Standard implementation of Server Connector Factory supporting HTTP/2 and HTTP/1.1 with TLS or simple HTTP/1.1
 */
public class StandardServerConnectorFactory implements ServerConnectorFactory {
    private static final boolean SEND_SERVER_VERSION = false;

    private static final String[] INCLUDE_ALL_SECURITY_PROTOCOLS = new String[0];

    private static final Set<ApplicationLayerProtocol> DEFAULT_APPLICATION_LAYER_PROTOCOLS = Collections.singleton(ApplicationLayerProtocol.HTTP_1_1);

    private static final String ALPN_PROTOCOL = "alpn";

    private static final String[] APPLICATION_PROTOCOLS = new String[]{
            ALPN_PROTOCOL,
            ApplicationLayerProtocol.H2.getProtocol(),
            ApplicationLayerProtocol.HTTP_1_1.getProtocol()
    };

    private final Server server;

    private final int port;

    private Set<ApplicationLayerProtocol> applicationLayerProtocols = DEFAULT_APPLICATION_LAYER_PROTOCOLS;

    private SSLContext sslContext;

    private boolean needClientAuth;

    private boolean wantClientAuth;

    private String[] includeSecurityProtocols = INCLUDE_ALL_SECURITY_PROTOCOLS;

    private int requestHeaderSize = 8192;

    /**
     * Standard Server Connector Factory Constructor with required properties
     *
     * @param server Jetty Server
     * @param port Secure Port Number
     */
    public StandardServerConnectorFactory(
            final Server server,
            final int port
    ) {
        this.server = Objects.requireNonNull(server, "Server required");
        this.port = port;
    }

    /**
     * Get Server Connector configured with HTTP/2 and ALPN as well as fallback to HTTP/1.1 with TLS
     *
     * @return Secure Server Connector
     */
    @Override
    public ServerConnector getServerConnector() {
        final HttpConfiguration httpConfiguration = getHttpConfiguration();
        final HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory(httpConfiguration);

        final ServerConnector serverConnector;
        if (sslContext == null) {
            serverConnector = new ServerConnector(server, httpConnectionFactory);
        } else {
            final List<ConnectionFactory> connectionFactories = new ArrayList<>();
            if (applicationLayerProtocols.contains(ApplicationLayerProtocol.H2)) {
                final ALPNServerConnectionFactory alpnServerConnectionFactory = new ALPNServerConnectionFactory(APPLICATION_PROTOCOLS);
                final HTTP2ServerConnectionFactory http2ServerConnectionFactory = new HTTP2ServerConnectionFactory(httpConfiguration);

                connectionFactories.add(alpnServerConnectionFactory);
                connectionFactories.add(http2ServerConnectionFactory);
            }
            // Add HTTP/1.1 Connection Factory after HTTP/2
            if (applicationLayerProtocols.contains(ApplicationLayerProtocol.HTTP_1_1)) {
                connectionFactories.add(httpConnectionFactory);
            }

            // SslConnectionFactory must be first and must indicate the next protocol
            final String nextProtocol = connectionFactories.get(0).getProtocol();
            final SslConnectionFactory sslConnectionFactory = new SslConnectionFactory(getSslContextFactory(), nextProtocol);
            connectionFactories.add(0, sslConnectionFactory);

            final ConnectionFactory[] factories = connectionFactories.toArray(new ConnectionFactory[0]);
            serverConnector = new ServerConnector(server, factories);
        }

        serverConnector.setPort(port);
        return serverConnector;
    }

    /**
     * Set SSL Context enables TLS communication
     *
     * @param sslContext SSL Context
     */
    public void setSslContext(final SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    /**
     * Set Need Client Authentication requires clients to provide certificates for mutual TLS
     *
     * @param needClientAuth Need Client Authentication status
     */
    public void setNeedClientAuth(final boolean needClientAuth) {
        this.needClientAuth = needClientAuth;
    }

    /**
     * Set Want Client Authentication requests clients to provide certificates for mutual TLS but does not require certificates
     *
     * @param wantClientAuth Want Client Authentication status
     */
    public void setWantClientAuth(final boolean wantClientAuth) {
        this.wantClientAuth = wantClientAuth;
    }

    /**
     * Set Include Security Protocols limits enabled TLS Protocols to the values provided
     *
     * @param includeSecurityProtocols Security Protocols with null or empty enabling all standard TLS protocol versions
     */
    public void setIncludeSecurityProtocols(final String[] includeSecurityProtocols) {
        this.includeSecurityProtocols = includeSecurityProtocols == null ? INCLUDE_ALL_SECURITY_PROTOCOLS : includeSecurityProtocols;
    }

    /**
     * Set Application Layer Protocols applicable when TLS is enabled
     *
     * @param applicationLayerProtocols Protocols requires at one Application Layer Protocol
     */
    public void setApplicationLayerProtocols(final Set<ApplicationLayerProtocol> applicationLayerProtocols) {
        if (Objects.requireNonNull(applicationLayerProtocols, "Application Layer Protocols required").isEmpty()) {
            throw new IllegalArgumentException("Application Layer Protocols not specified");
        }
        this.applicationLayerProtocols = applicationLayerProtocols;
    }

    /**
     * Set the maximum HTTP request header size. The default is 8 KB.
     *
     * @param requestHeaderSize maximum HTTP request header size
     */
    public void setRequestHeaderSize(int requestHeaderSize) {
        this.requestHeaderSize = requestHeaderSize;
    }

    protected Server getServer() {
        return server;
    }

    protected HttpConfiguration getHttpConfiguration() {
        final HttpConfiguration httpConfiguration = new HttpConfiguration();
        httpConfiguration.setRequestHeaderSize(requestHeaderSize);

        if (sslContext != null) {
            httpConfiguration.setSecurePort(port);
            httpConfiguration.setSecureScheme(HttpScheme.HTTPS.asString());
            httpConfiguration.setSendServerVersion(SEND_SERVER_VERSION);

            final SecureRequestCustomizer secureRequestCustomizer = new SecureRequestCustomizer();
            httpConfiguration.addCustomizer(secureRequestCustomizer);
        }

        return httpConfiguration;
    }

    protected SslContextFactory.Server getSslContextFactory() {
        final SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
        sslContextFactory.setSslContext(sslContext);
        sslContextFactory.setNeedClientAuth(needClientAuth);
        sslContextFactory.setWantClientAuth(wantClientAuth);
        sslContextFactory.setIncludeProtocols(includeSecurityProtocols);

        if (applicationLayerProtocols.contains(ApplicationLayerProtocol.H2)) {
            sslContextFactory.setCipherComparator(HTTP2Cipher.COMPARATOR);
        }

        return sslContextFactory;
    }
}
