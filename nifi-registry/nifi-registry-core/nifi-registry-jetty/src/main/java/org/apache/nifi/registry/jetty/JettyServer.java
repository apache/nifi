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
package org.apache.nifi.registry.jetty;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.jetty.configuration.connector.ServerConnectorFactory;
import org.apache.nifi.registry.jetty.connector.ApplicationServerConnectorFactory;
import org.apache.nifi.registry.jetty.handler.HandlerProvider;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Registry Jetty Server
 */
public class JettyServer {

    private static final Logger logger = LoggerFactory.getLogger(JettyServer.class);

    private static final String APPLICATION_PATH = "/nifi-registry";

    private static final String HTTPS_SCHEME = "https";

    private static final String HTTP_SCHEME = "http";

    private static final String HOST_UNSPECIFIED = "0.0.0.0";

    private final NiFiRegistryProperties properties;

    private final Server server;

    public JettyServer(final NiFiRegistryProperties properties, final HandlerProvider handlerProvider) {
        Objects.requireNonNull(properties, "Properties required");
        Objects.requireNonNull(handlerProvider, "Handler Provider required");

        final QueuedThreadPool threadPool = new QueuedThreadPool(properties.getWebThreads());
        threadPool.setName("NiFi Registry Web Server");

        this.properties = properties;
        this.server = new Server(threadPool);

        try {
            configureConnectors();
            final Handler handler = handlerProvider.getHandler(properties);
            server.setHandler(handler);
        } catch (final Throwable t) {
            shutdown(t);
        }
    }

    private void configureConnectors() {
        final ServerConnectorFactory serverConnectorFactory = new ApplicationServerConnectorFactory(server, properties);
        final Set<String> interfaceNames = properties.isHTTPSConfigured() ? properties.getHttpsNetworkInterfaceNames() : Collections.emptySet();
        if (interfaceNames.isEmpty()) {
            final ServerConnector serverConnector = serverConnectorFactory.getServerConnector();
            server.addConnector(serverConnector);
        } else {
            interfaceNames.stream()
                    // Map interface name properties to Network Interfaces
                    .map(interfaceName -> {
                        try {
                            return NetworkInterface.getByName(interfaceName);
                        } catch (final SocketException e) {
                            throw new UncheckedIOException(String.format("Network Interface [%s] not found", interfaceName), e);
                        }
                    })
                    // Map Network Interfaces to host addresses
                    .filter(Objects::nonNull)
                    .flatMap(networkInterface -> Collections.list(networkInterface.getInetAddresses()).stream())
                    .map(InetAddress::getHostAddress)
                    // Map host addresses to Server Connectors
                    .map(host -> {
                        final ServerConnector serverConnector = serverConnectorFactory.getServerConnector();
                        serverConnector.setHost(host);
                        return serverConnector;
                    })
                    .forEach(server::addConnector);
        }
    }

    public void start() {
        try {
            server.start();
            final List<URI> applicationUrls = getApplicationUrls();
            if (applicationUrls.isEmpty()) {
                logger.warn("Started Server without connectors");
            } else {
                for (final URI applicationUrl : applicationUrls) {
                    logger.info("Started Server on {}", applicationUrl);
                }
            }
        } catch (final Throwable t) {
            shutdown(t);
        }
    }

    public void stop() {
        try {
            server.stop();
        } catch (final Exception e) {
            logger.warn("Server stop failed", e);
        }
    }

    protected List<URI> getApplicationUrls() {
        return Arrays.stream(server.getConnectors())
                .map(connector -> (ServerConnector) connector)
                .map(serverConnector -> {
                    final SslConnectionFactory sslConnectionFactory = serverConnector.getConnectionFactory(SslConnectionFactory.class);
                    final String scheme = sslConnectionFactory == null ? HTTP_SCHEME : HTTPS_SCHEME;
                    final int port = serverConnector.getLocalPort();
                    final String connectorHost = serverConnector.getHost();
                    final String host = StringUtils.defaultIfEmpty(connectorHost, HOST_UNSPECIFIED);
                    try {
                        return new URI(scheme, null, host, port, APPLICATION_PATH, null, null);
                    } catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
    }

    private void shutdown(final Throwable throwable) {
        System.err.printf("Server start failed: %s%n", throwable.getMessage());
        logger.error("Server start failed", throwable);
        System.exit(1);
    }
}
