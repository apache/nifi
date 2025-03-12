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
package org.apache.nifi.web.server;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.server.connector.FrameworkServerConnectorFactory;
import org.apache.nifi.web.server.handler.ContextPathRedirectPatternRule;
import org.apache.nifi.web.server.handler.HeaderWriterHandler;
import org.apache.nifi.web.server.log.RequestLogProvider;
import org.apache.nifi.web.server.log.StandardRequestLogProvider;
import org.eclipse.jetty.rewrite.handler.RedirectPatternRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import javax.net.ssl.SSLContext;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Standard implementation of Server Provider with default Handlers
 */
class StandardServerProvider implements ServerProvider {
    private static final String ALL_PATHS_PATTERN = "/*";

    private static final String FRONTEND_CONTEXT_PATH = "/nifi";

    private final SSLContext sslContext;

    StandardServerProvider(final SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    @Override
    public Server getServer(final NiFiProperties properties) {
        Objects.requireNonNull(properties, "Properties required");

        final QueuedThreadPool threadPool = new QueuedThreadPool(properties.getWebThreads());
        threadPool.setName("NiFi Web Server");
        final Server server = new Server(threadPool);
        addConnectors(server, properties, sslContext);

        final Handler standardHandler = getStandardHandler();
        server.setHandler(standardHandler);

        final RewriteHandler defaultRewriteHandler = new RewriteHandler();
        final List<String> allowedContextPaths = properties.getAllowedContextPathsAsList();
        final RedirectPatternRule redirectDefault = new ContextPathRedirectPatternRule(ALL_PATHS_PATTERN, FRONTEND_CONTEXT_PATH, allowedContextPaths);
        defaultRewriteHandler.addRule(redirectDefault);
        server.setDefaultHandler(defaultRewriteHandler);

        final String requestLogFormat = properties.getProperty(NiFiProperties.WEB_REQUEST_LOG_FORMAT);
        final RequestLogProvider requestLogProvider = new StandardRequestLogProvider(requestLogFormat);
        final RequestLog requestLog = requestLogProvider.getRequestLog();
        server.setRequestLog(requestLog);

        return server;
    }

    private void addConnectors(final Server server, final NiFiProperties properties, final SSLContext sslContext) {
        final FrameworkServerConnectorFactory serverConnectorFactory = new FrameworkServerConnectorFactory(server, properties);
        if (properties.isHTTPSConfigured()) {
            serverConnectorFactory.setSslContext(sslContext);
        }

        final Map<String, String> interfaces = properties.isHTTPSConfigured() ? properties.getHttpsNetworkInterfaces() : properties.getHttpNetworkInterfaces();
        final Set<String> interfaceNames = interfaces.values().stream().filter(StringUtils::isNotBlank).collect(Collectors.toSet());
        // Add Server Connectors based on configured Network Interface Names
        if (interfaceNames.isEmpty()) {
            final ServerConnector serverConnector = serverConnectorFactory.getServerConnector();
            final String host = properties.isHTTPSConfigured() ? properties.getProperty(NiFiProperties.WEB_HTTPS_HOST) : properties.getProperty(NiFiProperties.WEB_HTTP_HOST);
            if (StringUtils.isNotBlank(host)) {
                serverConnector.setHost(host);
            }
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

    private Handler getStandardHandler() {
        // Standard Handler supporting an ordered sequence of Handlers invoked until completion
        final Handler.Collection standardHandler = new Handler.Sequence();

        // Set Handler for standard response headers
        standardHandler.addHandler(new HeaderWriterHandler());

        return standardHandler;
    }
}
