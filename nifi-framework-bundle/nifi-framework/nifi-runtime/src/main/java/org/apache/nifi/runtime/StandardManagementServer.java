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
package org.apache.nifi.runtime;

import com.sun.net.httpserver.HttpServer;
import org.apache.nifi.NiFiServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * Standard Management Server based on Java HttpServer
 */
public class StandardManagementServer implements ManagementServer {

    private static final Logger logger = LoggerFactory.getLogger(StandardManagementServer.class);

    private static final String HEALTH_PATH = "/health";

    private static final String HEALTH_CLUSTER_PATH = "/health/cluster";

    private static final String HEALTH_DIAGNOSTICS_PATH = "/health/diagnostics";

    private static final String HEALTH_STATUS_HISTORY_PATH = "/health/status-history";

    private static final int STOP_DELAY = 0;

    private static final int CONNECTION_BACKLOG = 10;

    private final InetSocketAddress bindAddress;

    private final NiFiServer server;

    private HttpServer httpServer;

    public StandardManagementServer(final InetSocketAddress bindAddress, final NiFiServer server) {
        this.bindAddress = Objects.requireNonNull(bindAddress, "Bind Address required");
        this.server = Objects.requireNonNull(server, "Server required");
    }

    @Override
    public void start() {
        if (httpServer == null) {
            try {
                httpServer = HttpServer.create();

                httpServer.createContext(HEALTH_PATH, new HealthHttpHandler());
                httpServer.createContext(HEALTH_CLUSTER_PATH, new HealthClusterHttpHandler(server));
                httpServer.createContext(HEALTH_DIAGNOSTICS_PATH, new HealthDiagnosticsHttpHandler(server));
                httpServer.createContext(HEALTH_STATUS_HISTORY_PATH, new HealthStatusHistoryHttpHandler(server));

                httpServer.bind(bindAddress, CONNECTION_BACKLOG);
                httpServer.start();

                final InetSocketAddress serverAddress = getServerAddress();

                logger.info("Started Management Server on http://{}:{}", serverAddress.getHostString(), serverAddress.getPort());
            } catch (final IOException e) {
                throw new UncheckedIOException("Management Server start failed", e);
            }
        } else {
            throw new IllegalStateException("Management Server running");
        }
    }

    @Override
    public void stop() {
        if (httpServer == null) {
            logger.info("Management Server not running");
        } else {
            httpServer.stop(STOP_DELAY);
            logger.info("Management Server stopped");
        }
    }

    protected InetSocketAddress getServerAddress() {
        return httpServer == null ? bindAddress : httpServer.getAddress();
    }
}
