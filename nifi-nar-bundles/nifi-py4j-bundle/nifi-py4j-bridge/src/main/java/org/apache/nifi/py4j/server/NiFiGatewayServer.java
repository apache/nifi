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

package org.apache.nifi.py4j.server;

import py4j.Gateway;
import py4j.GatewayServer;
import py4j.Py4JServerConnection;
import py4j.commands.Command;

import javax.net.ServerSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.List;

/**
 * A custom implementation of the GatewayServer that makes more sense for NiFi's use case.
 * There are only two differences between this implementation and the default GatewayServer implementation:
 * when a Connection is created, we use a {@link NiFiGatewayConnection} instead of a GatewayConnection,
 * and when shutdown() is called, we set a boolean flag so that we can provide an isShutdown() method.
 */
public class NiFiGatewayServer extends GatewayServer {
    private volatile boolean shutdown = false;
    private volatile boolean startProcessForked = false;
    private final String componentType;
    private final String componentId;
    private final String authToken;
    private final ClassLoader contextClassLoader;

    public NiFiGatewayServer(final Gateway gateway,
                             final int port,
                             final InetAddress address,
                             final int connectTimeout,
                             final int readTimeout,
                             final List<Class<? extends Command>> customCommands,
                             final ServerSocketFactory sSocketFactory,
                             final String authToken,
                             final String componentType,
                             final String componentId) {
        super(gateway, port, address, connectTimeout, readTimeout, customCommands, sSocketFactory);
        this.componentType = componentType;
        this.componentId = componentId;
        this.authToken = authToken;
        this.contextClassLoader = getClass().getClassLoader();
    }

    protected Py4JServerConnection createConnection(final Gateway gateway, final Socket socket) throws IOException {
        final NiFiGatewayConnection connection = new NiFiGatewayConnection(this, gateway, socket, authToken, Collections.emptyList(), getListeners());
        connection.startConnection();
        return connection;
    }

    @Override
    public void shutdown(final boolean shutdownCallbackClient) {
        shutdown = true;
        super.shutdown(shutdownCallbackClient);
    }

    public boolean isShutdown() {
        return shutdown;
    }

    // If the server is started with forked == true, we want to set the name of the thread to something more meaningful.
    // But we don't want to repeat the logic in the super class, so we'll just set a flag here and then set the name in the run() method.
    @Override
    public void start(final boolean fork) {
        this.startProcessForked = fork;
        super.start(fork);
    }

    @Override
    public void run() {
        final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(this.contextClassLoader);
            if (startProcessForked) {
                Thread.currentThread().setName("NiFiGatewayServer Thread for " + componentType + " " + componentId);
            }

            super.run();
        } finally {
            if (originalClassLoader != null) {
                Thread.currentThread().setContextClassLoader(originalClassLoader);
            }
        }
    }

    @Override
    public String toString() {
        return "NiFiGatewayServer[" +
            "shutdown=" + shutdown +
            ", forked=" + startProcessForked +
            ", componentType=" + componentType +
            ", componentId=" + componentId +
            "]";
    }

    public String getComponentId() {
        return componentId;
    }

    public String getComponentType() {
        return componentType;
    }
}
