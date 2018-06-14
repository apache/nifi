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

package org.apache.nifi.controller.queue.clustered.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;


public class ConnectionLoadBalanceServer {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionLoadBalanceServer.class);

    private final String hostname;
    private final int port;
    private final SSLContext sslContext;
    private final ExecutorService threadPool;
    private final LoadBalanceProtocol loadBalanceProtocol;
    private final int connectionTimeoutMillis;

    private volatile AcceptConnection acceptConnection;
    private volatile ServerSocket serverSocket;

    public ConnectionLoadBalanceServer(final String hostname, final int port, final SSLContext sslContext, final ExecutorService threadPool, final LoadBalanceProtocol loadBalanceProtocol,
                                       final int connectionTimeoutMillis) {
        this.hostname = hostname;
        this.port = port;
        this.sslContext = sslContext;
        this.threadPool = threadPool;
        this.loadBalanceProtocol = loadBalanceProtocol;
        this.connectionTimeoutMillis = connectionTimeoutMillis;
    }

    public void start() throws IOException {
        if (serverSocket != null) {
            return;
        }

        try {
            serverSocket = createServerSocket();
        } catch (final Exception e) {
            throw new IOException("Could not begin listening for incoming connections in order to load balance data across the cluster. Please verify the values of the " +
                    "'nifi.cluster.load.balance.port' and 'nifi.cluster.load.balance.host' properties as well as the 'nifi.security.*' properties", e);
        }

        acceptConnection = new AcceptConnection(serverSocket, threadPool, loadBalanceProtocol);
        final Thread receiveConnectionThread = new Thread(acceptConnection);
        receiveConnectionThread.setName("Receive Queue Load-Balancing Connections");
        receiveConnectionThread.setDaemon(true);
        receiveConnectionThread.start();
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public void stop() {
        if (acceptConnection != null) {
            acceptConnection.stop();
        }

        if (serverSocket == null) {
            return;
        }

        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.warn("Unable to close Server that is responsible for load-balancing connections. Some resources may not be cleaned up appropriately.");
        }

        serverSocket = null;
    }

    private ServerSocket createServerSocket() throws IOException {
        if (hostname == null) {
            if (sslContext == null) {
                return new ServerSocket(port, 50);
            } else {
                return sslContext.getServerSocketFactory().createServerSocket(port, 50);
            }
        } else {
            if (sslContext == null) {
                return new ServerSocket(port, 50, InetAddress.getByName(hostname));
            } else {
                return sslContext.getServerSocketFactory().createServerSocket(port, 50, InetAddress.getByName(hostname));
            }
        }
    }

    private class AcceptConnection implements Runnable {
        private final ServerSocket serverSocket;
        private final ExecutorService executor;
        private final LoadBalanceProtocol loadBalanceProtocol;
        private volatile boolean stopped = false;

        public AcceptConnection(final ServerSocket serverSocket, final ExecutorService executor, final LoadBalanceProtocol loadBalanceProtocol) {
            this.serverSocket = serverSocket;
            this.executor = executor;
            this.loadBalanceProtocol = loadBalanceProtocol;
        }

        public void stop() {
            stopped = true;
        }

        @Override
        public void run() {
            while (!stopped) {
                try {
                    final Socket socket = serverSocket.accept();
                    executor.submit(() -> {
                        try {
                            socket.setSoTimeout(connectionTimeoutMillis);
                            loadBalanceProtocol.receiveFlowFiles(socket);
                        } catch (final Exception e) {
                            logger.error("{} Failed to communicate with {} to receive FlowFiles", this, socket.getInetAddress(), e);
                        } finally {
                            try {
                                socket.close();
                            } catch (IOException e) {
                                logger.warn("{} After communicating with {}, failed to properly close socket", this, socket.getInetAddress(), e);
                            }
                        }
                    });
                } catch (final Exception e) {
                    if (stopped) {
                        // don't bother logging the Exception because it's likely just generated by the server being stopped.
                        return;
                    }

                    logger.error("{} Failed to accept connection from other node in cluster", ConnectionLoadBalanceServer.this, e);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "ConnectionLoadBalanceServer[hostname=" + hostname + ", port=" + port + ", secure=" + (sslContext != null) + "]";
    }
}
