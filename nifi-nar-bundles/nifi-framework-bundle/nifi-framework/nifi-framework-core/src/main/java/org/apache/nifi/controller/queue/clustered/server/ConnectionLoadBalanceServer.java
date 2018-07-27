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

import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class ConnectionLoadBalanceServer {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionLoadBalanceServer.class);

    private final String hostname;
    private final int port;
    private final SSLContext sslContext;
    private final ExecutorService threadPool;
    private final LoadBalanceProtocol loadBalanceProtocol;
    private final int connectionTimeoutMillis;
    private final int numThreads;
    private final EventReporter eventReporter;

    private volatile Set<CommunicateAction> communicationActions = Collections.emptySet();
    private final BlockingQueue<Socket> connectionQueue = new LinkedBlockingQueue<>();

    private volatile AcceptConnection acceptConnection;
    private volatile ServerSocket serverSocket;
    private volatile boolean stopped = true;

    public ConnectionLoadBalanceServer(final String hostname, final int port, final SSLContext sslContext, final int numThreads, final LoadBalanceProtocol loadBalanceProtocol,
                                       final EventReporter eventReporter, final int connectionTimeoutMillis) {
        this.hostname = hostname;
        this.port = port;
        this.sslContext = sslContext;
        this.loadBalanceProtocol = loadBalanceProtocol;
        this.connectionTimeoutMillis = connectionTimeoutMillis;
        this.numThreads = numThreads;
        this.eventReporter = eventReporter;

        threadPool = new FlowEngine(numThreads, "Load Balance Server");
    }

    public void start() throws IOException {
        if (!stopped) {
            return;
        }

        stopped = false;
        if (serverSocket != null) {
            return;
        }

        try {
            serverSocket = createServerSocket();
        } catch (final Exception e) {
            throw new IOException("Could not begin listening for incoming connections in order to load balance data across the cluster. Please verify the values of the " +
                    "'nifi.cluster.load.balance.port' and 'nifi.cluster.load.balance.host' properties as well as the 'nifi.security.*' properties", e);
        }

        final Set<CommunicateAction> actions = new HashSet<>(numThreads);
        for (int i=0; i < numThreads; i++) {
            final CommunicateAction action = new CommunicateAction(loadBalanceProtocol);
            actions.add(action);
            threadPool.submit(action);
        }

        this.communicationActions = actions;

        acceptConnection = new AcceptConnection(serverSocket);
        final Thread receiveConnectionThread = new Thread(acceptConnection);
        receiveConnectionThread.setName("Receive Queue Load-Balancing Connections");
        receiveConnectionThread.start();
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public void stop() {
        stopped = false;
        threadPool.shutdown();

        if (acceptConnection != null) {
            acceptConnection.stop();
        }

        communicationActions.forEach(CommunicateAction::stop);

        Socket socket;
        while ((socket = connectionQueue.poll()) != null) {
            try {
                socket.close();
                logger.info("{} Closed connection to {} on Server stop", this, socket.getRemoteSocketAddress());
            } catch (final IOException ioe) {
                logger.warn("Failed to properly close socket to " + socket.getRemoteSocketAddress(), ioe);
            }
        }
    }

    private ServerSocket createServerSocket() throws IOException {
        final InetAddress inetAddress = hostname == null ? null : InetAddress.getByName(hostname);

        if (sslContext == null) {
            return new ServerSocket(port, 50, InetAddress.getByName(hostname));
        } else {
            final ServerSocket serverSocket = sslContext.getServerSocketFactory().createServerSocket(port, 50, inetAddress);
            ((SSLServerSocket) serverSocket).setNeedClientAuth(true);
            return serverSocket;
        }
    }


    private class CommunicateAction implements Runnable {
        private final LoadBalanceProtocol loadBalanceProtocol;
        private volatile boolean stopped = false;

        public CommunicateAction(final LoadBalanceProtocol loadBalanceProtocol) {
            this.loadBalanceProtocol = loadBalanceProtocol;
        }

        public void stop() {
            this.stopped = true;
        }

        @Override
        public void run() {
            String peerDescription = "<Unknown Client>";

            while (!stopped) {
                Socket socket = null;
                try {
                    socket = connectionQueue.poll(1, TimeUnit.SECONDS);
                    if (socket == null) {
                        continue;
                    }

                    peerDescription = socket.getRemoteSocketAddress().toString();

                    if (socket.isClosed()) {
                        logger.debug("Connection to Peer {} is closed. Will not attempt to communicate over this Socket.", peerDescription);
                        continue;
                    }

                    logger.debug("Receiving FlowFiles from Peer {}", peerDescription);
                    loadBalanceProtocol.receiveFlowFiles(socket);

                    if (socket.isConnected()) {
                        logger.debug("Finished receiving FlowFiles from Peer {}. Will recycle connection.", peerDescription);
                        connectionQueue.offer(socket);
                    } else {
                        logger.debug("Finished receiving FlowFiles from Peer {}. Socket is no longer connected so will not recycle connection.", peerDescription);
                    }
                } catch (final Exception e) {
                    if (socket != null) {
                        try {
                            socket.close();
                        } catch (final IOException ioe) {
                            e.addSuppressed(ioe);
                        }
                    }

                    logger.error("Failed to communicate with Peer {}", peerDescription, e);
                    eventReporter.reportEvent(Severity.ERROR, "Load Balanced Connection", "Failed to receive FlowFiles for Load Balancing due to " + e);
                }
            }

            logger.info("Connection Load Balance Server shutdown. Will no longer handle incoming requests.");
        }
    }


    private class AcceptConnection implements Runnable {
        private final ServerSocket serverSocket;
        private volatile boolean stopped = false;

        public AcceptConnection(final ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        public void stop() {
            stopped = true;
        }

        @Override
        public void run() {
            try {
                serverSocket.setSoTimeout(1000);
            } catch (final Exception e) {
                logger.error("Failed to set soTimeout on Server Socket for Load Balancing data across cluster", e);
            }

            while (!stopped) {
                try {
                    final Socket socket;
                    try {
                        socket = serverSocket.accept();
                    } catch (final SocketTimeoutException ste) {
                        continue;
                    }

                    socket.setSoTimeout(connectionTimeoutMillis);
                    connectionQueue.offer(socket);
                } catch (final Exception e) {
                    logger.error("{} Failed to accept connection from other node in cluster", ConnectionLoadBalanceServer.this, e);
                }
            }

            try {
                serverSocket.close();
            } catch (final Exception e) {
                logger.warn("Failed to properly shutdown Server Socket for Load Balancing", e);
            }
        }
    }

    @Override
    public String toString() {
        return "ConnectionLoadBalanceServer[hostname=" + hostname + ", port=" + port + ", secure=" + (sslContext != null) + "]";
    }
}
