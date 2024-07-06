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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.security.util.TlsPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLServerSocket;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ConnectionLoadBalanceServer {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionLoadBalanceServer.class);
    private static final AtomicLong threadCounter = new AtomicLong(1L);

    private final String hostname;
    private final int port;
    private final SSLContext sslContext;
    private final LoadBalanceProtocol loadBalanceProtocol;
    private final int connectionTimeoutMillis;
    private final EventReporter eventReporter;

    private final List<CommunicateAction> communicationActions = Collections.synchronizedList(new ArrayList<>());

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
        this.eventReporter = eventReporter;
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

        acceptConnection = new AcceptConnection(serverSocket);
        final Thread receiveConnectionThread = new Thread(acceptConnection);
        receiveConnectionThread.setName("Receive Queue Load-Balancing Connections");
        receiveConnectionThread.start();
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public void stop() {
        stopped = true;

        if (acceptConnection != null) {
            acceptConnection.stop();
        }

        synchronized (communicationActions) { // Must synchronize on Synchronized List when using iterator
            final Iterator<CommunicateAction> itr = communicationActions.iterator();
            while (itr.hasNext()) {
                itr.next().stop();
                itr.remove();
            }
        }
    }

    private ServerSocket createServerSocket() throws IOException {
        final InetAddress inetAddress = hostname == null ? null : InetAddress.getByName(hostname);

        if (sslContext == null) {
            return new ServerSocket(port, 50, InetAddress.getByName(hostname));
        } else {
            final SSLServerSocket serverSocket = (SSLServerSocket) sslContext.getServerSocketFactory().createServerSocket(port, 50, inetAddress);
            serverSocket.setNeedClientAuth(true);
            // Set Preferred TLS Protocol Versions
            serverSocket.setEnabledProtocols(TlsPlatform.getPreferredProtocols().toArray(new String[0]));
            return serverSocket;
        }
    }

    // Use a static nested class and pass the ER in the constructor to avoid instantiation issues in tests
    protected static class CommunicateAction implements Runnable {
        private final LoadBalanceProtocol loadBalanceProtocol;
        private final Socket socket;
        private final InputStream in;
        private final OutputStream out;
        private final EventReporter eventReporter;

        private volatile boolean stopped = false;

        private static final int EXCEPTION_THRESHOLD_MILLIS = 10_000;
        private volatile long tlsErrorLastSeen = -1;

        public CommunicateAction(final LoadBalanceProtocol loadBalanceProtocol, final Socket socket, final EventReporter eventReporter) throws IOException {
            this.loadBalanceProtocol = loadBalanceProtocol;
            this.socket = socket;
            this.eventReporter = eventReporter;

            this.in = new BufferedInputStream(socket.getInputStream());
            this.out = new BufferedOutputStream(socket.getOutputStream());
        }

        public void stop() {
            this.stopped = true;
            IOUtils.closeQuietly(socket);
        }

        @Override
        public void run() {
            String channelDescription = "<Unknown Channel>";

            while (!stopped) {
                try {
                    channelDescription = socket.getLocalSocketAddress() + "::" + socket.getRemoteSocketAddress();

                    logger.debug("Receiving FlowFiles from Channel {}", channelDescription);
                    loadBalanceProtocol.receiveFlowFiles(socket, in, out);

                    if (socket.isClosed()) {
                        logger.debug("Finished Receiving FlowFiles from Channel {}", channelDescription);
                        break;
                    }
                } catch (final Exception e) {
                    stopped = true;
                    if (socket != null) {
                        try {
                            socket.close();
                        } catch (final IOException ioe) {
                            e.addSuppressed(ioe);
                        }
                    }

                    /* The exceptions can fill the log very quickly and make it difficult to use. SSLPeerUnverifiedExceptions
                    especially repeat and have a long stacktrace, and are not likely to be resolved instantaneously. Suppressing
                    them for a period of time is helpful */
                    if (isTlsError(e)) {
                        handleTlsError(channelDescription, e);
                    } else {
                        logger.error("Failed to communicate over Channel {}", channelDescription, e);
                        eventReporter.reportEvent(Severity.ERROR, "Load Balanced Connection", "Failed to receive FlowFiles for Load Balancing due to " + e);
                    }

                    return;
                }
            }
        }

        private static boolean isTlsError(Throwable e) {
            if (e == null) {
                return false;
            } else {
                if (e instanceof CertificateException || e instanceof TlsException || e instanceof SSLException) {
                    return true;
                } else if (e.getCause() != null) {
                    return isTlsError(e.getCause());
                } else {
                    return false;
                }
            }
        }

        /**
         * Determines how to record the TLS-related error
         * ({@link org.apache.nifi.security.util.TlsException}, {@link SSLPeerUnverifiedException},
         * {@link java.security.cert.CertificateException}, etc.) to the log, based on how recently it was last seen.
         *
         * @param channelDescription the channel's String representation for the log message
         * @param e               the exception
         * @return true if the error was printed at ERROR severity and reported to the event reporter
         */
        private boolean handleTlsError(String channelDescription, Throwable e) {
            final String populatedMessage = "Failed to communicate over Channel " + channelDescription + " due to " + e.getLocalizedMessage();
            // If the exception has been seen recently, log as debug
            if (tlsErrorRecentlySeen()) {
                logger.debug(populatedMessage);
                return false;
            } else {
                // If this is the first exception in X seconds, log as error
                logger.error(populatedMessage);
                logger.info("\tPrinted above error because it has been {} ms since the last printing", System.currentTimeMillis() - tlsErrorLastSeen);
                eventReporter.reportEvent(Severity.ERROR, "Load Balanced Connection", populatedMessage);

                // Reset the timer
                tlsErrorLastSeen = System.currentTimeMillis();
                return true;
            }
        }


        /**
         * Returns {@code true} if any related exception (determined by TLS error status) has occurred within the last
         * {@link #EXCEPTION_THRESHOLD_MILLIS} milliseconds. Does not evaluate the error locally,
         * simply checks the last time the timestamp was updated.
         *
         * @return true if the time since the last similar exception occurred is below the threshold
         */
        private boolean tlsErrorRecentlySeen() {
            long now = System.currentTimeMillis();
            return now - tlsErrorLastSeen < EXCEPTION_THRESHOLD_MILLIS;
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

                    final CommunicateAction communicateAction = new CommunicateAction(loadBalanceProtocol, socket, eventReporter);
                    communicationActions.add(communicateAction);

                    final Thread commsThread = new Thread(() -> {
                        try {
                            communicateAction.run();
                        } finally {
                            communicationActions.remove(communicateAction);
                        }
                    });

                    commsThread.setName("Load-Balance Server Thread-" + threadCounter.getAndIncrement());
                    commsThread.start();
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
