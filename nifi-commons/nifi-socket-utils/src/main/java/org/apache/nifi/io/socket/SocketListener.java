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
package org.apache.nifi.io.socket;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a listener for TCP/IP messages sent over unicast socket.
 *
 */
public abstract class SocketListener {

    private static final int DEFAULT_SHUTDOWN_LISTENER_SECONDS = 5;
    private static final Logger logger = LoggerFactory.getLogger(SocketListener.class);
    private volatile ExecutorService executorService;  // volatile to guarantee most current value is visible
    private volatile ServerSocket serverSocket;        // volatile to guarantee most current value is visible
    private final int numThreads;
    private final int port;
    private final ServerSocketConfiguration configuration;
    private final AtomicInteger shutdownListenerSeconds = new AtomicInteger(DEFAULT_SHUTDOWN_LISTENER_SECONDS);

    public SocketListener(
            final int numThreads,
            final int port,
            final ServerSocketConfiguration configuration) {

        if (numThreads <= 0) {
            throw new IllegalArgumentException("Number of threads may not be less than or equal to zero.");
        } else if (configuration == null) {
            throw new IllegalArgumentException("Server socket configuration may not be null.");
        }

        this.numThreads = numThreads;
        this.port = port;
        this.configuration = configuration;
    }

    /**
     * Implements the action to perform when a new socket request is received.
     * This class will close the socket.
     *
     * @param socket the socket
     */
    public abstract void dispatchRequest(final Socket socket);

    public void start() throws IOException {

        if (isRunning()) {
            return;
        }

        try {
            serverSocket = SocketUtils.createServerSocket(port, configuration);
        } catch (KeyManagementException | UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException | CertificateException e) {
            throw new IOException(e);
        }

        final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
        executorService = Executors.newFixedThreadPool(numThreads, new ThreadFactory() {
            private final AtomicLong threadCounter = new AtomicLong(0L);

            @Override
            public Thread newThread(final Runnable r) {
                final Thread newThread = defaultThreadFactory.newThread(r);
                newThread.setName("Process Cluster Protocol Request-" + threadCounter.incrementAndGet());
                return newThread;
            }
        });

        final ExecutorService runnableExecServiceRef = executorService;
        final ServerSocket runnableServerSocketRef = serverSocket;

        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                while (runnableExecServiceRef.isShutdown() == false) {
                    Socket socket = null;
                    try {
                        try {
                            socket = runnableServerSocketRef.accept();
                            if (configuration.getSocketTimeout() != null) {
                                socket.setSoTimeout(configuration.getSocketTimeout());
                            }
                        } catch (final SocketTimeoutException ste) {
                            // nobody connected to us. Go ahead and call closeQuietly just to make sure we don't leave
                            // any sockets lingering
                            SocketUtils.closeQuietly(socket);
                            continue;
                        } catch (final SocketException se) {
                            logger.warn("Failed to communicate with " + (socket == null ? "Unknown Host" : socket.getInetAddress().getHostName()) + " due to " + se, se);
                            SocketUtils.closeQuietly(socket);
                            continue;
                        } catch (final Throwable t) {
                            logger.warn("Socket Listener encountered exception: " + t, t);
                            SocketUtils.closeQuietly(socket);
                            continue;
                        }

                        final Socket finalSocket = socket;
                        runnableExecServiceRef.execute(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    dispatchRequest(finalSocket);
                                } catch (final Throwable t) {
                                    logger.warn("Dispatching socket request encountered exception due to: " + t, t);
                                } finally {
                                    SocketUtils.closeQuietly(finalSocket);
                                }
                            }
                        });
                    } catch (final Throwable t) {
                        logger.error("Socket Listener encountered exception: " + t, t);
                        SocketUtils.closeQuietly(socket);
                    }
                }
            }
        });
        t.setName("Cluster Socket Listener");
        t.start();

        logger.info("Now listening for connections from nodes on port " + port);
    }

    public boolean isRunning() {
        return (executorService != null && executorService.isShutdown() == false);
    }

    public void stop() throws IOException {

        if (isRunning() == false) {
            return;
        }

        // shutdown executor service
        try {
            if (getShutdownListenerSeconds() <= 0) {
                executorService.shutdownNow();
            } else {
                executorService.shutdown();
            }
            executorService.awaitTermination(getShutdownListenerSeconds(), TimeUnit.SECONDS);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            if (executorService.isTerminated()) {
                logger.info("Socket Listener has been terminated successfully.");
            } else {
                logger.warn("Socket Listener has not terminated properly.  There exists an uninterruptable thread that will take an indeterminate amount of time to stop.");
            }
        }

        // shutdown server socket
        SocketUtils.closeQuietly(serverSocket);

    }

    public int getShutdownListenerSeconds() {
        return shutdownListenerSeconds.get();
    }

    public void setShutdownListenerSeconds(final int shutdownListenerSeconds) {
        this.shutdownListenerSeconds.set(shutdownListenerSeconds);
    }

    public ServerSocketConfiguration getConfiguration() {
        return configuration;
    }

    public int getPort() {
        if (isRunning()) {
            return serverSocket.getLocalPort();
        } else {
            return port;
        }
    }

}
