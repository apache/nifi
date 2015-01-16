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
package org.apache.nifi.io.socket.multicast;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a listener for protocol messages sent over multicast. If a message
 * is of type MulticastProtocolMessage, then the underlying protocol message is
 * passed to the handler. If the receiving handler produces a message response,
 * then the message is wrapped with a MulticastProtocolMessage before being sent
 * to the originator.
 *
 * @author unattributed
 */
public abstract class MulticastListener {

    // constants
    private static final int DEFAULT_SHUTDOWN_LISTENER_SECONDS = 5;
    private static final int DEFAULT_MAX_PACKET_SIZE_BYTES = 512;

    private static final Logger logger = new org.apache.nifi.logging.NiFiLog(LoggerFactory.getLogger(MulticastListener.class));

    // immutable members
    private final int numThreads;
    private final InetSocketAddress multicastAddress;
    private final MulticastConfiguration configuration;

    private volatile ExecutorService executorService;     // volatile to guarantee most current value is visible
    private volatile MulticastSocket multicastSocket;     // volatile to guarantee most current value is visible

    private int shutdownListenerSeconds = DEFAULT_SHUTDOWN_LISTENER_SECONDS;
    private int maxPacketSizeBytes = DEFAULT_MAX_PACKET_SIZE_BYTES;

    public MulticastListener(
            final int numThreads,
            final InetSocketAddress multicastAddress,
            final MulticastConfiguration configuration) {

        if (numThreads <= 0) {
            throw new IllegalArgumentException("Number of threads may not be less than or equal to zero.");
        } else if (multicastAddress == null) {
            throw new IllegalArgumentException("Multicast address may not be null.");
        } else if (multicastAddress.getAddress().isMulticastAddress() == false) {
            throw new IllegalArgumentException("Multicast group must be a Class D address.");
        } else if (configuration == null) {
            throw new IllegalArgumentException("Multicast configuration may not be null.");
        }

        this.numThreads = numThreads;
        this.multicastAddress = multicastAddress;
        this.configuration = configuration;
    }

    /**
     * Implements the action to perform when a new datagram is received. This
     * class must not close the multicast socket.
     *
     * @param multicastSocket
     * @param packet the datagram socket
     */
    public abstract void dispatchRequest(final MulticastSocket multicastSocket, final DatagramPacket packet);

    public void start() throws IOException {

        if (isRunning()) {
            return;
        }

        multicastSocket = MulticastUtils.createMulticastSocket(multicastAddress.getPort(), configuration);
        multicastSocket.joinGroup(multicastAddress.getAddress());

        executorService = Executors.newFixedThreadPool(numThreads);

        final ExecutorService runnableExecServiceRef = executorService;
        final MulticastSocket runnableMulticastSocketRef = multicastSocket;

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (runnableExecServiceRef.isShutdown() == false) {
                    try {
                        final byte[] buf = new byte[maxPacketSizeBytes];
                        final DatagramPacket packet = new DatagramPacket(buf, maxPacketSizeBytes);
                        runnableMulticastSocketRef.receive(packet);
                        runnableExecServiceRef.execute(new Runnable() {
                            @Override
                            public void run() {
                                dispatchRequest(multicastSocket, packet);
                            }
                        });
                    } catch (final SocketException | SocketTimeoutException ste) {
                        /* ignore so that we can accept connections in approximately a non-blocking fashion */
                    } catch (final Exception e) {
                        logger.warn("Cluster protocol receiver encountered exception: " + e, e);
                    }
                }
            }
        }).start();
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
                logger.info("Multicast Listener has been terminated successfully.");
            } else {
                logger.warn("Multicast Listener has not terminated properly.  There exists an uninterruptable thread that will take an indeterminate amount of time to stop.");
            }
        }

        // shutdown server socket
        if (multicastSocket.isClosed() == false) {
            multicastSocket.leaveGroup(multicastAddress.getAddress());
            multicastSocket.close();
        }

    }

    public int getShutdownListenerSeconds() {
        return shutdownListenerSeconds;
    }

    public void setShutdownListenerSeconds(final int shutdownListenerSeconds) {
        this.shutdownListenerSeconds = shutdownListenerSeconds;
    }

    public int getMaxPacketSizeBytes() {
        return maxPacketSizeBytes;
    }

    public void setMaxPacketSizeBytes(int maxPacketSizeBytes) {
        if (maxPacketSizeBytes <= 0) {
            throw new IllegalArgumentException("Max packet size must be greater than zero bytes.");
        }
        this.maxPacketSizeBytes = maxPacketSizeBytes;
    }

    public MulticastConfiguration getConfiguration() {
        return configuration;
    }

    public InetSocketAddress getMulticastAddress() {
        return multicastAddress;
    }

    public int getNumThreads() {
        return numThreads;
    }

}
