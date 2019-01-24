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

package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.clustered.FlowFileContentAccess;
import org.apache.nifi.controller.queue.clustered.SimpleLimitThreshold;
import org.apache.nifi.controller.queue.clustered.TransactionThreshold;
import org.apache.nifi.controller.queue.clustered.client.LoadBalanceFlowFileCodec;
import org.apache.nifi.controller.queue.clustered.client.async.AsyncLoadBalanceClient;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionCompleteCallback;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionFailureCallback;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;


public class NioAsyncLoadBalanceClient implements AsyncLoadBalanceClient {
    private static final Logger logger = LoggerFactory.getLogger(NioAsyncLoadBalanceClient.class);
    private static final long PENALIZATION_MILLIS = TimeUnit.SECONDS.toMillis(1L);

    private final NodeIdentifier nodeIdentifier;
    private final SSLContext sslContext;
    private final int timeoutMillis;
    private final FlowFileContentAccess flowFileContentAccess;
    private final LoadBalanceFlowFileCodec flowFileCodec;
    private final EventReporter eventReporter;

    private volatile boolean running = false;
    private final AtomicLong penalizationEnd = new AtomicLong(0L);

    private final Map<String, RegisteredPartition> registeredPartitions = new HashMap<>();
    private final Queue<RegisteredPartition> partitionQueue = new LinkedBlockingQueue<>();

    // guarded by synchronizing on this
    private PeerChannel channel;
    private Selector selector;
    private SelectionKey selectionKey;

    // While we use synchronization to guard most of the Class's state, we use a separate lock for the LoadBalanceSession.
    // We do this because we need to atomically decide whether or not we are able to communicate over the socket with another node and if so, continue on and do so.
    // However, we cannot do this within a synchronized block because if we did, then if Thread 1 were communicating with the remote node, and Thread 2 wanted to attempt
    // to do so, it would have to wait until Thread 1 released the synchronization. Instead, we want Thread 2 to determine that the resource is not free and move on.
    // I.e., we need to use the capability of Lock#tryLock, and the synchronized keyword does not offer this sort of functionality.
    private final Lock loadBalanceSessionLock = new ReentrantLock();
    private LoadBalanceSession loadBalanceSession = null;


    public NioAsyncLoadBalanceClient(final NodeIdentifier nodeIdentifier, final SSLContext sslContext, final int timeoutMillis, final FlowFileContentAccess flowFileContentAccess,
                                     final LoadBalanceFlowFileCodec flowFileCodec, final EventReporter eventReporter) {
        this.nodeIdentifier = nodeIdentifier;
        this.sslContext = sslContext;
        this.timeoutMillis = timeoutMillis;
        this.flowFileContentAccess = flowFileContentAccess;
        this.flowFileCodec = flowFileCodec;
        this.eventReporter = eventReporter;
    }

    @Override
    public NodeIdentifier getNodeIdentifier() {
        return nodeIdentifier;
    }

    public synchronized void register(final String connectionId, final BooleanSupplier emptySupplier, final Supplier<FlowFileRecord> flowFileSupplier,
                                      final TransactionFailureCallback failureCallback, final TransactionCompleteCallback successCallback,
                                      final Supplier<LoadBalanceCompression> compressionSupplier, final BooleanSupplier honorBackpressureSupplier) {

        if (registeredPartitions.containsKey(connectionId)) {
            throw new IllegalStateException("Connection with ID " + connectionId + " is already registered");
        }

        final RegisteredPartition partition = new RegisteredPartition(connectionId, emptySupplier, flowFileSupplier, failureCallback, successCallback, compressionSupplier, honorBackpressureSupplier);
        registeredPartitions.put(connectionId, partition);
        partitionQueue.add(partition);
    }

    public synchronized void unregister(final String connectionId) {
        registeredPartitions.remove(connectionId);
    }

    public synchronized int getRegisteredConnectionCount() {
        return registeredPartitions.size();
    }

    private synchronized Map<String, RegisteredPartition> getRegisteredPartitions() {
        return new HashMap<>(registeredPartitions);
    }

    public void start() {
        running = true;
        logger.debug("{} started", this);
    }

    public void stop() {
        running = false;
        logger.debug("{} stopped", this);
        close();
    }

    private synchronized void close() {
        if (selector != null && selector.isOpen()) {
            try {
                selector.close();
            } catch (final Exception e) {
                logger.warn("Failed to close NIO Selector", e);
            }
        }

        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (final Exception e) {
                logger.warn("Failed to close Socket Channel to {} for Load Balancing", nodeIdentifier, e);
            }
        }

        channel = null;
        selector = null;
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isPenalized() {
        final long endTimestamp = penalizationEnd.get();
        if (endTimestamp == 0) {
            return false;
        }

        if (endTimestamp < System.currentTimeMillis()) {
            // set penalization end to 0 so that next time we don't need to check System.currentTimeMillis() because
            // systems calls are expensive enough that we'd like to avoid them when we can.
            penalizationEnd.compareAndSet(endTimestamp, 0L);
            return false;
        }

        return true;
    }

    private void penalize() {
        logger.debug("Penalizing {}", this);
        this.penalizationEnd.set(System.currentTimeMillis() + PENALIZATION_MILLIS);
    }


    public boolean communicate() throws IOException {
        if (!running) {
            return false;
        }

        // Use #tryLock here so that if another thread is already communicating with this Client, this thread
        // will not block and wait but instead will just return so that the Thread Pool can proceed to the next Client.
        if (!loadBalanceSessionLock.tryLock()) {
            return false;
        }

        try {
            RegisteredPartition readyPartition = null;

            if (!isConnectionEstablished()) {
                readyPartition = getReadyPartition();
                if (readyPartition == null) {
                    logger.debug("{} has no connection with data ready to be transmitted so will penalize Client without communicating", this);
                    penalize();
                    return false;
                }

                try {
                    establishConnection();
                } catch (IOException e) {
                    penalize();

                    partitionQueue.offer(readyPartition);

                    for (final RegisteredPartition partition : getRegisteredPartitions().values()) {
                        logger.debug("Triggering Transaction Failure Callback for {} with Transaction Phase of CONNECTING", partition);
                        partition.getFailureCallback().onTransactionFailed(Collections.emptyList(), e, TransactionFailureCallback.TransactionPhase.CONNECTING);
                    }

                    return false;
                }
            }

            final LoadBalanceSession loadBalanceSession = getActiveTransaction(readyPartition);
            if (loadBalanceSession == null) {
                penalize();
                return false;
            }

            selector.selectNow();
            final boolean ready = (loadBalanceSession.getDesiredReadinessFlag() & selectionKey.readyOps()) != 0;
            if (!ready) {
                return false;
            }

            boolean anySuccess = false;
            boolean success;
            do {
                try {
                    success = loadBalanceSession.communicate();
                } catch (final Exception e) {
                    logger.error("Failed to communicate with Peer {}", nodeIdentifier.toString(), e);
                    eventReporter.reportEvent(Severity.ERROR, "Load Balanced Connection", "Failed to communicate with Peer " + nodeIdentifier + " when load balancing data for Connection with ID " +
                        loadBalanceSession.getPartition().getConnectionId() + " due to " + e);

                    penalize();
                    loadBalanceSession.getPartition().getFailureCallback().onTransactionFailed(loadBalanceSession.getFlowFilesSent(), e, TransactionFailureCallback.TransactionPhase.SENDING);
                    close();

                    return false;
                }

                anySuccess = anySuccess || success;
            } while (success);

            if (loadBalanceSession.isComplete()) {
                loadBalanceSession.getPartition().getSuccessCallback().onTransactionComplete(loadBalanceSession.getFlowFilesSent(), nodeIdentifier);
            }

            return anySuccess;
        } catch (final Exception e) {
            close();
            loadBalanceSession = null;
            throw e;
        } finally {
            loadBalanceSessionLock.unlock();
        }
    }

    /**
     * If any FlowFiles have been transferred in an active session, fail the transaction. Otherwise, gather up to the Transaction Threshold's limits
     * worth of FlowFiles and treat them as a failed transaction. In either case, terminate the session. This allows us to transfer FlowFiles from
     * queue partitions where the partitioner indicates that the data should be rebalanced, but does so in a way that we don't immediately rebalance
     * all FlowFiles. This is desirable in a case such as when we have a lot of data queued up in a connection and then a node temporarily disconnects.
     * We don't want to then just push all data to other nodes. We'd rather push the data out to other nodes slowly while waiting for the disconnected
     * node to reconnect. And if the node reconnects, we want to keep sending it data.
     */
    public void nodeDisconnected() {
        if (!loadBalanceSessionLock.tryLock()) {
            // If we are not able to obtain the loadBalanceSessionLock, we cannot access the load balance session.
            return;
        }

        try {
            final LoadBalanceSession session = getFailoverSession();
            if (session != null) {
                loadBalanceSession = null;

                logger.debug("Node {} disconnected so will terminate the Load Balancing Session", nodeIdentifier);
                final List<FlowFileRecord> flowFilesSent = session.getFlowFilesSent();

                if (!flowFilesSent.isEmpty()) {
                    session.getPartition().getFailureCallback().onTransactionFailed(session.getFlowFilesSent(), TransactionFailureCallback.TransactionPhase.SENDING);
                }

                close();
                penalize();
                return;
            }

            // Obtain a partition that needs to be rebalanced on failure
            final RegisteredPartition readyPartition = getReadyPartition(partition -> partition.getFailureCallback().isRebalanceOnFailure());
            if (readyPartition == null) {
                return;
            }

            partitionQueue.offer(readyPartition); // allow partition to be obtained again
            final TransactionThreshold threshold = newTransactionThreshold();

            final List<FlowFileRecord> flowFiles = new ArrayList<>();
            while (!threshold.isThresholdMet()) {
                final FlowFileRecord flowFile = readyPartition.getFlowFileRecordSupplier().get();
                if (flowFile == null) {
                    break;
                }

                flowFiles.add(flowFile);
                threshold.adjust(1, flowFile.getSize());
            }

            logger.debug("Node {} not connected so failing {} FlowFiles for Load Balancing", nodeIdentifier, flowFiles.size());
            readyPartition.getFailureCallback().onTransactionFailed(flowFiles, TransactionFailureCallback.TransactionPhase.SENDING);
            penalize(); // Don't just transfer FlowFiles out of queue's partition as fast as possible, because the node may only be disconnected for a short time.
        } finally {
            loadBalanceSessionLock.unlock();
        }
    }

    private synchronized LoadBalanceSession getFailoverSession() {
        if (loadBalanceSession != null && !loadBalanceSession.isComplete()) {
            return loadBalanceSession;
        }

        return null;
    }


    private RegisteredPartition getReadyPartition() {
        return getReadyPartition(partition -> true);
    }

    private synchronized RegisteredPartition getReadyPartition(final Predicate<RegisteredPartition> filter) {
        final List<RegisteredPartition> polledPartitions = new ArrayList<>();

        try {
            RegisteredPartition partition;
            while ((partition = partitionQueue.poll()) != null) {
                if (partition.isEmpty() || !filter.test(partition)) {
                    polledPartitions.add(partition);
                    continue;
                }

                return partition;
            }

            return null;
        } finally {
            polledPartitions.forEach(partitionQueue::offer);
        }
    }

    private synchronized LoadBalanceSession getActiveTransaction(final RegisteredPartition proposedPartition) {
        if (loadBalanceSession != null && !loadBalanceSession.isComplete()) {
            return loadBalanceSession;
        }

        final RegisteredPartition readyPartition = proposedPartition == null ? getReadyPartition() : proposedPartition;
        if (readyPartition == null) {
            return null;
        }

        loadBalanceSession = new LoadBalanceSession(readyPartition, flowFileContentAccess, flowFileCodec, channel, timeoutMillis, newTransactionThreshold());
        partitionQueue.offer(readyPartition);

        return loadBalanceSession;
    }

    private TransactionThreshold newTransactionThreshold() {
         return new SimpleLimitThreshold(1000, 10_000_000L);
    }

    private synchronized boolean isConnectionEstablished() {
        return selector != null && channel != null && channel.isConnected();
    }

    private synchronized void establishConnection() throws IOException {
        SocketChannel socketChannel = null;

        try {
            selector = Selector.open();
            socketChannel = createChannel();

            socketChannel.configureBlocking(true);

            channel = createPeerChannel(socketChannel, nodeIdentifier.toString());
            channel.performHandshake();

            socketChannel.configureBlocking(false);
            selectionKey = socketChannel.register(selector, SelectionKey.OP_WRITE | SelectionKey.OP_READ);
        } catch (Exception e) {
            logger.error("Unable to connect to {} for load balancing", nodeIdentifier, e);

            if (selector != null) {
                try {
                    selector.close();
                } catch (final Exception e1) {
                    e.addSuppressed(e1);
                }
            }

            if (channel != null) {
                try {
                    channel.close();
                } catch (final Exception e1) {
                    e.addSuppressed(e1);
                }
            }

            if (socketChannel != null) {
                try {
                    socketChannel.close();
                } catch (final Exception e1) {
                    e.addSuppressed(e1);
                }
            }

            throw e;
        }
    }


    private PeerChannel createPeerChannel(final SocketChannel channel, final String peerDescription) {
        if (sslContext == null) {
            logger.debug("No SSL Context is available so will not perform SSL Handshake with Peer {}", peerDescription);
            return new PeerChannel(channel, null, peerDescription);
        }

        logger.debug("Performing SSL Handshake with Peer {}", peerDescription);

        final SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(true);
        sslEngine.setNeedClientAuth(true);

        return new PeerChannel(channel, sslEngine, peerDescription);
    }


    private SocketChannel createChannel() throws IOException {
        final SocketChannel socketChannel = SocketChannel.open();
        try {
            socketChannel.configureBlocking(true);
            final Socket socket = socketChannel.socket();
            socket.setSoTimeout(timeoutMillis);

            socket.connect(new InetSocketAddress(nodeIdentifier.getLoadBalanceAddress(), nodeIdentifier.getLoadBalancePort()));
            socket.setSoTimeout(timeoutMillis);

            return socketChannel;
        } catch (final Exception e) {
            try {
                socketChannel.close();
            } catch (final Exception closeException) {
                e.addSuppressed(closeException);
            }

            throw e;
        }
    }


    @Override
    public String toString() {
        return "NioAsyncLoadBalanceClient[nodeId=" + nodeIdentifier + "]";
    }
}
