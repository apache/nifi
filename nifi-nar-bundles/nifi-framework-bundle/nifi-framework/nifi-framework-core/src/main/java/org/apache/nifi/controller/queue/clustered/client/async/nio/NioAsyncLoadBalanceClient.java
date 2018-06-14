package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.queue.clustered.FlowFileContentAccess;
import org.apache.nifi.controller.queue.clustered.client.LoadBalanceFlowFileCodec;
import org.apache.nifi.controller.queue.clustered.client.async.AsyncLoadBalanceClient;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionCompleteCallback;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionFailureCallback;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.EOFException;
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
import java.util.function.Supplier;

import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.ABORT_PROTOCOL_NEGOTIATION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.REQEUST_DIFFERENT_VERSION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.VERSION_ACCEPTED;


// TODO: Support SSL!!!
public class NioAsyncLoadBalanceClient implements AsyncLoadBalanceClient {
    private static final Logger logger = LoggerFactory.getLogger(NioAsyncLoadBalanceClient.class);
    private static final long PENALIZATION_MILLIS = TimeUnit.SECONDS.toMillis(1L);

    private final NodeIdentifier nodeIdentifier;
    private final SSLContext sslContext;
    private final int timeoutMillis;
    private final FlowFileContentAccess flowFileContentAccess;
    private final LoadBalanceFlowFileCodec flowFileCodec;

    private volatile boolean running = false;
    private final AtomicLong penalizationEnd = new AtomicLong(0L);

    private final Map<String, RegisteredPartition> registeredPartitions = new HashMap<>();
    private final Queue<RegisteredPartition> partitionQueue = new LinkedBlockingQueue<>();

    private final Lock lock = new ReentrantLock();

    // guarded by synchronizing on this
    private PeerChannel channel;
    private Selector selector;
    private SelectionKey selectionKey;
    private ActiveTransaction activeTransaction = null;


    public NioAsyncLoadBalanceClient(final NodeIdentifier nodeIdentifier, final SSLContext sslContext, final int timeoutMillis, final FlowFileContentAccess flowFileContentAccess,
                                     final LoadBalanceFlowFileCodec flowFileCodec) {
        this.nodeIdentifier = nodeIdentifier;
        this.sslContext = sslContext;
        this.timeoutMillis = timeoutMillis;
        this.flowFileContentAccess = flowFileContentAccess;
        this.flowFileCodec = flowFileCodec;
    }

    @Override
    public NodeIdentifier getNodeIdentifier() {
        return nodeIdentifier;
    }

    public synchronized void register(final String connectionId, final BooleanSupplier emptySupplier, final Supplier<FlowFileRecord> flowFileSupplier,
                                      final TransactionFailureCallback failureCallback, final TransactionCompleteCallback successCallback) {

        if (registeredPartitions.containsKey(connectionId)) {
            throw new IllegalStateException("Connection with ID " + connectionId + " is already registered");
        }

        final RegisteredPartition partition = new RegisteredPartition(connectionId, emptySupplier, flowFileSupplier, failureCallback, successCallback);
        registeredPartitions.put(connectionId, partition);
        partitionQueue.add(partition);
    }

    public synchronized void unregister(final String connectionId) {
        registeredPartitions.remove(connectionId);
    }

    private synchronized Map<String, RegisteredPartition> getRegisteredPartitions() {
        return new HashMap<>(registeredPartitions);
    }

    public void start() {
        running = true;
        logger.info("{} started", this);
    }

    public void stop() {
        running = false;
        logger.info("{} stopped", this);
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
        if (!lock.tryLock()) {
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

            final ActiveTransaction transaction = getActiveTransaction(readyPartition);
            if (transaction == null) {
                penalize();
                return false;
            }

            selector.selectNow();
            final boolean ready = (transaction.getDesiredReadinessFlag() & selectionKey.readyOps()) != 0;
            if (!ready) {
                return false;
            }

            boolean success;
            try {
                success = transaction.communicate();
            } catch (final Exception e) {
                logger.error("Failed to communicate with Peer {}", nodeIdentifier.toString(), e);
                penalize();
                transaction.getPartition().getFailureCallback().onTransactionFailed(transaction.getFlowFilesSent(), e, TransactionFailureCallback.TransactionPhase.SENDING);
                close();

                return false;
            }

            final boolean anySuccess = success;

            while (success) {
                success = transaction.communicate();
            }

            if (transaction.isComplete()) {
                transaction.getPartition().getSuccessCallback().onTransactionComplete(transaction.getFlowFilesSent());

                // TODO: This is temporary until the server supports long-lived connections!
                close();
            }

            return anySuccess;
        } catch (final Exception e) {
            close();
            activeTransaction  = null;
            throw e;
        } finally {
            lock.unlock();
        }
    }


    private synchronized RegisteredPartition getReadyPartition() {
        final List<RegisteredPartition> polledPartitions = new ArrayList<>();

        try {
            RegisteredPartition partition;
            while ((partition = partitionQueue.poll()) != null) {
                if (partition.isEmpty()) {
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

    private synchronized ActiveTransaction getActiveTransaction(final RegisteredPartition partition) {
        if (activeTransaction != null && !activeTransaction.isComplete()) {
            return activeTransaction;
        }

        final RegisteredPartition readyPartition = partition == null ? getReadyPartition() : partition;
        if (readyPartition == null) {
            return null;
        }

        activeTransaction = new ActiveTransaction(readyPartition, flowFileContentAccess, flowFileCodec, channel, timeoutMillis);
        partitionQueue.offer(partition);

        return activeTransaction;
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

            final int protocolVersion = negotiateProtocolVersion(channel, nodeIdentifier.toString());
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


    private PeerChannel createPeerChannel(final SocketChannel channel, final String peerDescription) throws IOException {
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


    // TODO: Make this part of the transaction
    private int negotiateProtocolVersion(final PeerChannel channel, final String peerDescription) throws IOException {
        final VersionNegotiator negotiator = new StandardVersionNegotiator(1);

        int recommendedVersion = 1;
        while (true) {
            logger.debug("Recommending to Peer {} that we use version {} of the Load Balance Protocol", peerDescription, recommendedVersion);
            final boolean written = channel.write((byte) recommendedVersion);
            if (!written) {
                logger.error("Failed to write Recommended Protocol Version to Peer " + peerDescription);
            }

            final int response = channel.read().getAsInt();
            if (response < 0) {
                throw new EOFException("Requested that Peer " + peerDescription + " use version " + recommendedVersion
                        + " of the Load Balance Protocol but encountered EOFException while waiting for a response");
            }

            if (response == VERSION_ACCEPTED) {
                logger.debug("Peer {} accepted version {} of the Load Balance Protocol. Will use this version.", peerDescription, recommendedVersion);
                return recommendedVersion;
            } else if ( response == REQEUST_DIFFERENT_VERSION) {
                final int requestedVersion = channel.read().getAsInt();
                logger.debug("Peer {} requested that we use version {} of Load Balance Protocol instead of version {}", peerDescription, requestedVersion, recommendedVersion);

                if (negotiator.isVersionSupported(requestedVersion)) {
                    logger.debug("Accepting Version {}", requestedVersion);
                    return requestedVersion;
                } else {
                    final Integer preferred = negotiator.getPreferredVersion(requestedVersion);
                    if (preferred == null) {
                        logger.debug("Peer {} requested version {} of the Load Balance Protocol. This version is not acceptable. Aborting communications.", peerDescription, requestedVersion);
                        channel.write((byte) ABORT_PROTOCOL_NEGOTIATION);
                    } else {
                        recommendedVersion = preferred;
                        logger.debug("Recommending version {} instead", recommendedVersion);
                        continue;
                    }
                }
            } else {
                throw new IOException("Failed to negotiate Protocol Version with Peer " + peerDescription + ". Recommended version " + recommendedVersion + " but instead of an ACCEPT or REJECT " +
                        "response got back a response of " + response);
            }
        }
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
