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
package org.apache.nifi.controller;

import org.apache.nifi.asset.AssetSynchronizer;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerCapabilityDetection;
import org.apache.nifi.authorization.ManagedAuthorizer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.cluster.ConnectionException;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.ClusterRoles;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.coordination.node.OffloadCode;
import org.apache.nifi.cluster.exception.NoClusterCoordinatorException;
import org.apache.nifi.cluster.protocol.ComponentRevisionSnapshot;
import org.apache.nifi.cluster.protocol.ConnectionRequest;
import org.apache.nifi.cluster.protocol.ConnectionResponse;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.cluster.protocol.impl.NodeProtocolSenderListener;
import org.apache.nifi.cluster.protocol.message.ConnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.DisconnectMessage;
import org.apache.nifi.cluster.protocol.message.FlowRequestMessage;
import org.apache.nifi.cluster.protocol.message.FlowResponseMessage;
import org.apache.nifi.cluster.protocol.message.OffloadMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ReconnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ReconnectionResponseMessage;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.groups.BundleUpdateStrategy;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.lifecycle.LifeCycleStartException;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.nar.NarManager;
import org.apache.nifi.persistence.FlowConfigurationDAO;
import org.apache.nifi.persistence.StandardFlowConfigurationDAO;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.revision.RevisionManager;
import org.apache.nifi.web.revision.RevisionSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class StandardFlowService implements FlowService, ProtocolHandler {

    private static final String EVENT_CATEGORY = "Controller";
    private static final String CLUSTER_NODE_CONFIG = "Cluster Node Configuration";

    // state keys
    private static final String NODE_UUID = "Node UUID";

    private final FlowController controller;
    private final FlowConfigurationDAO dao;
    private final int gracefulShutdownSeconds;
    private final boolean autoResumeState;
    private final Authorizer authorizer;

    // Lock is used to protect the flow.json.gz file.
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<ScheduledExecutorService> executor = new AtomicReference<>(null);
    private final AtomicReference<SaveHolder> saveHolder = new AtomicReference<>(null);
    private final ClusterCoordinator clusterCoordinator;
    private final RevisionManager revisionManager;
    private final NarManager narManager;
    private final AssetSynchronizer assetSynchronizer;
    private volatile SaveReportingTask saveReportingTask;

    /**
     * listener/sender for internal cluster communication
     */
    private final NodeProtocolSenderListener senderListener;

    /**
     * flag indicating whether we are operating in a clustered environment
     */
    private final boolean configuredForClustering;

    /**
     * the node identifier
     */
    private NodeIdentifier nodeId;

    // guardedBy rwLock
    private boolean firstControllerInitialization = true;

    private final NiFiProperties nifiProperties;

    private static final String CONNECTION_EXCEPTION_MSG_PREFIX = "Failed to connect node to cluster";
    private static final Logger logger = LoggerFactory.getLogger(StandardFlowService.class);

    public static StandardFlowService createStandaloneInstance(
            final FlowController controller,
            final NiFiProperties nifiProperties,
            final RevisionManager revisionManager,
            final NarManager narManager,
            final AssetSynchronizer assetSynchronizer,
            final Authorizer authorizer) throws IOException {

        return new StandardFlowService(controller, nifiProperties, null, false, null, revisionManager, narManager, assetSynchronizer, authorizer);
    }

    public static StandardFlowService createClusteredInstance(
            final FlowController controller,
            final NiFiProperties nifiProperties,
            final NodeProtocolSenderListener senderListener,
            final ClusterCoordinator coordinator,
            final RevisionManager revisionManager,
            final NarManager narManager,
            final AssetSynchronizer assetSynchronizer,
            final Authorizer authorizer) throws IOException {

        return new StandardFlowService(controller, nifiProperties, senderListener, true, coordinator, revisionManager, narManager, assetSynchronizer, authorizer);
    }

    private StandardFlowService(
            final FlowController controller,
            final NiFiProperties nifiProperties,
            final NodeProtocolSenderListener senderListener,
            final boolean configuredForClustering,
            final ClusterCoordinator clusterCoordinator,
            final RevisionManager revisionManager,
            final NarManager narManager,
            final AssetSynchronizer assetSynchronizer,
            final Authorizer authorizer) throws IOException {

        this.nifiProperties = nifiProperties;
        this.controller = controller;

        gracefulShutdownSeconds = (int) FormatUtils.getTimeDuration(nifiProperties.getProperty(NiFiProperties.FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD), TimeUnit.SECONDS);
        autoResumeState = nifiProperties.getAutoResumeState();

        dao = new StandardFlowConfigurationDAO(nifiProperties, controller.getExtensionManager());
        this.clusterCoordinator = clusterCoordinator;
        if (clusterCoordinator != null) {
            clusterCoordinator.setFlowService(this);
        }
        this.revisionManager = revisionManager;
        this.narManager = narManager;
        this.assetSynchronizer = assetSynchronizer;
        this.authorizer = authorizer;

        if (configuredForClustering) {
            this.configuredForClustering = configuredForClustering;

            this.senderListener = senderListener;
            senderListener.addHandler(this);

            final InetSocketAddress nodeApiAddress = nifiProperties.getNodeApiAddress();
            final InetSocketAddress nodeSocketAddress = nifiProperties.getClusterNodeProtocolAddress();
            final InetSocketAddress loadBalanceAddress = nifiProperties.getClusterLoadBalanceAddress();

            String nodeUuid = null;
            final StateManager stateManager = controller.getStateManagerProvider().getStateManager(CLUSTER_NODE_CONFIG);
            if (stateManager != null) {
                nodeUuid = stateManager.getState(Scope.LOCAL).get(NODE_UUID);
            }

            if (nodeUuid == null) {
                nodeUuid = UUID.randomUUID().toString();
            }

            // use a random UUID as the proposed node identifier
            this.nodeId = new NodeIdentifier(nodeUuid,
                    nodeApiAddress.getHostName(), nodeApiAddress.getPort(),
                    nodeSocketAddress.getHostName(), nodeSocketAddress.getPort(),
                    loadBalanceAddress.getHostName(), loadBalanceAddress.getPort(),
                    nifiProperties.getRemoteInputHost(), nifiProperties.getRemoteInputPort(),
                    nifiProperties.getRemoteInputHttpPort(), nifiProperties.isSiteToSiteSecure());

        } else {
            this.configuredForClustering = false;
            this.senderListener = null;
        }
    }

    @Override
    public void saveFlowChanges() throws IOException {
        writeLock.lock();
        try {
            dao.save(controller);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void saveFlowChanges(final TimeUnit delayUnit, final long delay) {
        final boolean archiveEnabled = nifiProperties.isFlowConfigurationArchiveEnabled();
        saveFlowChanges(delayUnit, delay, archiveEnabled);
    }

    @Override
    public void saveFlowChanges(final TimeUnit delayUnit, final long delay, final boolean archive) {
        final Calendar saveTime = Calendar.getInstance();
        final long delayInMs = TimeUnit.MILLISECONDS.convert(delay, delayUnit);
        int finalDelayMs = 500; //default to 500 ms.
        if (delayInMs <= Integer.MAX_VALUE) {
            finalDelayMs = (int) delayInMs;
        }
        saveTime.add(Calendar.MILLISECOND, finalDelayMs);

        if (logger.isTraceEnabled()) {
            logger.trace(" A request to save the flow has been made with delay {} for time {}", finalDelayMs, saveTime.getTime());
        }

        saveHolder.set(new SaveHolder(saveTime, archive));
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void start() throws LifeCycleStartException {
        writeLock.lock();
        try {
            if (isRunning()) {
                return;
            }

            running.set(true);

            final ScheduledExecutorService newExecutor = new FlowEngine(2, "Flow Service Tasks");
            saveReportingTask = new SaveReportingTask();
            newExecutor.scheduleWithFixedDelay(saveReportingTask, 0L, 500L, TimeUnit.MILLISECONDS);
            this.executor.set(newExecutor);

            if (configuredForClustering) {
                senderListener.start();
            }

        } catch (final IOException ioe) {
            try {
                stop(/* force */true);
            } catch (final Exception ignored) {
            }

            throw new LifeCycleStartException("Failed to start Flow Service due to: " + ioe, ioe);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void stop(final boolean force) {
        writeLock.lock();
        try {

            if (!isRunning()) {
                return;
            }

            running.set(false);

            if (clusterCoordinator != null) {
                try {
                    clusterCoordinator.shutdown();
                } catch (final Throwable t) {
                    logger.error("Failed to properly shutdown coordinator", t);
                }
            }

            if (!controller.isTerminated()) {
                controller.shutdown(force);
            }

            if (configuredForClustering && senderListener != null) {
                try {
                    senderListener.stop();
                } catch (final IOException ioe) {
                    logger.warn("Protocol sender/listener did not stop gracefully", ioe);
                }
            }
        } finally {
            writeLock.unlock();
        }

        final ScheduledExecutorService executorService = executor.get();
        if (executorService != null) {
            if (force) {
                executorService.shutdownNow();
            } else {
                executorService.shutdown();
            }

            boolean graceful;
            try {
                graceful = executorService.awaitTermination(gracefulShutdownSeconds, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                graceful = false;
            }

            if (!graceful) {
                logger.warn("Scheduling service did not gracefully shutdown within configured {} second window", gracefulShutdownSeconds);
            }
        }

        // Ensure that our background save reporting task has a chance to run, because we've now shut down the executor, which could cause the save reporting task to get canceled.
        saveReportingTask.run();
    }

    @Override
    public boolean canHandle(final ProtocolMessage msg) {
        return switch (msg.getType()) {
            case RECONNECTION_REQUEST, OFFLOAD_REQUEST, DISCONNECTION_REQUEST, FLOW_REQUEST -> true;
            default -> false;
        };
    }

    @Override
    public ProtocolMessage handle(final ProtocolMessage request, final Set<String> nodeIdentities) throws ProtocolException {
        final long startNanos = System.nanoTime();
        try {
            switch (request.getType()) {
                case FLOW_REQUEST:
                    return handleFlowRequest((FlowRequestMessage) request);
                case RECONNECTION_REQUEST: {
                    // Suspend heartbeats until we've reconnected. Otherwise,
                    // we may send a heartbeat while we are still in the process of
                    // connecting, which will cause the Cluster Manager to mark us
                    // as "Connected," which becomes problematic as the FlowController's lock
                    // may still be held, causing this node to take a long time to respond to requests.
                    controller.suspendHeartbeats();

                    final Thread t = new Thread(() -> handleReconnectionRequest((ReconnectionRequestMessage) request), "Reconnect to Cluster");
                    t.setDaemon(true);
                    t.start();

                    return new ReconnectionResponseMessage();
                }
                case OFFLOAD_REQUEST: {
                    final Thread t = new Thread(() -> {
                        try {
                            handleOffloadRequest((OffloadMessage) request);
                        } catch (InterruptedException e) {
                            throw new ProtocolException("Could not complete offload request", e);
                        }
                    }, "Offload Flow Files from Node");
                    t.setDaemon(true);
                    t.start();

                    return null;
                }
                case DISCONNECTION_REQUEST: {
                    final Thread t = new Thread(() -> handleDisconnectionRequest((DisconnectMessage) request), "Disconnect from Cluster");
                    t.setDaemon(true);
                    t.start();

                    return null;
                }
                default:
                    throw new ProtocolException("Handler cannot handle message type: " + request.getType());
            }
        } finally {
            if (logger.isDebugEnabled()) {
                final long procNanos = System.nanoTime() - startNanos;
                final long procMillis = TimeUnit.MILLISECONDS.convert(procNanos, TimeUnit.NANOSECONDS);
                logger.debug("Finished Processing Protocol Message of type {} in {} millis", request.getType(), procMillis);
            }
        }
    }

    @Override
    public void load(final DataFlow dataFlow) throws IOException, FlowSerializationException, FlowSynchronizationException, UninheritableFlowException, MissingBundleException {
        if (configuredForClustering) {
            // Create the initial flow from disk if it exists, or from serializing the empty root group in flow controller
            final DataFlow initialFlow = (dataFlow == null) ? createDataFlow() : dataFlow;
            if (logger.isTraceEnabled()) {
                logger.trace("InitialFlow = {}", new String(initialFlow.getFlow(), StandardCharsets.UTF_8));
            }

            final ConnectionResponse response = connect(true, true, initialFlow);

            // obtain write lock while we are updating the controller. We need to ensure that we don't
            // obtain the lock before calling connect(), though, or we will end up getting a deadlock
            // because the node that is receiving the connection request won't be able to get the current
            // flow, as that requires a read lock.
            writeLock.lock();
            try {
                if (response == null || response.shouldTryLater()) {
                    logger.info("Flow controller will load local dataflow and suspend connection handshake until a cluster connection response is received.");

                    // set node ID on controller before we start heartbeating because heartbeat needs node ID
                    controller.setNodeId(nodeId);
                    clusterCoordinator.setLocalNodeIdentifier(nodeId);

                    // set node as clustered, since it is trying to connect to a cluster
                    controller.setClustered(true, null);
                    clusterCoordinator.setConnected(false);

                    controller.setConnectionStatus(new NodeConnectionStatus(nodeId, DisconnectionCode.NOT_YET_CONNECTED));

                    /*
                     * Start heartbeating. Heartbeats will fail because we can't reach
                     * the manager, but when we locate the manager, the node will
                     * reconnect and establish a connection to the cluster. The
                     * heartbeat is the trigger that will cause the manager to
                     * issue a reconnect request.
                     */
                    controller.startHeartbeating();

                } else {
                    try {
                        loadFromConnectionResponse(response);
                        dao.save(controller, true);
                    } catch (final Exception e) {
                        logger.error("Failed to load flow from cluster", e);
                        handleConnectionFailure(e);
                        throw new IOException(e);
                    }
                }

            } finally {
                writeLock.unlock();
            }
        } else {
            writeLock.lock();
            try {
                // operating in standalone mode, so load proposed flow and initialize the controller
                loadFromBytes(dataFlow, true, BundleUpdateStrategy.USE_SPECIFIED_OR_COMPATIBLE_OR_GHOST);
                initializeController();
                dao.save(controller, true);
            } finally {
                writeLock.unlock();
            }
        }
    }

    private void handleConnectionFailure(final Exception ex) {
        DisconnectionCode disconnectionCode;
        if (ex instanceof UninheritableFlowException) {
            disconnectionCode = DisconnectionCode.MISMATCHED_FLOWS;
        } else if (ex instanceof MissingBundleException) {
            disconnectionCode = DisconnectionCode.MISSING_BUNDLE;
        } else if (ex instanceof FlowSynchronizationException) {
            disconnectionCode = DisconnectionCode.MISMATCHED_FLOWS;
        } else {
            disconnectionCode = DisconnectionCode.STARTUP_FAILURE;
        }
        clusterCoordinator.disconnectionRequestedByNode(getNodeId(), disconnectionCode, ex.toString());
        controller.setClustered(false, null);
        clusterCoordinator.setConnected(false);
    }

    private FlowResponseMessage handleFlowRequest(final FlowRequestMessage request) throws ProtocolException {
        readLock.lock();
        try {
            logger.info("Received flow request message from cluster coordinator.");

            // create the response
            final FlowResponseMessage response = new FlowResponseMessage();
            response.setDataFlow(createDataFlowFromController());
            return response;
        } catch (final Exception ex) {
            throw new ProtocolException("Failed serializing flow controller state for flow request due to: " + ex, ex);
        } finally {
            readLock.unlock();
        }
    }

    private byte[] getAuthorizerFingerprint() {
        final boolean isInternalAuthorizer = AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer);
        return isInternalAuthorizer ? ((ManagedAuthorizer) authorizer).getFingerprint().getBytes(StandardCharsets.UTF_8) : null;
    }

    @Override
    public StandardDataFlow createDataFlow() throws IOException {
        // Load the flow from disk if the file exists.
        if (dao.isFlowPresent()) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            dao.load(baos);
            final byte[] bytes = baos.toByteArray();

            final byte[] snippetBytes = controller.getSnippetManager().export();
            final byte[] authorizerFingerprint = getAuthorizerFingerprint();
            final StandardDataFlow fromDisk = new StandardDataFlow(bytes, snippetBytes, authorizerFingerprint, new HashSet<>());
            return fromDisk;
        }

        // Flow from disk does not exist, so serialize the Flow Controller and use that.
        // This is done because on startup, if there is no flow, the Flow Controller
        // will automatically create a Root Process Group, and we need to ensure that
        // we replicate that Process Group to all nodes in the cluster, so that they all
        // end up with the same ID for the root Process Group.
        return createDataFlowFromController();
    }

    @Override
    public StandardDataFlow createDataFlowFromController() throws IOException {
        final byte[] snippetBytes = controller.getSnippetManager().export();
        final byte[] authorizerFingerprint = getAuthorizerFingerprint();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        dao.save(controller, baos);
        final byte[] flowBytes = baos.toByteArray();
        baos.reset();

        final FlowManager flowManager = controller.getFlowManager();

        final Set<String> missingComponents = new HashSet<>();
        flowManager.getRootGroup().findAllProcessors().stream().filter(AbstractComponentNode::isExtensionMissing).forEach(p -> missingComponents.add(p.getIdentifier()));
        flowManager.getAllControllerServices().stream().filter(ComponentNode::isExtensionMissing).forEach(cs -> missingComponents.add(cs.getIdentifier()));
        controller.getAllReportingTasks().stream().filter(ComponentNode::isExtensionMissing).forEach(r -> missingComponents.add(r.getIdentifier()));
        controller.getFlowManager().getAllParameterProviders().stream().filter(ComponentNode::isExtensionMissing).forEach(r -> missingComponents.add(r.getIdentifier()));
        controller.getFlowManager().getAllFlowRegistryClients().stream().filter(ComponentNode::isExtensionMissing).forEach(c -> missingComponents.add(c.getIdentifier()));

        return new StandardDataFlow(flowBytes, snippetBytes, authorizerFingerprint, missingComponents);
    }


    private NodeIdentifier getNodeId() {
        readLock.lock();
        try {
            return nodeId;
        } finally {
            readLock.unlock();
        }
    }

    private void handleReconnectionRequest(final ReconnectionRequestMessage request) {
        try {
            logger.info("Processing reconnection request from cluster coordinator.");

            // We are no longer connected to the cluster. But the intent is to reconnect to the cluster.
            // So we don't want to call FlowController.setClustered(false, null).
            // It is important, though, that we perform certain tasks, such as un-registering the node as Cluster Coordinator/Primary Node
            if (controller.isConnected()) {
                controller.onClusterDisconnect();
            }

            // reconnect
            ConnectionResponse connectionResponse = new ConnectionResponse(getNodeId(), request.getDataFlow(),
                    request.getInstanceId(), request.getNodeConnectionStatuses(), request.getComponentRevisions());

            if (connectionResponse.getDataFlow() == null) {
                logger.info("Received a Reconnection Request that contained no DataFlow. Will attempt to connect to cluster using local flow.");
                connectionResponse = connect(false, false, createDataFlowFromController());
            }

            if (connectionResponse == null) {
                // If we could not communicate with the cluster, just log a warning and return.
                // If the node is currently in a CONNECTING state, it will continue to heartbeat, and that will continue to
                // result in attempting to connect to the cluster.
                logger.warn("Received a Reconnection Request that contained no DataFlow, and was unable to communicate with an active Cluster Coordinator. Cannot connect to cluster at this time.");
                controller.resumeHeartbeats();
                return;
            }

            loadFromConnectionResponse(connectionResponse);

            clusterCoordinator.resetNodeStatuses(connectionResponse.getNodeConnectionStatuses().stream()
                    .collect(Collectors.toMap(NodeConnectionStatus::getNodeIdentifier, status -> status)));
            // reconnected, this node needs to explicitly write the inherited flow to disk, and resume heartbeats
            saveFlowChanges();
            controller.onClusterConnect();

            logger.info("Node reconnected.");
        } catch (final Exception ex) {
            // disconnect controller
            if (controller.isClustered()) {
                disconnect("Failed to properly handle Reconnection request due to " + ex.toString());
            }

            logger.error("Handling reconnection request failed", ex);
            handleConnectionFailure(ex);
        }
    }

    private void handleOffloadRequest(final OffloadMessage request) throws InterruptedException {
        logger.info("Received offload request message from cluster coordinator with explanation: {}", request.getExplanation());
        offload(request.getExplanation());
    }

    private void offload(final String explanation) throws InterruptedException {
        writeLock.lock();
        try {
            logger.info("Offloading node due to {}", explanation);

            // mark node as offloading
            controller.setConnectionStatus(new NodeConnectionStatus(nodeId, NodeConnectionState.OFFLOADING, OffloadCode.OFFLOADED, explanation));

            final FlowManager flowManager = controller.getFlowManager();

            // request to stop all processors on node
            flowManager.getRootGroup().stopProcessing();

            // terminate all processors
            flowManager.getRootGroup().findAllProcessors()
                    // filter stream, only stopped processors can be terminated
                    .stream().filter(pn -> pn.getScheduledState() == ScheduledState.STOPPED)
                    .forEach(pn -> pn.getProcessGroup().terminateProcessor(pn));

            // request to stop all remote process groups
            flowManager.getRootGroup().findAllRemoteProcessGroups()
                    .stream().filter(RemoteProcessGroup::isTransmitting)
                    .forEach(rpg -> {
                        try {
                            rpg.stopTransmitting().get(rpg.getCommunicationsTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
                        } catch (final Exception e) {
                            logger.warn("Encountered failure while waiting for {} to shutdown", rpg, e);
                        }
                    });

            // offload all queues on node
            final Set<Connection> connections = flowManager.findAllConnections();
            for (final Connection connection : connections) {
                connection.getFlowFileQueue().offloadQueue();
            }

            final EventAccess eventAccess = controller.getEventAccess();
            ProcessGroupStatus controllerStatus;

            // wait for rebalance of flowfiles on all queues
            while (true) {
                controllerStatus = eventAccess.getControllerStatus();
                if (controllerStatus.getQueuedCount() <= 0) {
                    break;
                }

                logger.debug("Offloading queues on node {}, remaining queued count: {}", getNodeId(), controllerStatus.getQueuedCount());
                Thread.sleep(1000);
            }

            // finish offload
            for (final Connection connection : connections) {
                connection.getFlowFileQueue().resetOffloadedQueue();
            }

            controller.setConnectionStatus(new NodeConnectionStatus(nodeId, NodeConnectionState.OFFLOADED, OffloadCode.OFFLOADED, explanation));
            clusterCoordinator.finishNodeOffload(getNodeId());

            logger.info("Node offloaded due to {}", explanation);

        } finally {
            writeLock.unlock();
        }
    }

    private void handleDisconnectionRequest(final DisconnectMessage request) {
        logger.info("Received disconnection request message from cluster coordinator with explanation: {}", request.getExplanation());
        disconnect(request.getExplanation());
    }

    private void disconnect(final String explanation) {
        writeLock.lock();
        try {

            logger.info("Disconnecting node due to {}", explanation);

            // mark node as not connected
            controller.setConnectionStatus(new NodeConnectionStatus(nodeId, DisconnectionCode.UNKNOWN, explanation));

            // turn off primary flag
            controller.setPrimary(false);

            // stop heartbeating
            controller.stopHeartbeating();

            // set node to not clustered
            controller.setClustered(false, null);
            clusterCoordinator.setConnected(false);

            logger.info("Node disconnected due to {}", explanation);

        } finally {
            writeLock.unlock();
        }
    }

    // write lock must already be acquired
    private void loadFromBytes(final DataFlow proposedFlow, final boolean allowEmptyFlow, final BundleUpdateStrategy bundleUpdateStrategy)
            throws IOException, FlowSerializationException, FlowSynchronizationException, UninheritableFlowException, MissingBundleException {
        logger.trace("Loading flow from bytes");

        // resolve the given flow (null means load flow from disk)
        final DataFlow actualProposedFlow;
        final byte[] flowBytes;
        final byte[] authorizerFingerprint;
        final Set<String> missingComponents;

        if (proposedFlow == null) {
            final ByteArrayOutputStream flowOnDisk = new ByteArrayOutputStream();
            copyCurrentFlow(flowOnDisk);
            flowBytes = flowOnDisk.toByteArray();
            authorizerFingerprint = getAuthorizerFingerprint();
            missingComponents = new HashSet<>();
            logger.debug("Loaded Flow from bytes");
        } else {
            flowBytes = proposedFlow.getFlow();
            authorizerFingerprint = proposedFlow.getAuthorizerFingerprint();
            missingComponents = proposedFlow.getMissingComponents();
            logger.debug("Loaded flow from proposed flow");
        }

        actualProposedFlow = new StandardDataFlow(flowBytes, null, authorizerFingerprint, missingComponents);

        // load the flow
        logger.debug("Loading proposed flow into FlowController");
        dao.load(controller, actualProposedFlow, this, bundleUpdateStrategy);

        final ProcessGroup rootGroup = controller.getFlowManager().getRootGroup();
        if (rootGroup.isEmpty() && !allowEmptyFlow) {
            throw new FlowSynchronizationException("Failed to load flow because unable to connect to cluster and local flow is empty");
        }
    }

    private ConnectionResponse connect(final boolean retryOnCommsFailure, final boolean retryIndefinitely, final DataFlow dataFlow) throws ConnectionException {
        readLock.lock();
        try {
            logger.info("Connecting Node: {}", nodeId);

            // create connection request message
            final ConnectionRequest request = new ConnectionRequest(nodeId, dataFlow);

            final ConnectionRequestMessage requestMsg = new ConnectionRequestMessage();
            requestMsg.setConnectionRequest(request);

            // send connection request to cluster manager
            /*
             * Try to get a current copy of the cluster's dataflow from the manager
             * for ten times, sleeping between attempts. Ten times should be
             * enough because the manager will register the node as connecting
             * and therefore, no other changes to the cluster flow can occur.
             *
             * However, the manager needs to obtain a current data flow within
             * maxAttempts * tryLaterSeconds or else the node will fail to startup.
             */
            final int maxAttempts = 10;
            ConnectionResponse response = null;
            for (int i = 0; i < maxAttempts || retryIndefinitely; i++) {
                try {
                    // Upon NiFi startup, the node will register for the Cluster Coordinator role with the Leader Election Manager.
                    // Sometimes the node will register as an active participant, meaning that it wants to be elected. This happens when the entire cluster starts up,
                    // for example. (This is determined by checking whether or not there already is a Cluster Coordinator registered).
                    // Other times, it registers as a 'silent' member, meaning that it will not be elected.
                    // If the leader election timeout is long (say 30 or 60 seconds), it is possible that this node was the Leader and was then restarted,
                    // and upon restart found that itself was already registered as the Cluster Coordinator. As a result, it registers as a Silent member of the
                    // election, and then connects to itself as the Cluster Coordinator. At this point, since the node has just restarted, it doesn't know about
                    // any of the nodes in the cluster. As a result, it will get the Cluster Topology from itself, and think there are no other nodes in the cluster.
                    // This causes all other nodes to send in their heartbeats, which then results in them being disconnected because they were previously unknown and
                    // as a result asked to reconnect to the cluster.
                    //
                    // To avoid this, we do not allow the node to connect to itself if it's not an active participant. This means that when the entire cluster is started
                    // up, the node can still connect to itself because it will be an active participant. But if it is then restarted, it won't be allowed to connect
                    // to itself. It will instead have to wait until another node is elected Cluster Coordinator.
                    final boolean activeCoordinatorParticipant = controller.getLeaderElectionManager().isActiveParticipant(ClusterRoles.CLUSTER_COORDINATOR);

                    response = senderListener.requestConnection(requestMsg, activeCoordinatorParticipant).getConnectionResponse();

                    if (response.shouldTryLater()) {
                        logger.info("Requested by cluster coordinator to retry connection in {} seconds with explanation: {}", response.getTryLaterSeconds(), response.getRejectionReason());
                        try {
                            Thread.sleep(response.getTryLaterSeconds() * 1000);
                        } catch (final InterruptedException ie) {
                            // we were interrupted, so finish quickly
                            Thread.currentThread().interrupt();
                            break;
                        }
                    } else if (response.getRejectionReason() != null) {
                        logger.warn("Connection request was blocked by cluster coordinator with the explanation: {}", response.getRejectionReason());
                        // set response to null and treat a firewall blockage the same as getting no response from cluster coordinator
                        response = null;
                        break;
                    } else {
                        logger.info("Received successful response from Cluster Coordinator to Connection Request");
                        // we received a successful connection response from cluster coordinator
                        break;
                    }
                } catch (final NoClusterCoordinatorException ncce) {
                    logger.warn("There is currently no Cluster Coordinator. This often happens upon restart of NiFi when running an embedded ZooKeeper. Will register this node "
                        + "to become the active Cluster Coordinator and will attempt to connect to cluster again");
                    controller.registerForClusterCoordinator(true);

                    try {
                        Thread.sleep(1000L);
                    } catch (final InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } catch (final Exception pe) {
                    // could not create a socket and communicate with manager
                    logger.warn("Failed to connect to cluster", pe);
                    if (logger.isDebugEnabled()) {
                        logger.warn("", pe);
                    }

                    if (retryOnCommsFailure) {
                        try {
                            Thread.sleep(response == null ? 5000 : response.getTryLaterSeconds());
                        } catch (final InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }

            if (response == null) {
                // if response is null, then either we had IO problems or we were blocked by firewall or we couldn't determine manager's address
                return response;
            } else if (response.shouldTryLater()) {
                // if response indicates we should try later, then coordinator was unable to service our request. Just load local flow and move on.
                // when the cluster coordinator is able to service requests, this node's heartbeat will trigger the cluster coordinator to reach
                // out to this node and re-connect to the cluster.
                logger.info("Received a 'try again' response from Cluster Coordinator when attempting to connect to cluster with explanation '"
                    + response.getRejectionReason() + "'. However, the maximum number of retries have already completed. Will load local flow and connect to the cluster when able.");
                return null;
            } else {
                // cluster manager provided a successful response with a current dataflow
                // persist node uuid and index returned by coordinator and return the response to the caller
                try {
                    // Ensure that we have registered our 'cluster node configuration' state key
                    final Map<String, String> map = Collections.singletonMap(NODE_UUID, response.getNodeIdentifier().getId());
                    controller.getStateManagerProvider().getStateManager(CLUSTER_NODE_CONFIG).setState(map, Scope.LOCAL);
                } catch (final IOException ioe) {
                    logger.warn("Received successful response from Cluster Manager but failed to persist state about the Node's Unique Identifier and the Node's Index. "
                            + "This node may be assigned a different UUID when the node is restarted.", ioe);
                }

                return response;
            }
        } finally {
            readLock.unlock();
        }
    }

    private void loadFromConnectionResponse(final ConnectionResponse response) throws ConnectionException {
        writeLock.lock();
        try {
            if (response.getNodeConnectionStatuses() != null) {
                clusterCoordinator.resetNodeStatuses(response.getNodeConnectionStatuses().stream()
                    .collect(Collectors.toMap(NodeConnectionStatus::getNodeIdentifier, status -> status)));
            }

            // get the dataflow from the response
            final DataFlow dataFlow = response.getDataFlow();
            if (logger.isTraceEnabled()) {
                logger.trace("ResponseFlow = {}", new String(dataFlow.getFlow(), StandardCharsets.UTF_8));
            }

            logger.info("Setting Flow Controller's Node ID: {}", nodeId);
            nodeId = response.getNodeIdentifier();
            controller.setNodeId(nodeId);

            // sync NARs before loading flow, otherwise components could be ghosted and fail to join the cluster
            narManager.syncWithClusterCoordinator();

            // load new controller state
            loadFromBytes(dataFlow, true, BundleUpdateStrategy.USE_SPECIFIED_OR_COMPATIBLE_OR_GHOST);

            // sync assets after loading the flow so that parameter contexts exist first
            assetSynchronizer.synchronize();

            // set node ID on controller before we start heartbeating because heartbeat needs node ID
            clusterCoordinator.setLocalNodeIdentifier(nodeId);
            clusterCoordinator.setConnected(true);

            final ComponentRevisionSnapshot componentRevisionSnapshot = response.getComponentRevisions();
            final RevisionSnapshot revisionSnapshot = componentRevisionSnapshot.toRevisionSnapshot();
            revisionManager.reset(revisionSnapshot);

            // mark the node as clustered
            controller.setClustered(true, response.getInstanceId());

            controller.setConnectionStatus(new NodeConnectionStatus(nodeId, NodeConnectionState.CONNECTED));

            // Initialize the controller after the flow is loaded so we don't take any actions on repos until everything is good
            initializeController();

            // start the processors as indicated by the dataflow
            controller.onFlowInitialized(autoResumeState);

            loadSnippets(dataFlow.getSnippets());

            controller.startHeartbeating();

        } catch (final UninheritableFlowException ufe) {
            throw new UninheritableFlowException(CONNECTION_EXCEPTION_MSG_PREFIX, ufe);
        } catch (final MissingBundleException mbe) {
            throw new MissingBundleException(CONNECTION_EXCEPTION_MSG_PREFIX + " because cluster flow contains bundles that do not exist on the current node", mbe);
        } catch (final FlowSerializationException fse) {
            throw new ConnectionException(CONNECTION_EXCEPTION_MSG_PREFIX + " because local or cluster flow is malformed.", fse);
        } catch (final FlowSynchronizationException fse) {
            throw new FlowSynchronizationException(CONNECTION_EXCEPTION_MSG_PREFIX + " because local flow controller partially updated. "
                    + "Administrator should disconnect node and review flow for corruption.", fse);
        } catch (final Exception ex) {
            throw new ConnectionException("Failed to connect node to cluster due to: " + ex, ex);
        } finally {
            writeLock.unlock();
        }

    }

    private void initializeController() throws IOException {
        if (firstControllerInitialization) {
            logger.debug("First controller initialization, initializing controller...");
            controller.initializeFlow();
            firstControllerInitialization = false;
        }
    }

    @Override
    public void copyCurrentFlow(final OutputStream os) throws IOException {
        readLock.lock();
        try {
            dao.load(os);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void copyCurrentFlow(final File file) throws IOException {
        try (final OutputStream fos = new FileOutputStream(file);
             final OutputStream gzipOut = new GZIPOutputStream(fos, 1)) {
            copyCurrentFlow(gzipOut);
        }
    }

    public void loadSnippets(final byte[] bytes) {
        if (bytes.length == 0) {
            return;
        }

        final SnippetManager snippetManager = controller.getSnippetManager();
        snippetManager.clear();

        for (final StandardSnippet snippet : SnippetManager.parseBytes(bytes)) {
            snippetManager.addSnippet(snippet);
        }
    }

    private class SaveReportingTask implements Runnable {

        @Override
        public synchronized void run() {
            ClassLoader currentCl = null;

            final Bundle frameworkBundle = NarClassLoadersHolder.getInstance().getFrameworkBundle();
            if (frameworkBundle != null) {
                currentCl = Thread.currentThread().getContextClassLoader();
                final ClassLoader cl = frameworkBundle.getClassLoader();
                Thread.currentThread().setContextClassLoader(cl);
            }

            try {
                //Hang onto the SaveHolder here rather than setting it to null because if the save fails we will try again
                final SaveHolder holder = StandardFlowService.this.saveHolder.get();
                if (holder == null) {
                    return;
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("Save request time {} // Current time {}", holder.saveTime.getTime(), new Date());
                }

                final Calendar now = Calendar.getInstance();
                if (holder.saveTime.before(now)) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Waiting for write lock and then will save");
                    }
                    writeLock.lock();
                    try {
                        dao.save(controller, holder.shouldArchive);
                        // Nulling it out if it is still set to our current SaveHolder.  Otherwise leave it alone because it means
                        // another save is already pending.
                        final boolean noSavePending = StandardFlowService.this.saveHolder.compareAndSet(holder, null);
                        logger.info("Saved flow controller {} // Another save pending = {}", controller, !noSavePending);
                    } finally {
                        writeLock.unlock();
                    }
                }
            } catch (final Throwable t) {
                logger.error("Unable to save flow controller configuration", t);

                // record the failed save as a bulletin
                final Bulletin saveFailureBulletin = BulletinFactory.createBulletin(EVENT_CATEGORY, LogLevel.ERROR.name(), "Unable to save flow controller configuration.");
                controller.getBulletinRepository().addBulletin(saveFailureBulletin);
            } finally {
                if (currentCl != null) {
                    Thread.currentThread().setContextClassLoader(currentCl);
                }
            }
        }
    }

    private class SaveHolder {

        private final Calendar saveTime;
        private final boolean shouldArchive;

        private SaveHolder(final Calendar moment, final boolean archive) {
            saveTime = moment;
            shouldArchive = archive;
        }
    }
}
