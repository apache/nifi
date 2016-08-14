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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AbstractPolicyBasedAuthorizer;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.cluster.ConnectionException;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
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
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ReconnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ReconnectionResponseMessage;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.lifecycle.LifeCycleStartException;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.persistence.FlowConfigurationDAO;
import org.apache.nifi.persistence.StandardXMLFlowConfigurationDAO;
import org.apache.nifi.persistence.TemplateDeserializer;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.revision.RevisionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardFlowService implements FlowService, ProtocolHandler {

    private static final String EVENT_CATEGORY = "Controller";
    private static final String CLUSTER_NODE_CONFIG = "Cluster Node Configuration";

    // state keys
    private static final String NODE_UUID = "Node UUID";

    private final FlowController controller;
    private final Path flowXml;
    private final FlowConfigurationDAO dao;
    private final int gracefulShutdownSeconds;
    private final boolean autoResumeState;
    private final Authorizer authorizer;

    // Lock is used to protect the flow.xml file.
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<ScheduledExecutorService> executor = new AtomicReference<>(null);
    private final AtomicReference<SaveHolder> saveHolder = new AtomicReference<>(null);
    private final ClusterCoordinator clusterCoordinator;
    private final RevisionManager revisionManager;

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

    private static final String CONNECTION_EXCEPTION_MSG_PREFIX = "Failed to connect node to cluster because ";
    private static final Logger logger = LoggerFactory.getLogger(StandardFlowService.class);

    public static StandardFlowService createStandaloneInstance(
        final FlowController controller,
        final NiFiProperties properties,
        final StringEncryptor encryptor,
        final RevisionManager revisionManager,
        final Authorizer authorizer) throws IOException {

        return new StandardFlowService(controller, properties, null, encryptor, false, null, revisionManager, authorizer);
    }

    public static StandardFlowService createClusteredInstance(
        final FlowController controller,
        final NiFiProperties properties,
        final NodeProtocolSenderListener senderListener,
        final ClusterCoordinator coordinator,
        final StringEncryptor encryptor,
        final RevisionManager revisionManager,
        final Authorizer authorizer) throws IOException {

        return new StandardFlowService(controller, properties, senderListener, encryptor, true, coordinator, revisionManager, authorizer);
    }

    private StandardFlowService(
        final FlowController controller,
        final NiFiProperties properties,
        final NodeProtocolSenderListener senderListener,
        final StringEncryptor encryptor,
        final boolean configuredForClustering,
        final ClusterCoordinator clusterCoordinator,
        final RevisionManager revisionManager,
        final Authorizer authorizer) throws IOException {

        this.controller = controller;
        flowXml = Paths.get(properties.getProperty(NiFiProperties.FLOW_CONFIGURATION_FILE));

        gracefulShutdownSeconds = (int) FormatUtils.getTimeDuration(properties.getProperty(NiFiProperties.FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD), TimeUnit.SECONDS);
        autoResumeState = properties.getAutoResumeState();

        dao = new StandardXMLFlowConfigurationDAO(flowXml, encryptor);
        this.clusterCoordinator = clusterCoordinator;
        if (clusterCoordinator != null) {
            clusterCoordinator.setFlowService(this);
        }
        this.revisionManager = revisionManager;
        this.authorizer = authorizer;

        if (configuredForClustering) {
            this.configuredForClustering = configuredForClustering;

            this.senderListener = senderListener;
            senderListener.addHandler(this);

            final InetSocketAddress nodeApiAddress = properties.getNodeApiAddress();
            final InetSocketAddress nodeSocketAddress = properties.getClusterNodeProtocolAddress();

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
                properties.getRemoteInputHost(), properties.getRemoteInputPort(),
                properties.getRemoteInputHttpPort(), properties.isSiteToSiteSecure());

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
    public void saveFlowChanges(final OutputStream outStream) throws IOException {
        writeLock.lock();
        try {
            dao.save(controller, outStream);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void overwriteFlow(final InputStream is) throws IOException {
        writeLock.lock();
        try (final OutputStream output = Files.newOutputStream(flowXml, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            final OutputStream gzipOut = new GZIPOutputStream(output);) {
            FileUtils.copy(is, gzipOut);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void saveFlowChanges(final TimeUnit delayUnit, final long delay) {
        final boolean archiveEnabled = NiFiProperties.getInstance().isFlowConfigurationArchiveEnabled();
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
            newExecutor.scheduleWithFixedDelay(new SaveReportingTask(), 0L, 500L, TimeUnit.MILLISECONDS);
            this.executor.set(newExecutor);

            if (configuredForClustering) {
                senderListener.start();
            }

        } catch (final IOException ioe) {
            try {
                stop(/* force */true);
            } catch (final Exception e) {
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
                final Thread shutdownClusterCoordinator = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        clusterCoordinator.shutdown();
                    }
                });
                shutdownClusterCoordinator.setDaemon(true);
                shutdownClusterCoordinator.setName("Shutdown Cluster Coordinator");
                shutdownClusterCoordinator.start();
            }

            if (!controller.isTerminated()) {
                controller.shutdown(force);
            }

            if (configuredForClustering && senderListener != null) {
                try {
                    senderListener.stop();
                } catch (final IOException ioe) {
                    logger.warn("Protocol sender/listener did not stop gracefully due to: " + ioe);
                }
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
                    logger.warn("Scheduling service did not gracefully shutdown within configured " + gracefulShutdownSeconds + " second window");
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean canHandle(final ProtocolMessage msg) {
        switch (msg.getType()) {
            case RECONNECTION_REQUEST:
            case DISCONNECTION_REQUEST:
            case FLOW_REQUEST:
                return true;
            default:
                return false;
        }
    }

    @Override
    public ProtocolMessage handle(final ProtocolMessage request) throws ProtocolException {
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

                    final Thread t = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            handleReconnectionRequest((ReconnectionRequestMessage) request);
                        }
                    }, "Reconnect to Cluster");
                    t.setDaemon(true);
                    t.start();

                    return new ReconnectionResponseMessage();
                }
                case DISCONNECTION_REQUEST: {
                    final Thread t = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            handleDisconnectionRequest((DisconnectMessage) request);
                        }
                    }, "Disconnect from Cluster");
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
    public void load(final DataFlow dataFlow) throws IOException, FlowSerializationException, FlowSynchronizationException, UninheritableFlowException {
        if (configuredForClustering) {
            final DataFlow proposedFlow = (dataFlow == null) ? createDataFlow() : dataFlow;

            /*
             * Attempt to connect to the cluster. If the manager is able to
             * provide a data flow, then the manager will send a connection
             * response. If the manager was unable to be located, then
             * the response will be null and we should load the local dataflow
             * and heartbeat until a manager is located.
             */
            final boolean localFlowEmpty = StandardFlowSynchronizer.isEmpty(proposedFlow);
            final ConnectionResponse response = connect(localFlowEmpty, localFlowEmpty);

            // obtain write lock while we are updating the controller. We need to ensure that we don't
            // obtain the lock before calling connect(), though, or we will end up getting a deadlock
            // because the node that is receiving the connection request won't be able to get the current
            // flow, as that requires a read lock.
            writeLock.lock();
            try {
                if (response == null) {
                    logger.info("Flow controller will load local dataflow and suspend connection handshake until a cluster connection response is received.");

                    // load local proposed flow
                    loadFromBytes(proposedFlow, false);

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

                    // notify controller that flow is initialized
                    try {
                        controller.onFlowInitialized(autoResumeState);
                    } catch (final Exception ex) {
                        logger.warn("Unable to start all processors due to invalid flow configuration.");
                        if (logger.isDebugEnabled()) {
                            logger.warn(StringUtils.EMPTY, ex);
                        }
                    }
                } else {
                    try {
                        loadFromConnectionResponse(response);
                    } catch (final Exception e) {
                        logger.error("Failed to load flow from cluster due to: " + e, e);
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
                // operating in standalone mode, so load proposed flow
                loadFromBytes(dataFlow, true);
            } finally {
                writeLock.unlock();
            }
        }
    }

    private void handleConnectionFailure(final Exception ex) {
        DisconnectionCode disconnectionCode;
        if (ex instanceof UninheritableFlowException) {
            disconnectionCode = DisconnectionCode.MISMATCHED_FLOWS;
        } else if (ex instanceof FlowSynchronizationException) {
            disconnectionCode = DisconnectionCode.MISMATCHED_FLOWS;
        } else {
            disconnectionCode = DisconnectionCode.STARTUP_FAILURE;
        }
        clusterCoordinator.disconnectionRequestedByNode(getNodeId(), disconnectionCode, ex.toString());
        controller.setClustered(false, null);
    }

    private FlowResponseMessage handleFlowRequest(final FlowRequestMessage request) throws ProtocolException {
        readLock.lock();
        try {
            logger.info("Received flow request message from manager.");

            // create the response
            final FlowResponseMessage response = new FlowResponseMessage();
            response.setDataFlow(createDataFlow());
            return response;
        } catch (final Exception ex) {
            throw new ProtocolException("Failed serializing flow controller state for flow request due to: " + ex, ex);
        } finally {
            readLock.unlock();
        }
    }

    private byte[] getAuthorizerFingerprint() {
        final boolean isInternalAuthorizer = (authorizer instanceof AbstractPolicyBasedAuthorizer);
        return isInternalAuthorizer ? ((AbstractPolicyBasedAuthorizer) authorizer).getFingerprint().getBytes(StandardCharsets.UTF_8) : null;
    }

    @Override
    public StandardDataFlow createDataFlow() throws IOException {
        final byte[] snippetBytes = controller.getSnippetManager().export();
        final byte[] authorizerFingerprint = getAuthorizerFingerprint();

        // Load the flow from disk if the file exists.
        if (dao.isFlowPresent()) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            dao.load(baos);
            final byte[] bytes = baos.toByteArray();
            final StandardDataFlow fromDisk = new StandardDataFlow(bytes, snippetBytes, authorizerFingerprint);
            return fromDisk;
        }

        // Flow from disk does not exist, so serialize the Flow Controller and use that.
        // This is done because on startup, if there is no flow, the Flow Controller
        // will automatically create a Root Process Group, and we need to ensure that
        // we replicate that Process Group to all nodes in the cluster, so that they all
        // end up with the same ID for the root Process Group.
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        dao.save(controller, baos);
        final byte[] flowBytes = baos.toByteArray();
        baos.reset();

        return new StandardDataFlow(flowBytes, snippetBytes, authorizerFingerprint);
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
            logger.info("Processing reconnection request from manager.");

            // reconnect
            final ConnectionResponse connectionResponse = new ConnectionResponse(getNodeId(), request.getDataFlow(),
                request.getInstanceId(), request.getNodeConnectionStatuses(), request.getComponentRevisions());

            loadFromConnectionResponse(connectionResponse);

            clusterCoordinator.resetNodeStatuses(connectionResponse.getNodeConnectionStatuses().stream()
                .collect(Collectors.toMap(status -> status.getNodeIdentifier(), status -> status)));
            controller.resumeHeartbeats();  // we are now connected, so resume sending heartbeats.

            logger.info("Node reconnected.");
        } catch (final Exception ex) {
            // disconnect controller
            if (controller.isClustered()) {
                disconnect("Failed to properly handle Reconnection request due to " + ex.toString());
            }

            logger.error("Handling reconnection request failed due to: " + ex, ex);
            handleConnectionFailure(ex);
        }
    }

    private void handleDisconnectionRequest(final DisconnectMessage request) {
        logger.info("Received disconnection request message from manager with explanation: " + request.getExplanation());
        disconnect(request.getExplanation());
    }

    private void disconnect(final String explanation) {
        writeLock.lock();
        try {

            logger.info("Disconnecting node.");

            // mark node as not connected
            controller.setConnectionStatus(new NodeConnectionStatus(nodeId, DisconnectionCode.UNKNOWN, explanation));

            // turn off primary flag
            controller.setPrimary(false);

            // stop heartbeating
            controller.stopHeartbeating();

            // set node to not clustered
            controller.setClustered(false, null);
            clusterCoordinator.setConnected(false);

            logger.info("Node disconnected.");

        } finally {
            writeLock.unlock();
        }
    }

    // write lock must already be acquired
    private void loadFromBytes(final DataFlow proposedFlow, final boolean allowEmptyFlow)
        throws IOException, FlowSerializationException, FlowSynchronizationException, UninheritableFlowException {
        logger.trace("Loading flow from bytes");

        // resolve the given flow (null means load flow from disk)
        final DataFlow actualProposedFlow;
        final byte[] flowBytes;
        final byte[] authorizerFingerprint;
        if (proposedFlow == null) {
            final ByteArrayOutputStream flowOnDisk = new ByteArrayOutputStream();
            copyCurrentFlow(flowOnDisk);
            flowBytes = flowOnDisk.toByteArray();
            authorizerFingerprint = getAuthorizerFingerprint();
            logger.debug("Loaded Flow from bytes");
        } else {
            flowBytes = proposedFlow.getFlow();
            authorizerFingerprint = proposedFlow.getAuthorizerFingerprint();
            logger.debug("Loaded flow from proposed flow");
        }

        actualProposedFlow = new StandardDataFlow(flowBytes, null, authorizerFingerprint);

        if (firstControllerInitialization) {
            // load the controller services
            logger.debug("Loading controller services");
        }

        // load the flow
        logger.debug("Loading proposed flow into FlowController");
        dao.load(controller, actualProposedFlow);

        final ProcessGroup rootGroup = controller.getGroup(controller.getRootGroupId());
        if (rootGroup.isEmpty() && !allowEmptyFlow) {
            throw new FlowSynchronizationException("Failed to load flow because unable to connect to cluster and local flow is empty");
        }

        final List<Template> templates = loadTemplates();
        for (final Template template : templates) {
            final Template existing = rootGroup.getTemplate(template.getIdentifier());
            if (existing == null) {
                logger.info("Imported Template '{}' to Root Group", template.getDetails().getName());
                rootGroup.addTemplate(template);
            } else {
                logger.info("Template '{}' was already present in Root Group so will not import from file", template.getDetails().getName());
            }
        }

        // lazy initialization of controller tasks and flow
        if (firstControllerInitialization) {
            logger.debug("First controller initialization. Loading reporting tasks and initializing controller.");

            // initialize the flow
            controller.initializeFlow();

            firstControllerInitialization = false;
        }
    }

    /**
     * In NiFi 0.x, templates were stored in a templates directory as separate files. They are
     * now stored in the flow itself. If there already are templates in that directory, though,
     * we want to restore them.
     *
     * @return the templates found in the templates directory
     * @throws IOException if unable to read from the file system
     */
    public List<Template> loadTemplates() throws IOException {
        final NiFiProperties properties = NiFiProperties.getInstance();
        final Path templatePath = properties.getTemplateDirectory();

        final File[] files = templatePath.toFile().listFiles(pathname -> {
            final String lowerName = pathname.getName().toLowerCase();
            return lowerName.endsWith(".template") || lowerName.endsWith(".xml");
        });

        if (files == null) {
            return Collections.emptyList();
        }

        final List<Template> templates = new ArrayList<>();
        for (final File file : files) {
            try (final FileInputStream fis = new FileInputStream(file);
                final BufferedInputStream bis = new BufferedInputStream(fis)) {

                final TemplateDTO templateDto;
                try {
                    templateDto = TemplateDeserializer.deserialize(bis);
                } catch (final Exception e) {
                    logger.error("Unable to interpret " + file + " as a Template. Skipping file.");
                    continue;
                }

                if (templateDto.getId() == null) {
                    // If there is no ID assigned, we need to assign one. We do this by generating
                    // an ID from the name. This is because we know that Template Names are unique
                    // and are consistent across all nodes in the cluster.
                    final String uuid = UUID.nameUUIDFromBytes(templateDto.getName().getBytes(StandardCharsets.UTF_8)).toString();
                    templateDto.setId(uuid);
                }

                final Template template = new Template(templateDto);
                templates.add(template);
            }
        }

        return templates;
    }

    private ConnectionResponse connect(final boolean retryOnCommsFailure, final boolean retryIndefinitely) throws ConnectionException {
        readLock.lock();
        try {
            logger.info("Connecting Node: " + nodeId);

            // create connection request message
            final ConnectionRequest request = new ConnectionRequest(nodeId);
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
                    response = senderListener.requestConnection(requestMsg).getConnectionResponse();
                    if (response.getRejectionReason() != null) {
                        logger.warn("Connection request was blocked by cluster coordinator with the explanation: " + response.getRejectionReason());
                        // set response to null and treat a firewall blockage the same as getting no response from manager
                        response = null;
                        break;
                    } else if (response.shouldTryLater()) {
                        logger.info("Flow controller requested by cluster coordinator to retry connection in " + response.getTryLaterSeconds() + " seconds.");
                        try {
                            Thread.sleep(response.getTryLaterSeconds() * 1000);
                        } catch (final InterruptedException ie) {
                            // we were interrupted, so finish quickly
                            break;
                        }
                    } else {
                        // we received a successful connection response from manager
                        break;
                    }
                } catch (final Exception pe) {
                    // could not create a socket and communicate with manager
                    logger.warn("Failed to connect to cluster due to: " + pe);
                    if (logger.isDebugEnabled()) {
                        logger.warn("", pe);
                    }

                    if (retryOnCommsFailure) {
                        try {
                            Thread.sleep(response == null ? 5000 : response.getTryLaterSeconds());
                        } catch (final InterruptedException ie) {
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
                // if response indicates we should try later, then manager was unable to service our request. Just load local flow and move on.
                return null;
            } else {
                // cluster manager provided a successful response with a current dataflow
                // persist node uuid and index returned by NCM and return the response to the caller
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
            clusterCoordinator.resetNodeStatuses(response.getNodeConnectionStatuses().stream()
                .collect(Collectors.toMap(status -> status.getNodeIdentifier(), status -> status)));

            // get the dataflow from the response
            final DataFlow dataFlow = response.getDataFlow();

            // load new controller state
            loadFromBytes(dataFlow, true);

            // set node ID on controller before we start heartbeating because heartbeat needs node ID
            nodeId = response.getNodeIdentifier();
            logger.info("Setting Flow Controller's Node ID: " + nodeId);
            controller.setNodeId(nodeId);
            clusterCoordinator.setLocalNodeIdentifier(nodeId);
            clusterCoordinator.setConnected(true);
            revisionManager.reset(response.getComponentRevisions().stream().map(rev -> rev.toRevision()).collect(Collectors.toList()));

            // mark the node as clustered
            controller.setClustered(true, response.getInstanceId());

            controller.setConnectionStatus(new NodeConnectionStatus(nodeId, NodeConnectionState.CONNECTED));

            // start the processors as indicated by the dataflow
            controller.onFlowInitialized(autoResumeState);

            loadSnippets(dataFlow.getSnippets());

            controller.startHeartbeating();
        } catch (final UninheritableFlowException ufe) {
            throw new UninheritableFlowException(CONNECTION_EXCEPTION_MSG_PREFIX + "local flow is different than cluster flow.", ufe);
        } catch (final FlowSerializationException fse) {
            throw new ConnectionException(CONNECTION_EXCEPTION_MSG_PREFIX + "local or cluster flow is malformed.", fse);
        } catch (final FlowSynchronizationException fse) {
            throw new FlowSynchronizationException(CONNECTION_EXCEPTION_MSG_PREFIX + "local flow controller partially updated. "
                + "Administrator should disconnect node and review flow for corruption.", fse);
        } catch (final Exception ex) {
            throw new ConnectionException("Failed to connect node to cluster due to: " + ex, ex);
        } finally {
            writeLock.unlock();
        }

    }

    @Override
    public void copyCurrentFlow(final OutputStream os) throws IOException {
        readLock.lock();
        try {
            if (!Files.exists(flowXml) || Files.size(flowXml) == 0) {
                return;
            }

            try (final InputStream in = Files.newInputStream(flowXml, StandardOpenOption.READ);
                final InputStream gzipIn = new GZIPInputStream(in)) {
                FileUtils.copy(gzipIn, os);
            }
        } finally {
            readLock.unlock();
        }
    }


    public void loadSnippets(final byte[] bytes) throws IOException {
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
        public void run() {
            final ClassLoader currentCl = Thread.currentThread().getContextClassLoader();
            final ClassLoader cl = NarClassLoaders.getInstance().getFrameworkClassLoader();
            Thread.currentThread().setContextClassLoader(cl);
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
                logger.error("Unable to save flow controller configuration due to: " + t, t);
                if (logger.isDebugEnabled()) {
                    logger.error("", t);
                }

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
