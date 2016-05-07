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
package org.apache.nifi.cluster.manager.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;
import javax.ws.rs.HttpMethod;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.lifecycle.OnConfigurationRestored;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.cluster.context.ClusterContext;
import org.apache.nifi.cluster.context.ClusterContextImpl;
import org.apache.nifi.cluster.coordination.heartbeat.ClusterProtocolHeartbeatMonitor;
import org.apache.nifi.cluster.coordination.heartbeat.NodeHeartbeat;
import org.apache.nifi.cluster.coordination.http.HttpResponseMerger;
import org.apache.nifi.cluster.coordination.http.StandardHttpResponseMerger;
import org.apache.nifi.cluster.coordination.http.replication.AsyncClusterResponse;
import org.apache.nifi.cluster.coordination.http.replication.RequestCompletionCallback;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.cluster.coordination.http.replication.ThreadPoolRequestReplicator;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.event.Event;
import org.apache.nifi.cluster.event.EventManager;
import org.apache.nifi.cluster.firewall.ClusterNodeFirewall;
import org.apache.nifi.cluster.flow.ClusterDataFlow;
import org.apache.nifi.cluster.flow.DaoException;
import org.apache.nifi.cluster.flow.DataFlowManagementService;
import org.apache.nifi.cluster.flow.PersistedFlowState;
import org.apache.nifi.cluster.manager.HttpClusterManager;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.ConflictingNodeIdException;
import org.apache.nifi.cluster.manager.exception.ConnectingNodeMutableRequestException;
import org.apache.nifi.cluster.manager.exception.DisconnectedNodeMutableRequestException;
import org.apache.nifi.cluster.manager.exception.IllegalClusterStateException;
import org.apache.nifi.cluster.manager.exception.IllegalNodeDeletionException;
import org.apache.nifi.cluster.manager.exception.IllegalNodeDisconnectionException;
import org.apache.nifi.cluster.manager.exception.IllegalNodeReconnectionException;
import org.apache.nifi.cluster.manager.exception.NoConnectedNodesException;
import org.apache.nifi.cluster.manager.exception.NoResponseFromNodesException;
import org.apache.nifi.cluster.manager.exception.NodeDisconnectionException;
import org.apache.nifi.cluster.manager.exception.NodeReconnectionException;
import org.apache.nifi.cluster.manager.exception.SafeModeMutableRequestException;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.manager.exception.UriConstructionException;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.node.Node.Status;
import org.apache.nifi.cluster.protocol.ConnectionRequest;
import org.apache.nifi.cluster.protocol.ConnectionResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.cluster.protocol.impl.ClusterManagerProtocolSenderListener;
import org.apache.nifi.cluster.protocol.impl.ClusterServicesBroadcaster;
import org.apache.nifi.cluster.protocol.message.ConnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ConnectionResponseMessage;
import org.apache.nifi.cluster.protocol.message.DisconnectMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage.MessageType;
import org.apache.nifi.cluster.protocol.message.ReconnectionRequestMessage;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.StandardProcessorNode;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;
import org.apache.nifi.controller.reporting.ClusteredReportingTaskNode;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.reporting.ReportingTaskProvider;
import org.apache.nifi.controller.reporting.StandardReportingInitializationContext;
import org.apache.nifi.controller.scheduling.QuartzSchedulingAgent;
import org.apache.nifi.controller.scheduling.StandardProcessScheduler;
import org.apache.nifi.controller.scheduling.TimerDrivenSchedulingAgent;
import org.apache.nifi.controller.serialization.StandardFlowSerializer;
import org.apache.nifi.controller.service.ControllerServiceLoader;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardControllerServiceProvider;
import org.apache.nifi.controller.state.manager.StandardStateManagerProvider;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.framework.security.util.SslContextFactory;
import org.apache.nifi.io.socket.multicast.DiscoverableService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.ControllerServiceLogObserver;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepository;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.logging.NiFiLog;
import org.apache.nifi.logging.ReportingTaskLogObserver;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.processor.StandardValidationContextFactory;
import org.apache.nifi.remote.RemoteResourceManager;
import org.apache.nifi.remote.RemoteSiteListener;
import org.apache.nifi.remote.SocketRemoteSiteListener;
import org.apache.nifi.remote.cluster.ClusterNodeInformation;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.protocol.socket.ClusterManagerServerProtocol;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.DomUtils;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.ObjectHolder;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.web.OptimisticLockingManager;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.UpdateRevision;
import org.apache.nifi.web.api.dto.BulletinDTO;
import org.apache.nifi.web.util.WebUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.DefaultClientConfig;

/**
 * Provides a cluster manager implementation. The manager federates incoming HTTP client requests to the nodes' external API using the HTTP protocol. The manager also communicates with nodes using the
 * nodes' internal socket protocol.
 *
 * The manager's socket address may broadcasted using multicast if a MulticastServiceBroadcaster instance is set on this instance. The manager instance must be started after setting the broadcaster.
 *
 * The manager may be configured with an EventManager for recording noteworthy lifecycle events (e.g., first heartbeat received, node status change).
 *
 * The start() and stop() methods must be called to initialize and stop the instance.
 *
 */
public class WebClusterManager implements HttpClusterManager, ProtocolHandler, ControllerServiceProvider, ReportingTaskProvider, RequestCompletionCallback {

    public static final String ROOT_GROUP_ID_ALIAS = "root";
    public static final String BULLETIN_CATEGORY = "Clustering";

    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(WebClusterManager.class));

    /**
     * The HTTP header to store a cluster context. An example of what may be stored in the context is a node's auditable actions in response to a cluster request. The cluster context is serialized
     * using Java's serialization mechanism and hex encoded.
     */
    public static final String CLUSTER_CONTEXT_HTTP_HEADER = "X-ClusterContext";

    /**
     * HTTP Header that stores a unique ID for each request that is replicated to the nodes. This is used for logging purposes so that request information, such as timing, can be correlated between
     * the NCM and the nodes
     */
    public static final String REQUEST_ID_HEADER = "X-RequestID";

    /**
     * The HTTP header that the NCM specifies to ask a node if they are able to process a given request. The value is always 150-NodeContinue. The node will respond with 150 CONTINUE if it is able to
     * process the request, 417 EXPECTATION_FAILED otherwise.
     */
    public static final String NCM_EXPECTS_HTTP_HEADER = "X-NcmExpects";
    public static final int NODE_CONTINUE_STATUS_CODE = 150;

    /**
     * The HTTP header that the NCM specifies to indicate that a node should invalidate the specified user group. This is done to ensure that user cache is not stale when an administrator modifies a
     * group through the UI.
     */
    public static final String CLUSTER_INVALIDATE_USER_GROUP_HEADER = "X-ClusterInvalidateUserGroup";

    /**
     * The HTTP header that the NCM specifies to indicate that a node should invalidate the specified user. This is done to ensure that user cache is not stale when an administrator modifies a user
     * through the UI.
     */
    public static final String CLUSTER_INVALIDATE_USER_HEADER = "X-ClusterInvalidateUser";

    /**
     * The default number of seconds to respond to a connecting node if the manager cannot provide it with a current data flow.
     */
    private static final int DEFAULT_CONNECTION_REQUEST_TRY_AGAIN_SECONDS = 5;


    public static final Pattern CLUSTER_PROCESSOR_URI_PATTERN = Pattern.compile("/nifi-api/cluster/processors/[a-f0-9\\-]{36}");


    public static final Pattern REPORTING_TASK_URI_PATTERN = Pattern.compile("/nifi-api/controller/reporting-tasks/node/[a-f0-9\\-]{36}");
    public static final Pattern COUNTER_URI_PATTERN = Pattern.compile("/nifi-api/controller/counters/[a-f0-9\\-]{36}");

    private final NiFiProperties properties;
    private final DataFlowManagementService dataFlowManagementService;
    private final ClusterManagerProtocolSenderListener senderListener;
    private final OptimisticLockingManager optimisticLockingManager;
    private final StringEncryptor encryptor;
    private final ReentrantReadWriteLock resourceRWLock = new ReentrantReadWriteLock(true);
    private final ClusterManagerLock readLock = new ClusterManagerLock(resourceRWLock.readLock(), "Read");
    private final ClusterManagerLock writeLock = new ClusterManagerLock(resourceRWLock.writeLock(), "Write");
    private final ClusterProtocolHeartbeatMonitor heartbeatMonitor;
    private final WebClusterManagerCoordinator clusterCoordinator;

    private final Set<Node> nodes = new HashSet<>();
    private final ConcurrentMap<String, ReportingTaskNode> reportingTasks = new ConcurrentHashMap<>();

    // null means the dataflow should be read from disk
    private StandardDataFlow cachedDataFlow = null;
    private NodeIdentifier primaryNodeId = null;
    private volatile ClusterServicesBroadcaster servicesBroadcaster = null;
    private volatile EventManager eventManager = null;
    private volatile ClusterNodeFirewall clusterFirewall = null;
    private volatile AuditService auditService = null;
    private volatile ControllerServiceProvider controllerServiceProvider = null;

    private final RemoteSiteListener remoteSiteListener;
    private final Integer remoteInputPort;
    private final Boolean remoteCommsSecure;
    private final BulletinRepository bulletinRepository;
    private final String instanceId;
    private final FlowEngine reportingTaskEngine;
    private final StandardProcessScheduler processScheduler;
    private final StateManagerProvider stateManagerProvider;

    private final HttpResponseMerger responseMerger = new StandardHttpResponseMerger(this);
    private final RequestReplicator httpRequestReplicator;

    public WebClusterManager(
            final DataFlowManagementService dataFlowManagementService, final ClusterManagerProtocolSenderListener senderListener,
            final NiFiProperties properties, final StringEncryptor encryptor, final OptimisticLockingManager optimisticLockingManager) {

        if (dataFlowManagementService == null) {
            throw new IllegalArgumentException("DataFlowManagementService may not be null.");
        } else if (senderListener == null) {
            throw new IllegalArgumentException("ClusterManagerProtocolSenderListener may not be null.");
        } else if (properties == null) {
            throw new IllegalArgumentException("NiFiProperties may not be null.");
        }

        this.dataFlowManagementService = dataFlowManagementService;
        this.properties = properties;
        this.bulletinRepository = new VolatileBulletinRepository();
        this.instanceId = UUID.randomUUID().toString();
        this.senderListener = senderListener;
        this.encryptor = encryptor;
        this.optimisticLockingManager = optimisticLockingManager;
        senderListener.addHandler(this);
        senderListener.setBulletinRepository(bulletinRepository);

        remoteInputPort = properties.getRemoteInputPort();
        if (remoteInputPort == null) {
            remoteSiteListener = null;
            remoteCommsSecure = null;
        } else {
            // Register the ClusterManagerServerProtocol as the appropriate resource for site-to-site Server Protocol
            RemoteResourceManager.setServerProtocolImplementation(ClusterManagerServerProtocol.RESOURCE_NAME, ClusterManagerServerProtocol.class);
            remoteCommsSecure = properties.isSiteToSiteSecure();
            if (remoteCommsSecure) {
                final SSLContext sslContext = SslContextFactory.createSslContext(properties, false);

                if (sslContext == null) {
                    throw new IllegalStateException("NiFi Configured to allow Secure Site-to-Site communications but the Keystore/Truststore properties are not configured");
                }

                remoteSiteListener = new SocketRemoteSiteListener(remoteInputPort.intValue(), sslContext, this);
            } else {
                remoteSiteListener = new SocketRemoteSiteListener(remoteInputPort.intValue(), null, this);
            }
        }

        reportingTaskEngine = new FlowEngine(8, "Reporting Task Thread");

        try {
            this.stateManagerProvider = StandardStateManagerProvider.create(properties);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        processScheduler = new StandardProcessScheduler(this, encryptor, stateManagerProvider);

        // When we construct the scheduling agents, we can pass null for a lot of the arguments because we are only
        // going to be scheduling Reporting Tasks. Otherwise, it would not be okay.
        processScheduler.setSchedulingAgent(SchedulingStrategy.TIMER_DRIVEN, new TimerDrivenSchedulingAgent(null, reportingTaskEngine, null, encryptor));
        processScheduler.setSchedulingAgent(SchedulingStrategy.CRON_DRIVEN, new QuartzSchedulingAgent(null, reportingTaskEngine, null, encryptor));
        processScheduler.setMaxThreadCount(SchedulingStrategy.TIMER_DRIVEN, 10);
        processScheduler.setMaxThreadCount(SchedulingStrategy.CRON_DRIVEN, 10);

        controllerServiceProvider = new StandardControllerServiceProvider(processScheduler, bulletinRepository, stateManagerProvider);

        clusterCoordinator = new WebClusterManagerCoordinator(this, senderListener, dataFlowManagementService);
        heartbeatMonitor = new ClusterProtocolHeartbeatMonitor(clusterCoordinator, properties);
        senderListener.addHandler(heartbeatMonitor);
        httpRequestReplicator = createRequestReplicator(properties);
    }

    private RequestReplicator createRequestReplicator(final NiFiProperties properties) {
        final int numThreads = properties.getClusterManagerNodeApiRequestThreads();
        final String connectionTimeout = properties.getClusterManagerNodeApiConnectionTimeout();
        final String readTimeout = properties.getClusterManagerNodeApiReadTimeout();
        final EventReporter eventReporter = createEventReporter();

        final Client jerseyClient = WebUtils.createClient(new DefaultClientConfig(), SslContextFactory.createSslContext(properties));
        return new ThreadPoolRequestReplicator(numThreads, jerseyClient, clusterCoordinator, connectionTimeout, readTimeout, this,
            eventReporter, this, optimisticLockingManager, dataFlowManagementService);
    }

    private EventReporter createEventReporter() {
        return new EventReporter() {
            private static final long serialVersionUID = 7770887158588031619L;

            @Override
            public void reportEvent(Severity severity, String category, String message) {
                final Bulletin bulletin = BulletinFactory.createBulletin(category, severity.name(), message);
                getBulletinRepository().addBulletin(bulletin);
            }
        };
    }

    public void start() throws IOException {
        writeLock.lock();
        try {
            if (isRunning()) {
                throw new IllegalStateException("Instance is already started.");
            }

            try {
                heartbeatMonitor.start();

                // start request replication service
                httpRequestReplicator.start();

                // start protocol service
                senderListener.start();

                // start flow management service
                dataFlowManagementService.start();

                if (remoteSiteListener != null) {
                    remoteSiteListener.start();
                }

                // load flow
                final ClusterDataFlow clusterDataFlow;
                if (dataFlowManagementService.isFlowCurrent()) {
                    clusterDataFlow = dataFlowManagementService.loadDataFlow();
                    cachedDataFlow = clusterDataFlow.getDataFlow();
                    primaryNodeId = clusterDataFlow.getPrimaryNodeId();
                } else {
                    throw new IOException("Flow is not current.");
                }

                final byte[] serializedServices = clusterDataFlow.getControllerServices();
                if (serializedServices != null && serializedServices.length > 0) {
                    ControllerServiceLoader.loadControllerServices(this, new ByteArrayInputStream(serializedServices), null, encryptor, bulletinRepository, properties.getAutoResumeState());
                }

                // start multicast broadcasting service, if configured
                if (servicesBroadcaster != null) {
                    servicesBroadcaster.start();
                }

                // Load and start running Reporting Tasks
                final byte[] serializedReportingTasks = clusterDataFlow.getReportingTasks();
                if (serializedReportingTasks != null && serializedReportingTasks.length > 0) {
                    loadReportingTasks(serializedReportingTasks);
                }

                notifyComponentsConfigurationRestored();
            } catch (final IOException ioe) {
                logger.warn("Failed to initialize cluster services due to: " + ioe, ioe);
                stop();
                throw ioe;
            }

        } finally {
            writeLock.unlock("START");
        }
    }

    public void stop() throws IOException {
        writeLock.lock();
        try {

            // returns true if any service is running
            if (isRunning() == false) {
                throw new IllegalArgumentException("Instance is already stopped.");
            }

            boolean encounteredException = false;

            heartbeatMonitor.stop();

            // stop the HTTP request replicator service
            if (httpRequestReplicator.isRunning()) {
                httpRequestReplicator.stop();
            }

            // stop the flow management service
            if (dataFlowManagementService.isRunning()) {
                dataFlowManagementService.stop();
            }

            if (remoteSiteListener != null) {
                remoteSiteListener.stop();
            }

            // stop the protocol listener service
            if (senderListener.isRunning()) {
                try {
                    senderListener.stop();
                } catch (final IOException ioe) {
                    encounteredException = true;
                    logger.warn("Failed to shutdown protocol service due to: " + ioe, ioe);
                }
            }

            // stop the service broadcaster
            if (isBroadcasting()) {
                servicesBroadcaster.stop();
            }

            if (processScheduler != null) {
                processScheduler.shutdown();
            }

            if (encounteredException) {
                throw new IOException("Failed to shutdown Cluster Manager because one or more cluster services failed to shutdown.  Check the logs for details.");
            }

        } finally {
            writeLock.unlock("STOP");
        }
    }

    public boolean isRunning() {
        readLock.lock();
        try {
            return httpRequestReplicator.isRunning()
                    || senderListener.isRunning()
                    || dataFlowManagementService.isRunning()
                    || isBroadcasting();
        } finally {
            readLock.unlock("isRunning");
        }
    }

    @Override
    public boolean canHandle(ProtocolMessage msg) {
        return MessageType.CONNECTION_REQUEST == msg.getType();
    }

    @Override
    public ProtocolMessage handle(final ProtocolMessage protocolMessage) throws ProtocolException {
        switch (protocolMessage.getType()) {
            case CONNECTION_REQUEST:
                return handleConnectionRequest((ConnectionRequestMessage) protocolMessage);
            default:
                throw new ProtocolException("No handler defined for message type: " + protocolMessage.getType());
        }
    }


    private void notifyComponentsConfigurationRestored() {
        for (final ControllerServiceNode serviceNode : getAllControllerServices()) {
            final ControllerService service = serviceNode.getControllerServiceImplementation();

            try (final NarCloseable nc = NarCloseable.withNarLoader()) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, service);
            }
        }

        for (final ReportingTaskNode taskNode : getAllReportingTasks()) {
            final ReportingTask task = taskNode.getReportingTask();

            try (final NarCloseable nc = NarCloseable.withNarLoader()) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, task);
            }
        }
    }

    /**
     * Services connection requests. If the data flow management service is unable to provide a current copy of the data flow, then the returned connection response will indicate the node should try
     * later. Otherwise, the connection response will contain the the flow and the node identifier.
     *
     * If this instance is configured with a firewall and the request is blocked, then the response will not contain a node identifier.
     *
     * @param request a connection request
     *
     * @return a connection response
     */
    @Override
    public ConnectionResponse requestConnection(final ConnectionRequest request) {
        final boolean lockObtained = writeLock.tryLock(3, TimeUnit.SECONDS);
        if (!lockObtained) {
            // Create try-later response because we are too busy to service the request right now. We do not want
            // to wait long because we want Node/NCM comms to be very responsive
            final int tryAgainSeconds;
            if (dataFlowManagementService.getRetrievalDelaySeconds() <= 0) {
                tryAgainSeconds = DEFAULT_CONNECTION_REQUEST_TRY_AGAIN_SECONDS;
            } else {
                tryAgainSeconds = dataFlowManagementService.getRetrievalDelaySeconds();
            }

            // record event
            final String msg = "Connection requested from node, but manager was too busy to service request.  Instructing node to try again in " + tryAgainSeconds + " seconds.";
            addEvent(request.getProposedNodeIdentifier(), msg);
            addBulletin(request.getProposedNodeIdentifier(), Severity.INFO, msg);

            // return try later response
            return new ConnectionResponse(tryAgainSeconds);
        }

        try {
            // resolve the proposed node identifier to a valid node identifier
            final NodeIdentifier resolvedNodeIdentifier;
            try {
                resolvedNodeIdentifier = resolveProposedNodeIdentifier(request.getProposedNodeIdentifier());
            } catch (final ConflictingNodeIdException e) {
                logger.info("Rejecting node {} from connecting to cluster because it provided a Node ID of {} but that Node ID already belongs to {}:{}",
                    request.getProposedNodeIdentifier().getSocketAddress(), request.getProposedNodeIdentifier().getId(), e.getConflictingNodeAddress(), e.getConflictingNodePort());
                return ConnectionResponse.createConflictingNodeIdResponse(e.getConflictingNodeAddress() + ":" + e.getConflictingNodePort());
            }

            if (isBlockedByFirewall(resolvedNodeIdentifier.getSocketAddress())) {
                // if the socket address is not listed in the firewall, then return a null response
                logger.info("Firewall blocked connection request from node " + resolvedNodeIdentifier);
                return ConnectionResponse.createBlockedByFirewallResponse();
            }

            // get a raw reference to the node (if it doesn't exist, node will be null)
            Node node = getRawNode(resolvedNodeIdentifier.getId());

            // create a new node if necessary and set status to connecting
            if (node == null) {
                node = new Node(resolvedNodeIdentifier, Status.CONNECTING);
                addEvent(node.getNodeId(), "Connection requested from new node.  Setting status to connecting.");
                nodes.add(node);
            } else {
                clusterCoordinator.updateNodeStatus(node, Status.CONNECTING);
                addEvent(resolvedNodeIdentifier, "Connection requested from existing node.  Setting status to connecting");
            }

            // record the time of the connection request
            node.setConnectionRequestedTimestamp(new Date().getTime());

            // try to obtain a current flow
            if (dataFlowManagementService.isFlowCurrent()) {
                // if a cached copy does not exist, load it from disk
                if (cachedDataFlow == null) {
                    final ClusterDataFlow clusterDataFlow = dataFlowManagementService.loadDataFlow();
                    cachedDataFlow = clusterDataFlow.getDataFlow();
                    primaryNodeId = clusterDataFlow.getPrimaryNodeId();
                }

                return new ConnectionResponse(node.getNodeId(), cachedDataFlow, remoteInputPort, remoteCommsSecure, instanceId);
            }

            /*
             * The manager does not have a current copy of the data flow,
             * so it will instruct the node to try connecting at a later
             * time.  Meanwhile, the flow will be locked down from user
             * changes because the node is marked as connecting.
             */

            /*
             * Create try-later response based on flow retrieval delay to give
             * the flow management service a chance to retrieve a curren flow
             */
            final int tryAgainSeconds;
            if (dataFlowManagementService.getRetrievalDelaySeconds() <= 0) {
                tryAgainSeconds = DEFAULT_CONNECTION_REQUEST_TRY_AGAIN_SECONDS;
            } else {
                tryAgainSeconds = dataFlowManagementService.getRetrievalDelaySeconds();
            }

            // record event
            addEvent(node.getNodeId(), "Connection requested from node, but manager was unable to obtain current flow.  Instructing node to try again in " + tryAgainSeconds + " seconds.");

            // return try later response
            return new ConnectionResponse(tryAgainSeconds);

        } finally {
            writeLock.unlock("requestConnection");
        }
    }

    /**
     * Services reconnection requests for a given node. If the node indicates reconnection failure, then the node will be set to disconnected and if the node has primary role, then the role will be
     * revoked. Otherwise, a reconnection request will be sent to the node, initiating the connection handshake.
     *
     * @param nodeId a node identifier
     *
     * @throws UnknownNodeException if the node does not exist
     * @throws IllegalNodeReconnectionException if the node cannot be reconnected because the node is not disconnected
     * @throws NodeReconnectionException if the reconnection message failed to be sent or the cluster could not provide a current data flow for the reconnection request
     */
    @Override
    public void requestReconnection(final String nodeId, final String userDn) throws UnknownNodeException, IllegalNodeReconnectionException {
        Node node = null;

        final boolean primaryRole;
        final int tryAgainSeconds;

        writeLock.lock();
        try {
            // check if we know about this node and that it is disconnected
            node = getRawNode(nodeId);
            logger.info("Request was made by {} to reconnect node {} to cluster", userDn, node == null ? nodeId : node);

            if (node == null) {
                throw new UnknownNodeException("Node does not exist.");
            } else if (Status.DISCONNECTED != node.getStatus()) {
                throw new IllegalNodeReconnectionException("Node must be disconnected before it can reconnect.");
            }

            // get the dataflow to send with the reconnection request
            if (!dataFlowManagementService.isFlowCurrent()) {
                /* node remains disconnected */
                final String msg = "Reconnection requested for node, but manager was unable to obtain current flow.  Setting node to disconnected.";
                addEvent(node.getNodeId(), msg);
                addBulletin(node, Severity.WARNING, msg);
                throw new NodeReconnectionException("Manager was unable to obtain current flow to provide in reconnection request to node.  Try again in a few seconds.");
            }

            // if a cached copy does not exist, load it from disk
            if (cachedDataFlow == null) {
                final ClusterDataFlow clusterDataFlow = dataFlowManagementService.loadDataFlow();
                cachedDataFlow = clusterDataFlow.getDataFlow();
                primaryNodeId = clusterDataFlow.getPrimaryNodeId();
            }

            clusterCoordinator.updateNodeStatus(node, Status.CONNECTING);
            addEvent(node.getNodeId(), "Reconnection requested for node.  Setting status to connecting.");

            // determine if this node should be assigned the primary role
            if (primaryNodeId == null || primaryNodeId.logicallyEquals(node.getNodeId())) {
                setPrimaryNodeId(node.getNodeId());
                addEvent(node.getNodeId(), "Setting primary role in reconnection request.");
                primaryRole = true;
            } else {
                primaryRole = false;
            }

            if (dataFlowManagementService.getRetrievalDelaySeconds() <= 0) {
                tryAgainSeconds = DEFAULT_CONNECTION_REQUEST_TRY_AGAIN_SECONDS;
            } else {
                tryAgainSeconds = dataFlowManagementService.getRetrievalDelaySeconds();
            }
        } catch (final UnknownNodeException | IllegalNodeReconnectionException | NodeReconnectionException une) {
            throw une;
        } catch (final Exception ex) {
            logger.warn("Problem encountered issuing reconnection request to node " + node.getNodeId() + " due to: " + ex, ex);

            clusterCoordinator.updateNodeStatus(node, Status.DISCONNECTED);
            final String eventMsg = "Problem encountered issuing reconnection request. Node will remain disconnected: " + ex;
            addEvent(node.getNodeId(), eventMsg);
            addBulletin(node, Severity.WARNING, eventMsg);

            // Exception thrown will include node ID but event/bulletin do not because the node/id is passed along with the message
            throw new NodeReconnectionException("Problem encountered issuing reconnection request to " + node.getNodeId() + ". Node will remain disconnected: " + ex, ex);
        } finally {
            writeLock.unlock("requestReconnection");
        }

        // Asynchronously start attempting reconnection. This is not completely thread-safe, as
        // we do this by releasing the write lock and then obtaining a read lock for each attempt,
        // so we suffer from the ABA problem. However, we are willing to accept the consequences of
        // this situation in order to avoid holding a lock for the entire duration. "The consequences"
        // are that a second thread could potentially be doing the same thing, issuing a reconnection request.
        // However, this is very unlikely to happen, based on the conditions under which we issue a reconnection
        // request. And if we do, the node will simply reconnect multiple times, which is not a big deal.
        requestReconnectionAsynchronously(node, primaryRole, 10, tryAgainSeconds);
    }

    private void requestReconnectionAsynchronously(final Node node, final boolean primaryRole, final int reconnectionAttempts, final int retrySeconds) {
        final Thread reconnectionThread = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < reconnectionAttempts; i++) {
                    final ReconnectionRequestMessage request = new ReconnectionRequestMessage();

                    try {
                        readLock.lock();
                        try {
                            if (Status.CONNECTING != node.getStatus()) {
                                // the node status has changed. It's no longer appropriate to attempt reconnection.
                                return;
                            }

                            // create the request
                            request.setNodeId(node.getNodeId());
                            request.setDataFlow(cachedDataFlow);
                            request.setPrimary(primaryRole);
                            request.setManagerRemoteSiteCommsSecure(remoteCommsSecure);
                            request.setManagerRemoteSiteListeningPort(remoteInputPort);
                            request.setInstanceId(instanceId);
                        } finally {
                            readLock.unlock("Reconnect " + node.getNodeId());
                        }

                        // Issue a reconnection request to the node.
                        senderListener.requestReconnection(request);

                        node.setConnectionRequestedTimestamp(System.currentTimeMillis());

                        // successfully told node to reconnect -- we're done!
                        return;
                    } catch (final Exception e) {
                        logger.warn("Problem encountered issuing reconnection request to node " + node.getNodeId() + " due to: " + e);
                        if (logger.isDebugEnabled()) {
                            logger.warn("", e);
                        }

                        addBulletin(node, Severity.WARNING, "Problem encountered issuing reconnection request to node " + node.getNodeId() + " due to: " + e);
                    }

                    try {
                        Thread.sleep(1000L * retrySeconds);
                    } catch (final InterruptedException ie) {
                        break;
                    }
                }

                // We failed to reconnect 10 times. We must now mark node as disconnected.
                writeLock.lock();
                try {
                    if (Status.CONNECTING == node.getStatus()) {
                        requestDisconnectionQuietly(node.getNodeId(), "Failed to issue Reconnection Request " + reconnectionAttempts + " times");
                    }
                } finally {
                    writeLock.unlock("Mark node as Disconnected as a result of reconnection failure");
                }
            }
        }, "Reconnect " + node.getNodeId());

        reconnectionThread.start();
    }

    private Map<String, ReportingTaskNode> loadReportingTasks(final byte[] serialized) {
        final Map<String, ReportingTaskNode> tasks = new HashMap<>();

        try {
            final Document document = parse(serialized);

            final NodeList tasksNodes = document.getElementsByTagName("reportingTasks");
            final Element tasksElement = (Element) tasksNodes.item(0);

            //optional properties for all ReportingTasks
            for (final Element taskElement : DomUtils.getChildElementsByTagName(tasksElement, "reportingTask")) {
                //add global properties common to all tasks
                final Map<String, String> properties = new HashMap<>();

                //get properties for the specific reporting task - id, name, class,
                //and schedulingPeriod must be set
                final String taskId = DomUtils.getChild(taskElement, "id").getTextContent().trim();
                final String taskName = DomUtils.getChild(taskElement, "name").getTextContent().trim();

                final List<Element> schedulingStrategyNodeList = DomUtils.getChildElementsByTagName(taskElement, "schedulingStrategy");
                String schedulingStrategyValue = SchedulingStrategy.TIMER_DRIVEN.name();
                if (schedulingStrategyNodeList.size() == 1) {
                    final String specifiedValue = schedulingStrategyNodeList.get(0).getTextContent();

                    try {
                        schedulingStrategyValue = SchedulingStrategy.valueOf(specifiedValue).name();
                    } catch (final Exception e) {
                        throw new RuntimeException("Cannot start Reporting Task with id " + taskId + " because its Scheduling Strategy does not have a valid value", e);
                    }
                }

                final SchedulingStrategy schedulingStrategy = SchedulingStrategy.valueOf(schedulingStrategyValue);
                final String taskSchedulingPeriod = DomUtils.getChild(taskElement, "schedulingPeriod").getTextContent().trim();
                final String taskClass = DomUtils.getChild(taskElement, "class").getTextContent().trim();

                final String scheduleStateValue = DomUtils.getChild(taskElement, "scheduledState").getTextContent().trim();
                final ScheduledState scheduledState = ScheduledState.valueOf(scheduleStateValue);

                // Reporting Task Properties
                for (final Element property : DomUtils.getChildElementsByTagName(taskElement, "property")) {
                    final String name = DomUtils.getChildText(property, "name");
                    final String value = DomUtils.getChildText(property, "value");
                    properties.put(name, value);
                }

                //set the class to be used for the configured reporting task
                final ReportingTaskNode reportingTaskNode;
                try {
                    reportingTaskNode = createReportingTask(taskClass, taskId, false);
                } catch (final ReportingTaskInstantiationException e) {
                    logger.error("Unable to load reporting task {} due to {}", new Object[]{taskId, e});
                    if (logger.isDebugEnabled()) {
                        logger.error("", e);
                    }
                    continue;
                }

                final ReportingTask reportingTask = reportingTaskNode.getReportingTask();

                final ComponentLog componentLog = new SimpleProcessLogger(taskId, reportingTask);
                final ReportingInitializationContext config = new StandardReportingInitializationContext(taskId, taskName,
                        schedulingStrategy, taskSchedulingPeriod, componentLog, this);
                reportingTask.initialize(config);

                final String annotationData = DomUtils.getChildText(taskElement, "annotationData");
                if (annotationData != null) {
                    reportingTaskNode.setAnnotationData(annotationData.trim());
                }

                final Map<PropertyDescriptor, String> resolvedProps;
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    resolvedProps = new HashMap<>();
                    for (final Map.Entry<String, String> entry : properties.entrySet()) {
                        final PropertyDescriptor descriptor = reportingTask.getPropertyDescriptor(entry.getKey());
                        if (entry.getValue() == null) {
                            resolvedProps.put(descriptor, descriptor.getDefaultValue());
                        } else {
                            resolvedProps.put(descriptor, entry.getValue());
                        }
                    }
                }

                for (final Map.Entry<PropertyDescriptor, String> entry : resolvedProps.entrySet()) {
                    if (entry.getValue() != null) {
                        reportingTaskNode.setProperty(entry.getKey().getName(), entry.getValue());
                    }
                }

                final String comments = DomUtils.getChildText(taskElement, "comment");
                if (comments != null) {
                    reportingTaskNode.setComments(comments);
                }

                reportingTaskNode.setScheduledState(scheduledState);
                if (ScheduledState.RUNNING.equals(scheduledState)) {
                    if (reportingTaskNode.isValid()) {
                        try {
                            processScheduler.schedule(reportingTaskNode);
                        } catch (final Exception e) {
                            logger.error("Failed to start {} due to {}", reportingTaskNode, e);
                            if (logger.isDebugEnabled()) {
                                logger.error("", e);
                            }
                        }
                    } else {
                        logger.error("Failed to start {} because it is invalid due to {}", reportingTaskNode, reportingTaskNode.getValidationErrors());
                    }
                }

                tasks.put(reportingTaskNode.getIdentifier(), reportingTaskNode);
            }
        } catch (final SAXException | ParserConfigurationException | IOException | DOMException | NumberFormatException | InitializationException t) {
            logger.error("Unable to load reporting tasks due to {}", new Object[]{t});
            if (logger.isDebugEnabled()) {
                logger.error("", t);
            }
        }

        return tasks;
    }

    @Override
    public ReportingTaskNode createReportingTask(final String type, final String id, final boolean firstTimeAdded) throws ReportingTaskInstantiationException {
        if (type == null) {
            throw new NullPointerException();
        }
        ReportingTask task = null;
        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final ClassLoader detectedClassLoader = ExtensionManager.getClassLoader(type);
            final Class<?> rawClass;
            if (detectedClassLoader == null) {
                rawClass = Class.forName(type);
            } else {
                rawClass = Class.forName(type, false, detectedClassLoader);
            }

            Thread.currentThread().setContextClassLoader(detectedClassLoader);
            final Class<? extends ReportingTask> reportingTaskClass = rawClass.asSubclass(ReportingTask.class);
            final Object reportingTaskObj = reportingTaskClass.newInstance();
            task = reportingTaskClass.cast(reportingTaskObj);
        } catch (final ClassNotFoundException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException t) {
            throw new ReportingTaskInstantiationException(type, t);
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }

        final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(this);
        final ReportingTaskNode taskNode = new ClusteredReportingTaskNode(task, id, processScheduler,
            new ClusteredEventAccess(this, auditService), bulletinRepository, controllerServiceProvider,
            validationContextFactory, stateManagerProvider.getStateManager(id));
        taskNode.setName(task.getClass().getSimpleName());

        reportingTasks.put(id, taskNode);
        if (firstTimeAdded) {
            try (final NarCloseable x = NarCloseable.withNarLoader()) {
                ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, task);
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, taskNode.getReportingTask());
            } catch (final Exception e) {
                throw new ComponentLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + task, e);
            }
        }

        // Register log observer to provide bulletins when reporting task logs anything at WARN level or above
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
        logRepository.addObserver(StandardProcessorNode.BULLETIN_OBSERVER_ID, LogLevel.WARN,
                new ReportingTaskLogObserver(getBulletinRepository(), taskNode));

        return taskNode;
    }

    private Document parse(final byte[] serialized) throws SAXException, ParserConfigurationException, IOException {
        final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        docFactory.setNamespaceAware(true);

        final DocumentBuilder builder = docFactory.newDocumentBuilder();
        builder.setErrorHandler(new org.xml.sax.ErrorHandler() {
            @Override
            public void fatalError(final SAXParseException err) throws SAXException {
                logger.error("Config file line " + err.getLineNumber() + ", col " + err.getColumnNumber() + ", uri " + err.getSystemId() + " :message: " + err.getMessage());
                if (logger.isDebugEnabled()) {
                    logger.error("Error Stack Dump", err);
                }
                throw err;
            }

            @Override
            public void error(final SAXParseException err) throws SAXParseException {
                logger.error("Config file line " + err.getLineNumber() + ", col " + err.getColumnNumber() + ", uri " + err.getSystemId() + " :message: " + err.getMessage());
                if (logger.isDebugEnabled()) {
                    logger.error("Error Stack Dump", err);
                }
                throw err;
            }

            @Override
            public void warning(final SAXParseException err) throws SAXParseException {
                logger.warn(" Config file line " + err.getLineNumber() + ", uri " + err.getSystemId() + " : message : " + err.getMessage());
                if (logger.isDebugEnabled()) {
                    logger.warn("Warning stack dump", err);
                }
                throw err;
            }
        });

        // build the docuemnt
        final Document document = builder.parse(new ByteArrayInputStream(serialized));
        return document;
    }

    private void addBulletin(final Node node, final Severity severity, final String msg) {
        addBulletin(node.getNodeId(), severity, msg);
    }

    private void addBulletin(final NodeIdentifier nodeId, final Severity severity, final String msg) {
        bulletinRepository.addBulletin(BulletinFactory.createBulletin(BULLETIN_CATEGORY, severity.toString(),
                nodeId.getApiAddress() + ":" + nodeId.getApiPort() + " -- " + msg));
    }

    /**
     * Services a disconnection request.
     *
     * @param nodeId a node identifier
     * @param userDn the DN of the user requesting the disconnection
     *
     * @throws UnknownNodeException if the node does not exist
     * @throws IllegalNodeDisconnectionException if the node cannot be disconnected due to the cluster's state (e.g., node is last connected node or node is primary)
     * @throws NodeDisconnectionException if the disconnection message fails to be sent.
     */
    @Override
    public void requestDisconnection(final String nodeId, final String userDn) throws UnknownNodeException, IllegalNodeDisconnectionException, NodeDisconnectionException {
        writeLock.lock();
        try {
            // check that the node is known
            final Node node = getNode(nodeId);
            if (node == null) {
                throw new UnknownNodeException("Node does not exist.");
            }

            clusterCoordinator.requestNodeDisconnect(node.getNodeId(), DisconnectionCode.USER_DISCONNECTED, "User " + userDn + " Disconnected Node");
        } finally {
            writeLock.unlock("requestDisconnection(String)");
        }
    }

    /**
     * Requests a disconnection to the node with the given node ID, but any exception thrown is suppressed.
     *
     * @param nodeId the node ID
     */
    private void requestDisconnectionQuietly(final NodeIdentifier nodeId, final String explanation) {
        try {
            requestDisconnection(nodeId, /* ignore node check */ true, explanation);
        } catch (final IllegalNodeDisconnectionException | NodeDisconnectionException ex) { /* suppress exception */ }
    }

    /**
     * Issues a disconnection message to the node identified by the given node ID. If the node is not known, then a UnknownNodeException is thrown. If the node cannot be disconnected due to the
     * cluster's state and ignoreLastNodeCheck is false, then a IllegalNodeDisconnectionException is thrown. Otherwise, a disconnection message is issued to the node.
     *
     * Whether the disconnection message is successfully sent to the node, the node is marked as disconnected and if the node is the primary node, then the primary role is revoked.
     *
     * @param nodeId the ID of the node
     * @param ignoreNodeChecks if false, checks will be made to ensure the cluster supports the node's disconnection (e.g., the node is not the last connected node in the cluster; the node is not the
     * primary); otherwise, the request is made regardless of the cluster state
     * @param explanation
     *
     * @throws IllegalNodeDisconnectionException if the node cannot be disconnected due to the cluster's state (e.g., node is last connected node or node is primary). Not thrown if ignoreNodeChecks is
     * true.
     * @throws NodeDisconnectionException if the disconnection message fails to be sent.
     */
    void requestDisconnection(final NodeIdentifier nodeId, final boolean ignoreNodeChecks, final String explanation)
            throws IllegalNodeDisconnectionException, NodeDisconnectionException {

        writeLock.lock();
        try {

            // check that the node is known
            final Node node = getRawNode(nodeId.getId());
            if (node == null) {
                if (ignoreNodeChecks) {
                    // issue the disconnection
                    final DisconnectMessage request = new DisconnectMessage();
                    request.setNodeId(nodeId);
                    request.setExplanation(explanation);

                    addEvent(nodeId, "Disconnection requested due to " + explanation);
                    senderListener.disconnect(request);
                    addEvent(nodeId, "Node disconnected due to " + explanation);
                    addBulletin(nodeId, Severity.INFO, "Node disconnected due to " + explanation);
                    return;
                } else {
                    throw new UnknownNodeException("Node does not exist");
                }
            }

            // if necessary, check that the node may be disconnected
            if (!ignoreNodeChecks) {
                final Set<NodeIdentifier> connectedNodes = getNodeIds(Status.CONNECTED);
                // cannot disconnect the last connected node in the cluster
                if (connectedNodes.size() == 1 && connectedNodes.iterator().next().equals(nodeId)) {
                    throw new IllegalNodeDisconnectionException("Node may not be disconnected because it is the only connected node in the cluster.");
                }
            }

            // update status
            clusterCoordinator.updateNodeStatus(node, Status.DISCONNECTED);
            notifyDataFlowManagementServiceOfNodeStatusChange();

            // issue the disconnection
            final DisconnectMessage request = new DisconnectMessage();
            request.setNodeId(nodeId);
            request.setExplanation(explanation);

            addEvent(nodeId, "Disconnection requested due to " + explanation);
            senderListener.disconnect(request);
            addEvent(nodeId, "Node disconnected due to " + explanation);
            addBulletin(node, Severity.INFO, "Node disconnected due to " + explanation);

            heartbeatMonitor.removeHeartbeat(nodeId);
        } finally {
            writeLock.unlock("requestDisconnection(NodeIdentifier, boolean)");
        }
    }


    private NodeIdentifier addRequestorDn(final NodeIdentifier nodeId, final String dn) {
        return new NodeIdentifier(nodeId.getId(), nodeId.getApiAddress(), nodeId.getApiPort(),
            nodeId.getSocketAddress(), nodeId.getSocketPort(),
            nodeId.getSiteToSiteAddress(), nodeId.getSiteToSitePort(), nodeId.isSiteToSiteSecure(), dn);
    }

    private ConnectionResponseMessage handleConnectionRequest(final ConnectionRequestMessage requestMessage) {
        final NodeIdentifier proposedIdentifier = requestMessage.getConnectionRequest().getProposedNodeIdentifier();
        final ConnectionRequest requestWithDn = new ConnectionRequest(addRequestorDn(proposedIdentifier, requestMessage.getRequestorDN()));

        final ConnectionResponse response = requestConnection(requestWithDn);
        final ConnectionResponseMessage responseMessage = new ConnectionResponseMessage();
        responseMessage.setConnectionResponse(response);
        return responseMessage;
    }


    @Override
    public ControllerServiceNode createControllerService(final String type, final String id, final boolean firstTimeAdded) {
        final ControllerServiceNode serviceNode = controllerServiceProvider.createControllerService(type, id, firstTimeAdded);

        // Register log observer to provide bulletins when reporting task logs anything at WARN level or above
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
        logRepository.addObserver(StandardProcessorNode.BULLETIN_OBSERVER_ID, LogLevel.WARN,
                new ControllerServiceLogObserver(getBulletinRepository(), serviceNode));

        if (firstTimeAdded) {
            final ControllerService service = serviceNode.getControllerServiceImplementation();

            try (final NarCloseable nc = NarCloseable.withNarLoader()) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, service);
            }
        }

        return serviceNode;
    }

    public StateManagerProvider getStateManagerProvider() {
        return stateManagerProvider;
    }

    @Override
    public ControllerService getControllerService(String serviceIdentifier) {
        return controllerServiceProvider.getControllerService(serviceIdentifier);
    }

    @Override
    public ControllerServiceNode getControllerServiceNode(final String id) {
        return controllerServiceProvider.getControllerServiceNode(id);
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        return controllerServiceProvider.isControllerServiceEnabled(service);
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        return controllerServiceProvider.isControllerServiceEnabled(serviceIdentifier);
    }

    @Override
    public boolean isControllerServiceEnabling(final String serviceIdentifier) {
        return controllerServiceProvider.isControllerServiceEnabling(serviceIdentifier);
    }

    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
        return controllerServiceProvider.getControllerServiceName(serviceIdentifier);
    }

    @Override
    public void removeControllerService(final ControllerServiceNode serviceNode) {
        controllerServiceProvider.removeControllerService(serviceNode);
    }

    @Override
    public void enableControllerService(final ControllerServiceNode serviceNode) {
        controllerServiceProvider.enableControllerService(serviceNode);
    }

    @Override
    public void enableControllerServices(final Collection<ControllerServiceNode> serviceNodes) {
        controllerServiceProvider.enableControllerServices(serviceNodes);
    }

    @Override
    public void disableControllerService(final ControllerServiceNode serviceNode) {
        controllerServiceProvider.disableControllerService(serviceNode);
    }

    @Override
    public Set<ControllerServiceNode> getAllControllerServices() {
        return controllerServiceProvider.getAllControllerServices();
    }

    @Override
    public Set<ConfiguredComponent> disableReferencingServices(final ControllerServiceNode serviceNode) {
        return controllerServiceProvider.disableReferencingServices(serviceNode);
    }

    @Override
    public Set<ConfiguredComponent> enableReferencingServices(final ControllerServiceNode serviceNode) {
        return controllerServiceProvider.enableReferencingServices(serviceNode);
    }

    @Override
    public Set<ConfiguredComponent> scheduleReferencingComponents(final ControllerServiceNode serviceNode) {
        return controllerServiceProvider.scheduleReferencingComponents(serviceNode);
    }

    @Override
    public Set<ConfiguredComponent> unscheduleReferencingComponents(final ControllerServiceNode serviceNode) {
        return controllerServiceProvider.unscheduleReferencingComponents(serviceNode);
    }

    @Override
    public void verifyCanEnableReferencingServices(final ControllerServiceNode serviceNode) {
        controllerServiceProvider.verifyCanEnableReferencingServices(serviceNode);
    }

    @Override
    public void verifyCanScheduleReferencingComponents(final ControllerServiceNode serviceNode) {
        controllerServiceProvider.verifyCanScheduleReferencingComponents(serviceNode);
    }

    @Override
    public void verifyCanDisableReferencingServices(final ControllerServiceNode serviceNode) {
        controllerServiceProvider.verifyCanDisableReferencingServices(serviceNode);
    }

    @Override
    public void verifyCanStopReferencingComponents(final ControllerServiceNode serviceNode) {
        controllerServiceProvider.verifyCanStopReferencingComponents(serviceNode);
    }

    private byte[] serialize(final Document doc) throws TransformerException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DOMSource domSource = new DOMSource(doc);
        final StreamResult streamResult = new StreamResult(baos);

        // configure the transformer and convert the DOM
        final TransformerFactory transformFactory = TransformerFactory.newInstance();
        final Transformer transformer = transformFactory.newTransformer();
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");

        // transform the document to byte stream
        transformer.transform(domSource, streamResult);
        return baos.toByteArray();
    }


    private byte[] serializeReportingTasks() throws ParserConfigurationException, TransformerException {
        final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        docFactory.setNamespaceAware(true);

        final DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
        final Document document = docBuilder.newDocument();
        final Element rootElement = document.createElement("reportingTasks");
        document.appendChild(rootElement);

        for (final ReportingTaskNode taskNode : getAllReportingTasks()) {
            StandardFlowSerializer.addReportingTask(rootElement, taskNode, encryptor);
        }

        return serialize(document);
    }


    public void saveReportingTasks() {
        try {
            dataFlowManagementService.updateReportingTasks(serializeReportingTasks());
        } catch (final Exception e) {
            logger.error("Failed to save changes to NCM's Reporting Tasks; changes may be lost on restart due to " + e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }

            getBulletinRepository().addBulletin(BulletinFactory.createBulletin("Reporting Tasks", Severity.ERROR.name(),
                    "Failed to save changes to NCM's Reporting Tasks; changes may be lost on restart. See logs for more details."));
        }
    }

    @Override
    public Set<ReportingTaskNode> getAllReportingTasks() {
        readLock.lock();
        try {
            return new HashSet<>(reportingTasks.values());
        } finally {
            readLock.unlock("getReportingTasks");
        }
    }

    @Override
    public ReportingTaskNode getReportingTaskNode(final String taskId) {
        readLock.lock();
        try {
            return reportingTasks.get(taskId);
        } finally {
            readLock.unlock("getReportingTaskNode");
        }
    }

    @Override
    public void startReportingTask(final ReportingTaskNode reportingTaskNode) {
        reportingTaskNode.verifyCanStart();
        processScheduler.schedule(reportingTaskNode);
    }

    @Override
    public void stopReportingTask(final ReportingTaskNode reportingTaskNode) {
        reportingTaskNode.verifyCanStop();
        processScheduler.unschedule(reportingTaskNode);
    }

    @Override
    public void removeReportingTask(final ReportingTaskNode reportingTaskNode) {
        writeLock.lock();
        try {
            final ReportingTaskNode existing = reportingTasks.get(reportingTaskNode.getIdentifier());
            if (existing == null || existing != reportingTaskNode) {
                throw new IllegalStateException("Reporting Task " + reportingTaskNode + " does not exist in this Flow");
            }

            reportingTaskNode.verifyCanDelete();

            try (final NarCloseable x = NarCloseable.withNarLoader()) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, reportingTaskNode.getReportingTask(), reportingTaskNode.getConfigurationContext());
            }

            for (final Map.Entry<PropertyDescriptor, String> entry : reportingTaskNode.getProperties().entrySet()) {
                final PropertyDescriptor descriptor = entry.getKey();
                if (descriptor.getControllerServiceDefinition() != null) {
                    final String value = entry.getValue() == null ? descriptor.getDefaultValue() : entry.getValue();
                    if (value != null) {
                        final ControllerServiceNode serviceNode = controllerServiceProvider.getControllerServiceNode(value);
                        if (serviceNode != null) {
                            serviceNode.removeReference(reportingTaskNode);
                        }
                    }
                }
            }

            reportingTasks.remove(reportingTaskNode.getIdentifier());
        } finally {
            writeLock.unlock("removeReportingTask");
        }
    }

    @Override
    public void disableReportingTask(final ReportingTaskNode reportingTask) {
        reportingTask.verifyCanDisable();
        processScheduler.disableReportingTask(reportingTask);
    }

    @Override
    public void enableReportingTask(final ReportingTaskNode reportingTask) {
        reportingTask.verifyCanEnable();
        processScheduler.enableReportingTask(reportingTask);
    }


    @Override
    public Set<Node> getNodes(final Status... statuses) {
        final Set<Status> desiredStatusSet = new HashSet<>();
        for (final Status status : statuses) {
            desiredStatusSet.add(status);
        }

        readLock.lock();
        try {
            final Set<Node> clonedNodes = new HashSet<>();
            for (final Node node : nodes) {
                if (desiredStatusSet.isEmpty() || desiredStatusSet.contains(node.getStatus())) {
                    clonedNodes.add(node.clone());
                }
            }
            return Collections.unmodifiableSet(clonedNodes);
        } finally {
            readLock.unlock("getNodes(Status...)");
        }
    }

    @Override
    public Node getNode(final String nodeId) {
        readLock.lock();
        try {
            for (final Node node : nodes) {
                if (node.getNodeId().getId().equals(nodeId)) {
                    return node.clone();
                }
            }
            return null;
        } finally {
            readLock.unlock("getNode(String)");
        }
    }

    @Override
    public Node getPrimaryNode() {
        readLock.lock();
        try {
            if (primaryNodeId == null) {
                return null;
            } else {
                return getNode(primaryNodeId.getId());
            }
        } finally {
            readLock.unlock("getPrimaryNode");
        }
    }

    @Override
    public void deleteNode(final String nodeId, final String userDn) throws UnknownNodeException, IllegalNodeDeletionException {
        writeLock.lock();
        try {
            final Node node = getNode(nodeId);
            if (node == null) {
                throw new UnknownNodeException("Node does not exist.");
            } else if (Status.DISCONNECTED == node.getStatus()) {
                nodes.remove(node);

                if (eventManager != null) {
                    eventManager.clearEventHistory(node.getNodeId().getId());
                }

                logger.info("Removing node {} from cluster because this action was requested by {}", node, userDn);
            } else {
                throw new IllegalNodeDeletionException("Node may not be deleted because it is not disconnected.");
            }
        } finally {
            writeLock.unlock("deleteNode");
        }
    }

    @Override
    public Set<NodeIdentifier> getNodeIds(final Status... statuses) {
        readLock.lock();
        try {
            final Set<NodeIdentifier> nodeIds = new HashSet<>();
            for (final Node node : nodes) {
                if (statuses == null || statuses.length == 0) {
                    nodeIds.add(node.getNodeId());
                } else {
                    for (final Node.Status status : statuses) {
                        if (node.getStatus() == status) {
                            nodeIds.add(node.getNodeId());
                            break;
                        }
                    }
                }
            }
            return nodeIds;
        } finally {
            readLock.unlock("getNodeIds(Status...)");
        }
    }

    private int getClusterProtocolHeartbeatSeconds() {
        return (int) FormatUtils.getTimeDuration(properties.getClusterProtocolHeartbeatInterval(), TimeUnit.SECONDS);
    }

    @Override
    public int getHeartbeatMonitoringIntervalSeconds() {
        return 4 * getClusterProtocolHeartbeatSeconds();
    }

    @Override
    public int getMaxHeartbeatGapSeconds() {
        return 8 * getClusterProtocolHeartbeatSeconds();
    }

    @Override
    public List<Event> getNodeEvents(final String nodeId) {
        readLock.lock();
        try {
            List<Event> events = null;
            final EventManager eventMgr = eventManager;
            if (eventMgr != null) {
                events = eventMgr.getEvents(nodeId);
            }

            if (events == null) {
                return Collections.emptyList();
            } else {
                return Collections.unmodifiableList(events);
            }
        } finally {
            readLock.unlock("getNodeEvents");
        }
    }

    @Override
    public NodeResponse applyRequest(final String method, final URI uri, final Map<String, List<String>> parameters, final Map<String, String> headers)
            throws NoConnectedNodesException, NoResponseFromNodesException, UriConstructionException,
            ConnectingNodeMutableRequestException, DisconnectedNodeMutableRequestException, SafeModeMutableRequestException {
        return applyRequest(method, uri, parameters, headers, getNodeIds(Status.CONNECTED));
    }

    @Override
    public NodeResponse applyRequest(final String method, final URI uri, final Map<String, List<String>> parameters, final Map<String, String> headers, final Set<NodeIdentifier> nodeIdentifiers)
            throws NoConnectedNodesException, NoResponseFromNodesException, UriConstructionException,
            ConnectingNodeMutableRequestException, DisconnectedNodeMutableRequestException, SafeModeMutableRequestException {

        final boolean mutableRequest = canChangeNodeState(method, uri);
        final ClusterManagerLock lock = mutableRequest ? writeLock : readLock;

        lock.lock();
        try {
            final NodeResponse clientResponse = federateRequest(method, uri, parameters, null, headers, nodeIdentifiers);
            if (clientResponse == null) {
                if (mutableRequest) {
                    throw new NoConnectedNodesException(String.format("All nodes were disconnected as a result of applying request %s %s", method, uri));
                } else {
                    throw new NoResponseFromNodesException("No nodes were able to process this request.");
                }
            } else {
                return clientResponse;
            }
        } finally {
            lock.unlock("applyRequest(String, URI, Map<String, List<String>>, Map<String, String>, Set<NodeIdentifier>");
        }
    }

    @Override
    public NodeResponse applyRequest(final String method, final URI uri, final Object entity, final Map<String, String> headers)
            throws NoConnectedNodesException, NoResponseFromNodesException, UriConstructionException,
            ConnectingNodeMutableRequestException, DisconnectedNodeMutableRequestException, SafeModeMutableRequestException {
        return applyRequest(method, uri, entity, headers, getNodeIds(Status.CONNECTED));
    }

    @Override
    public NodeResponse applyRequest(final String method, final URI uri, final Object entity, final Map<String, String> headers, final Set<NodeIdentifier> nodeIdentifiers)
            throws NoConnectedNodesException, NoResponseFromNodesException, UriConstructionException,
            ConnectingNodeMutableRequestException, DisconnectedNodeMutableRequestException, SafeModeMutableRequestException {

        final boolean mutableRequest = canChangeNodeState(method, uri);
        final ClusterManagerLock lock = mutableRequest ? writeLock : readLock;

        lock.lock();
        try {
            // check that the request can be applied
            if (mutableRequest) {
                if (isInSafeMode()) {
                    throw new SafeModeMutableRequestException("Received a mutable request [" + method + " -- " + uri + "] while in safe mode");
                } else if (!getNodeIds(Status.DISCONNECTED, Status.DISCONNECTING).isEmpty()) {
                    throw new DisconnectedNodeMutableRequestException("Received a mutable request [" + method + " -- " + uri + "] while a node is disconnected from the cluster");
                } else if (!getNodeIds(Status.CONNECTING).isEmpty()) {
                    // if any node is connecting and a request can change the flow, then we throw an exception
                    throw new ConnectingNodeMutableRequestException("Received a mutable request [" + method + " -- " + uri + "] while a node is trying to connect to the cluster");
                }
            }

            final NodeResponse clientResponse = federateRequest(method, uri, null, entity, headers, nodeIdentifiers);
            if (clientResponse == null) {
                if (mutableRequest) {
                    throw new NoConnectedNodesException(String.format("All nodes were disconnected as a result of applying request %s %s", method, uri));
                } else {
                    throw new NoResponseFromNodesException("No nodes were able to process this request.");
                }
            } else {
                return clientResponse;
            }

        } finally {
            lock.unlock("applyRequest(String, URI, Object, Map<String, String>, Set<NodeIdentifier>");
        }
    }

    public void setServicesBroadcaster(final ClusterServicesBroadcaster servicesBroadcaster) {
        writeLock.lock();
        try {
            this.servicesBroadcaster = servicesBroadcaster;
        } finally {
            writeLock.unlock("setServicesBroadcaster");
        }
    }

    public boolean addBroadcastedService(final DiscoverableService service) {
        writeLock.lock();
        try {
            final ClusterServicesBroadcaster broadcaster = this.servicesBroadcaster;
            if (broadcaster == null) {
                throw new IllegalStateException("Service broadcasting is not configured.");
            }
            return broadcaster.addService(service);
        } finally {
            writeLock.unlock("addBroadcastedService");
        }
    }

    public boolean removeBroadcastedService(final String serviceName) {
        writeLock.lock();
        try {
            final ClusterServicesBroadcaster broadcaster = this.servicesBroadcaster;
            if (broadcaster == null) {
                throw new IllegalStateException("Service broadcasting is not configured.");
            }
            return broadcaster.removeService(serviceName);
        } finally {
            writeLock.unlock("removeBroadcastedService");
        }
    }

    public boolean isBroadcastingConfigured() {
        readLock.lock();
        try {
            return servicesBroadcaster != null;
        } finally {
            readLock.unlock("isBroadcastingConfigured");
        }
    }

    public boolean isBroadcasting() {
        readLock.lock();
        try {
            final ClusterServicesBroadcaster broadcaster = this.servicesBroadcaster;
            return broadcaster != null && broadcaster.isRunning();
        } finally {
            readLock.unlock("isBroadcasting");
        }
    }

    public void addEvent(final NodeIdentifier nodeId, String eventMsg) {
        writeLock.lock();
        try {
            final Event event = new Event(nodeId.getId(), eventMsg);
            final EventManager eventMgr = eventManager;
            if (eventMgr != null) {
                eventMgr.addEvent(event);
            }
            logger.info(String.format("Node Event: %s -- '%s'", nodeId, eventMsg));
        } finally {
            writeLock.unlock("addEvent");
        }
    }

    public void setEventManager(final EventManager eventManager) {
        writeLock.lock();
        try {
            this.eventManager = eventManager;
        } finally {
            writeLock.unlock("setEventManager");
        }
    }

    public void setClusterFirewall(final ClusterNodeFirewall clusterFirewall) {
        writeLock.lock();
        try {
            this.clusterFirewall = clusterFirewall;
        } finally {
            writeLock.unlock("setClusterFirewall");
        }
    }

    public boolean isFirewallConfigured() {
        readLock.lock();
        try {
            return clusterFirewall != null;
        } finally {
            readLock.unlock("isFirewallConfigured");
        }
    }

    public void setAuditService(final AuditService auditService) {
        writeLock.lock();
        try {
            this.auditService = auditService;
        } finally {
            writeLock.unlock("setAuditService");
        }
    }

    public boolean isAuditingConfigured() {
        readLock.lock();
        try {
            return auditService != null;
        } finally {
            readLock.unlock("isAuditingConfigured");
        }
    }

    private boolean isInSafeMode() {
        readLock.lock();
        try {
            return primaryNodeId == null || getRawNode(primaryNodeId.getId()) == null;
        } finally {
            readLock.unlock("isInSafeMode");
        }
    }

    void setPrimaryNodeId(final NodeIdentifier primaryNodeId) throws DaoException {
        writeLock.lock();
        try {
            dataFlowManagementService.updatePrimaryNode(primaryNodeId);

            // update the cached copy reference to minimize loading file from disk
            this.primaryNodeId = primaryNodeId;
        } finally {
            writeLock.unlock("setPrimaryNodeId");
        }
    }

    // requires write lock to already be acquired unless method cannot change node state
    private NodeResponse federateRequest(
            final String method, final URI uri, final Map<String, List<String>> parameters, final Object entity, final Map<String, String> headers, final Set<NodeIdentifier> nodeIds)
            throws UriConstructionException {
        // ensure some nodes are connected
        if (nodeIds.isEmpty()) {
            throw new NoConnectedNodesException("Cannot apply " + method + " request to " + uri + " because there are currently no connected Nodes");
        }

        logger.debug("Applying prototype request " + uri + " to nodes.");

        // the starting state of the flow (current, stale, unknown)
        final PersistedFlowState originalPersistedFlowState = dataFlowManagementService.getPersistedFlowState();

        // check if this request can change the flow
        final boolean mutableRequest = canChangeNodeState(method, uri);

        final ObjectHolder<NodeResponse> holder = new ObjectHolder<>(null);
        final UpdateRevision federateRequest = new UpdateRevision() {
            @Override
            public Revision execute(Revision currentRevision) {
                // update headers to contain cluster contextual information to send to the node
                final Map<String, String> updatedHeaders = new HashMap<>(headers);
                final ClusterContext clusterCtx = new ClusterContextImpl();
                clusterCtx.setRequestSentByClusterManager(true);                 // indicate request is sent from cluster manager
                clusterCtx.setRevision(currentRevision);

                // serialize cluster context and add to request header
                final String serializedClusterCtx = WebUtils.serializeObjectToHex(clusterCtx);
                updatedHeaders.put(CLUSTER_CONTEXT_HTTP_HEADER, serializedClusterCtx);

                // replicate request
                final AsyncClusterResponse clusterResponse = httpRequestReplicator.replicate(nodeIds, method, uri, entity == null ? parameters : entity, updatedHeaders);

                final NodeResponse clientResponse;
                try {
                    clientResponse = clusterResponse.awaitMergedResponse();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Thread was interrupted while waiting for a response from one or more nodes", e);
                    final Set<NodeIdentifier> noResponses = clusterResponse.getNodesInvolved();
                    noResponses.removeAll(clusterResponse.getCompletedNodeIdentifiers());
                    throw new IllegalClusterStateException("Interrupted while waiting for a response from the following nodes: " + noResponses, e);
                }

                holder.set(clientResponse);

                // if we have a response get the updated cluster context for auditing and revision updating
                Revision updatedRevision = null;
                if (mutableRequest && clientResponse != null) {
                    try {
                        // get the cluster context from the response header
                        final String serializedClusterContext = clientResponse.getClientResponse().getHeaders().getFirst(CLUSTER_CONTEXT_HTTP_HEADER);
                        if (StringUtils.isNotBlank(serializedClusterContext)) {
                            // deserialize object
                            final Serializable clusterContextObj = WebUtils.deserializeHexToObject(serializedClusterContext);

                            // if we have a valid object, audit the actions
                            if (clusterContextObj instanceof ClusterContext) {
                                final ClusterContext clusterContext = (ClusterContext) clusterContextObj;
                                if (auditService != null) {
                                    try {
                                        auditService.addActions(clusterContext.getActions());
                                    } catch (final Throwable t) {
                                        logger.warn("Unable to record actions: " + t.getMessage());
                                        if (logger.isDebugEnabled()) {
                                            logger.warn(StringUtils.EMPTY, t);
                                        }
                                    }
                                }
                                updatedRevision = clusterContext.getRevision();
                            }
                        }
                    } catch (final ClassNotFoundException cnfe) {
                        logger.warn("Classpath issue detected because failed to deserialize cluster context from node response due to: " + cnfe, cnfe);
                    }
                }

                return updatedRevision;
            }
        };

        // federate the request and lock on the revision
        if (mutableRequest) {
            optimisticLockingManager.setRevision(federateRequest);
        } else {
            federateRequest.execute(optimisticLockingManager.getLastModification().getRevision());
        }

        return holder.get();
    }


    private static boolean isCounterEndpoint(final String uriPath) {
        return COUNTER_URI_PATTERN.matcher(uriPath).matches();
    }


    public List<BulletinDTO> mergeNCMBulletins(final List<BulletinDTO> nodeBulletins, final List<Bulletin> ncmBulletins) {
        if (ncmBulletins == null || ncmBulletins.isEmpty()) {
            return nodeBulletins;
        }

        final List<BulletinDTO> mergedBulletins = new ArrayList<>(nodeBulletins.size() + ncmBulletins.size());
        mergedBulletins.addAll(nodeBulletins);
        mergedBulletins.addAll(createBulletinDtos(ncmBulletins));
        return mergedBulletins;
    }


    /**
     * Creates BulletinDTOs for the specified Bulletins.
     *
     * @param bulletins bulletin
     * @return dto
     */
    public List<BulletinDTO> createBulletinDtos(final List<Bulletin> bulletins) {
        final List<BulletinDTO> bulletinDtos = new ArrayList<>(bulletins.size());
        for (final Bulletin bulletin : bulletins) {
            bulletinDtos.add(createBulletinDto(bulletin));
        }
        return bulletinDtos;
    }

    /**
     * Creates a BulletinDTO for the specified Bulletin.
     *
     * @param bulletin bulletin
     * @return dto
     */
    public BulletinDTO createBulletinDto(final Bulletin bulletin) {
        final BulletinDTO dto = new BulletinDTO();
        dto.setId(bulletin.getId());
        dto.setNodeAddress(bulletin.getNodeAddress());
        dto.setTimestamp(bulletin.getTimestamp());
        dto.setGroupId(bulletin.getGroupId());
        dto.setSourceId(bulletin.getSourceId());
        dto.setSourceName(bulletin.getSourceName());
        dto.setCategory(bulletin.getCategory());
        dto.setLevel(bulletin.getLevel());
        dto.setMessage(bulletin.getMessage());
        return dto;
    }


    /**
     * Merges the validation errors into the specified map, recording the corresponding node identifier.
     *
     * @param validationErrorMap map
     * @param nodeId id
     * @param nodeValidationErrors errors
     */
    public void mergeValidationErrors(final Map<String, Set<NodeIdentifier>> validationErrorMap, final NodeIdentifier nodeId, final Collection<String> nodeValidationErrors) {
        if (nodeValidationErrors != null) {
            for (final String nodeValidationError : nodeValidationErrors) {
                Set<NodeIdentifier> nodeSet = validationErrorMap.get(nodeValidationError);
                if (nodeSet == null) {
                    nodeSet = new HashSet<>();
                    validationErrorMap.put(nodeValidationError, nodeSet);
                }
                nodeSet.add(nodeId);
            }
        }
    }

    /**
     * Normalizes the validation errors by prepending the corresponding nodes when the error does not exist across all nodes.
     *
     * @param validationErrorMap map
     * @param totalNodes total
     * @return normalized errors
     */
    public Set<String> normalizedMergedValidationErrors(final Map<String, Set<NodeIdentifier>> validationErrorMap, int totalNodes) {
        final Set<String> normalizedValidationErrors = new HashSet<>();
        for (final Map.Entry<String, Set<NodeIdentifier>> validationEntry : validationErrorMap.entrySet()) {
            final String msg = validationEntry.getKey();
            final Set<NodeIdentifier> nodeIds = validationEntry.getValue();

            if (nodeIds.size() == totalNodes) {
                normalizedValidationErrors.add(msg);
            } else {
                for (final NodeIdentifier nodeId : nodeIds) {
                    normalizedValidationErrors.add(nodeId.getApiAddress() + ":" + nodeId.getApiPort() + " -- " + msg);
                }
            }
        }
        return normalizedValidationErrors;
    }


    // TODO: This is temporary. Only here because while we have NCM, we must merge its bulletins. Once we get rid
    // of the NCM, this goes away completely.
    public static final Pattern CONTROLLER_STATUS_URI_PATTERN = Pattern.compile("/nifi-api/controller/status");

    public boolean isControllerStatusEndpoint(final URI uri, final String method) {
        return "GET".equalsIgnoreCase(method) && CONTROLLER_STATUS_URI_PATTERN.matcher(uri.getPath()).matches();
    }


    @Override
    public void afterRequest(final String uriPath, final String method, final Set<NodeResponse> nodeResponses) {
        final boolean mutableRequest = canChangeNodeState(method, null);

        /*
         * Nodes that encountered issues handling the request are marked as
         * disconnected for mutable requests (e.g., post, put, delete).  For
         * other requests (e.g., get, head), the nodes remain in their current
         * state even if they had problems handling the request.
         */
        if (mutableRequest) {
            final Set<NodeResponse> problematicNodeResponses = responseMerger.getProblematicNodeResponses(nodeResponses);

            // all nodes failed
            final boolean allNodesFailed = problematicNodeResponses.size() == nodeResponses.size();

            // some nodes had a problematic response because of a missing counter, ensure the are not disconnected
            final boolean someNodesFailedMissingCounter = !problematicNodeResponses.isEmpty()
                && problematicNodeResponses.size() < nodeResponses.size() && isMissingCounter(problematicNodeResponses, uriPath);

            // ensure nodes stay connected in certain scenarios
            if (allNodesFailed || someNodesFailedMissingCounter) {
                for (final NodeResponse nodeResponse : nodeResponses) {
                    final Node node = getRawNode(nodeResponse.getNodeId().getId());

                    if (problematicNodeResponses.contains(nodeResponse)) {
                        node.setStatus(Status.CONNECTED);
                        problematicNodeResponses.remove(nodeResponse);
                    }
                }
            }

            // disconnect problematic nodes
            if (!problematicNodeResponses.isEmpty()) {
                if (problematicNodeResponses.size() < nodeResponses.size()) {
                    logger.warn(String.format("The following nodes failed to process URI '%s'.  Requesting each node to disconnect from cluster: ", uriPath, problematicNodeResponses));
                    disconnectNodes(problematicNodeResponses, "Failed to process URI " + uriPath);
                } else {
                    logger.warn("All nodes failed to process URI {}. As a result, no node will be disconnected from cluster", uriPath);
                }
            }
        }
    }


    /**
     * Determines if all problematic responses were due to 404 NOT_FOUND. Assumes that problematicNodeResponses is not empty and is not comprised of responses from all nodes in the cluster (at least
     * one node contained the counter in question).
     *
     * @param problematicNodeResponses The problematic node responses
     * @param uriPath The path of the URI for the request
     * @return Whether all problematic node responses were due to a missing counter
     */
    private boolean isMissingCounter(final Set<NodeResponse> problematicNodeResponses, final String uriPath) {
        if (isCounterEndpoint(uriPath)) {
            boolean notFound = true;
            for (final NodeResponse problematicResponse : problematicNodeResponses) {
                if (problematicResponse.getStatus() != 404) {
                    notFound = false;
                    break;
                }
            }
            return notFound;
        }
        return false;
    }


    /**
     * A helper method to disconnect nodes that returned unsuccessful HTTP responses because of a replicated request. Disconnection requests are sent concurrently.
     *
     */
    private void disconnectNodes(final Set<NodeResponse> nodeResponses, final String explanation) {
        // return fast if nothing to do
        if (nodeResponses == null || nodeResponses.isEmpty()) {
            return;
        }

        final ExecutorService executorService = Executors.newFixedThreadPool(properties.getClusterManagerProtocolThreads());
        final CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);
        for (final NodeResponse nodeResponse : nodeResponses) {
            completionService.submit(new Runnable() {
                @Override
                public void run() {
                    final NodeIdentifier nodeId = nodeResponse.getNodeId();
                    final int responseStatus = nodeResponse.getStatus();
                    final URI requestUri = nodeResponse.getRequestUri();
                    final StringBuilder msgBuilder = new StringBuilder();
                    msgBuilder
                            .append("Requesting disconnection for node ")
                            .append(nodeId)
                            .append(" for request URI ")
                            .append(requestUri);
                    if (nodeResponse.hasThrowable()) {
                        msgBuilder.append(" because manager encountered exception when issuing request: ")
                                .append(nodeResponse.getThrowable());
                        // log stack trace anytime we have a throwable
                        ((NiFiLog) logger).getWrappedLog().info(msgBuilder.toString(), nodeResponse.getThrowable());
                        addEvent(nodeId, "Manager encountered exception when issuing request for URI " + requestUri);
                        addBulletin(nodeId, Severity.ERROR, "Manager encountered exception when issuing request for URI " + requestUri + "; node will be disconnected");
                    } else {
                        msgBuilder.append(" because HTTP response status was ")
                                .append(responseStatus);
                        logger.info(msgBuilder.toString());
                        addEvent(nodeId, "HTTP response status was unsuccessful (" + responseStatus + ") for request URI " + requestUri);
                        addBulletin(nodeId, Severity.ERROR, "HTTP response status was unsuccessful (" + responseStatus + ") for request URI " + requestUri);
                    }
                    requestDisconnectionQuietly(nodeId, explanation);
                }
            }, null);
        }

        executorService.shutdown();
    }

    /**
     * Returns false if an internal protocol message was received by a node listed in the firewall. If no firewall is configured, then false is always returned.
     *
     * @param ip the IP of the remote machine
     *
     * @return false if the IP is listed in the firewall or if the firewall is not configured; true otherwise
     */
    boolean isBlockedByFirewall(final String ip) {
        if (isFirewallConfigured()) {
            return !clusterFirewall.isPermissible(ip);
        } else {
            return false;
        }
    }

    private Set<Node> getRawNodes(final Status... statuses) {
        readLock.lock();
        try {
            final Set<Node> result = new HashSet<>();
            if (statuses == null || statuses.length == 0) {
                result.addAll(nodes);
            } else {
                for (final Node node : nodes) {
                    for (final Node.Status status : statuses) {
                        if (node.getStatus() == status) {
                            result.add(node);
                            break;
                        }
                    }
                }
            }
            return result;
        } finally {
            readLock.unlock("getRawNodes(Status...)");
        }
    }

    Node getRawNode(final String nodeId) {
        readLock.lock();
        try {
            for (final Node node : nodes) {
                if (node.getNodeId().getId().equals(nodeId)) {
                    return node;
                }
            }
            return null;
        } finally {
            readLock.unlock("getRawNode(String)");
        }
    }

    /**
     * Resolves a proposed node identifier to a node identifier that the manager approves. If the proposed node identifier conflicts with an existing node identifier, then an approved node identifier
     * is generated and returned to the caller.
     *
     * @param proposedNodeId a proposed identifier
     *
     * @return the node identifier that should be used
     */
    private NodeIdentifier resolveProposedNodeIdentifier(final NodeIdentifier proposedNodeId) throws ConflictingNodeIdException {
        readLock.lock();
        try {
            for (final Node node : nodes) {
                final NodeIdentifier nodeId = node.getNodeId();

                // are the ids the same
                final boolean sameId = nodeId.equals(proposedNodeId);

                // are the service coordinates the same
                final boolean sameServiceCoordinates = nodeId.logicallyEquals(proposedNodeId);

                if (sameId && sameServiceCoordinates) {
                    // we know about this node and it has the same ID, so the proposal is fine
                    return proposedNodeId;
                } else if (sameId && !sameServiceCoordinates) {
                    throw new ConflictingNodeIdException(nodeId.getId(), node.getNodeId().getApiAddress(), node.getNodeId().getApiPort());
                } else if (!sameId && sameServiceCoordinates) {
                    // we know about this node, so we'll use the existing ID
                    logger.debug(String.format("Using Node Identifier %s because proposed node identifier %s matches the service coordinates", nodeId, proposedNodeId));

                    // return a new Node Identifier that uses the existing Node UUID, Node Index, and ZooKeeper Port from the existing Node (because these are the
                    // elements that are assigned by the NCM), but use the other parameters from the proposed identifier, since these elements are determined by
                    // the node rather than the NCM.
                    return new NodeIdentifier(nodeId.getId(),
                        proposedNodeId.getApiAddress(), proposedNodeId.getApiPort(),
                        proposedNodeId.getSocketAddress(), proposedNodeId.getSocketPort(),
                        proposedNodeId.getSiteToSiteAddress(), proposedNodeId.getSiteToSitePort(), proposedNodeId.isSiteToSiteSecure());
                }

            }

            // proposal does not conflict with existing nodes - this is a new node. Assign a new Node Index to it
            return new NodeIdentifier(proposedNodeId.getId(), proposedNodeId.getApiAddress(), proposedNodeId.getApiPort(),
                proposedNodeId.getSocketAddress(), proposedNodeId.getSocketPort(),
                proposedNodeId.getSiteToSiteAddress(), proposedNodeId.getSiteToSitePort(), proposedNodeId.isSiteToSiteSecure());
        } finally {
            readLock.unlock("resolveProposedNodeIdentifier");
        }
    }

    private boolean canChangeNodeState(final String method, final URI uri) {
        return HttpMethod.DELETE.equalsIgnoreCase(method) || HttpMethod.POST.equalsIgnoreCase(method) || HttpMethod.PUT.equalsIgnoreCase(method);
    }

    private void notifyDataFlowManagementServiceOfNodeStatusChange() {
        writeLock.lock();
        try {
            // tell service about the currently connected nodes
            logger.debug("Notifying DataFlow Management Service of current set of connected nodes.");
            dataFlowManagementService.setNodeIds(getNodeIds(Status.CONNECTED));
        } finally {
            writeLock.unlock("notifyDataFlowManagementServiceOfNodeStatusChange");
        }
    }

    private void notifyDataFlowManagmentServiceOfFlowStateChange(final PersistedFlowState newState) {
        writeLock.lock();
        try {
            logger.debug("Notifying DataFlow Management Service that flow state is " + newState);
            dataFlowManagementService.setPersistedFlowState(newState);
            if (newState != PersistedFlowState.CURRENT) {
                cachedDataFlow = null;
                /* do not reset primary node ID  because only the data flow has changed */
            }
        } finally {
            writeLock.unlock("notifyDataFlowManagementServiceOfFlowStateChange");
        }
    }

    public NodeHeartbeat getLatestHeartbeat(final NodeIdentifier nodeId) {
        return heartbeatMonitor.getLatestHeartbeat(nodeId);
    }

    @Override
    public ClusterNodeInformation getNodeInformation() {
        readLock.lock();
        try {
            final Collection<NodeInformation> nodeInfos = new ArrayList<>();
            for (final Node node : getRawNodes(Status.CONNECTED)) {
                final NodeIdentifier id = node.getNodeId();

                final NodeHeartbeat nodeHeartbeat = heartbeatMonitor.getLatestHeartbeat(id);
                if (nodeHeartbeat == null) {
                    continue;
                }

                final Integer siteToSitePort = id.getSiteToSitePort();
                if (siteToSitePort == null) {
                    continue;
                }

                final int flowFileCount = nodeHeartbeat.getFlowFileCount();
                final NodeInformation nodeInfo = new NodeInformation(id.getSiteToSiteAddress(), siteToSitePort, id.getApiPort(),
                    id.isSiteToSiteSecure(), flowFileCount);
                nodeInfos.add(nodeInfo);
            }

            final ClusterNodeInformation clusterNodeInfo = new ClusterNodeInformation();
            clusterNodeInfo.setNodeInformation(nodeInfos);
            return clusterNodeInfo;
        } finally {
            readLock.unlock("getNodeInformation");
        }
    }

    @Override
    public BulletinRepository getBulletinRepository() {
        return bulletinRepository;
    }



    private static class ClusterManagerLock {

        private final Lock lock;
        private static final Logger logger = LoggerFactory.getLogger("cluster.lock");
        private long lockTime;
        private final String name;

        public ClusterManagerLock(final Lock lock, final String name) {
            this.lock = lock;
            this.name = name;
        }

        @SuppressWarnings("unused")
        public boolean tryLock() {
            logger.trace("Trying to obtain Cluster Manager Lock: {}", name);
            final boolean success = lock.tryLock();
            if (!success) {
                logger.trace("TryLock failed for Cluster Manager Lock: {}", name);
                return false;
            }
            logger.trace("TryLock successful");
            return true;
        }

        public boolean tryLock(final long timeout, final TimeUnit timeUnit) {
            logger.trace("Trying to obtain Cluster Manager Lock {} with a timeout of {} {}", name, timeout, timeUnit);
            final boolean success;
            try {
                success = lock.tryLock(timeout, timeUnit);
            } catch (final InterruptedException ie) {
                return false;
            }

            if (!success) {
                logger.trace("TryLock failed for Cluster Manager Lock {} with a timeout of {} {}", name, timeout, timeUnit);
                return false;
            }
            logger.trace("TryLock successful");
            return true;
        }

        public void lock() {
            logger.trace("Obtaining Cluster Manager Lock {}", name);
            lock.lock();
            lockTime = System.nanoTime();
            logger.trace("Obtained Cluster Manager Lock {}", name);
        }

        public void unlock(final String task) {
            logger.trace("Releasing Cluster Manager Lock {}", name);
            final long nanosLocked = System.nanoTime() - lockTime;
            lock.unlock();
            logger.trace("Released Cluster Manager Lock {}", name);

            final long millisLocked = TimeUnit.MILLISECONDS.convert(nanosLocked, TimeUnit.NANOSECONDS);
            if (millisLocked > 100L) {
                logger.debug("Cluster Manager Lock {} held for {} milliseconds for task: {}", name, millisLocked, task);
            }
        }
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType, String groupId) {
        return controllerServiceProvider.getControllerServiceIdentifiers(serviceType, groupId);
    }

    public void reportEvent(final NodeIdentifier nodeId, final Severity severity, final String message) {
        bulletinRepository.addBulletin(BulletinFactory.createBulletin(nodeId == null ? "Cluster" : nodeId.getId(), severity.name(), message));
        if (nodeId != null) {
            addEvent(nodeId, message);
        }
    }
}
