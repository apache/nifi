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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.annotation.lifecycle.OnConfigurationRestored;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.heartbeat.HeartbeatMonitor;
import org.apache.nifi.cluster.coordination.node.ClusterRoles;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.Heartbeat;
import org.apache.nifi.cluster.protocol.HeartbeatPayload;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.NodeProtocolSender;
import org.apache.nifi.cluster.protocol.UnknownServiceAddressException;
import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.StandardValidationTrigger;
import org.apache.nifi.components.validation.TriggerValidationTask;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.StandardConnection;
import org.apache.nifi.controller.cluster.ClusterProtocolHeartbeater;
import org.apache.nifi.controller.cluster.Heartbeater;
import org.apache.nifi.controller.exception.CommunicationsException;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.flow.StandardFlowManager;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.controller.leader.election.LeaderElectionStateChangeListener;
import org.apache.nifi.controller.queue.ConnectionEventListener;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.FlowFileQueueFactory;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.queue.StandardFlowFileQueue;
import org.apache.nifi.controller.queue.clustered.ContentRepositoryFlowFileAccess;
import org.apache.nifi.controller.queue.clustered.SocketLoadBalancedFlowFileQueue;
import org.apache.nifi.controller.queue.clustered.client.StandardLoadBalanceFlowFileCodec;
import org.apache.nifi.controller.queue.clustered.client.async.nio.NioAsyncLoadBalanceClientFactory;
import org.apache.nifi.controller.queue.clustered.client.async.nio.NioAsyncLoadBalanceClientRegistry;
import org.apache.nifi.controller.queue.clustered.client.async.nio.NioAsyncLoadBalanceClientTask;
import org.apache.nifi.controller.queue.clustered.server.ClusterLoadBalanceAuthorizer;
import org.apache.nifi.controller.queue.clustered.server.ConnectionLoadBalanceServer;
import org.apache.nifi.controller.queue.clustered.server.LoadBalanceAuthorizer;
import org.apache.nifi.controller.queue.clustered.server.LoadBalanceProtocol;
import org.apache.nifi.controller.queue.clustered.server.StandardLoadBalanceProtocol;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.reporting.ReportingTaskProvider;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.CounterRepository;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.StandardCounterRepository;
import org.apache.nifi.controller.repository.StandardFlowFileRecord;
import org.apache.nifi.controller.repository.StandardQueueProvider;
import org.apache.nifi.controller.repository.StandardRepositoryRecord;
import org.apache.nifi.controller.repository.SwapManagerInitializationContext;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ContentDirection;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.controller.repository.io.LimitedInputStream;
import org.apache.nifi.controller.scheduling.EventDrivenSchedulingAgent;
import org.apache.nifi.controller.scheduling.QuartzSchedulingAgent;
import org.apache.nifi.controller.scheduling.RepositoryContextFactory;
import org.apache.nifi.controller.scheduling.StandardProcessScheduler;
import org.apache.nifi.controller.scheduling.TimerDrivenSchedulingAgent;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.serialization.FlowSerializer;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;
import org.apache.nifi.controller.serialization.FlowSynchronizer;
import org.apache.nifi.controller.serialization.ScheduledStateLookup;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.controller.service.StandardControllerServiceProvider;
import org.apache.nifi.controller.state.manager.StandardStateManagerProvider;
import org.apache.nifi.controller.state.server.ZooKeeperStateServer;
import org.apache.nifi.controller.status.history.ComponentStatusRepository;
import org.apache.nifi.controller.status.history.GarbageCollectionHistory;
import org.apache.nifi.controller.status.history.GarbageCollectionStatus;
import org.apache.nifi.controller.status.history.StandardGarbageCollectionStatus;
import org.apache.nifi.controller.status.history.StatusHistoryUtil;
import org.apache.nifi.controller.tasks.ExpireFlowFiles;
import org.apache.nifi.diagnostics.SystemDiagnostics;
import org.apache.nifi.diagnostics.SystemDiagnosticsFactory;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.framework.security.util.SslContextFactory;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.StandardProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ComponentIdentifierLookup;
import org.apache.nifi.provenance.IdentifierLookup;
import org.apache.nifi.provenance.ProvenanceAuthorizableFactory;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.provenance.StandardProvenanceAuthorizableFactory;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.VersionedConnection;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.variable.MutableVariableRegistry;
import org.apache.nifi.remote.HttpRemoteSiteListener;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.RemoteResourceManager;
import org.apache.nifi.remote.RemoteSiteListener;
import org.apache.nifi.remote.SocketRemoteSiteListener;
import org.apache.nifi.remote.cluster.NodeInformant;
import org.apache.nifi.remote.protocol.socket.SocketFlowFileServerProtocol;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.reporting.StandardEventAccess;
import org.apache.nifi.reporting.UserAwareEventAccess;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.stream.io.LimitingInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.ComponentIdGenerator;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.util.concurrency.TimedLock;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class FlowController implements ReportingTaskProvider, Authorizable, NodeTypeProvider {

    // default repository implementations
    public static final String DEFAULT_FLOWFILE_REPO_IMPLEMENTATION = "org.apache.nifi.controller.repository.WriteAheadFlowFileRepository";
    public static final String DEFAULT_CONTENT_REPO_IMPLEMENTATION = "org.apache.nifi.controller.repository.FileSystemRepository";
    public static final String DEFAULT_PROVENANCE_REPO_IMPLEMENTATION = "org.apache.nifi.provenance.VolatileProvenanceRepository";
    public static final String DEFAULT_SWAP_MANAGER_IMPLEMENTATION = "org.apache.nifi.controller.FileSystemSwapManager";
    public static final String DEFAULT_COMPONENT_STATUS_REPO_IMPLEMENTATION = "org.apache.nifi.controller.status.history.VolatileComponentStatusRepository";

    public static final String SCHEDULE_MINIMUM_NANOSECONDS = "flowcontroller.minimum.nanoseconds";
    public static final String GRACEFUL_SHUTDOWN_PERIOD = "nifi.flowcontroller.graceful.shutdown.seconds";
    public static final long DEFAULT_GRACEFUL_SHUTDOWN_SECONDS = 10;
    public static final int METRICS_RESERVOIR_SIZE = 288; // 1 day worth of 5-minute captures


    // default properties for scaling the positions of components from pre-1.0 flow encoding versions.
    public static final double DEFAULT_POSITION_SCALE_FACTOR_X = 1.5;
    public static final double DEFAULT_POSITION_SCALE_FACTOR_Y = 1.34;

    private final AtomicInteger maxTimerDrivenThreads;
    private final AtomicInteger maxEventDrivenThreads;
    private final AtomicReference<FlowEngine> timerDrivenEngineRef;
    private final AtomicReference<FlowEngine> eventDrivenEngineRef;
    private final EventDrivenSchedulingAgent eventDrivenSchedulingAgent;

    private final ContentRepository contentRepository;
    private final FlowFileRepository flowFileRepository;
    private final FlowFileEventRepository flowFileEventRepository;
    private final ProvenanceRepository provenanceRepository;
    private final BulletinRepository bulletinRepository;
    private final StandardProcessScheduler processScheduler;
    private final SnippetManager snippetManager;
    private final long gracefulShutdownSeconds;
    private final ExtensionManager extensionManager;
    private final NiFiProperties nifiProperties;
    private final SSLContext sslContext;
    private final Set<RemoteSiteListener> externalSiteListeners = new HashSet<>();
    private final AtomicReference<CounterRepository> counterRepositoryRef;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean flowSynchronized = new AtomicBoolean(false);
    private final StandardControllerServiceProvider controllerServiceProvider;
    private final Authorizer authorizer;
    private final AuditService auditService;
    private final EventDrivenWorkerQueue eventDrivenWorkerQueue;
    private final ComponentStatusRepository componentStatusRepository;
    private final StateManagerProvider stateManagerProvider;
    private final long systemStartTime = System.currentTimeMillis(); // time at which the node was started
    private final VariableRegistry variableRegistry;

    private final ConnectionLoadBalanceServer loadBalanceServer;
    private final NioAsyncLoadBalanceClientRegistry loadBalanceClientRegistry;
    private final FlowEngine loadBalanceClientThreadPool;
    private final Set<NioAsyncLoadBalanceClientTask> loadBalanceClientTasks = new HashSet<>();

    private final ConcurrentMap<String, ProcessorNode> allProcessors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ProcessGroup> allProcessGroups = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Connection> allConnections = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Port> allInputPorts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Port> allOutputPorts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Funnel> allFunnels = new ConcurrentHashMap<>();

    private volatile ZooKeeperStateServer zooKeeperStateServer;

    // The Heartbeat Bean is used to provide an Atomic Reference to data that is used in heartbeats that may
    // change while the instance is running. We do this because we want to generate heartbeats even if we
    // are unable to obtain a read lock on the entire FlowController.
    private final AtomicReference<HeartbeatBean> heartbeatBeanRef = new AtomicReference<>();
    private final AtomicBoolean heartbeatsSuspended = new AtomicBoolean(false);

    private final Integer remoteInputSocketPort;
    private final Integer remoteInputHttpPort;
    private final Boolean isSiteToSiteSecure;

    private final List<Connectable> startConnectablesAfterInitialization;
    private final List<RemoteGroupPort> startRemoteGroupPortsAfterInitialization;
    private final LeaderElectionManager leaderElectionManager;
    private final ClusterCoordinator clusterCoordinator;
    private final FlowRegistryClient flowRegistryClient;
    private final FlowEngine validationThreadPool;
    private final ValidationTrigger validationTrigger;
    private final ReloadComponent reloadComponent;
    private final ProvenanceAuthorizableFactory provenanceAuthorizableFactory;
    private final UserAwareEventAccess eventAccess;
    private final StandardFlowManager flowManager;

    /**
     * true if controller is configured to operate in a clustered environment
     */
    private final boolean configuredForClustering;

    /**
     * the time to wait between heartbeats
     */
    private final int heartbeatDelaySeconds;

    /**
     * The sensitive property string encryptor *
     */
    private final StringEncryptor encryptor;

    private final ScheduledExecutorService clusterTaskExecutor = new FlowEngine(3, "Clustering Tasks", true);
    private final ResourceClaimManager resourceClaimManager = new StandardResourceClaimManager();

    // guarded by rwLock
    /**
     * timer to periodically send heartbeats to the cluster
     */
    private ScheduledFuture<?> heartbeatSenderFuture;
    private final Heartbeater heartbeater;
    private final HeartbeatMonitor heartbeatMonitor;

    // guarded by FlowController lock
    /**
     * timer task to generate heartbeats
     */
    private final AtomicReference<HeartbeatSendTask> heartbeatSendTask = new AtomicReference<>(null);

    // guarded by rwLock
    /**
     * the node identifier;
     */
    private volatile NodeIdentifier nodeId;

    // guarded by rwLock
    /**
     * true if controller is connected or trying to connect to the cluster
     */
    private boolean clustered;

    // guarded by rwLock
    private NodeConnectionStatus connectionStatus;

    // guarded by rwLock
    private String instanceId;

    private volatile boolean shutdown = false;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final TimedLock readLock = new TimedLock(rwLock.readLock(), "FlowControllerReadLock", 1);
    private final TimedLock writeLock = new TimedLock(rwLock.writeLock(), "FlowControllerWriteLock", 1);

    private static final Logger LOG = LoggerFactory.getLogger(FlowController.class);

    public static FlowController createStandaloneInstance(
            final FlowFileEventRepository flowFileEventRepo,
            final NiFiProperties properties,
            final Authorizer authorizer,
            final AuditService auditService,
            final StringEncryptor encryptor,
            final BulletinRepository bulletinRepo,
            final VariableRegistry variableRegistry,
            final FlowRegistryClient flowRegistryClient,
            final ExtensionManager extensionManager) {

        return new FlowController(
                flowFileEventRepo,
                properties,
                authorizer,
                auditService,
                encryptor,
                /* configuredForClustering */ false,
                /* NodeProtocolSender */ null,
                bulletinRepo,
                /* cluster coordinator */ null,
                /* heartbeat monitor */ null,
                /* leader election manager */ null,
                /* variable registry */ variableRegistry,
                flowRegistryClient,
                extensionManager);
    }

    public static FlowController createClusteredInstance(
            final FlowFileEventRepository flowFileEventRepo,
            final NiFiProperties properties,
            final Authorizer authorizer,
            final AuditService auditService,
            final StringEncryptor encryptor,
            final NodeProtocolSender protocolSender,
            final BulletinRepository bulletinRepo,
            final ClusterCoordinator clusterCoordinator,
            final HeartbeatMonitor heartbeatMonitor,
            final LeaderElectionManager leaderElectionManager,
            final VariableRegistry variableRegistry,
            final FlowRegistryClient flowRegistryClient,
            final ExtensionManager extensionManager) {

        final FlowController flowController = new FlowController(
                flowFileEventRepo,
                properties,
                authorizer,
                auditService,
                encryptor,
                /* configuredForClustering */ true,
                protocolSender,
                bulletinRepo,
                clusterCoordinator,
                heartbeatMonitor,
                leaderElectionManager,
                variableRegistry,
                flowRegistryClient,
                extensionManager);

        return flowController;
    }

    @SuppressWarnings("deprecation")
    private FlowController(
            final FlowFileEventRepository flowFileEventRepo,
            final NiFiProperties nifiProperties,
            final Authorizer authorizer,
            final AuditService auditService,
            final StringEncryptor encryptor,
            final boolean configuredForClustering,
            final NodeProtocolSender protocolSender,
            final BulletinRepository bulletinRepo,
            final ClusterCoordinator clusterCoordinator,
            final HeartbeatMonitor heartbeatMonitor,
            final LeaderElectionManager leaderElectionManager,
            final VariableRegistry variableRegistry,
            final FlowRegistryClient flowRegistryClient,
            final ExtensionManager extensionManager) {

        maxTimerDrivenThreads = new AtomicInteger(10);
        maxEventDrivenThreads = new AtomicInteger(5);

        this.encryptor = encryptor;
        this.nifiProperties = nifiProperties;
        this.heartbeatMonitor = heartbeatMonitor;
        this.leaderElectionManager = leaderElectionManager;
        this.sslContext = SslContextFactory.createSslContext(nifiProperties);
        this.extensionManager = extensionManager;
        this.clusterCoordinator = clusterCoordinator;
        this.authorizer = authorizer;
        this.auditService = auditService;
        this.configuredForClustering = configuredForClustering;
        this.flowRegistryClient = flowRegistryClient;

        timerDrivenEngineRef = new AtomicReference<>(new FlowEngine(maxTimerDrivenThreads.get(), "Timer-Driven Process"));
        eventDrivenEngineRef = new AtomicReference<>(new FlowEngine(maxEventDrivenThreads.get(), "Event-Driven Process"));

        final FlowFileRepository flowFileRepo = createFlowFileRepository(nifiProperties, extensionManager, resourceClaimManager);
        flowFileRepository = flowFileRepo;
        flowFileEventRepository = flowFileEventRepo;
        counterRepositoryRef = new AtomicReference<>(new StandardCounterRepository());

        bulletinRepository = bulletinRepo;
        this.variableRegistry = variableRegistry == null ? VariableRegistry.EMPTY_REGISTRY : variableRegistry;

        try {
            this.provenanceAuthorizableFactory = new StandardProvenanceAuthorizableFactory(this);
            this.provenanceRepository = createProvenanceRepository(nifiProperties);

            final IdentifierLookup identifierLookup = new ComponentIdentifierLookup(this);

            this.provenanceRepository.initialize(createEventReporter(), authorizer, provenanceAuthorizableFactory, identifierLookup);
        } catch (final Exception e) {
            throw new RuntimeException("Unable to create Provenance Repository", e);
        }

        try {
            this.contentRepository = createContentRepository(nifiProperties);
        } catch (final Exception e) {
            throw new RuntimeException("Unable to create Content Repository", e);
        }

        try {
            this.stateManagerProvider = StandardStateManagerProvider.create(nifiProperties, this.variableRegistry, extensionManager);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        processScheduler = new StandardProcessScheduler(timerDrivenEngineRef.get(), this, encryptor, stateManagerProvider, this.nifiProperties);
        eventDrivenWorkerQueue = new EventDrivenWorkerQueue(false, false, processScheduler);

        final RepositoryContextFactory contextFactory = new RepositoryContextFactory(contentRepository, flowFileRepository, flowFileEventRepository, counterRepositoryRef.get(), provenanceRepository);

        this.flowManager = new StandardFlowManager(nifiProperties, sslContext, this, flowFileEventRepository);

        controllerServiceProvider = new StandardControllerServiceProvider(this, processScheduler, bulletinRepository);

        eventDrivenSchedulingAgent = new EventDrivenSchedulingAgent(
            eventDrivenEngineRef.get(), controllerServiceProvider, stateManagerProvider, eventDrivenWorkerQueue, contextFactory, maxEventDrivenThreads.get(), encryptor, extensionManager);
        processScheduler.setSchedulingAgent(SchedulingStrategy.EVENT_DRIVEN, eventDrivenSchedulingAgent);

        final QuartzSchedulingAgent quartzSchedulingAgent = new QuartzSchedulingAgent(this, timerDrivenEngineRef.get(), contextFactory, encryptor);
        final TimerDrivenSchedulingAgent timerDrivenAgent = new TimerDrivenSchedulingAgent(this, timerDrivenEngineRef.get(), contextFactory, encryptor, this.nifiProperties);
        processScheduler.setSchedulingAgent(SchedulingStrategy.TIMER_DRIVEN, timerDrivenAgent);
        // PRIMARY_NODE_ONLY is deprecated, but still exists to handle processors that are still defined with it (they haven't been re-configured with executeNode = PRIMARY).
        processScheduler.setSchedulingAgent(SchedulingStrategy.PRIMARY_NODE_ONLY, timerDrivenAgent);
        processScheduler.setSchedulingAgent(SchedulingStrategy.CRON_DRIVEN, quartzSchedulingAgent);

        startConnectablesAfterInitialization = new ArrayList<>();
        startRemoteGroupPortsAfterInitialization = new ArrayList<>();

        final String gracefulShutdownSecondsVal = nifiProperties.getProperty(GRACEFUL_SHUTDOWN_PERIOD);
        long shutdownSecs;
        try {
            shutdownSecs = Long.parseLong(gracefulShutdownSecondsVal);
            if (shutdownSecs < 1) {
                shutdownSecs = DEFAULT_GRACEFUL_SHUTDOWN_SECONDS;
            }
        } catch (final NumberFormatException nfe) {
            shutdownSecs = DEFAULT_GRACEFUL_SHUTDOWN_SECONDS;
        }
        gracefulShutdownSeconds = shutdownSecs;

        remoteInputSocketPort = nifiProperties.getRemoteInputPort();
        remoteInputHttpPort = nifiProperties.getRemoteInputHttpPort();
        isSiteToSiteSecure = nifiProperties.isSiteToSiteSecure();

        if (isSiteToSiteSecure && sslContext == null && remoteInputSocketPort != null) {
            throw new IllegalStateException("NiFi Configured to allow Secure Site-to-Site communications but the Keystore/Truststore properties are not configured");
        }

        this.heartbeatDelaySeconds = (int) FormatUtils.getTimeDuration(nifiProperties.getNodeHeartbeatInterval(), TimeUnit.SECONDS);

        this.snippetManager = new SnippetManager();
        this.reloadComponent = new StandardReloadComponent(this);

        final ProcessGroup rootGroup = new StandardProcessGroup(ComponentIdGenerator.generateId().toString(), controllerServiceProvider, processScheduler,
            nifiProperties, encryptor, this, new MutableVariableRegistry(this.variableRegistry));
        rootGroup.setName(FlowManager.DEFAULT_ROOT_GROUP_NAME);
        setRootGroup(rootGroup);
        instanceId = ComponentIdGenerator.generateId().toString();

        this.validationThreadPool = new FlowEngine(5, "Validate Components", true);
        this.validationTrigger = new StandardValidationTrigger(validationThreadPool, this::isInitialized);

        if (remoteInputSocketPort == null) {
            LOG.info("Not enabling RAW Socket Site-to-Site functionality because nifi.remote.input.socket.port is not set");
        } else if (isSiteToSiteSecure && sslContext == null) {
            LOG.error("Unable to create Secure Site-to-Site Listener because not all required Keystore/Truststore "
                    + "Properties are set. Site-to-Site functionality will be disabled until this problem is has been fixed.");
        } else {
            // Register the SocketFlowFileServerProtocol as the appropriate resource for site-to-site Server Protocol
            RemoteResourceManager.setServerProtocolImplementation(SocketFlowFileServerProtocol.RESOURCE_NAME, SocketFlowFileServerProtocol.class);

            final NodeInformant nodeInformant = configuredForClustering ? new ClusterCoordinatorNodeInformant(clusterCoordinator) : null;
            externalSiteListeners.add(new SocketRemoteSiteListener(remoteInputSocketPort, isSiteToSiteSecure ? sslContext : null, nifiProperties, nodeInformant));
        }

        if (remoteInputHttpPort == null) {
            LOG.info("Not enabling HTTP(S) Site-to-Site functionality because the '" + NiFiProperties.SITE_TO_SITE_HTTP_ENABLED + "' property is not true");
        } else {
            externalSiteListeners.add(HttpRemoteSiteListener.getInstance(nifiProperties));
        }

        for (final RemoteSiteListener listener : externalSiteListeners) {
            listener.setRootGroup(rootGroup);
        }

        // Determine frequency for obtaining component status snapshots
        final String snapshotFrequency = nifiProperties.getProperty(NiFiProperties.COMPONENT_STATUS_SNAPSHOT_FREQUENCY, NiFiProperties.DEFAULT_COMPONENT_STATUS_SNAPSHOT_FREQUENCY);
        long snapshotMillis;
        try {
            snapshotMillis = FormatUtils.getTimeDuration(snapshotFrequency, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            snapshotMillis = FormatUtils.getTimeDuration(NiFiProperties.DEFAULT_COMPONENT_STATUS_SNAPSHOT_FREQUENCY, TimeUnit.MILLISECONDS);
        }

        // Initialize the Embedded ZooKeeper server, if applicable
        if (nifiProperties.isStartEmbeddedZooKeeper() && configuredForClustering) {
            try {
                zooKeeperStateServer = ZooKeeperStateServer.create(nifiProperties);
                zooKeeperStateServer.start();
            } catch (final IOException | ConfigException e) {
                throw new IllegalStateException("Unable to initailize Flow because NiFi was configured to start an Embedded Zookeeper server but failed to do so", e);
            }
        } else {
            zooKeeperStateServer = null;
        }

        eventAccess = new StandardEventAccess(this, flowFileEventRepository);
        componentStatusRepository = createComponentStatusRepository();
        timerDrivenEngineRef.get().scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    componentStatusRepository.capture(eventAccess.getControllerStatus(), getGarbageCollectionStatus());
                } catch (final Exception e) {
                    LOG.error("Failed to capture component stats for Stats History", e);
                }
            }
        }, snapshotMillis, snapshotMillis, TimeUnit.MILLISECONDS);

        this.connectionStatus = new NodeConnectionStatus(nodeId, DisconnectionCode.NOT_YET_CONNECTED);
        heartbeatBeanRef.set(new HeartbeatBean(rootGroup, false));

        if (configuredForClustering) {
            heartbeater = new ClusterProtocolHeartbeater(protocolSender, clusterCoordinator, leaderElectionManager);

            // Check if there is already a cluster coordinator elected. If not, go ahead
            // and register for coordinator role. If there is already one elected, do not register until
            // we have connected to the cluster. This allows us to avoid becoming the coordinator with a
            // flow that is different from the rest of the cluster (especially an empty flow) and then
            // kicking everyone out. This way, we instead inherit the cluster flow before we attempt to be
            // the coordinator.
            LOG.info("Checking if there is already a Cluster Coordinator Elected...");
            final String clusterCoordinatorAddress = leaderElectionManager.getLeader(ClusterRoles.CLUSTER_COORDINATOR);
            if (StringUtils.isEmpty(clusterCoordinatorAddress)) {
                LOG.info("It appears that no Cluster Coordinator has been Elected yet. Registering for Cluster Coordinator Role.");
                registerForClusterCoordinator(true);
            } else {
                // At this point, we have determined that there is a Cluster Coordinator elected. It is important to note, though,
                // that if we are running an embedded ZooKeeper, and we have just restarted the cluster (at least the nodes that run the
                // embedded ZooKeeper), that we could possibly determine that the Cluster Coordinator is at an address that is not really
                // valid. This is because the latest stable ZooKeeper does not support "Container ZNodes" and as a result the ZNodes that
                // are created are persistent, not ephemeral. Upon restart, we can get this persisted value, even though the node that belongs
                // to that address has not started. ZooKeeper/Curator will recognize this after a while and delete the ZNode. As a result,
                // we may later determine that there is in fact no Cluster Coordinator. If this happens, we will automatically register for
                // Cluster Coordinator through the StandardFlowService.
                LOG.info("The Election for Cluster Coordinator has already begun (Leader is {}). Will not register to be elected for this role until after connecting "
                    + "to the cluster and inheriting the cluster's flow.", clusterCoordinatorAddress);
                registerForClusterCoordinator(false);
            }

            leaderElectionManager.start();
            heartbeatMonitor.start();

            final InetSocketAddress loadBalanceAddress = nifiProperties.getClusterLoadBalanceAddress();
            // Setup Load Balancing Server
            final EventReporter eventReporter = createEventReporter();

            final LoadBalanceAuthorizer authorizeConnection = new ClusterLoadBalanceAuthorizer(clusterCoordinator, eventReporter);
            final LoadBalanceProtocol loadBalanceProtocol = new StandardLoadBalanceProtocol(flowFileRepo, contentRepository, provenanceRepository, this, authorizeConnection);

            final int numThreads = nifiProperties.getIntegerProperty(NiFiProperties.LOAD_BALANCE_MAX_THREAD_COUNT, NiFiProperties.DEFAULT_LOAD_BALANCE_MAX_THREAD_COUNT);
            final String timeoutPeriod = nifiProperties.getProperty(NiFiProperties.LOAD_BALANCE_COMMS_TIMEOUT, NiFiProperties.DEFAULT_LOAD_BALANCE_COMMS_TIMEOUT);
            final int timeoutMillis = (int) FormatUtils.getTimeDuration(timeoutPeriod, TimeUnit.MILLISECONDS);

            loadBalanceServer = new ConnectionLoadBalanceServer(loadBalanceAddress.getHostName(), loadBalanceAddress.getPort(), sslContext,
                    numThreads, loadBalanceProtocol, eventReporter, timeoutMillis);


            final int connectionsPerNode = nifiProperties.getIntegerProperty(NiFiProperties.LOAD_BALANCE_CONNECTIONS_PER_NODE, NiFiProperties.DEFAULT_LOAD_BALANCE_CONNECTIONS_PER_NODE);
            final NioAsyncLoadBalanceClientFactory asyncClientFactory = new NioAsyncLoadBalanceClientFactory(sslContext, timeoutMillis, new ContentRepositoryFlowFileAccess(contentRepository),
                eventReporter, new StandardLoadBalanceFlowFileCodec());
            loadBalanceClientRegistry = new NioAsyncLoadBalanceClientRegistry(asyncClientFactory, connectionsPerNode);

            final int loadBalanceClientThreadCount = nifiProperties.getIntegerProperty(NiFiProperties.LOAD_BALANCE_MAX_THREAD_COUNT, NiFiProperties.DEFAULT_LOAD_BALANCE_MAX_THREAD_COUNT);
            loadBalanceClientThreadPool = new FlowEngine(loadBalanceClientThreadCount, "Load-Balanced Client", true);

            for (int i=0; i < loadBalanceClientThreadCount; i++) {
                final NioAsyncLoadBalanceClientTask clientTask = new NioAsyncLoadBalanceClientTask(loadBalanceClientRegistry, clusterCoordinator, eventReporter);
                loadBalanceClientTasks.add(clientTask);
                loadBalanceClientThreadPool.submit(clientTask);
            }
        } else {
            loadBalanceClientRegistry = null;
            heartbeater = null;
            loadBalanceServer = null;
            loadBalanceClientThreadPool = null;
        }
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return null;
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getControllerResource();
    }

    private static FlowFileRepository createFlowFileRepository(final NiFiProperties properties, final ExtensionManager extensionManager, final ResourceClaimManager contentClaimManager) {
        final String implementationClassName = properties.getProperty(NiFiProperties.FLOWFILE_REPOSITORY_IMPLEMENTATION, DEFAULT_FLOWFILE_REPO_IMPLEMENTATION);
        if (implementationClassName == null) {
            throw new RuntimeException("Cannot create FlowFile Repository because the NiFi Properties is missing the following property: "
                    + NiFiProperties.FLOWFILE_REPOSITORY_IMPLEMENTATION);
        }

        try {
            final FlowFileRepository created = NarThreadContextClassLoader.createInstance(extensionManager, implementationClassName, FlowFileRepository.class, properties);
            synchronized (created) {
                created.initialize(contentClaimManager);
            }
            return created;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public FlowFileSwapManager createSwapManager() {
        final String implementationClassName = nifiProperties.getProperty(NiFiProperties.FLOWFILE_SWAP_MANAGER_IMPLEMENTATION, DEFAULT_SWAP_MANAGER_IMPLEMENTATION);
        if (implementationClassName == null) {
            return null;
        }

        try {
            final FlowFileSwapManager swapManager = NarThreadContextClassLoader.createInstance(extensionManager, implementationClassName, FlowFileSwapManager.class, nifiProperties);

            final EventReporter eventReporter = createEventReporter();
            try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                final SwapManagerInitializationContext initializationContext = new SwapManagerInitializationContext() {
                    @Override
                    public ResourceClaimManager getResourceClaimManager() {
                        return resourceClaimManager;
                    }

                    @Override
                    public FlowFileRepository getFlowFileRepository() {
                        return flowFileRepository;
                    }

                    @Override
                    public EventReporter getEventReporter() {
                        return eventReporter;
                    }
                };

                swapManager.initialize(initializationContext);
            }

            return swapManager;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public EventReporter createEventReporter() {
        return new EventReporter() {
            private static final long serialVersionUID = 1L;

            @Override
            public void reportEvent(final Severity severity, final String category, final String message) {
                final Bulletin bulletin = BulletinFactory.createBulletin(category, severity.name(), message);
                bulletinRepository.addBulletin(bulletin);
            }
        };
    }

    public void initializeFlow() throws IOException {
        writeLock.lock();
        try {
            // get all connections/queues and recover from swap files.
            final List<Connection> connections = flowManager.getRootGroup().findAllConnections();

            long maxIdFromSwapFiles = -1L;
            if (flowFileRepository.isVolatile()) {
                for (final Connection connection : connections) {
                    final FlowFileQueue queue = connection.getFlowFileQueue();
                    queue.purgeSwapFiles();
                }
            } else {
                for (final Connection connection : connections) {
                    final FlowFileQueue queue = connection.getFlowFileQueue();
                    final SwapSummary swapSummary = queue.recoverSwappedFlowFiles();
                    if (swapSummary != null) {
                        final Long maxFlowFileId = swapSummary.getMaxFlowFileId();
                        if (maxFlowFileId != null && maxFlowFileId > maxIdFromSwapFiles) {
                            maxIdFromSwapFiles = maxFlowFileId;
                        }

                        for (final ResourceClaim resourceClaim : swapSummary.getResourceClaims()) {
                            resourceClaimManager.incrementClaimantCount(resourceClaim);
                        }
                    }
                }
            }

            flowFileRepository.loadFlowFiles(new StandardQueueProvider(this), maxIdFromSwapFiles + 1);

            // Begin expiring FlowFiles that are old
            final RepositoryContextFactory contextFactory = new RepositoryContextFactory(contentRepository, flowFileRepository,
                flowFileEventRepository, counterRepositoryRef.get(), provenanceRepository);
            processScheduler.scheduleFrameworkTask(new ExpireFlowFiles(this, contextFactory), "Expire FlowFiles", 30L, 30L, TimeUnit.SECONDS);

            // now that we've loaded the FlowFiles, this has restored our ContentClaims' states, so we can tell the
            // ContentRepository to purge superfluous files
            contentRepository.cleanup();

            for (final RemoteSiteListener listener : externalSiteListeners) {
                listener.start();
            }

            if (loadBalanceServer != null) {
                loadBalanceServer.start();
            }

            notifyComponentsConfigurationRestored();

            timerDrivenEngineRef.get().scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        updateRemoteProcessGroups();
                    } catch (final Throwable t) {
                        LOG.warn("Unable to update Remote Process Groups due to " + t);
                        if (LOG.isDebugEnabled()) {
                            LOG.warn("", t);
                        }
                    }
                }
            }, 0L, 30L, TimeUnit.SECONDS);

            timerDrivenEngineRef.get().scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    final ProcessGroup rootGroup = flowManager.getRootGroup();
                    final List<ProcessGroup> allGroups = rootGroup.findAllProcessGroups();
                    allGroups.add(rootGroup);

                    for (final ProcessGroup group : allGroups) {
                        try {
                            group.synchronizeWithFlowRegistry(flowRegistryClient);
                        } catch (final Exception e) {
                            LOG.error("Failed to synchronize {} with Flow Registry", group, e);
                        }
                    }
                }
            }, 5, 60, TimeUnit.SECONDS);

            initialized.set(true);
        } finally {
            writeLock.unlock("initializeFlow");
        }
    }

    private void notifyComponentsConfigurationRestored() {
        for (final ProcessorNode procNode : flowManager.getRootGroup().findAllProcessors()) {
            final Processor processor = procNode.getProcessor();
            try (final NarCloseable nc = NarCloseable.withComponentNarLoader(extensionManager, processor.getClass(), processor.getIdentifier())) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, processor);
            }
        }

        for (final ControllerServiceNode serviceNode : flowManager.getAllControllerServices()) {
            final ControllerService service = serviceNode.getControllerServiceImplementation();

            try (final NarCloseable nc = NarCloseable.withComponentNarLoader(extensionManager, service.getClass(), service.getIdentifier())) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, service);
            }
        }

        for (final ReportingTaskNode taskNode : getAllReportingTasks()) {
            final ReportingTask task = taskNode.getReportingTask();

            try (final NarCloseable nc = NarCloseable.withComponentNarLoader(extensionManager, task.getClass(), task.getIdentifier())) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, task);
            }
        }
    }

    /**
     * <p>
     * Causes any processors that were added to the flow with a 'delayStart'
     * flag of true to now start
     * </p>
     *
     * @param startDelayedComponents true if start
     */
    public void onFlowInitialized(final boolean startDelayedComponents) {
        writeLock.lock();
        try {
            // Perform validation of all components before attempting to start them.
            LOG.debug("Triggering initial validation of all components");
            final long start = System.nanoTime();

            final ValidationTrigger triggerIfValidating = new ValidationTrigger() {
                @Override
                public void triggerAsync(final ComponentNode component) {
                    final ValidationStatus status = component.getValidationStatus();

                    if (component.getValidationStatus() == ValidationStatus.VALIDATING) {
                        LOG.debug("Will trigger async validation for {} because its status is VALIDATING", component);
                        validationTrigger.triggerAsync(component);
                    } else {
                        LOG.debug("Will not trigger async validation for {} because its status is {}", component, status);
                    }
                }

                @Override
                public void trigger(final ComponentNode component) {
                    final ValidationStatus status = component.getValidationStatus();

                    if (component.getValidationStatus() == ValidationStatus.VALIDATING) {
                        LOG.debug("Will trigger immediate validation for {} because its status is VALIDATING", component);
                        validationTrigger.trigger(component);
                    } else {
                        LOG.debug("Will not trigger immediate validation for {} because its status is {}", component, status);
                    }
                }
            };

            new TriggerValidationTask(flowManager, triggerIfValidating).run();

            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            LOG.info("Performed initial validation of all components in {} milliseconds", millis);

            // Trigger component validation to occur every 5 seconds.
            validationThreadPool.scheduleWithFixedDelay(new TriggerValidationTask(flowManager, validationTrigger), 5, 5, TimeUnit.SECONDS);

            if (startDelayedComponents) {
                LOG.info("Starting {} processors/ports/funnels", startConnectablesAfterInitialization.size() + startRemoteGroupPortsAfterInitialization.size());
                for (final Connectable connectable : startConnectablesAfterInitialization) {
                    if (connectable.getScheduledState() == ScheduledState.DISABLED) {
                        continue;
                    }

                    try {
                        if (connectable instanceof ProcessorNode) {
                            connectable.getProcessGroup().startProcessor((ProcessorNode) connectable, true);
                        } else {
                            startConnectable(connectable);
                        }
                    } catch (final Throwable t) {
                        LOG.error("Unable to start {} due to {}", new Object[]{connectable, t.toString()});
                        if (LOG.isDebugEnabled()) {
                            LOG.error("", t);
                        }
                    }
                }

                startConnectablesAfterInitialization.clear();

                int startedTransmitting = 0;
                for (final RemoteGroupPort remoteGroupPort : startRemoteGroupPortsAfterInitialization) {
                    try {
                        remoteGroupPort.getRemoteProcessGroup().startTransmitting(remoteGroupPort);
                        startedTransmitting++;
                    } catch (final Throwable t) {
                        LOG.error("Unable to start transmitting with {} due to {}", new Object[]{remoteGroupPort, t});
                    }
                }

                LOG.info("Started {} Remote Group Ports transmitting", startedTransmitting);
                startRemoteGroupPortsAfterInitialization.clear();
            } else {
                // We don't want to start all of the delayed components. However, funnels need to be started anyway
                // because we don't provide users the ability to start or stop them - they are just notional.
                for (final Connectable connectable : startConnectablesAfterInitialization) {
                    try {
                        if (connectable instanceof Funnel) {
                            startConnectable(connectable);
                        }
                    } catch (final Throwable t) {
                        LOG.error("Unable to start {} due to {}", new Object[]{connectable, t});
                    }
                }

                startConnectablesAfterInitialization.clear();
                startRemoteGroupPortsAfterInitialization.clear();
            }

            for (final Connection connection : flowManager.getRootGroup().findAllConnections()) {
                connection.getFlowFileQueue().startLoadBalancing();
            }
        } finally {
            writeLock.unlock("onFlowInitialized");
        }
    }

    public boolean isStartAfterInitialization(final Connectable component) {
        return startConnectablesAfterInitialization.contains(component) || startRemoteGroupPortsAfterInitialization.contains(component);
    }

    private ContentRepository createContentRepository(final NiFiProperties properties) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        final String implementationClassName = properties.getProperty(NiFiProperties.CONTENT_REPOSITORY_IMPLEMENTATION, DEFAULT_CONTENT_REPO_IMPLEMENTATION);
        if (implementationClassName == null) {
            throw new RuntimeException("Cannot create Content Repository because the NiFi Properties is missing the following property: "
                    + NiFiProperties.CONTENT_REPOSITORY_IMPLEMENTATION);
        }

        try {
            final ContentRepository contentRepo = NarThreadContextClassLoader.createInstance(extensionManager, implementationClassName, ContentRepository.class, properties);
            synchronized (contentRepo) {
                contentRepo.initialize(resourceClaimManager);
            }
            return contentRepo;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ProvenanceRepository createProvenanceRepository(final NiFiProperties properties) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        final String implementationClassName = properties.getProperty(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS, DEFAULT_PROVENANCE_REPO_IMPLEMENTATION);
        if (implementationClassName == null) {
            throw new RuntimeException("Cannot create Provenance Repository because the NiFi Properties is missing the following property: "
                    + NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS);
        }

        try {
            return NarThreadContextClassLoader.createInstance(extensionManager, implementationClassName, ProvenanceRepository.class, properties);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ComponentStatusRepository createComponentStatusRepository() {
        final String implementationClassName = nifiProperties.getProperty(NiFiProperties.COMPONENT_STATUS_REPOSITORY_IMPLEMENTATION, DEFAULT_COMPONENT_STATUS_REPO_IMPLEMENTATION);
        if (implementationClassName == null) {
            throw new RuntimeException("Cannot create Component Status Repository because the NiFi Properties is missing the following property: "
                    + NiFiProperties.COMPONENT_STATUS_REPOSITORY_IMPLEMENTATION);
        }

        try {
            return NarThreadContextClassLoader.createInstance(extensionManager, implementationClassName, ComponentStatusRepository.class, nifiProperties);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }




    public KerberosConfig createKerberosConfig(final NiFiProperties nifiProperties) {
        final String principal = nifiProperties.getKerberosServicePrincipal();
        final String keytabLocation = nifiProperties.getKerberosServiceKeytabLocation();
        final File kerberosConfigFile = nifiProperties.getKerberosConfigurationFile();

        if (principal == null && keytabLocation == null && kerberosConfigFile == null) {
            return KerberosConfig.NOT_CONFIGURED;
        }

        final File keytabFile = keytabLocation == null ? null : new File(keytabLocation);
        return new KerberosConfig(principal, keytabFile, kerberosConfigFile);
    }


    public ValidationTrigger getValidationTrigger() {
        return validationTrigger;
    }

    public StringEncryptor getEncryptor() {
        return encryptor;
    }

    /**
     * @return the ExtensionManager used for instantiating Processors,
     * Prioritizers, etc.
     */
    public ExtensionManager getExtensionManager() {
        return extensionManager;
    }

    public String getInstanceId() {
        readLock.lock();
        try {
            return instanceId;
        } finally {
            readLock.unlock("getInstanceId");
        }
    }

    public Heartbeater getHeartbeater() {
        return heartbeater;
    }

    /**
     * @return the BulletinRepository for storing and retrieving Bulletins
     */
    public BulletinRepository getBulletinRepository() {
        return bulletinRepository;
    }

    public SnippetManager getSnippetManager() {
        return snippetManager;
    }

    public StateManagerProvider getStateManagerProvider() {
        return stateManagerProvider;
    }

    public Authorizer getAuthorizer() {
        return authorizer;
    }


    /**
     * @return <code>true</code> if the scheduling engine for this controller
     * has been terminated.
     */
    public boolean isTerminated() {
        this.readLock.lock();
        try {
            return null == this.timerDrivenEngineRef.get() || this.timerDrivenEngineRef.get().isTerminated();
        } finally {
            this.readLock.unlock("isTerminated");
        }
    }

    /**
     * Triggers the controller to begin shutdown, stopping all processors and
     * terminating the scheduling engine. After calling this method, the
     * {@link #isTerminated()} method will indicate whether or not the shutdown
     * has finished.
     *
     * @param kill if <code>true</code>, attempts to stop all active threads,
     * but makes no guarantee that this will happen
     *
     * @throws IllegalStateException if the controller is already stopped or
     * currently in the processor of stopping
     */
    public void shutdown(final boolean kill) {
        this.shutdown = true;
        flowManager.getRootGroup().stopProcessing();

        readLock.lock();
        try {
            if (isTerminated() || timerDrivenEngineRef.get().isTerminating()) {
                throw new IllegalStateException("Controller already stopped or still stopping...");
            }

            if (leaderElectionManager != null) {
                leaderElectionManager.stop();
            }

            if (heartbeatMonitor != null) {
                heartbeatMonitor.stop();
            }

            if (kill) {
                this.timerDrivenEngineRef.get().shutdownNow();
                this.eventDrivenEngineRef.get().shutdownNow();
                LOG.info("Initiated immediate shutdown of flow controller...");
            } else {
                this.timerDrivenEngineRef.get().shutdown();
                this.eventDrivenEngineRef.get().shutdown();
                LOG.info("Initiated graceful shutdown of flow controller...waiting up to " + gracefulShutdownSeconds + " seconds");
            }

            validationThreadPool.shutdown();
            clusterTaskExecutor.shutdownNow();

            if (zooKeeperStateServer != null) {
                zooKeeperStateServer.shutdown();
            }

            if (loadBalanceClientThreadPool != null) {
                loadBalanceClientThreadPool.shutdownNow();
            }
            loadBalanceClientTasks.forEach(NioAsyncLoadBalanceClientTask::stop);

            // Trigger any processors' methods marked with @OnShutdown to be called
            flowManager.getRootGroup().shutdown();

            stateManagerProvider.shutdown();

            // invoke any methods annotated with @OnShutdown on Controller Services
            for (final ControllerServiceNode serviceNode : flowManager.getAllControllerServices()) {
                final Class<?> serviceImplClass = serviceNode.getControllerServiceImplementation().getClass();
                try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, serviceImplClass, serviceNode.getIdentifier())) {
                    final ConfigurationContext configContext = new StandardConfigurationContext(serviceNode, controllerServiceProvider, null, variableRegistry);
                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnShutdown.class, serviceNode.getControllerServiceImplementation(), configContext);
                }
            }

            // invoke any methods annotated with @OnShutdown on Reporting Tasks
            for (final ReportingTaskNode taskNode : getAllReportingTasks()) {
                final ConfigurationContext configContext = taskNode.getConfigurationContext();
                try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, taskNode.getReportingTask().getClass(), taskNode.getIdentifier())) {
                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnShutdown.class, taskNode.getReportingTask(), configContext);
                }
            }

            try {
                this.timerDrivenEngineRef.get().awaitTermination(gracefulShutdownSeconds / 2, TimeUnit.SECONDS);
                this.eventDrivenEngineRef.get().awaitTermination(gracefulShutdownSeconds / 2, TimeUnit.SECONDS);
            } catch (final InterruptedException ie) {
                LOG.info("Interrupted while waiting for controller termination.");
            }

            try {
                flowFileRepository.close();
            } catch (final Throwable t) {
                LOG.warn("Unable to shut down FlowFileRepository due to {}", new Object[]{t});
            }

            if (this.timerDrivenEngineRef.get().isTerminated() && eventDrivenEngineRef.get().isTerminated()) {
                LOG.info("Controller has been terminated successfully.");
            } else {
                LOG.warn("Controller hasn't terminated properly.  There exists an uninterruptable thread that "
                        + "will take an indeterminate amount of time to stop.  Might need to kill the program manually.");
            }

            for (final RemoteSiteListener listener : externalSiteListeners) {
                listener.stop();
            }

            if (loadBalanceServer != null) {
                loadBalanceServer.stop();
            }

            if (loadBalanceClientRegistry != null) {
                loadBalanceClientRegistry.stop();
            }

            if (processScheduler != null) {
                processScheduler.shutdown();
            }

            if (contentRepository != null) {
                contentRepository.shutdown();
            }

            if (provenanceRepository != null) {
                try {
                    provenanceRepository.close();
                } catch (final IOException ioe) {
                    LOG.warn("There was a problem shutting down the Provenance Repository: " + ioe.toString());
                    if (LOG.isDebugEnabled()) {
                        LOG.warn("", ioe);
                    }
                }
            }
        } finally {
            readLock.unlock("shutdown");
        }
    }

    /**
     * Serializes the current state of the controller to the given OutputStream
     *
     * @param serializer serializer
     * @param os stream
     * @throws FlowSerializationException if serialization of the flow fails for
     * any reason
     */
    public synchronized <T> void serialize(final FlowSerializer<T> serializer, final OutputStream os) throws FlowSerializationException {
        T flowConfiguration;

        readLock.lock();
        try {
            final ScheduledStateLookup scheduledStateLookup = new ScheduledStateLookup() {
                @Override
                public ScheduledState getScheduledState(final ProcessorNode procNode) {
                    if (startConnectablesAfterInitialization.contains(procNode)) {
                        return ScheduledState.RUNNING;
                    }

                    return procNode.getDesiredState();
                }

                @Override
                public ScheduledState getScheduledState(final Port port) {
                    if (startConnectablesAfterInitialization.contains(port)) {
                        return ScheduledState.RUNNING;
                    }
                    if (startRemoteGroupPortsAfterInitialization.contains(port)) {
                        return ScheduledState.RUNNING;
                    }

                    return port.getScheduledState();
                }
            };

            flowConfiguration = serializer.transform(this, scheduledStateLookup);
        } finally {
            readLock.unlock("serialize");
        }

        serializer.serialize(flowConfiguration, os);
    }

    /**
     * Synchronizes this controller with the proposed flow.
     *
     * For more details, see
     * {@link FlowSynchronizer#sync(FlowController, DataFlow, StringEncryptor)}.
     *
     * @param synchronizer synchronizer
     * @param dataFlow the flow to load the controller with. If the flow is null
     * or zero length, then the controller must not have a flow or else an
     * UninheritableFlowException will be thrown.
     *
     * @throws FlowSerializationException if proposed flow is not a valid flow
     * configuration file
     * @throws UninheritableFlowException if the proposed flow cannot be loaded
     * by the controller because in doing so would risk orphaning flow files
     * @throws FlowSynchronizationException if updates to the controller failed.
     * If this exception is thrown, then the controller should be considered
     * unsafe to be used
     * @throws MissingBundleException if the proposed flow cannot be loaded by the
     * controller because it contains a bundle that does not exist in the controller
     */
    public void synchronize(final FlowSynchronizer synchronizer, final DataFlow dataFlow)
            throws FlowSerializationException, FlowSynchronizationException, UninheritableFlowException, MissingBundleException {
        writeLock.lock();
        try {
            LOG.debug("Synchronizing controller with proposed flow");
            synchronizer.sync(this, dataFlow, encryptor);
            flowSynchronized.set(true);
            LOG.info("Successfully synchronized controller with proposed flow");
        } finally {
            writeLock.unlock("synchronize");
        }
    }

    /**
     * @return the currently configured maximum number of threads that can be
     * used for executing processors at any given time.
     */
    public int getMaxTimerDrivenThreadCount() {
        return maxTimerDrivenThreads.get();
    }

    public int getMaxEventDrivenThreadCount() {
        return maxEventDrivenThreads.get();
    }

    public int getActiveEventDrivenThreadCount() {
        return eventDrivenEngineRef.get().getActiveCount();
    }

    public int getActiveTimerDrivenThreadCount() {
        return timerDrivenEngineRef.get().getActiveCount();
    }

    public void setMaxTimerDrivenThreadCount(final int maxThreadCount) {
        writeLock.lock();
        try {
            setMaxThreadCount(maxThreadCount, this.timerDrivenEngineRef.get(), this.maxTimerDrivenThreads);
        } finally {
            writeLock.unlock("setMaxTimerDrivenThreadCount");
        }
    }

    public void setMaxEventDrivenThreadCount(final int maxThreadCount) {
        writeLock.lock();
        try {
            setMaxThreadCount(maxThreadCount, this.eventDrivenEngineRef.get(), this.maxEventDrivenThreads);
            processScheduler.setMaxThreadCount(SchedulingStrategy.EVENT_DRIVEN, maxThreadCount);
        } finally {
            writeLock.unlock("setMaxEventDrivenThreadCount");
        }
    }

    /**
     * Updates the number of threads that can be simultaneously used for executing processors.
     * This method must be called while holding the write lock!
     *
     * @param maxThreadCount max number of threads
     */
    private void setMaxThreadCount(final int maxThreadCount, final FlowEngine engine, final AtomicInteger maxThreads) {
        if (maxThreadCount < 1) {
            throw new IllegalArgumentException("Cannot set max number of threads to less than 2");
        }

        maxThreads.getAndSet(maxThreadCount);
        if (null != engine && engine.getCorePoolSize() < maxThreadCount) {
            engine.setCorePoolSize(maxThreads.intValue());
        }
    }

    public UserAwareEventAccess getEventAccess() {
        return eventAccess;
    }

    /**
     * Sets the root group to the given group
     *
     * @param group the ProcessGroup that is to become the new Root Group
     *
     * @throws IllegalArgumentException if the ProcessGroup has a parent
     * @throws IllegalStateException if the FlowController does not know about
     * the given process group
     */
    void setRootGroup(final ProcessGroup group) {
        if (requireNonNull(group).getParent() != null) {
            throw new IllegalArgumentException("A ProcessGroup that has a parent cannot be the Root Group");
        }

        writeLock.lock();
        try {
            flowManager.setRootGroup(group);
            for (final RemoteSiteListener listener : externalSiteListeners) {
                listener.setRootGroup(group);
            }

            // update the heartbeat bean
            this.heartbeatBeanRef.set(new HeartbeatBean(group, isPrimary()));
            allProcessGroups.put(group.getIdentifier(), group);
            allProcessGroups.put(FlowManager.ROOT_GROUP_ID_ALIAS, group);
        } finally {
            writeLock.unlock("setRootGroup");
        }
    }

    public SystemDiagnostics getSystemDiagnostics() {
        final SystemDiagnosticsFactory factory = new SystemDiagnosticsFactory();
        return factory.create(flowFileRepository, contentRepository, provenanceRepository);
    }

    public String getContentRepoFileStoreName(final String containerName) {
        return contentRepository.getContainerFileStoreName(containerName);
    }

    public String getFlowRepoFileStoreName() {
        return flowFileRepository.getFileStoreName();
    }

    public String getProvenanceRepoFileStoreName(final String containerName) {
        return provenanceRepository.getContainerFileStoreName(containerName);
    }

    //
    // ProcessGroup access
    //

    private Position toPosition(final PositionDTO dto) {
        return new Position(dto.getX(), dto.getY());
    }

    //
    // Snippet
    //

    private void verifyBundleInVersionedFlow(final org.apache.nifi.registry.flow.Bundle requiredBundle, final Set<BundleCoordinate> supportedBundles) {
        final BundleCoordinate requiredCoordinate = new BundleCoordinate(requiredBundle.getGroup(), requiredBundle.getArtifact(), requiredBundle.getVersion());
        if (!supportedBundles.contains(requiredCoordinate)) {
            throw new IllegalStateException("Unsupported bundle: " + requiredCoordinate);
        }
    }


    private void verifyProcessorsInVersionedFlow(final VersionedProcessGroup versionedFlow, final Map<String, Set<BundleCoordinate>> supportedTypes) {
        if (versionedFlow.getProcessors() != null) {
            versionedFlow.getProcessors().forEach(processor -> {
                if (processor.getBundle() == null) {
                    throw new IllegalArgumentException("Processor bundle must be specified.");
                }

                if (supportedTypes.containsKey(processor.getType())) {
                    verifyBundleInVersionedFlow(processor.getBundle(), supportedTypes.get(processor.getType()));
                } else {
                    throw new IllegalStateException("Invalid Processor Type: " + processor.getType());
                }
            });
        }

        if (versionedFlow.getProcessGroups() != null) {
            versionedFlow.getProcessGroups().forEach(processGroup -> {
                verifyProcessorsInVersionedFlow(processGroup, supportedTypes);
            });
        }
    }


    private void verifyControllerServicesInVersionedFlow(final VersionedProcessGroup versionedFlow, final Map<String, Set<BundleCoordinate>> supportedTypes) {
        if (versionedFlow.getControllerServices() != null) {
            versionedFlow.getControllerServices().forEach(controllerService -> {
                if (supportedTypes.containsKey(controllerService.getType())) {
                    if (controllerService.getBundle() == null) {
                        throw new IllegalArgumentException("Controller Service bundle must be specified.");
                    }

                    verifyBundleInVersionedFlow(controllerService.getBundle(), supportedTypes.get(controllerService.getType()));
                } else {
                    throw new IllegalStateException("Invalid Controller Service Type: " + controllerService.getType());
                }
            });
        }

        if (versionedFlow.getProcessGroups() != null) {
            versionedFlow.getProcessGroups().forEach(processGroup -> {
                verifyControllerServicesInVersionedFlow(processGroup, supportedTypes);
            });
        }
    }

    public void verifyComponentTypesInSnippet(final VersionedProcessGroup versionedFlow) {
        final Map<String, Set<BundleCoordinate>> processorClasses = new HashMap<>();
        for (final Class<?> c : extensionManager.getExtensions(Processor.class)) {
            final String name = c.getName();
            processorClasses.put(name, extensionManager.getBundles(name).stream().map(bundle -> bundle.getBundleDetails().getCoordinate()).collect(Collectors.toSet()));
        }
        verifyProcessorsInVersionedFlow(versionedFlow, processorClasses);

        final Map<String, Set<BundleCoordinate>> controllerServiceClasses = new HashMap<>();
        for (final Class<?> c : extensionManager.getExtensions(ControllerService.class)) {
            final String name = c.getName();
            controllerServiceClasses.put(name, extensionManager.getBundles(name).stream().map(bundle -> bundle.getBundleDetails().getCoordinate()).collect(Collectors.toSet()));
        }
        verifyControllerServicesInVersionedFlow(versionedFlow, controllerServiceClasses);

        final Set<String> prioritizerClasses = new HashSet<>();
        for (final Class<?> c : extensionManager.getExtensions(FlowFilePrioritizer.class)) {
            prioritizerClasses.add(c.getName());
        }

        final Set<VersionedConnection> allConns = new HashSet<>();
        allConns.addAll(versionedFlow.getConnections());
        for (final VersionedProcessGroup childGroup : versionedFlow.getProcessGroups()) {
            allConns.addAll(findAllConnections(childGroup));
        }

        for (final VersionedConnection conn : allConns) {
            final List<String> prioritizers = conn.getPrioritizers();
            if (prioritizers != null) {
                for (final String prioritizer : prioritizers) {
                    if (!prioritizerClasses.contains(prioritizer)) {
                        throw new IllegalStateException("Invalid FlowFile Prioritizer Type: " + prioritizer);
                    }
                }
            }
        }
    }

    private Set<VersionedConnection> findAllConnections(final VersionedProcessGroup group) {
        final Set<VersionedConnection> conns = new HashSet<>();
        for (final VersionedConnection connection : group.getConnections()) {
            conns.add(connection);
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            conns.addAll(findAllConnections(childGroup));
        }
        return conns;
    }

    //
    // Processor access
    //


    /**
     * Returns the ProcessGroup with the given ID
     *
     * @param id group
     * @return the process group or null if not group is found
     */
    private ProcessGroup lookupGroup(final String id) {
        final ProcessGroup group = flowManager.getGroup(id);
        if (group == null) {
            throw new IllegalStateException("No Group with ID " + id + " exists");
        }
        return group;
    }



    public List<GarbageCollectionStatus> getGarbageCollectionStatus() {
        final List<GarbageCollectionStatus> statuses = new ArrayList<>();

        final Date now = new Date();
        for (final GarbageCollectorMXBean mbean : ManagementFactory.getGarbageCollectorMXBeans()) {
            final String managerName = mbean.getName();
            final long count = mbean.getCollectionCount();
            final long millis = mbean.getCollectionTime();
            final GarbageCollectionStatus status = new StandardGarbageCollectionStatus(managerName, now, count, millis);
            statuses.add(status);
        }

        return statuses;
    }

    public GarbageCollectionHistory getGarbageCollectionHistory() {
        return componentStatusRepository.getGarbageCollectionHistory(new Date(0L), new Date());
    }


    public ReloadComponent getReloadComponent() {
        return reloadComponent;
    }

    public void startProcessor(final String parentGroupId, final String processorId) {
        startProcessor(parentGroupId, processorId, true);
    }

    public void startProcessor(final String parentGroupId, final String processorId, final boolean failIfStopping) {
        final ProcessGroup group = lookupGroup(parentGroupId);
        final ProcessorNode node = group.getProcessor(processorId);
        if (node == null) {
            throw new IllegalStateException("Cannot find ProcessorNode with ID " + processorId + " within ProcessGroup with ID " + parentGroupId);
        }

        writeLock.lock();
        try {
            if (initialized.get()) {
                group.startProcessor(node, failIfStopping);
            } else {
                startConnectablesAfterInitialization.add(node);
            }
        } finally {
            writeLock.unlock("startProcessor");
        }
    }

    public boolean isInitialized() {
        return initialized.get();
    }

    public boolean isFlowSynchronized() {
        return flowSynchronized.get();
    }

    public void startConnectable(final Connectable connectable) {
        final ProcessGroup group = requireNonNull(connectable).getProcessGroup();

        writeLock.lock();
        try {
            if (initialized.get()) {
                switch (requireNonNull(connectable).getConnectableType()) {
                    case FUNNEL:
                        group.startFunnel((Funnel) connectable);
                        break;
                    case INPUT_PORT:
                    case REMOTE_INPUT_PORT:
                        group.startInputPort((Port) connectable);
                        break;
                    case OUTPUT_PORT:
                    case REMOTE_OUTPUT_PORT:
                        group.startOutputPort((Port) connectable);
                        break;
                    default:
                        throw new IllegalArgumentException();
                }
            } else {
                startConnectablesAfterInitialization.add(connectable);
            }
        } finally {
            writeLock.unlock("startConnectable");
        }
    }

    public void stopConnectable(final Connectable connectable) {
        final ProcessGroup group = requireNonNull(connectable).getProcessGroup();

        writeLock.lock();
        try {
            switch (requireNonNull(connectable).getConnectableType()) {
                case FUNNEL:
                    // Ignore. We don't support stopping funnels.
                    break;
                case INPUT_PORT:
                case REMOTE_INPUT_PORT:
                    startConnectablesAfterInitialization.remove(connectable);
                    group.stopInputPort((Port) connectable);
                    break;
                case OUTPUT_PORT:
                case REMOTE_OUTPUT_PORT:
                    startConnectablesAfterInitialization.remove(connectable);
                    group.stopOutputPort((Port) connectable);
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        } finally {
            writeLock.unlock("stopConnectable");
        }
    }

    public void startTransmitting(final RemoteGroupPort remoteGroupPort) {
        writeLock.lock();
        try {
            if (initialized.get()) {
                remoteGroupPort.getRemoteProcessGroup().startTransmitting(remoteGroupPort);
            } else {
                startRemoteGroupPortsAfterInitialization.add(remoteGroupPort);
            }
        } finally {
            writeLock.unlock("startTransmitting");
        }
    }

    public void stopTransmitting(final RemoteGroupPort remoteGroupPort) {
        writeLock.lock();
        try {
            if (initialized.get()) {
                remoteGroupPort.getRemoteProcessGroup().stopTransmitting(remoteGroupPort);
            } else {
                startRemoteGroupPortsAfterInitialization.remove(remoteGroupPort);
            }
        } finally {
            writeLock.unlock("stopTransmitting");
        }
    }

    public void stopProcessor(final String parentGroupId, final String processorId) {
        final ProcessGroup group = lookupGroup(parentGroupId);
        final ProcessorNode node = group.getProcessor(processorId);
        if (node == null) {
            throw new IllegalStateException("Cannot find ProcessorNode with ID " + processorId + " within ProcessGroup with ID " + parentGroupId);
        }
        group.stopProcessor(node);
        // If we are ready to start the processor upon initialization of the controller, don't.
        startConnectablesAfterInitialization.remove(node);
    }



    @Override
    public void startReportingTask(final ReportingTaskNode reportingTaskNode) {
        if (isTerminated()) {
            throw new IllegalStateException("Cannot start reporting task " + reportingTaskNode.getIdentifier() + " because the controller is terminated");
        }

        reportingTaskNode.verifyCanStart();
        reportingTaskNode.reloadAdditionalResourcesIfNecessary();
        processScheduler.schedule(reportingTaskNode);
    }

    @Override
    public void stopReportingTask(final ReportingTaskNode reportingTaskNode) {
        if (isTerminated()) {
            return;
        }

        reportingTaskNode.verifyCanStop();
        processScheduler.unschedule(reportingTaskNode);
    }

    public FlowManager getFlowManager() {
        return flowManager;
    }

    /**
     * Creates a connection between two Connectable objects.
     *
     * @param id required ID of the connection
     * @param name the name of the connection, or <code>null</code> to leave the
     * connection unnamed
     * @param source required source
     * @param destination required destination
     * @param relationshipNames required collection of relationship names
     * @return
     *
     * @throws NullPointerException if the ID, source, destination, or set of
     * relationships is null.
     * @throws IllegalArgumentException if <code>relationships</code> is an
     * empty collection
     */
    public Connection createConnection(final String id, final String name, final Connectable source, final Connectable destination, final Collection<String> relationshipNames) {
        final StandardConnection.Builder builder = new StandardConnection.Builder(processScheduler);

        final List<Relationship> relationships = new ArrayList<>();
        for (final String relationshipName : requireNonNull(relationshipNames)) {
            relationships.add(new Relationship.Builder().name(relationshipName).build());
        }

        // Create and initialize a FlowFileSwapManager for this connection
        final FlowFileSwapManager swapManager = createSwapManager();
        final EventReporter eventReporter = createEventReporter();

        try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
            final SwapManagerInitializationContext initializationContext = new SwapManagerInitializationContext() {
                @Override
                public ResourceClaimManager getResourceClaimManager() {
                    return resourceClaimManager;
                }

                @Override
                public FlowFileRepository getFlowFileRepository() {
                    return flowFileRepository;
                }

                @Override
                public EventReporter getEventReporter() {
                    return eventReporter;
                }
            };

            swapManager.initialize(initializationContext);
        }

        final FlowFileQueueFactory flowFileQueueFactory = new FlowFileQueueFactory() {
            @Override
            public FlowFileQueue createFlowFileQueue(final LoadBalanceStrategy loadBalanceStrategy, final String partitioningAttribute, final ConnectionEventListener eventListener) {
                final FlowFileQueue flowFileQueue;

                if (clusterCoordinator == null) {
                    flowFileQueue = new StandardFlowFileQueue(id, eventListener, flowFileRepository, provenanceRepository, resourceClaimManager, processScheduler, swapManager,
                        eventReporter, nifiProperties.getQueueSwapThreshold(), nifiProperties.getDefaultBackPressureObjectThreshold(), nifiProperties.getDefaultBackPressureDataSizeThreshold());
                } else {
                    flowFileQueue = new SocketLoadBalancedFlowFileQueue(id, eventListener, processScheduler, flowFileRepository, provenanceRepository, contentRepository, resourceClaimManager,
                        clusterCoordinator, loadBalanceClientRegistry, swapManager, nifiProperties.getQueueSwapThreshold(), eventReporter);

                    flowFileQueue.setBackPressureObjectThreshold(nifiProperties.getDefaultBackPressureObjectThreshold());
                    flowFileQueue.setBackPressureDataSizeThreshold(nifiProperties.getDefaultBackPressureDataSizeThreshold());
                }

                return flowFileQueue;
            }
        };

        final Connection connection = builder.id(requireNonNull(id).intern())
            .name(name == null ? null : name.intern())
            .relationships(relationships)
            .source(requireNonNull(source))
            .destination(destination)
            .flowFileQueueFactory(flowFileQueueFactory)
            .build();

        return connection;
    }



    @Override
    public ReportingTaskNode getReportingTaskNode(final String identifier) {
        return flowManager.getReportingTaskNode(identifier);
    }

    @Override
    public ReportingTaskNode createReportingTask(final String type, final String id, final BundleCoordinate bundleCoordinate, final boolean firstTimeAdded) throws ReportingTaskInstantiationException {
        return flowManager.createReportingTask(type, id, bundleCoordinate, firstTimeAdded);
    }

    @Override
    public Set<ReportingTaskNode> getAllReportingTasks() {
        return flowManager.getAllReportingTasks();
    }

    @Override
    public void removeReportingTask(final ReportingTaskNode reportingTaskNode) {
        flowManager.removeReportingTask(reportingTaskNode);
    }


    public FlowRegistryClient getFlowRegistryClient() {
        return flowRegistryClient;
    }

    public ControllerServiceProvider getControllerServiceProvider() {
        return controllerServiceProvider;
    }


    public VariableRegistry getVariableRegistry() {
        return variableRegistry;
    }

    public ProvenanceAuthorizableFactory getProvenanceAuthorizableFactory() {
        return provenanceAuthorizableFactory;
    }

    @Override
    public void enableReportingTask(final ReportingTaskNode reportingTaskNode) {
        reportingTaskNode.verifyCanEnable();
        reportingTaskNode.reloadAdditionalResourcesIfNecessary();
        processScheduler.enableReportingTask(reportingTaskNode);
    }

    @Override
    public void disableReportingTask(final ReportingTaskNode reportingTaskNode) {
        reportingTaskNode.verifyCanDisable();
        processScheduler.disableReportingTask(reportingTaskNode);
    }



    //
    // Counters
    //
    public List<Counter> getCounters() {
        final List<Counter> counters = new ArrayList<>();

        final CounterRepository counterRepo = counterRepositoryRef.get();
        for (final Counter counter : counterRepo.getCounters()) {
            counters.add(counter);
        }

        return counters;
    }

    public Counter resetCounter(final String identifier) {
        final CounterRepository counterRepo = counterRepositoryRef.get();
        final Counter resetValue = counterRepo.resetCounter(identifier);
        return resetValue;
    }

    //
    // Access to controller status
    //
    public QueueSize getTotalFlowFileCount(final ProcessGroup group) {
        int count = 0;
        long contentSize = 0L;

        for (final Connection connection : group.getConnections()) {
            final QueueSize size = connection.getFlowFileQueue().size();
            count += size.getObjectCount();
            contentSize += size.getByteCount();
        }
        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            final QueueSize size = getTotalFlowFileCount(childGroup);
            count += size.getObjectCount();
            contentSize += size.getByteCount();
        }

        return new QueueSize(count, contentSize);
    }

    public class GroupStatusCounts {
        private int queuedCount = 0;
        private long queuedContentSize = 0;
        private int activeThreadCount = 0;
        private int terminatedThreadCount = 0;

        public GroupStatusCounts(final ProcessGroup group) {
            calculateCounts(group);
        }

        private void calculateCounts(final ProcessGroup group) {
            for (final Connection connection : group.getConnections()) {
                final QueueSize size = connection.getFlowFileQueue().size();
                queuedCount += size.getObjectCount();
                queuedContentSize += size.getByteCount();

                final Connectable source = connection.getSource();
                if (ConnectableType.REMOTE_OUTPUT_PORT.equals(source.getConnectableType())) {
                    final RemoteGroupPort remoteOutputPort = (RemoteGroupPort) source;
                    activeThreadCount += processScheduler.getActiveThreadCount(remoteOutputPort);
                }

                final Connectable destination = connection.getDestination();
                if (ConnectableType.REMOTE_INPUT_PORT.equals(destination.getConnectableType())) {
                    final RemoteGroupPort remoteInputPort = (RemoteGroupPort) destination;
                    activeThreadCount += processScheduler.getActiveThreadCount(remoteInputPort);
                }
            }
            for (final ProcessorNode processor : group.getProcessors()) {
                activeThreadCount += processScheduler.getActiveThreadCount(processor);
                terminatedThreadCount += processor.getTerminatedThreadCount();
            }
            for (final Port port : group.getInputPorts()) {
                activeThreadCount += processScheduler.getActiveThreadCount(port);
            }
            for (final Port port : group.getOutputPorts()) {
                activeThreadCount += processScheduler.getActiveThreadCount(port);
            }
            for (final Funnel funnel : group.getFunnels()) {
                activeThreadCount += processScheduler.getActiveThreadCount(funnel);
            }
            for (final ProcessGroup childGroup : group.getProcessGroups()) {
                calculateCounts(childGroup);
            }
        }

        public int getQueuedCount() {
            return queuedCount;
        }

        public long getQueuedContentSize() {
            return queuedContentSize;
        }

        public int getActiveThreadCount() {
            return activeThreadCount;
        }

        public int getTerminatedThreadCount() {
            return terminatedThreadCount;
        }
    }

    public GroupStatusCounts getGroupStatusCounts(final ProcessGroup group) {
        return new GroupStatusCounts(group);
    }

    public int getActiveThreadCount() {
        final int timerDrivenCount = timerDrivenEngineRef.get().getActiveCount();
        final int eventDrivenCount = eventDrivenSchedulingAgent.getActiveThreadCount();
        return timerDrivenCount + eventDrivenCount;
    }


    //
    // Clustering methods
    //
    /**
     * Starts heartbeating to the cluster. May only be called if the instance
     * was constructed for a clustered environment.
     *
     * @throws IllegalStateException if not configured for clustering
     */
    public void startHeartbeating() throws IllegalStateException {
        if (!isConfiguredForClustering()) {
            throw new IllegalStateException("Unable to start heartbeating because heartbeating is not configured.");
        }

        writeLock.lock();
        try {
            stopHeartbeating();

            final HeartbeatSendTask sendTask = new HeartbeatSendTask();
            this.heartbeatSendTask.set(sendTask);
            heartbeatSenderFuture = clusterTaskExecutor.scheduleWithFixedDelay(sendTask, 0, heartbeatDelaySeconds, TimeUnit.SECONDS);
        } finally {
            writeLock.unlock("startHeartbeating");
        }
    }

    /**
     * Notifies controller that the sending of heartbeats should be temporarily
     * suspended. This method does not cancel any background tasks as does
     * {@link #stopHeartbeating()} and does not require any lock on the
     * FlowController. Background tasks will still generate heartbeat messages
     * and any background task currently in the process of sending a Heartbeat
     * to the cluster will continue.
     */
    public void suspendHeartbeats() {
        heartbeatsSuspended.set(true);
    }

    /**
     * Notifies controller that the sending of heartbeats should be re-enabled.
     * This method does not submit any background tasks to take affect as does
     * {@link #startHeartbeating()} and does not require any lock on the
     * FlowController.
     */
    public void resumeHeartbeats() {
        heartbeatsSuspended.set(false);
    }

    /**
     * Stops heartbeating to the cluster. May only be called if the instance was
     * constructed for a clustered environment. If the controller was not
     * heartbeating, then this method has no effect.
     *
     * @throws IllegalStateException if not clustered
     */
    public void stopHeartbeating() throws IllegalStateException {
        if (!isConfiguredForClustering()) {
            throw new IllegalStateException("Unable to stop heartbeating because heartbeating is not configured.");
        }

        writeLock.lock();
        try {
            if (!isHeartbeating()) {
                return;
            }

            if (heartbeatSenderFuture != null) {
                heartbeatSenderFuture.cancel(false);
            }
        } finally {
            writeLock.unlock("stopHeartbeating");
        }

    }

    /**
     * @return true if the instance is heartbeating; false otherwise
     */
    public boolean isHeartbeating() {
        readLock.lock();
        try {
            return heartbeatSenderFuture != null && !heartbeatSenderFuture.isCancelled();
        } finally {
            readLock.unlock("isHeartbeating");
        }
    }

    /**
     * @return the number of seconds to wait between successive heartbeats
     */
    public int getHeartbeatDelaySeconds() {
        readLock.lock();
        try {
            return heartbeatDelaySeconds;
        } finally {
            readLock.unlock("getHeartbeatDelaySeconds");
        }
    }

    /**
     * The node identifier of this instance.
     *
     * @return the node identifier or null if no identifier is set
     */
    public NodeIdentifier getNodeId() {
        return nodeId;
    }

    /**
     * Sets the node identifier for this instance.
     *
     * @param nodeId the node identifier, which may be null
     */
    public void setNodeId(final NodeIdentifier nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return true if this instance is clustered; false otherwise. Clustered
     * means that a node is either connected or trying to connect to the
     * cluster.
     */
    @Override
    public boolean isClustered() {
        readLock.lock();
        try {
            return clustered;
        } finally {
            readLock.unlock("isClustered");
        }
    }

    public boolean isConfiguredForClustering() {
        return configuredForClustering;
    }

    void registerForClusterCoordinator(final boolean participate) {
        final String participantId = participate ? heartbeatMonitor.getHeartbeatAddress() : null;

        leaderElectionManager.register(ClusterRoles.CLUSTER_COORDINATOR, new LeaderElectionStateChangeListener() {
            @Override
            public synchronized void onLeaderRelinquish() {
                LOG.info("This node is no longer the elected Active Cluster Coordinator");
                bulletinRepository.addBulletin(BulletinFactory.createBulletin("Cluster Coordinator", Severity.INFO.name(), participantId + " is no longer the Cluster Coordinator"));

                // We do not want to stop the heartbeat monitor. This is because even though ZooKeeper offers guarantees
                // that watchers will see changes on a ZNode in the order they happened, there does not seem to be any
                // guarantee that Curator will notify us that our leadership was gained or loss in the order that it happened.
                // As a result, if nodes connect/disconnect from cluster quickly, we could invoke stop() then start() or
                // start() then stop() in the wrong order, which can cause the cluster to behavior improperly. As a result, we simply
                // call start() when we become the leader, and this will ensure that initialization is handled. The heartbeat monitor
                // then will check the zookeeper znode to check if it is the cluster coordinator before kicking any nodes out of the
                // cluster.
            }

            @Override
            public synchronized void onLeaderElection() {
                LOG.info("This node elected Active Cluster Coordinator");
                bulletinRepository.addBulletin(BulletinFactory.createBulletin("Cluster Coordinator", Severity.INFO.name(), participantId + " has been elected the Cluster Coordinator"));

                // Purge any heartbeats that we already have. If we don't do this, we can have a scenario where we receive heartbeats
                // from a node, and then another node becomes Cluster Coordinator. As a result, we stop receiving heartbeats. Now that
                // we are again the Cluster Coordinator, we will detect that there are old heartbeat messages and start disconnecting
                // nodes due to a lack of heartbeat. By purging the heartbeats here, we remove any old heartbeat messages so that this
                // does not happen.
                FlowController.this.heartbeatMonitor.purgeHeartbeats();
            }
        }, participantId);
    }

    void registerForPrimaryNode() {
        final String participantId = heartbeatMonitor.getHeartbeatAddress();

        leaderElectionManager.register(ClusterRoles.PRIMARY_NODE, new LeaderElectionStateChangeListener() {
            @Override
            public void onLeaderElection() {
                setPrimary(true);
            }

            @Override
            public void onLeaderRelinquish() {
                setPrimary(false);
            }
        }, participantId);
    }

    /**
     * Sets whether this instance is clustered. Clustered means that a node is
     * either connected or trying to connect to the cluster.
     *
     * @param clustered true if clustered
     * @param clusterInstanceId if clustered is true, indicates the InstanceID
     * of the Cluster Manager
     */
    public void setClustered(final boolean clustered, final String clusterInstanceId) {
        writeLock.lock();
        try {
            // verify whether the this node's clustered status is changing
            boolean isChanging = false;
            if (this.clustered != clustered) {
                isChanging = true;

                if (clustered) {
                    LOG.info("Cluster State changed from Not Clustered to Clustered");
                } else {
                    LOG.info("Cluster State changed from Clustered to Not Clustered");
                }
            }

            // mark the new cluster status
            this.clustered = clustered;
            eventDrivenWorkerQueue.setClustered(clustered);

            if (clusterInstanceId != null) {
                this.instanceId = clusterInstanceId;
            }

            // update the bulletin repository
            if (isChanging) {
                if (clustered) {
                    registerForPrimaryNode();

                    // Participate in Leader Election for Heartbeat Monitor. Start the heartbeat monitor
                    // if/when we become leader and stop it when we lose leader role
                    registerForClusterCoordinator(true);

                    leaderElectionManager.start();
                    stateManagerProvider.enableClusterProvider();

                    loadBalanceClientRegistry.start();

                    heartbeat();
                } else {
                    stateManagerProvider.disableClusterProvider();
                    setPrimary(false);
                }

                final List<RemoteProcessGroup> remoteGroups = flowManager.getRootGroup().findAllRemoteProcessGroups();
                for (final RemoteProcessGroup remoteGroup : remoteGroups) {
                    remoteGroup.reinitialize(clustered);
                }
            }

            if (!clustered) {
                leaderElectionManager.unregister(ClusterRoles.PRIMARY_NODE);
                leaderElectionManager.unregister(ClusterRoles.CLUSTER_COORDINATOR);
            }

            // update the heartbeat bean
            this.heartbeatBeanRef.set(new HeartbeatBean(flowManager.getRootGroup(), isPrimary()));
        } finally {
            writeLock.unlock("setClustered");
        }
    }



    /**
     * @return true if this instance is the primary node in the cluster; false
     * otherwise
     */
    @Override
    public boolean isPrimary() {
        return isClustered() && leaderElectionManager != null && leaderElectionManager.isLeader(ClusterRoles.PRIMARY_NODE);
    }

    public boolean isClusterCoordinator() {
        return isClustered() && leaderElectionManager != null && leaderElectionManager.isLeader(ClusterRoles.CLUSTER_COORDINATOR);
    }

    public void setPrimary(final boolean primary) {
        final PrimaryNodeState nodeState = primary ? PrimaryNodeState.ELECTED_PRIMARY_NODE : PrimaryNodeState.PRIMARY_NODE_REVOKED;
        final ProcessGroup rootGroup = flowManager.getRootGroup();
        for (final ProcessorNode procNode : rootGroup.findAllProcessors()) {
            try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, procNode.getProcessor().getClass(), procNode.getIdentifier())) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnPrimaryNodeStateChange.class, procNode.getProcessor(), nodeState);
            }
        }
        for (final ControllerServiceNode serviceNode : flowManager.getAllControllerServices()) {
            final Class<?> serviceImplClass = serviceNode.getControllerServiceImplementation().getClass();
            try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, serviceImplClass, serviceNode.getIdentifier())) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnPrimaryNodeStateChange.class, serviceNode.getControllerServiceImplementation(), nodeState);
            }
        }
        for (final ReportingTaskNode reportingTaskNode : getAllReportingTasks()) {
            try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, reportingTaskNode.getReportingTask().getClass(), reportingTaskNode.getIdentifier())) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnPrimaryNodeStateChange.class, reportingTaskNode.getReportingTask(), nodeState);
            }
        }

        // update primary
        eventDrivenWorkerQueue.setPrimary(primary);

        // update the heartbeat bean
        final HeartbeatBean oldBean = this.heartbeatBeanRef.getAndSet(new HeartbeatBean(rootGroup, primary));

        // Emit a bulletin detailing the fact that the primary node state has changed
        if (oldBean == null || oldBean.isPrimary() != primary) {
            final String message = primary ? "This node has been elected Primary Node" : "This node is no longer Primary Node";
            final Bulletin bulletin = BulletinFactory.createBulletin("Primary Node", Severity.INFO.name(), message);
            bulletinRepository.addBulletin(bulletin);
            LOG.info(message);
        }
    }

    static boolean areEqual(final String a, final String b) {
        if (a == null && b == null) {
            return true;
        }
        if (a == b) {
            return true;
        }

        if (a == null || b == null) {
            return false;
        }

        return a.equals(b);
    }

    static boolean areEqual(final Long a, final Long b) {
        if (a == null && b == null) {
            return true;
        }
        if (a == b) {
            return true;
        }

        if (a == null || b == null) {
            return false;
        }
        return a.compareTo(b) == 0;
    }

    public ContentAvailability getContentAvailability(final ProvenanceEventRecord event) {
        final String replayFailure = getReplayFailureReason(event);

        return new ContentAvailability() {
            @Override
            public String getReasonNotReplayable() {
                return replayFailure;
            }

            @Override
            public boolean isContentSame() {
                return areEqual(event.getPreviousContentClaimContainer(), event.getContentClaimContainer())
                        && areEqual(event.getPreviousContentClaimSection(), event.getContentClaimSection())
                        && areEqual(event.getPreviousContentClaimIdentifier(), event.getContentClaimIdentifier())
                        && areEqual(event.getPreviousContentClaimOffset(), event.getContentClaimOffset())
                        && areEqual(event.getPreviousFileSize(), event.getFileSize());
            }

            @Override
            public boolean isInputAvailable() {
                try {
                    return contentRepository.isAccessible(createClaim(event.getPreviousContentClaimContainer(), event.getPreviousContentClaimSection(),
                            event.getPreviousContentClaimIdentifier(), event.getPreviousContentClaimOffset()));
                } catch (final IOException e) {
                    return false;
                }
            }

            @Override
            public boolean isOutputAvailable() {
                try {
                    return contentRepository.isAccessible(createClaim(event.getContentClaimContainer(), event.getContentClaimSection(),
                            event.getContentClaimIdentifier(), event.getContentClaimOffset()));
                } catch (final IOException e) {
                    return false;
                }
            }

            private ContentClaim createClaim(final String container, final String section, final String identifier, final Long offset) {
                if (container == null || section == null || identifier == null) {
                    return null;
                }

                final ResourceClaim resourceClaim = resourceClaimManager.newResourceClaim(container, section, identifier, false, false);
                return new StandardContentClaim(resourceClaim, offset == null ? 0L : offset.longValue());
            }

            @Override
            public boolean isReplayable() {
                return replayFailure == null;
            }
        };
    }

    public InputStream getContent(final ProvenanceEventRecord provEvent, final ContentDirection direction, final String requestor, final String requestUri) throws IOException {
        requireNonNull(provEvent);
        requireNonNull(direction);
        requireNonNull(requestor);
        requireNonNull(requestUri);

        final ContentClaim claim;
        final long size;
        final long offset;
        if (direction == ContentDirection.INPUT) {
            if (provEvent.getPreviousContentClaimContainer() == null || provEvent.getPreviousContentClaimSection() == null || provEvent.getPreviousContentClaimIdentifier() == null) {
                throw new IllegalArgumentException("Input Content Claim not specified");
            }

            final ResourceClaim resourceClaim = resourceClaimManager.newResourceClaim(provEvent.getPreviousContentClaimContainer(), provEvent.getPreviousContentClaimSection(),
                provEvent.getPreviousContentClaimIdentifier(), false, false);
            claim = new StandardContentClaim(resourceClaim, provEvent.getPreviousContentClaimOffset());
            offset = provEvent.getPreviousContentClaimOffset() == null ? 0L : provEvent.getPreviousContentClaimOffset();
            size = provEvent.getPreviousFileSize();
        } else {
            if (provEvent.getContentClaimContainer() == null || provEvent.getContentClaimSection() == null || provEvent.getContentClaimIdentifier() == null) {
                throw new IllegalArgumentException("Output Content Claim not specified");
            }

            final ResourceClaim resourceClaim = resourceClaimManager.newResourceClaim(provEvent.getContentClaimContainer(), provEvent.getContentClaimSection(),
                provEvent.getContentClaimIdentifier(), false, false);

            claim = new StandardContentClaim(resourceClaim, provEvent.getContentClaimOffset());
            offset = provEvent.getContentClaimOffset() == null ? 0L : provEvent.getContentClaimOffset();
            size = provEvent.getFileSize();
        }

        final InputStream rawStream = contentRepository.read(claim);
        final ResourceClaim resourceClaim = claim.getResourceClaim();

        // Register a Provenance Event to indicate that we replayed the data.
        final ProvenanceEventRecord sendEvent = new StandardProvenanceEventRecord.Builder()
                .setEventType(ProvenanceEventType.DOWNLOAD)
                .setFlowFileUUID(provEvent.getFlowFileUuid())
                .setAttributes(provEvent.getAttributes(), Collections.emptyMap())
                .setCurrentContentClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(), offset, size)
                .setTransitUri(requestUri)
                .setEventTime(System.currentTimeMillis())
                .setFlowFileEntryDate(provEvent.getFlowFileEntryDate())
                .setLineageStartDate(provEvent.getLineageStartDate())
                .setComponentType(flowManager.getRootGroup().getName())
                .setComponentId(flowManager.getRootGroupId())
                .setDetails("Download of " + (direction == ContentDirection.INPUT ? "Input" : "Output") + " Content requested by " + requestor + " for Provenance Event " + provEvent.getEventId())
                .build();

        provenanceRepository.registerEvent(sendEvent);

        return new LimitedInputStream(rawStream, size);
    }

    public InputStream getContent(final FlowFileRecord flowFile, final String requestor, final String requestUri) throws IOException {
        requireNonNull(flowFile);
        requireNonNull(requestor);
        requireNonNull(requestUri);

        InputStream stream;
        final ResourceClaim resourceClaim;
        final ContentClaim contentClaim = flowFile.getContentClaim();
        if (contentClaim == null) {
            resourceClaim = null;
            stream = new ByteArrayInputStream(new byte[0]);
        } else {
            resourceClaim = flowFile.getContentClaim().getResourceClaim();
            stream = contentRepository.read(flowFile.getContentClaim());
            final long contentClaimOffset = flowFile.getContentClaimOffset();
            if (contentClaimOffset > 0L) {
                StreamUtils.skip(stream, contentClaimOffset);
            }

            stream = new LimitingInputStream(stream, flowFile.getSize());
        }

        // Register a Provenance Event to indicate that we replayed the data.
        final StandardProvenanceEventRecord.Builder sendEventBuilder = new StandardProvenanceEventRecord.Builder()
                .setEventType(ProvenanceEventType.DOWNLOAD)
                .setFlowFileUUID(flowFile.getAttribute(CoreAttributes.UUID.key()))
                .setAttributes(flowFile.getAttributes(), Collections.emptyMap())
                .setTransitUri(requestUri)
                .setEventTime(System.currentTimeMillis())
                .setFlowFileEntryDate(flowFile.getEntryDate())
                .setLineageStartDate(flowFile.getLineageStartDate())
                .setComponentType(flowManager.getRootGroup().getName())
                .setComponentId(flowManager.getRootGroupId())
                .setDetails("Download of Content requested by " + requestor + " for " + flowFile);

        if (contentClaim != null) {
            sendEventBuilder.setCurrentContentClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(),
                    contentClaim.getOffset() + flowFile.getContentClaimOffset(), flowFile.getSize());
        }

        final ProvenanceEventRecord sendEvent = sendEventBuilder.build();
        provenanceRepository.registerEvent(sendEvent);
        return stream;
    }

    private String getReplayFailureReason(final ProvenanceEventRecord event) {
        // Check that the event is a valid type.
        final ProvenanceEventType type = event.getEventType();
        if (type == ProvenanceEventType.JOIN) {
            return "Cannot replay events that are created from multiple parents";
        }

        // Make sure event has the Content Claim info
        final Long contentSize = event.getPreviousFileSize();
        final String contentClaimId = event.getPreviousContentClaimIdentifier();
        final String contentClaimSection = event.getPreviousContentClaimSection();
        final String contentClaimContainer = event.getPreviousContentClaimContainer();

        if (contentSize == null || contentClaimId == null || contentClaimSection == null || contentClaimContainer == null) {
            return "Cannot replay data from Provenance Event because the event does not contain the required Content Claim";
        }

        try {
            final ResourceClaim resourceClaim = resourceClaimManager.newResourceClaim(contentClaimContainer, contentClaimSection, contentClaimId, false, false);
            final ContentClaim contentClaim = new StandardContentClaim(resourceClaim, event.getPreviousContentClaimOffset());

            if (!contentRepository.isAccessible(contentClaim)) {
                return "Content is no longer available in Content Repository";
            }
        } catch (final IOException ioe) {
            return "Failed to determine whether or not content was available in Content Repository due to " + ioe.toString();
        }

        // Make sure that the source queue exists
        if (event.getSourceQueueIdentifier() == null) {
            return "Cannot replay data from Provenance Event because the event does not specify the Source FlowFile Queue";
        }

        final List<Connection> connections = flowManager.getRootGroup().findAllConnections();
        FlowFileQueue queue = null;
        for (final Connection connection : connections) {
            if (event.getSourceQueueIdentifier().equals(connection.getIdentifier())) {
                queue = connection.getFlowFileQueue();
                break;
            }
        }

        if (queue == null) {
            return "Cannot replay data from Provenance Event because the Source FlowFile Queue with ID " + event.getSourceQueueIdentifier() + " no longer exists";
        }

        return null;
    }

    public ProvenanceEventRecord replayFlowFile(final long provenanceEventRecordId, final NiFiUser user) throws IOException {
        final ProvenanceEventRecord record = provenanceRepository.getEvent(provenanceEventRecordId, user);
        if (record == null) {
            throw new IllegalStateException("Cannot find Provenance Event with ID " + provenanceEventRecordId);
        }

        return replayFlowFile(record, user);
    }

    public ProvenanceEventRecord replayFlowFile(final ProvenanceEventRecord event, final NiFiUser user) throws IOException {
        if (event == null) {
            throw new NullPointerException();
        }

        // Check that the event is a valid type.
        final ProvenanceEventType type = event.getEventType();
        if (type == ProvenanceEventType.JOIN) {
            throw new IllegalArgumentException("Cannot replay events that are created from multiple parents");
        }

        // Make sure event has the Content Claim info
        final Long contentSize = event.getPreviousFileSize();
        final String contentClaimId = event.getPreviousContentClaimIdentifier();
        final String contentClaimSection = event.getPreviousContentClaimSection();
        final String contentClaimContainer = event.getPreviousContentClaimContainer();

        if (contentSize == null || contentClaimId == null || contentClaimSection == null || contentClaimContainer == null) {
            throw new IllegalArgumentException("Cannot replay data from Provenance Event because the event does not contain the required Content Claim");
        }

        // Make sure that the source queue exists
        if (event.getSourceQueueIdentifier() == null) {
            throw new IllegalArgumentException("Cannot replay data from Provenance Event because the event does not specify the Source FlowFile Queue");
        }

        final List<Connection> connections = flowManager.getRootGroup().findAllConnections();
        FlowFileQueue queue = null;
        for (final Connection connection : connections) {
            if (event.getSourceQueueIdentifier().equals(connection.getIdentifier())) {
                queue = connection.getFlowFileQueue();
                break;
            }
        }

        if (queue == null) {
            throw new IllegalStateException("Cannot replay data from Provenance Event because the Source FlowFile Queue with ID " + event.getSourceQueueIdentifier() + " no longer exists");
        }

        // Create the ContentClaim. To do so, we first need the appropriate Resource Claim. Because we don't know whether or
        // not the Resource Claim is still active, we first call ResourceClaimManager.getResourceClaim. If this returns
        // null, then we know that the Resource Claim is no longer active and can just create a new one that is not writable.
        // It's critical though that we first call getResourceClaim because otherwise, if the Resource Claim is active and we
        // create a new one that is not writable, we could end up archiving or destroying the Resource Claim while it's still
        // being written to by the Content Repository. This is important only because we are creating a FlowFile with this Resource
        // Claim. If, for instance, we are simply creating the claim to request its content, as in #getContentAvailability, etc.
        // then this is not necessary.
        ResourceClaim resourceClaim = resourceClaimManager.getResourceClaim(event.getPreviousContentClaimContainer(),
            event.getPreviousContentClaimSection(), event.getPreviousContentClaimIdentifier());
        if (resourceClaim == null) {
            resourceClaim = resourceClaimManager.newResourceClaim(event.getPreviousContentClaimContainer(),
                event.getPreviousContentClaimSection(), event.getPreviousContentClaimIdentifier(), false, false);
        }

        // Increment Claimant Count, since we will now be referencing the Content Claim
        resourceClaimManager.incrementClaimantCount(resourceClaim);
        final long claimOffset = event.getPreviousContentClaimOffset() == null ? 0L : event.getPreviousContentClaimOffset().longValue();
        final StandardContentClaim contentClaim = new StandardContentClaim(resourceClaim, claimOffset);
        contentClaim.setLength(event.getPreviousFileSize() == null ? -1L : event.getPreviousFileSize());

        if (!contentRepository.isAccessible(contentClaim)) {
            resourceClaimManager.decrementClaimantCount(resourceClaim);
            throw new IllegalStateException("Cannot replay data from Provenance Event because the data is no longer available in the Content Repository");
        }

        final String parentUUID = event.getFlowFileUuid();

        final String newFlowFileUUID = UUID.randomUUID().toString();

        // We need to create a new FlowFile by populating it with information from the
        // Provenance Event. Particularly of note here is that we are setting the FlowFile's
        // contentClaimOffset to 0. This is done for backward compatibility reasons. ContentClaim
        // used to not have a concept of an offset, and the offset was tied only to the FlowFile. This
        // was later refactored, so that the offset was part of the ContentClaim. If we set the offset
        // in both places, we'll end up skipping over that many bytes twice instead of once (once to get
        // to the beginning of the Content Claim and again to get to the offset within that Content Claim).
        // To avoid this, we just always set the offset in the Content Claim itself and set the
        // FlowFileRecord's contentClaimOffset to 0.
        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                // Copy relevant info from source FlowFile
                .addAttributes(event.getPreviousAttributes())
                .contentClaim(contentClaim)
                .contentClaimOffset(0L) // use 0 because we used the content claim offset in the Content Claim itself
                .entryDate(System.currentTimeMillis())
                .id(flowFileRepository.getNextFlowFileSequence())
                .lineageStart(event.getLineageStartDate(), 0L)
                .size(contentSize.longValue())
                // Create a new UUID and add attributes indicating that this is a replay
                .addAttribute("flowfile.replay", "true")
                .addAttribute("flowfile.replay.timestamp", String.valueOf(new Date()))
                .addAttribute(CoreAttributes.UUID.key(), newFlowFileUUID)
                // remove attributes that may have existed on the source FlowFile that we don't want to exist on the new FlowFile
                .removeAttributes(CoreAttributes.DISCARD_REASON.key(), CoreAttributes.ALTERNATE_IDENTIFIER.key())
                // build the record
                .build();

        // Register a Provenance Event to indicate that we replayed the data.
        final ProvenanceEventRecord replayEvent = new StandardProvenanceEventRecord.Builder()
                .setEventType(ProvenanceEventType.REPLAY)
                .addChildUuid(newFlowFileUUID)
                .addParentUuid(parentUUID)
                .setFlowFileUUID(parentUUID)
                .setAttributes(Collections.emptyMap(), flowFileRecord.getAttributes())
                .setCurrentContentClaim(event.getContentClaimContainer(), event.getContentClaimSection(), event.getContentClaimIdentifier(), event.getContentClaimOffset(), event.getFileSize())
                .setDetails("Replay requested by " + user.getIdentity())
                .setEventTime(System.currentTimeMillis())
                .setFlowFileEntryDate(System.currentTimeMillis())
                .setLineageStartDate(event.getLineageStartDate())
                .setComponentType(event.getComponentType())
                .setComponentId(event.getComponentId())
                .build();
        provenanceRepository.registerEvent(replayEvent);

        // Update the FlowFile Repository to indicate that we have added the FlowFile to the flow
        final StandardRepositoryRecord record = new StandardRepositoryRecord(queue);
        record.setWorking(flowFileRecord);
        record.setDestination(queue);
        flowFileRepository.updateRepository(Collections.singleton(record));

        // Enqueue the data
        queue.put(flowFileRecord);

        return replayEvent;
    }

    public boolean isConnected() {
        rwLock.readLock().lock();
        try {
            return connectionStatus != null && connectionStatus.getState() == NodeConnectionState.CONNECTED;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void setConnectionStatus(final NodeConnectionStatus connectionStatus) {
        rwLock.writeLock().lock();
        try {
            this.connectionStatus = connectionStatus;

            // update the heartbeat bean
            this.heartbeatBeanRef.set(new HeartbeatBean(flowManager.getRootGroup(), isPrimary()));
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void heartbeat() {
        if (!isClustered()) {
            return;
        }
        if (this.shutdown) {
            return;
        }

        final HeartbeatSendTask task = heartbeatSendTask.get();
        if (task != null) {
            clusterTaskExecutor.submit(task);
        }
    }

    private class HeartbeatSendTask implements Runnable {
        @Override
        public void run() {
            try (final NarCloseable narCloseable = NarCloseable.withFrameworkNar()) {
                if (heartbeatsSuspended.get()) {
                    return;
                }

                final HeartbeatMessage message = createHeartbeatMessage();
                if (message == null) {
                    LOG.debug("No heartbeat to send");
                    return;
                }

                heartbeater.send(message);
            } catch (final UnknownServiceAddressException usae) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(usae.getMessage());
                }
            } catch (final Throwable ex) {
                LOG.warn("Failed to send heartbeat due to: " + ex);
                if (LOG.isDebugEnabled()) {
                    LOG.warn("", ex);
                }
            }
        }
    }

    HeartbeatMessage createHeartbeatMessage() {
        try {
            HeartbeatBean bean = heartbeatBeanRef.get();
            if (bean == null) {
                readLock.lock();
                try {
                    bean = new HeartbeatBean(flowManager.getRootGroup(), isPrimary());
                } finally {
                    readLock.unlock("createHeartbeatMessage");
                }
            }

            // create heartbeat payload
            final HeartbeatPayload hbPayload = new HeartbeatPayload();
            hbPayload.setSystemStartTime(systemStartTime);
            hbPayload.setActiveThreadCount(getActiveThreadCount());

            final QueueSize queueSize = getTotalFlowFileCount(bean.getRootGroup());
            hbPayload.setTotalFlowFileCount(queueSize.getObjectCount());
            hbPayload.setTotalFlowFileBytes(queueSize.getByteCount());
            hbPayload.setClusterStatus(clusterCoordinator.getConnectionStatuses());

            // create heartbeat message
            final NodeIdentifier nodeId = getNodeId();
            if (nodeId == null) {
                LOG.warn("Cannot create Heartbeat Message because node's identifier is not known at this time");
                return null;
            }

            final Heartbeat heartbeat = new Heartbeat(nodeId, connectionStatus, hbPayload.marshal());
            final HeartbeatMessage message = new HeartbeatMessage();
            message.setHeartbeat(heartbeat);

            LOG.debug("Generated heartbeat");

            return message;
        } catch (final Throwable ex) {
            LOG.warn("Failed to create heartbeat due to: " + ex, ex);
            return null;
        }
    }

    private void updateRemoteProcessGroups() {
        final List<RemoteProcessGroup> remoteGroups = flowManager.getRootGroup().findAllRemoteProcessGroups();
        for (final RemoteProcessGroup remoteGroup : remoteGroups) {
            try {
                remoteGroup.refreshFlowContents();
            } catch (final CommunicationsException e) {
                LOG.warn("Unable to communicate with remote instance {} due to {}", remoteGroup, e.toString());
                if (LOG.isDebugEnabled()) {
                    LOG.warn("", e);
                }
            }
        }
    }



    public Integer getRemoteSiteListeningPort() {
        return remoteInputSocketPort;
    }

    public Integer getRemoteSiteListeningHttpPort() {
        return remoteInputHttpPort;
    }

    public Boolean isRemoteSiteCommsSecure() {
        return isSiteToSiteSecure;
    }

    public StandardProcessScheduler getProcessScheduler() {
        return processScheduler;
    }

    public AuditService getAuditService() {
        return auditService;
    }

    public ProvenanceRepository getProvenanceRepository() {
        return provenanceRepository;
    }

    public StatusHistoryDTO getConnectionStatusHistory(final String connectionId) {
        return getConnectionStatusHistory(connectionId, null, null, Integer.MAX_VALUE);
    }

    public StatusHistoryDTO getConnectionStatusHistory(final String connectionId, final Date startTime, final Date endTime, final int preferredDataPoints) {
        return StatusHistoryUtil.createStatusHistoryDTO(componentStatusRepository.getConnectionStatusHistory(connectionId, startTime, endTime, preferredDataPoints));
    }

    public StatusHistoryDTO getProcessorStatusHistory(final String processorId, final boolean includeCounters) {
        return getProcessorStatusHistory(processorId, null, null, Integer.MAX_VALUE, includeCounters);
    }

    public StatusHistoryDTO getProcessorStatusHistory(final String processorId, final Date startTime, final Date endTime, final int preferredDataPoints, final boolean includeCounters) {
        return StatusHistoryUtil.createStatusHistoryDTO(componentStatusRepository.getProcessorStatusHistory(processorId, startTime, endTime, preferredDataPoints, includeCounters));
    }

    public StatusHistoryDTO getProcessGroupStatusHistory(final String processGroupId) {
        return getProcessGroupStatusHistory(processGroupId, null, null, Integer.MAX_VALUE);
    }

    public StatusHistoryDTO getProcessGroupStatusHistory(final String processGroupId, final Date startTime, final Date endTime, final int preferredDataPoints) {
        return StatusHistoryUtil.createStatusHistoryDTO(componentStatusRepository.getProcessGroupStatusHistory(processGroupId, startTime, endTime, preferredDataPoints));
    }

    public StatusHistoryDTO getRemoteProcessGroupStatusHistory(final String remoteGroupId) {
        return getRemoteProcessGroupStatusHistory(remoteGroupId, null, null, Integer.MAX_VALUE);
    }

    public StatusHistoryDTO getRemoteProcessGroupStatusHistory(final String remoteGroupId, final Date startTime, final Date endTime, final int preferredDataPoints) {
        return StatusHistoryUtil.createStatusHistoryDTO(componentStatusRepository.getRemoteProcessGroupStatusHistory(remoteGroupId, startTime, endTime, preferredDataPoints));
    }

    private static class HeartbeatBean {

        private final ProcessGroup rootGroup;
        private final boolean primary;

        public HeartbeatBean(final ProcessGroup rootGroup, final boolean primary) {
            this.rootGroup = rootGroup;
            this.primary = primary;
        }

        public ProcessGroup getRootGroup() {
            return rootGroup;
        }

        public boolean isPrimary() {
            return primary;
        }
    }
}
