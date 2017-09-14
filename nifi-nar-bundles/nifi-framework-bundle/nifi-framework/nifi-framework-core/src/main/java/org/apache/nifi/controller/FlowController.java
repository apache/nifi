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

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;

import org.apache.commons.collections4.Predicate;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.action.Action;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.lifecycle.OnAdded;
import org.apache.nifi.annotation.lifecycle.OnConfigurationRestored;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.DataAuthorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.bundle.Bundle;
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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.LocalPort;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Size;
import org.apache.nifi.connectable.StandardConnection;
import org.apache.nifi.controller.cluster.ClusterProtocolHeartbeater;
import org.apache.nifi.controller.cluster.Heartbeater;
import org.apache.nifi.controller.exception.CommunicationsException;
import org.apache.nifi.controller.exception.ComponentLifeCycleException;
import org.apache.nifi.controller.exception.ControllerServiceInstantiationException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.label.StandardLabel;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.controller.leader.election.LeaderElectionStateChangeListener;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.reporting.ReportingTaskProvider;
import org.apache.nifi.controller.reporting.StandardReportingInitializationContext;
import org.apache.nifi.controller.reporting.StandardReportingTaskNode;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.CounterRepository;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.QueueProvider;
import org.apache.nifi.controller.repository.RepositoryStatusReport;
import org.apache.nifi.controller.repository.StandardCounterRepository;
import org.apache.nifi.controller.repository.StandardFlowFileRecord;
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
import org.apache.nifi.controller.scheduling.ProcessContextFactory;
import org.apache.nifi.controller.scheduling.QuartzSchedulingAgent;
import org.apache.nifi.controller.scheduling.StandardProcessScheduler;
import org.apache.nifi.controller.scheduling.TimerDrivenSchedulingAgent;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.serialization.FlowSerializer;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;
import org.apache.nifi.controller.serialization.FlowSynchronizer;
import org.apache.nifi.controller.serialization.ScheduledStateLookup;
import org.apache.nifi.controller.service.ControllerServiceInvocationHandler;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.controller.service.StandardControllerServiceProvider;
import org.apache.nifi.controller.state.manager.StandardStateManagerProvider;
import org.apache.nifi.controller.state.server.ZooKeeperStateServer;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.RunStatus;
import org.apache.nifi.controller.status.TransmissionStatus;
import org.apache.nifi.controller.status.history.ComponentStatusRepository;
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
import org.apache.nifi.groups.RemoteProcessGroupPortDescriptor;
import org.apache.nifi.groups.StandardProcessGroup;
import org.apache.nifi.history.History;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.ControllerServiceLogObserver;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepository;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.logging.ProcessorLogObserver;
import org.apache.nifi.logging.ReportingTaskLogObserver;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.processor.GhostProcessor;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.processor.StandardProcessorInitializationContext;
import org.apache.nifi.processor.StandardValidationContextFactory;
import org.apache.nifi.provenance.IdentifierLookup;
import org.apache.nifi.provenance.ProvenanceAuthorizableFactory;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.variable.MutableVariableRegistry;
import org.apache.nifi.registry.variable.StandardComponentVariableRegistry;
import org.apache.nifi.remote.HttpRemoteSiteListener;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.RemoteResourceManager;
import org.apache.nifi.remote.RemoteSiteListener;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.SocketRemoteSiteListener;
import org.apache.nifi.remote.StandardRemoteProcessGroup;
import org.apache.nifi.remote.StandardRemoteProcessGroupPortDescriptor;
import org.apache.nifi.remote.StandardRootGroupPort;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.cluster.NodeInformant;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.protocol.socket.SocketFlowFileServerProtocol;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.GhostReportingTask;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.stream.io.LimitingInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.util.ComponentIdGenerator;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.BatchSettingsDTO;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RelationshipDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.ClientHandlerException;

public class FlowController implements EventAccess, ControllerServiceProvider, ReportingTaskProvider,
    QueueProvider, Authorizable, ProvenanceAuthorizableFactory, NodeTypeProvider, IdentifierLookup, ReloadComponent {

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

    public static final String ROOT_GROUP_ID_ALIAS = "root";
    public static final String DEFAULT_ROOT_GROUP_NAME = "NiFi Flow";

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
    private final ConcurrentMap<String, ReportingTaskNode> reportingTasks = new ConcurrentHashMap<>();
    private final VariableRegistry variableRegistry;
    private final ConcurrentMap<String, ControllerServiceNode> rootControllerServices = new ConcurrentHashMap<>();

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

    private final AtomicReference<ProcessGroup> rootGroupRef = new AtomicReference<>();
    private final List<Connectable> startConnectablesAfterInitialization;
    private final List<RemoteGroupPort> startRemoteGroupPortsAfterInitialization;
    private final LeaderElectionManager leaderElectionManager;
    private final ClusterCoordinator clusterCoordinator;

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
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private static final Logger LOG = LoggerFactory.getLogger(FlowController.class);

    public static FlowController createStandaloneInstance(
            final FlowFileEventRepository flowFileEventRepo,
            final NiFiProperties properties,
            final Authorizer authorizer,
            final AuditService auditService,
            final StringEncryptor encryptor,
            final BulletinRepository bulletinRepo,
            final VariableRegistry variableRegistry) {

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
                /* variable registry */ variableRegistry);
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
            final VariableRegistry variableRegistry) {

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
                variableRegistry);

        return flowController;
    }

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
            final VariableRegistry variableRegistry) {

        maxTimerDrivenThreads = new AtomicInteger(10);
        maxEventDrivenThreads = new AtomicInteger(5);

        this.encryptor = encryptor;
        this.nifiProperties = nifiProperties;
        this.heartbeatMonitor = heartbeatMonitor;
        sslContext = SslContextFactory.createSslContext(nifiProperties, false);
        extensionManager = new ExtensionManager();
        this.clusterCoordinator = clusterCoordinator;

        timerDrivenEngineRef = new AtomicReference<>(new FlowEngine(maxTimerDrivenThreads.get(), "Timer-Driven Process"));
        eventDrivenEngineRef = new AtomicReference<>(new FlowEngine(maxEventDrivenThreads.get(), "Event-Driven Process"));

        final FlowFileRepository flowFileRepo = createFlowFileRepository(nifiProperties, resourceClaimManager);
        flowFileRepository = flowFileRepo;
        flowFileEventRepository = flowFileEventRepo;
        counterRepositoryRef = new AtomicReference<>(new StandardCounterRepository());

        bulletinRepository = bulletinRepo;
        this.variableRegistry = variableRegistry == null ? VariableRegistry.EMPTY_REGISTRY : variableRegistry;

        try {
            this.provenanceRepository = createProvenanceRepository(nifiProperties);
            this.provenanceRepository.initialize(createEventReporter(bulletinRepository), authorizer, this, this);
        } catch (final Exception e) {
            throw new RuntimeException("Unable to create Provenance Repository", e);
        }

        try {
            this.contentRepository = createContentRepository(nifiProperties);
        } catch (final Exception e) {
            throw new RuntimeException("Unable to create Content Repository", e);
        }

        try {
            this.stateManagerProvider = StandardStateManagerProvider.create(nifiProperties, this.variableRegistry);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        processScheduler = new StandardProcessScheduler(this, encryptor, stateManagerProvider, this.nifiProperties);
        eventDrivenWorkerQueue = new EventDrivenWorkerQueue(false, false, processScheduler);

        final ProcessContextFactory contextFactory = new ProcessContextFactory(contentRepository, flowFileRepository, flowFileEventRepository, counterRepositoryRef.get(), provenanceRepository);

        eventDrivenSchedulingAgent = new EventDrivenSchedulingAgent(
            eventDrivenEngineRef.get(), this, stateManagerProvider, eventDrivenWorkerQueue, contextFactory, maxEventDrivenThreads.get(), encryptor);
        processScheduler.setSchedulingAgent(SchedulingStrategy.EVENT_DRIVEN, eventDrivenSchedulingAgent);

        final QuartzSchedulingAgent quartzSchedulingAgent = new QuartzSchedulingAgent(this, timerDrivenEngineRef.get(), contextFactory, encryptor);
        final TimerDrivenSchedulingAgent timerDrivenAgent = new TimerDrivenSchedulingAgent(this, timerDrivenEngineRef.get(), contextFactory, encryptor, this.nifiProperties);
        processScheduler.setSchedulingAgent(SchedulingStrategy.TIMER_DRIVEN, timerDrivenAgent);
        // PRIMARY_NODE_ONLY is deprecated, but still exists to handle processors that are still defined with it (they haven't been re-configured with executeNode = PRIMARY).
        processScheduler.setSchedulingAgent(SchedulingStrategy.PRIMARY_NODE_ONLY, timerDrivenAgent);
        processScheduler.setSchedulingAgent(SchedulingStrategy.CRON_DRIVEN, quartzSchedulingAgent);

        startConnectablesAfterInitialization = new ArrayList<>();
        startRemoteGroupPortsAfterInitialization = new ArrayList<>();
        this.authorizer = authorizer;
        this.auditService = auditService;

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

        this.configuredForClustering = configuredForClustering;
        this.heartbeatDelaySeconds = (int) FormatUtils.getTimeDuration(nifiProperties.getNodeHeartbeatInterval(), TimeUnit.SECONDS);

        this.snippetManager = new SnippetManager();

        final ProcessGroup rootGroup = new StandardProcessGroup(ComponentIdGenerator.generateId().toString(), this, processScheduler,
            nifiProperties, encryptor, this, new MutableVariableRegistry(this.variableRegistry));
        rootGroup.setName(DEFAULT_ROOT_GROUP_NAME);
        setRootGroup(rootGroup);
        instanceId = ComponentIdGenerator.generateId().toString();

        controllerServiceProvider = new StandardControllerServiceProvider(this, processScheduler, bulletinRepository, stateManagerProvider, this.variableRegistry, this.nifiProperties);

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

        componentStatusRepository = createComponentStatusRepository();
        timerDrivenEngineRef.get().scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                componentStatusRepository.capture(getControllerStatus());
            }
        }, snapshotMillis, snapshotMillis, TimeUnit.MILLISECONDS);

        this.connectionStatus = new NodeConnectionStatus(nodeId, DisconnectionCode.NOT_YET_CONNECTED);
        heartbeatBeanRef.set(new HeartbeatBean(rootGroup, false));
        this.leaderElectionManager = leaderElectionManager;

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
        } else {
            heartbeater = null;
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

    private static FlowFileRepository createFlowFileRepository(final NiFiProperties properties, final ResourceClaimManager contentClaimManager) {
        final String implementationClassName = properties.getProperty(NiFiProperties.FLOWFILE_REPOSITORY_IMPLEMENTATION, DEFAULT_FLOWFILE_REPO_IMPLEMENTATION);
        if (implementationClassName == null) {
            throw new RuntimeException("Cannot create FlowFile Repository because the NiFi Properties is missing the following property: "
                    + NiFiProperties.FLOWFILE_REPOSITORY_IMPLEMENTATION);
        }

        try {
            final FlowFileRepository created = NarThreadContextClassLoader.createInstance(implementationClassName, FlowFileRepository.class, properties);
            synchronized (created) {
                created.initialize(contentClaimManager);
            }
            return created;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static FlowFileSwapManager createSwapManager(final NiFiProperties properties) {
        final String implementationClassName = properties.getProperty(NiFiProperties.FLOWFILE_SWAP_MANAGER_IMPLEMENTATION, DEFAULT_SWAP_MANAGER_IMPLEMENTATION);
        if (implementationClassName == null) {
            return null;
        }

        try {
            return NarThreadContextClassLoader.createInstance(implementationClassName, FlowFileSwapManager.class, properties);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static EventReporter createEventReporter(final BulletinRepository bulletinRepository) {
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
            final List<Connection> connections = getGroup(getRootGroupId()).findAllConnections();

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

            flowFileRepository.loadFlowFiles(this, maxIdFromSwapFiles + 1);

            // Begin expiring FlowFiles that are old
            final ProcessContextFactory contextFactory = new ProcessContextFactory(contentRepository, flowFileRepository,
                flowFileEventRepository, counterRepositoryRef.get(), provenanceRepository);
            processScheduler.scheduleFrameworkTask(new ExpireFlowFiles(this, contextFactory), "Expire FlowFiles", 30L, 30L, TimeUnit.SECONDS);

            // now that we've loaded the FlowFiles, this has restored our ContentClaims' states, so we can tell the
            // ContentRepository to purge superfluous files
            contentRepository.cleanup();

            for (final RemoteSiteListener listener : externalSiteListeners) {
                listener.start();
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

            initialized.set(true);
        } finally {
            writeLock.unlock();
        }
    }

    private void notifyComponentsConfigurationRestored() {
        for (final ProcessorNode procNode : getGroup(getRootGroupId()).findAllProcessors()) {
            final Processor processor = procNode.getProcessor();
            try (final NarCloseable nc = NarCloseable.withComponentNarLoader(processor.getClass(), processor.getIdentifier())) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, processor);
            }
        }

        for (final ControllerServiceNode serviceNode : getAllControllerServices()) {
            final ControllerService service = serviceNode.getControllerServiceImplementation();

            try (final NarCloseable nc = NarCloseable.withComponentNarLoader(service.getClass(), service.getIdentifier())) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, service);
            }
        }

        for (final ReportingTaskNode taskNode : getAllReportingTasks()) {
            final ReportingTask task = taskNode.getReportingTask();

            try (final NarCloseable nc = NarCloseable.withComponentNarLoader(task.getClass(), task.getIdentifier())) {
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
            if (startDelayedComponents) {
                LOG.info("Starting {} processors/ports/funnels", startConnectablesAfterInitialization.size() + startRemoteGroupPortsAfterInitialization.size());
                for (final Connectable connectable : startConnectablesAfterInitialization) {
                    if (connectable.getScheduledState() == ScheduledState.DISABLED) {
                        continue;
                    }

                    try {
                        if (connectable instanceof ProcessorNode) {
                            connectable.getProcessGroup().startProcessor((ProcessorNode) connectable);
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
        } finally {
            writeLock.unlock();
        }
    }

    private ContentRepository createContentRepository(final NiFiProperties properties) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        final String implementationClassName = properties.getProperty(NiFiProperties.CONTENT_REPOSITORY_IMPLEMENTATION, DEFAULT_CONTENT_REPO_IMPLEMENTATION);
        if (implementationClassName == null) {
            throw new RuntimeException("Cannot create Content Repository because the NiFi Properties is missing the following property: "
                    + NiFiProperties.CONTENT_REPOSITORY_IMPLEMENTATION);
        }

        try {
            final ContentRepository contentRepo = NarThreadContextClassLoader.createInstance(implementationClassName, ContentRepository.class, properties);
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
            return NarThreadContextClassLoader.createInstance(implementationClassName, ProvenanceRepository.class, properties);
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
            return NarThreadContextClassLoader.createInstance(implementationClassName, ComponentStatusRepository.class, nifiProperties);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
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
        final FlowFileSwapManager swapManager = createSwapManager(nifiProperties);
        final EventReporter eventReporter = createEventReporter(getBulletinRepository());

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

        return builder.id(requireNonNull(id).intern())
                .name(name == null ? null : name.intern())
                .relationships(relationships)
                .source(requireNonNull(source))
                .destination(destination)
                .swapManager(swapManager)
                .queueSwapThreshold(nifiProperties.getQueueSwapThreshold())
                .eventReporter(eventReporter)
                .resourceClaimManager(resourceClaimManager)
                .flowFileRepository(flowFileRepository)
                .provenanceRepository(provenanceRepository)
                .build();
    }

    /**
     * Creates a new Label
     *
     * @param id identifier
     * @param text label text
     * @return new label
     * @throws NullPointerException if either argument is null
     */
    public Label createLabel(final String id, final String text) {
        return new StandardLabel(requireNonNull(id).intern(), text);
    }

    /**
     * Creates a funnel
     *
     * @param id funnel id
     * @return new funnel
     */
    public Funnel createFunnel(final String id) {
        return new StandardFunnel(id.intern(), null, processScheduler);
    }

    /**
     * Creates a Port to use as an Input Port for a Process Group
     *
     * @param id port identifier
     * @param name port name
     * @return new port
     * @throws NullPointerException if the ID or name is not unique
     * @throws IllegalStateException if an Input Port already exists with the
     * same name or id.
     */
    public Port createLocalInputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);
        return new LocalPort(id, name, null, ConnectableType.INPUT_PORT, processScheduler);
    }

    /**
     * Creates a Port to use as an Output Port for a Process Group
     *
     * @param id port id
     * @param name port name
     * @return new port
     * @throws NullPointerException if the ID or name is not unique
     * @throws IllegalStateException if an Input Port already exists with the
     * same name or id.
     */
    public Port createLocalOutputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);
        return new LocalPort(id, name, null, ConnectableType.OUTPUT_PORT, processScheduler);
    }

    /**
     * Creates a ProcessGroup with the given ID
     *
     * @param id group id
     * @return new group
     * @throws NullPointerException if the argument is null
     */
    public ProcessGroup createProcessGroup(final String id) {
        return new StandardProcessGroup(requireNonNull(id).intern(), this, processScheduler, nifiProperties, encryptor, this, new MutableVariableRegistry(variableRegistry));
    }

    /**
     * <p>
     * Creates a new ProcessorNode with the given type and identifier and
     * initializes it invoking the methods annotated with {@link OnAdded}.
     * </p>
     *
     * @param type processor type
     * @param id processor id
     * @param coordinate the coordinate of the bundle for this processor
     * @return new processor
     * @throws NullPointerException if either arg is null
     * @throws ProcessorInstantiationException if the processor cannot be
     * instantiated for any reason
     */
    public ProcessorNode createProcessor(final String type, final String id, final BundleCoordinate coordinate) throws ProcessorInstantiationException {
        return createProcessor(type, id, coordinate, true);
    }

    /**
     * <p>
     * Creates a new ProcessorNode with the given type and identifier and
     * optionally initializes it.
     * </p>
     *
     * @param type the fully qualified Processor class name
     * @param id the unique ID of the Processor
     * @param coordinate the bundle coordinate for this processor
     * @param firstTimeAdded whether or not this is the first time this
     * Processor is added to the graph. If {@code true}, will invoke methods
     * annotated with the {@link OnAdded} annotation.
     * @return new processor node
     * @throws NullPointerException if either arg is null
     * @throws ProcessorInstantiationException if the processor cannot be
     * instantiated for any reason
     */
    public ProcessorNode createProcessor(final String type, String id, final BundleCoordinate coordinate, final boolean firstTimeAdded) throws ProcessorInstantiationException {
        return createProcessor(type, id, coordinate, Collections.emptySet(), firstTimeAdded, true);
    }

    /**
     * <p>
     * Creates a new ProcessorNode with the given type and identifier and
     * optionally initializes it.
     * </p>
     *
     * @param type the fully qualified Processor class name
     * @param id the unique ID of the Processor
     * @param coordinate the bundle coordinate for this processor
     * @param firstTimeAdded whether or not this is the first time this
     * Processor is added to the graph. If {@code true}, will invoke methods
     * annotated with the {@link OnAdded} annotation.
     * @return new processor node
     * @throws NullPointerException if either arg is null
     * @throws ProcessorInstantiationException if the processor cannot be
     * instantiated for any reason
     */
    public ProcessorNode createProcessor(final String type, String id, final BundleCoordinate coordinate, final Set<URL> additionalUrls,
                                         final boolean firstTimeAdded, final boolean registerLogObserver) throws ProcessorInstantiationException {
        id = id.intern();

        boolean creationSuccessful;
        LoggableComponent<Processor> processor;
        try {
            processor = instantiateProcessor(type, id, coordinate, additionalUrls);
            creationSuccessful = true;
        } catch (final ProcessorInstantiationException pie) {
            LOG.error("Could not create Processor of type " + type + " for ID " + id + "; creating \"Ghost\" implementation", pie);
            final GhostProcessor ghostProc = new GhostProcessor();
            ghostProc.setIdentifier(id);
            ghostProc.setCanonicalClassName(type);
            processor = new LoggableComponent<>(ghostProc, coordinate, null);
            creationSuccessful = false;
        }

        final ComponentVariableRegistry componentVarRegistry = new StandardComponentVariableRegistry(this.variableRegistry);
        final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(controllerServiceProvider, componentVarRegistry);
        final ProcessorNode procNode;
        if (creationSuccessful) {
            procNode = new StandardProcessorNode(processor, id, validationContextFactory, processScheduler, controllerServiceProvider,
                nifiProperties, componentVarRegistry, this);
        } else {
            final String simpleClassName = type.contains(".") ? StringUtils.substringAfterLast(type, ".") : type;
            final String componentType = "(Missing) " + simpleClassName;
            procNode = new StandardProcessorNode(processor, id, validationContextFactory, processScheduler, controllerServiceProvider,
                componentType, type, nifiProperties, componentVarRegistry, this, true);
        }

        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
        if (registerLogObserver) {
            logRepository.addObserver(StandardProcessorNode.BULLETIN_OBSERVER_ID, LogLevel.WARN, new ProcessorLogObserver(getBulletinRepository(), procNode));
        }

        try {
            final Class<?> procClass = procNode.getProcessor().getClass();
            if(procClass.isAnnotationPresent(DefaultSettings.class)) {
                DefaultSettings ds = procClass.getAnnotation(DefaultSettings.class);
                try {
                    procNode.setYieldPeriod(ds.yieldDuration());
                } catch(Throwable ex) {
                    LOG.error(String.format("Error while setting yield period from DefaultSettings annotation:%s",ex.getMessage()),ex);
                }
                try {

                    procNode.setPenalizationPeriod(ds.penaltyDuration());
                } catch(Throwable ex) {
                    LOG.error(String.format("Error while setting penalty duration from DefaultSettings annotation:%s",ex.getMessage()),ex);
                }

                // calling setBulletinLevel changes the level in the LogRepository so we only want to do this when
                // the caller said to register the log observer, otherwise we could be changing the level when we didn't mean to
                if (registerLogObserver) {
                    try {
                        procNode.setBulletinLevel(ds.bulletinLevel());
                    } catch (Throwable ex) {
                        LOG.error(String.format("Error while setting bulletin level from DefaultSettings annotation:%s", ex.getMessage()), ex);
                    }
                }
            }
        } catch (Throwable ex) {
            LOG.error(String.format("Error while setting default settings from DefaultSettings annotation: %s",ex.getMessage()),ex);
        }

        if (firstTimeAdded) {
            try (final NarCloseable x = NarCloseable.withComponentNarLoader(procNode.getProcessor().getClass(), procNode.getProcessor().getIdentifier())) {
                ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, procNode.getProcessor());
            } catch (final Exception e) {
                if (registerLogObserver) {
                    logRepository.removeObserver(StandardProcessorNode.BULLETIN_OBSERVER_ID);
                }
                throw new ComponentLifeCycleException("Failed to invoke @OnAdded methods of " + procNode.getProcessor(), e);
            }

            if (firstTimeAdded) {
                try (final NarCloseable nc = NarCloseable.withComponentNarLoader(procNode.getProcessor().getClass(), procNode.getProcessor().getIdentifier())) {
                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, procNode.getProcessor());
                }
            }
        }

        return procNode;
    }

    private LoggableComponent<Processor> instantiateProcessor(final String type, final String identifier, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls)
            throws ProcessorInstantiationException {

        final Bundle processorBundle = ExtensionManager.getBundle(bundleCoordinate);
        if (processorBundle == null) {
            throw new ProcessorInstantiationException("Unable to find bundle for coordinate " + bundleCoordinate.getCoordinate());
        }

        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final ClassLoader detectedClassLoaderForInstance = ExtensionManager.createInstanceClassLoader(type, identifier, processorBundle, additionalUrls);
            final Class<?> rawClass = Class.forName(type, true, detectedClassLoaderForInstance);
            Thread.currentThread().setContextClassLoader(detectedClassLoaderForInstance);

            final Class<? extends Processor> processorClass = rawClass.asSubclass(Processor.class);
            final Processor processor = processorClass.newInstance();

            final ComponentLog componentLogger = new SimpleProcessLogger(identifier, processor);
            final ProcessorInitializationContext ctx = new StandardProcessorInitializationContext(identifier, componentLogger, this, this, nifiProperties);
            processor.initialize(ctx);

            LogRepositoryFactory.getRepository(identifier).setLogger(componentLogger);

            return new LoggableComponent<>(processor, bundleCoordinate, componentLogger);
        } catch (final Throwable t) {
            throw new ProcessorInstantiationException(type, t);
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }

    @Override
    public void reload(final ProcessorNode existingNode, final String newType, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls)
            throws ProcessorInstantiationException {
        if (existingNode == null) {
            throw new IllegalStateException("Existing ProcessorNode cannot be null");
        }

        final String id = existingNode.getProcessor().getIdentifier();

        // ghost components will have a null logger
        if (existingNode.getLogger() != null) {
            existingNode.getLogger().debug("Reloading component {} to type {} from bundle {}", new Object[]{id, newType, bundleCoordinate});
        }

        // createProcessor will create a new instance class loader for the same id so
        // save the instance class loader to use it for calling OnRemoved on the existing processor
        final ClassLoader existingInstanceClassLoader = ExtensionManager.getInstanceClassLoader(id);

        // create a new node with firstTimeAdded as true so lifecycle methods get fired
        // attempt the creation to make sure it works before firing the OnRemoved methods below
        final ProcessorNode newNode = createProcessor(newType, id, bundleCoordinate, additionalUrls, true, false);

        // call OnRemoved for the existing processor using the previous instance class loader
        try (final NarCloseable x = NarCloseable.withComponentNarLoader(existingInstanceClassLoader)) {
            final StandardProcessContext processContext = new StandardProcessContext(
                existingNode, controllerServiceProvider, encryptor, getStateManagerProvider().getStateManager(id));
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, existingNode.getProcessor(), processContext);
        } finally {
            ExtensionManager.closeURLClassLoader(id, existingInstanceClassLoader);
        }

        // set the new processor in the existing node
        final LoggableComponent<Processor> newProcessor = new LoggableComponent<>(newNode.getProcessor(), newNode.getBundleCoordinate(), newNode.getLogger());
        existingNode.setProcessor(newProcessor);
        existingNode.setExtensionMissing(newNode.isExtensionMissing());

        // need to refresh the properties in case we are changing from ghost component to real component
        existingNode.refreshProperties();
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
            readLock.unlock();
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
     * Creates a Port to use as an Input Port for the root Process Group, which
     * is used for Site-to-Site communications
     *
     * @param id port id
     * @param name port name
     * @return new port
     * @throws NullPointerException if the ID or name is not unique
     * @throws IllegalStateException if an Input Port already exists with the
     * same name or id.
     */
    public Port createRemoteInputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);
        return new StandardRootGroupPort(id, name, null, TransferDirection.RECEIVE, ConnectableType.INPUT_PORT,
                authorizer, getBulletinRepository(), processScheduler, Boolean.TRUE.equals(isSiteToSiteSecure), nifiProperties);
    }

    /**
     * Creates a Port to use as an Output Port for the root Process Group, which
     * is used for Site-to-Site communications and will queue flow files waiting
     * to be delivered to remote instances
     *
     * @param id port id
     * @param name port name
     * @return new port
     * @throws NullPointerException if the ID or name is not unique
     * @throws IllegalStateException if an Input Port already exists with the
     * same name or id.
     */
    public Port createRemoteOutputPort(String id, String name) {
        id = requireNonNull(id).intern();
        name = requireNonNull(name).intern();
        verifyPortIdDoesNotExist(id);
        return new StandardRootGroupPort(id, name, null, TransferDirection.SEND, ConnectableType.OUTPUT_PORT,
                authorizer, getBulletinRepository(), processScheduler, Boolean.TRUE.equals(isSiteToSiteSecure), nifiProperties);
    }

    /**
     * Creates a new Remote Process Group with the given ID that points to the
     * given URI
     *
     * @param id group id
     * @param uris group uris, multiple url can be specified in comma-separated format
     * @return new group
     * @throws NullPointerException if either argument is null
     * @throws IllegalArgumentException if <code>uri</code> is not a valid URI.
     */
    public RemoteProcessGroup createRemoteProcessGroup(final String id, final String uris) {
        return new StandardRemoteProcessGroup(requireNonNull(id).intern(), uris, null, this, sslContext, nifiProperties);
    }

    public ProcessGroup getRootGroup() {
        return rootGroupRef.get();
    }

    /**
     * Verifies that no output port exists with the given id or name. If this
     * does not hold true, throws an IllegalStateException
     *
     * @param id port identifier
     * @throws IllegalStateException port already exists
     */
    private void verifyPortIdDoesNotExist(final String id) {
        final ProcessGroup rootGroup = getRootGroup();
        Port port = rootGroup.findOutputPort(id);
        if (port != null) {
            throw new IllegalStateException("An Input Port already exists with ID " + id);
        }
        port = rootGroup.findInputPort(id);
        if (port != null) {
            throw new IllegalStateException("An Input Port already exists with ID " + id);
        }
    }

    /**
     * @return the name of this controller, which is also the name of the Root
     * Group.
     */
    public String getName() {
        return getRootGroup().getName();
    }

    /**
     * Sets the name for the Root Group, which also changes the name for the
     * controller.
     *
     * @param name of root group
     */
    public void setName(final String name) {
        getRootGroup().setName(name);
    }

    /**
     * @return the comments of this controller, which is also the comment of the
     * Root Group
     */
    public String getComments() {
        return getRootGroup().getComments();
    }

    /**
     * Sets the comments
     *
     * @param comments for the Root Group, which also changes the comment for
     * the controller
     */
    public void setComments(final String comments) {
        getRootGroup().setComments(comments);
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
            this.readLock.unlock();
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
        stopAllProcessors();

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

            clusterTaskExecutor.shutdownNow();

            if (zooKeeperStateServer != null) {
                zooKeeperStateServer.shutdown();
            }

            // Trigger any processors' methods marked with @OnShutdown to be called
            getRootGroup().shutdown();

            stateManagerProvider.shutdown();

            // invoke any methods annotated with @OnShutdown on Controller Services
            for (final ControllerServiceNode serviceNode : getAllControllerServices()) {
                try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(serviceNode.getControllerServiceImplementation().getClass(), serviceNode.getIdentifier())) {
                    final ConfigurationContext configContext = new StandardConfigurationContext(serviceNode, controllerServiceProvider, null, variableRegistry);
                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnShutdown.class, serviceNode.getControllerServiceImplementation(), configContext);
                }
            }

            // invoke any methods annotated with @OnShutdown on Reporting Tasks
            for (final ReportingTaskNode taskNode : getAllReportingTasks()) {
                final ConfigurationContext configContext = taskNode.getConfigurationContext();
                try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(taskNode.getReportingTask().getClass(), taskNode.getIdentifier())) {
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
            readLock.unlock();
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
    public void serialize(final FlowSerializer serializer, final OutputStream os) throws FlowSerializationException {
        readLock.lock();
        try {
            final ScheduledStateLookup scheduledStateLookup = new ScheduledStateLookup() {
                @Override
                public ScheduledState getScheduledState(final ProcessorNode procNode) {
                    if (startConnectablesAfterInitialization.contains(procNode)) {
                        return ScheduledState.RUNNING;
                    }

                    return procNode.getScheduledState();
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

            serializer.serialize(this, os, scheduledStateLookup);
        } finally {
            readLock.unlock();
        }
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
            writeLock.unlock();
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

    public void setMaxTimerDrivenThreadCount(final int maxThreadCount) {
        writeLock.lock();
        try {
            setMaxThreadCount(maxThreadCount, this.timerDrivenEngineRef.get(), this.maxTimerDrivenThreads);
        } finally {
            writeLock.unlock();
        }
    }

    public void setMaxEventDrivenThreadCount(final int maxThreadCount) {
        writeLock.lock();
        try {
            setMaxThreadCount(maxThreadCount, this.eventDrivenEngineRef.get(), this.maxEventDrivenThreads);
            processScheduler.setMaxThreadCount(SchedulingStrategy.EVENT_DRIVEN, maxThreadCount);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Updates the number of threads that can be simultaneously used for
     * executing processors.
     *
     * @param maxThreadCount This method must be called while holding the write
     * lock!
     */
    private void setMaxThreadCount(final int maxThreadCount, final FlowEngine engine, final AtomicInteger maxThreads) {
        if (maxThreadCount < 1) {
            throw new IllegalArgumentException();
        }

        maxThreads.getAndSet(maxThreadCount);
        if (null != engine && engine.getCorePoolSize() < maxThreadCount) {
            engine.setCorePoolSize(maxThreads.intValue());
        }
    }

    /**
     * @return the ID of the root group
     */
    public String getRootGroupId() {
        return getRootGroup().getIdentifier();
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
            rootGroupRef.set(group);
            for (final RemoteSiteListener listener : externalSiteListeners) {
                listener.setRootGroup(group);
            }

            // update the heartbeat bean
            this.heartbeatBeanRef.set(new HeartbeatBean(group, isPrimary()));
            allProcessGroups.put(group.getIdentifier(), group);
            allProcessGroups.put(ROOT_GROUP_ID_ALIAS, group);
        } finally {
            writeLock.unlock();
        }
    }

    public SystemDiagnostics getSystemDiagnostics() {
        final SystemDiagnosticsFactory factory = new SystemDiagnosticsFactory();
        return factory.create(flowFileRepository, contentRepository, provenanceRepository);
    }

    //
    // ProcessGroup access
    //
    /**
     * Updates the process group corresponding to the specified DTO. Any field
     * in DTO that is <code>null</code> (with the exception of the required ID)
     * will be ignored.
     *
     * @param dto group
     * @throws ProcessorInstantiationException
     *
     * @throws IllegalStateException if no process group can be found with the
     * ID of DTO or with the ID of the DTO's parentGroupId, if the template ID
     * specified is invalid, or if the DTO's Parent Group ID changes but the
     * parent group has incoming or outgoing connections
     *
     * @throws NullPointerException if the DTO or its ID is null
     */
    public void updateProcessGroup(final ProcessGroupDTO dto) throws ProcessorInstantiationException {
        final ProcessGroup group = lookupGroup(requireNonNull(dto).getId());

        final String name = dto.getName();
        final PositionDTO position = dto.getPosition();
        final String comments = dto.getComments();

        if (name != null) {
            group.setName(name);
        }
        if (position != null) {
            group.setPosition(toPosition(position));
        }
        if (comments != null) {
            group.setComments(comments);
        }
    }

    private Position toPosition(final PositionDTO dto) {
        return new Position(dto.getX(), dto.getY());
    }

    //
    // Snippet
    //
    /**
     * Creates an instance of the given snippet and adds the components to the
     * given group
     *
     * @param group group
     * @param dto dto
     *
     * @throws NullPointerException if either argument is null
     * @throws IllegalStateException if the snippet is not valid because a
     * component in the snippet has an ID that is not unique to this flow, or
     * because it shares an Input Port or Output Port at the root level whose
     * name already exists in the given ProcessGroup, or because the Template
     * contains a Processor or a Prioritizer whose class is not valid within
     * this instance of NiFi.
     * @throws ProcessorInstantiationException if unable to instantiate a
     * processor
     */
    public void instantiateSnippet(final ProcessGroup group, final FlowSnippetDTO dto) throws ProcessorInstantiationException {
        writeLock.lock();
        try {
            validateSnippetContents(requireNonNull(group), dto);

            //
            // Instantiate Controller Services
            //
            for (final ControllerServiceDTO controllerServiceDTO : dto.getControllerServices()) {
                final BundleCoordinate bundleCoordinate = BundleUtils.getBundle(controllerServiceDTO.getType(), controllerServiceDTO.getBundle());
                final ControllerServiceNode serviceNode = createControllerService(controllerServiceDTO.getType(), controllerServiceDTO.getId(), bundleCoordinate, Collections.emptySet(),true);

                serviceNode.setAnnotationData(controllerServiceDTO.getAnnotationData());
                serviceNode.setComments(controllerServiceDTO.getComments());
                serviceNode.setName(controllerServiceDTO.getName());

                group.addControllerService(serviceNode);
            }

            // configure controller services. We do this after creating all of them in case 1 service
            // references another service.
            for (final ControllerServiceDTO controllerServiceDTO : dto.getControllerServices()) {
                final String serviceId = controllerServiceDTO.getId();
                final ControllerServiceNode serviceNode = getControllerServiceNode(serviceId);
                serviceNode.setProperties(controllerServiceDTO.getProperties());
            }

            //
            // Instantiate the labels
            //
            for (final LabelDTO labelDTO : dto.getLabels()) {
                final Label label = createLabel(labelDTO.getId(), labelDTO.getLabel());
                label.setPosition(toPosition(labelDTO.getPosition()));
                if (labelDTO.getWidth() != null && labelDTO.getHeight() != null) {
                    label.setSize(new Size(labelDTO.getWidth(), labelDTO.getHeight()));
                }

                label.setStyle(labelDTO.getStyle());
                group.addLabel(label);
            }

            // Instantiate the funnels
            for (final FunnelDTO funnelDTO : dto.getFunnels()) {
                final Funnel funnel = createFunnel(funnelDTO.getId());
                funnel.setPosition(toPosition(funnelDTO.getPosition()));
                group.addFunnel(funnel);
            }

            //
            // Instantiate Input Ports & Output Ports
            //
            for (final PortDTO portDTO : dto.getInputPorts()) {
                final Port inputPort;
                if (group.isRootGroup()) {
                    inputPort = createRemoteInputPort(portDTO.getId(), portDTO.getName());
                    inputPort.setMaxConcurrentTasks(portDTO.getConcurrentlySchedulableTaskCount());
                    if (portDTO.getGroupAccessControl() != null) {
                        ((RootGroupPort) inputPort).setGroupAccessControl(portDTO.getGroupAccessControl());
                    }
                    if (portDTO.getUserAccessControl() != null) {
                        ((RootGroupPort) inputPort).setUserAccessControl(portDTO.getUserAccessControl());
                    }
                } else {
                    inputPort = createLocalInputPort(portDTO.getId(), portDTO.getName());
                }

                inputPort.setPosition(toPosition(portDTO.getPosition()));
                inputPort.setProcessGroup(group);
                inputPort.setComments(portDTO.getComments());
                group.addInputPort(inputPort);
            }

            for (final PortDTO portDTO : dto.getOutputPorts()) {
                final Port outputPort;
                if (group.isRootGroup()) {
                    outputPort = createRemoteOutputPort(portDTO.getId(), portDTO.getName());
                    outputPort.setMaxConcurrentTasks(portDTO.getConcurrentlySchedulableTaskCount());
                    if (portDTO.getGroupAccessControl() != null) {
                        ((RootGroupPort) outputPort).setGroupAccessControl(portDTO.getGroupAccessControl());
                    }
                    if (portDTO.getUserAccessControl() != null) {
                        ((RootGroupPort) outputPort).setUserAccessControl(portDTO.getUserAccessControl());
                    }
                } else {
                    outputPort = createLocalOutputPort(portDTO.getId(), portDTO.getName());
                }

                outputPort.setPosition(toPosition(portDTO.getPosition()));
                outputPort.setProcessGroup(group);
                outputPort.setComments(portDTO.getComments());
                group.addOutputPort(outputPort);
            }

            //
            // Instantiate the processors
            //
            for (final ProcessorDTO processorDTO : dto.getProcessors()) {
                final BundleCoordinate bundleCoordinate = BundleUtils.getBundle(processorDTO.getType(), processorDTO.getBundle());
                final ProcessorNode procNode = createProcessor(processorDTO.getType(), processorDTO.getId(), bundleCoordinate);

                procNode.setPosition(toPosition(processorDTO.getPosition()));
                procNode.setProcessGroup(group);

                final ProcessorConfigDTO config = processorDTO.getConfig();
                procNode.setComments(config.getComments());
                if (config.isLossTolerant() != null) {
                    procNode.setLossTolerant(config.isLossTolerant());
                }
                procNode.setName(processorDTO.getName());

                procNode.setYieldPeriod(config.getYieldDuration());
                procNode.setPenalizationPeriod(config.getPenaltyDuration());
                procNode.setBulletinLevel(LogLevel.valueOf(config.getBulletinLevel()));
                procNode.setAnnotationData(config.getAnnotationData());
                procNode.setStyle(processorDTO.getStyle());

                if (config.getRunDurationMillis() != null) {
                    procNode.setRunDuration(config.getRunDurationMillis(), TimeUnit.MILLISECONDS);
                }

                if (config.getSchedulingStrategy() != null) {
                    procNode.setSchedulingStrategy(SchedulingStrategy.valueOf(config.getSchedulingStrategy()));
                }

                if (config.getExecutionNode() != null) {
                    procNode.setExecutionNode(ExecutionNode.valueOf(config.getExecutionNode()));
                }

                if (processorDTO.getState().equals(ScheduledState.DISABLED.toString())) {
                    procNode.disable();
                }

                // ensure that the scheduling strategy is set prior to these values
                procNode.setMaxConcurrentTasks(config.getConcurrentlySchedulableTaskCount());
                procNode.setScheduldingPeriod(config.getSchedulingPeriod());

                final Set<Relationship> relationships = new HashSet<>();
                if (processorDTO.getRelationships() != null) {
                    for (final RelationshipDTO rel : processorDTO.getRelationships()) {
                        if (rel.isAutoTerminate()) {
                            relationships.add(procNode.getRelationship(rel.getName()));
                        }
                    }
                    procNode.setAutoTerminatedRelationships(relationships);
                }

                if (config.getProperties() != null) {
                    procNode.setProperties(config.getProperties());
                }

                group.addProcessor(procNode);
            }

            //
            // Instantiate Remote Process Groups
            //
            for (final RemoteProcessGroupDTO remoteGroupDTO : dto.getRemoteProcessGroups()) {
                final RemoteProcessGroup remoteGroup = createRemoteProcessGroup(remoteGroupDTO.getId(), remoteGroupDTO.getTargetUris());
                remoteGroup.setComments(remoteGroupDTO.getComments());
                remoteGroup.setPosition(toPosition(remoteGroupDTO.getPosition()));
                remoteGroup.setCommunicationsTimeout(remoteGroupDTO.getCommunicationsTimeout());
                remoteGroup.setYieldDuration(remoteGroupDTO.getYieldDuration());
                if (remoteGroupDTO.getTransportProtocol() == null) {
                    remoteGroup.setTransportProtocol(SiteToSiteTransportProtocol.RAW);
                } else {
                    remoteGroup.setTransportProtocol(SiteToSiteTransportProtocol.valueOf(remoteGroupDTO.getTransportProtocol()));
                }

                remoteGroup.setProxyHost(remoteGroupDTO.getProxyHost());
                remoteGroup.setProxyPort(remoteGroupDTO.getProxyPort());
                remoteGroup.setProxyUser(remoteGroupDTO.getProxyUser());
                remoteGroup.setProxyPassword(remoteGroupDTO.getProxyPassword());
                remoteGroup.setProcessGroup(group);

                // set the input/output ports
                if (remoteGroupDTO.getContents() != null) {
                    final RemoteProcessGroupContentsDTO contents = remoteGroupDTO.getContents();

                    // ensure there input ports
                    if (contents.getInputPorts() != null) {
                        remoteGroup.setInputPorts(convertRemotePort(contents.getInputPorts()));
                    }

                    // ensure there are output ports
                    if (contents.getOutputPorts() != null) {
                        remoteGroup.setOutputPorts(convertRemotePort(contents.getOutputPorts()));
                    }
                }

                group.addRemoteProcessGroup(remoteGroup);
            }

            //
            // Instantiate ProcessGroups
            //
            for (final ProcessGroupDTO groupDTO : dto.getProcessGroups()) {
                final ProcessGroup childGroup = createProcessGroup(groupDTO.getId());
                childGroup.setParent(group);
                childGroup.setPosition(toPosition(groupDTO.getPosition()));
                childGroup.setComments(groupDTO.getComments());
                childGroup.setName(groupDTO.getName());
                if (groupDTO.getVariables() != null) {
                    childGroup.setVariables(groupDTO.getVariables());
                }

                group.addProcessGroup(childGroup);

                final FlowSnippetDTO contents = groupDTO.getContents();

                // we want this to be recursive, so we will create a new template that contains only
                // the contents of this child group and recursively call ourselves.
                final FlowSnippetDTO childTemplateDTO = new FlowSnippetDTO();
                childTemplateDTO.setConnections(contents.getConnections());
                childTemplateDTO.setInputPorts(contents.getInputPorts());
                childTemplateDTO.setLabels(contents.getLabels());
                childTemplateDTO.setOutputPorts(contents.getOutputPorts());
                childTemplateDTO.setProcessGroups(contents.getProcessGroups());
                childTemplateDTO.setProcessors(contents.getProcessors());
                childTemplateDTO.setFunnels(contents.getFunnels());
                childTemplateDTO.setRemoteProcessGroups(contents.getRemoteProcessGroups());
                childTemplateDTO.setControllerServices(contents.getControllerServices());
                instantiateSnippet(childGroup, childTemplateDTO);
            }

            //
            // Instantiate Connections
            //
            for (final ConnectionDTO connectionDTO : dto.getConnections()) {
                final ConnectableDTO sourceDTO = connectionDTO.getSource();
                final ConnectableDTO destinationDTO = connectionDTO.getDestination();
                final Connectable source;
                final Connectable destination;

                // locate the source and destination connectable. if this is a remote port
                // we need to locate the remote process groups. otherwise we need to
                // find the connectable given its parent group.
                // NOTE: (getConnectable returns ANY connectable, when the parent is
                // not this group only input ports or output ports should be returned. if something
                // other than a port is returned, an exception will be thrown when adding the
                // connection below.)
                // see if the source connectable is a remote port
                if (ConnectableType.REMOTE_OUTPUT_PORT.name().equals(sourceDTO.getType())) {
                    final RemoteProcessGroup remoteGroup = group.getRemoteProcessGroup(sourceDTO.getGroupId());
                    source = remoteGroup.getOutputPort(sourceDTO.getId());
                } else {
                    final ProcessGroup sourceGroup = getConnectableParent(group, sourceDTO.getGroupId());
                    source = sourceGroup.getConnectable(sourceDTO.getId());
                }

                // see if the destination connectable is a remote port
                if (ConnectableType.REMOTE_INPUT_PORT.name().equals(destinationDTO.getType())) {
                    final RemoteProcessGroup remoteGroup = group.getRemoteProcessGroup(destinationDTO.getGroupId());
                    destination = remoteGroup.getInputPort(destinationDTO.getId());
                } else {
                    final ProcessGroup destinationGroup = getConnectableParent(group, destinationDTO.getGroupId());
                    destination = destinationGroup.getConnectable(destinationDTO.getId());
                }

                // determine the selection relationships for this connection
                final Set<String> relationships = new HashSet<>();
                if (connectionDTO.getSelectedRelationships() != null) {
                    relationships.addAll(connectionDTO.getSelectedRelationships());
                }

                final Connection connection = createConnection(connectionDTO.getId(), connectionDTO.getName(), source, destination, relationships);

                if (connectionDTO.getBends() != null) {
                    final List<Position> bendPoints = new ArrayList<>();
                    for (final PositionDTO bend : connectionDTO.getBends()) {
                        bendPoints.add(new Position(bend.getX(), bend.getY()));
                    }
                    connection.setBendPoints(bendPoints);
                }

                final FlowFileQueue queue = connection.getFlowFileQueue();
                queue.setBackPressureDataSizeThreshold(connectionDTO.getBackPressureDataSizeThreshold());
                queue.setBackPressureObjectThreshold(connectionDTO.getBackPressureObjectThreshold());
                queue.setFlowFileExpiration(connectionDTO.getFlowFileExpiration());

                final List<String> prioritizers = connectionDTO.getPrioritizers();
                if (prioritizers != null) {
                    final List<String> newPrioritizersClasses = new ArrayList<>(prioritizers);
                    final List<FlowFilePrioritizer> newPrioritizers = new ArrayList<>();
                    for (final String className : newPrioritizersClasses) {
                        try {
                            newPrioritizers.add(createPrioritizer(className));
                        } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                            throw new IllegalArgumentException("Unable to set prioritizer " + className + ": " + e);
                        }
                    }
                    queue.setPriorities(newPrioritizers);
                }

                connection.setProcessGroup(group);
                group.addConnection(connection);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Converts a set of ports into a set of remote process group ports.
     *
     * @param ports ports
     * @return group descriptors
     */
    private Set<RemoteProcessGroupPortDescriptor> convertRemotePort(final Set<RemoteProcessGroupPortDTO> ports) {
        Set<RemoteProcessGroupPortDescriptor> remotePorts = null;
        if (ports != null) {
            remotePorts = new LinkedHashSet<>(ports.size());
            for (final RemoteProcessGroupPortDTO port : ports) {
                final StandardRemoteProcessGroupPortDescriptor descriptor = new StandardRemoteProcessGroupPortDescriptor();
                descriptor.setId(port.getId());
                descriptor.setName(port.getName());
                descriptor.setComments(port.getComments());
                descriptor.setTargetRunning(port.isTargetRunning());
                descriptor.setConnected(port.isConnected());
                descriptor.setConcurrentlySchedulableTaskCount(port.getConcurrentlySchedulableTaskCount());
                descriptor.setTransmitting(port.isTransmitting());
                descriptor.setUseCompression(port.getUseCompression());
                final BatchSettingsDTO batchSettings = port.getBatchSettings();
                if (batchSettings != null) {
                    descriptor.setBatchCount(batchSettings.getCount());
                    descriptor.setBatchSize(batchSettings.getSize());
                    descriptor.setBatchDuration(batchSettings.getDuration());
                }
                remotePorts.add(descriptor);
            }
        }
        return remotePorts;
    }

    /**
     * Returns the parent of the specified Connectable. This only considers this
     * group and any direct child sub groups.
     *
     * @param parentGroupId group id
     * @return parent group
     */
    private ProcessGroup getConnectableParent(final ProcessGroup group, final String parentGroupId) {
        if (areGroupsSame(group.getIdentifier(), parentGroupId)) {
            return group;
        } else {
            return group.getProcessGroup(parentGroupId);
        }
    }

    private void verifyBundleInSnippet(final BundleDTO requiredBundle, final Set<BundleCoordinate> supportedBundles) {
        final BundleCoordinate requiredCoordinate = new BundleCoordinate(requiredBundle.getGroup(), requiredBundle.getArtifact(), requiredBundle.getVersion());
        if (!supportedBundles.contains(requiredCoordinate)) {
            throw new IllegalStateException("Unsupported bundle: " + requiredCoordinate);
        }
    }

    private void verifyProcessorsInSnippet(final FlowSnippetDTO templateContents, final Map<String, Set<BundleCoordinate>> supportedTypes) {
        if (templateContents.getProcessors() != null) {
            templateContents.getProcessors().forEach(processor -> {
                if (processor.getBundle() == null) {
                    throw new IllegalArgumentException("Processor bundle must be specified.");
                }

                if (supportedTypes.containsKey(processor.getType())) {
                    verifyBundleInSnippet(processor.getBundle(), supportedTypes.get(processor.getType()));
                } else {
                    throw new IllegalStateException("Invalid Processor Type: " + processor.getType());
                }
            });
        }

        if (templateContents.getProcessGroups() != null) {
            templateContents.getProcessGroups().forEach(processGroup -> {
                verifyProcessorsInSnippet(processGroup.getContents(), supportedTypes);
            });
        }
    }

    private void verifyControllerServicesInSnippet(final FlowSnippetDTO templateContents, final Map<String, Set<BundleCoordinate>> supportedTypes) {
        if (templateContents.getControllerServices() != null) {
            templateContents.getControllerServices().forEach(controllerService -> {
                if (supportedTypes.containsKey(controllerService.getType())) {
                    if (controllerService.getBundle() == null) {
                        throw new IllegalArgumentException("Controller Service bundle must be specified.");
                    }

                    verifyBundleInSnippet(controllerService.getBundle(), supportedTypes.get(controllerService.getType()));
                } else {
                    throw new IllegalStateException("Invalid Controller Service Type: " + controllerService.getType());
                }
            });
        }

        if (templateContents.getProcessGroups() != null) {
            templateContents.getProcessGroups().forEach(processGroup -> {
                verifyControllerServicesInSnippet(processGroup.getContents(), supportedTypes);
            });
        }
    }

    public void verifyComponentTypesInSnippet(final FlowSnippetDTO templateContents) {
        final Map<String, Set<BundleCoordinate>> processorClasses = new HashMap<>();
        for (final Class<?> c : ExtensionManager.getExtensions(Processor.class)) {
            final String name = c.getName();
            processorClasses.put(name, ExtensionManager.getBundles(name).stream().map(bundle -> bundle.getBundleDetails().getCoordinate()).collect(Collectors.toSet()));
        }
        verifyProcessorsInSnippet(templateContents, processorClasses);

        final Map<String, Set<BundleCoordinate>> controllerServiceClasses = new HashMap<>();
        for (final Class<?> c : ExtensionManager.getExtensions(ControllerService.class)) {
            final String name = c.getName();
            controllerServiceClasses.put(name, ExtensionManager.getBundles(name).stream().map(bundle -> bundle.getBundleDetails().getCoordinate()).collect(Collectors.toSet()));
        }
        verifyControllerServicesInSnippet(templateContents, controllerServiceClasses);

        final Set<String> prioritizerClasses = new HashSet<>();
        for (final Class<?> c : ExtensionManager.getExtensions(FlowFilePrioritizer.class)) {
            prioritizerClasses.add(c.getName());
        }

        final Set<ConnectionDTO> allConns = new HashSet<>();
        allConns.addAll(templateContents.getConnections());
        for (final ProcessGroupDTO childGroup : templateContents.getProcessGroups()) {
            allConns.addAll(findAllConnections(childGroup));
        }

        for (final ConnectionDTO conn : allConns) {
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

    /**
     * <p>
     * Verifies that the given DTO is valid, according to the following:
     *
     * <ul>
     * <li>None of the ID's in any component of the DTO can be used in this
     * flow.</li>
     * <li>The ProcessGroup to which the template's contents will be added must
     * not contain any InputPort or OutputPort with the same name as one of the
     * corresponding components in the root level of the template.</li>
     * <li>All Processors' classes must exist in this instance.</li>
     * <li>All Flow File Prioritizers' classes must exist in this instance.</li>
     * </ul>
     * </p>
     *
     * <p>
     * If any of the above statements does not hold true, an
     * {@link IllegalStateException} or a
     * {@link ProcessorInstantiationException} will be thrown.
     * </p>
     *
     * @param group group
     * @param templateContents contents
     */
    private void validateSnippetContents(final ProcessGroup group, final FlowSnippetDTO templateContents) {
        // validate the names of Input Ports
        for (final PortDTO port : templateContents.getInputPorts()) {
            if (group.getInputPortByName(port.getName()) != null) {
                throw new IllegalStateException("One or more of the proposed Port names is not available in the process group");
            }
        }

        // validate the names of Output Ports
        for (final PortDTO port : templateContents.getOutputPorts()) {
            if (group.getOutputPortByName(port.getName()) != null) {
                throw new IllegalStateException("One or more of the proposed Port names is not available in the process group");
            }
        }

        verifyComponentTypesInSnippet(templateContents);
    }

    /**
     * Recursively finds all ConnectionDTO's
     *
     * @param group group
     * @return connection dtos
     */
    private Set<ConnectionDTO> findAllConnections(final ProcessGroupDTO group) {
        final Set<ConnectionDTO> conns = new HashSet<>();
        for (final ConnectionDTO dto : group.getContents().getConnections()) {
            conns.add(dto);
        }

        for (final ProcessGroupDTO childGroup : group.getContents().getProcessGroups()) {
            conns.addAll(findAllConnections(childGroup));
        }
        return conns;
    }

    //
    // Processor access
    //
    /**
     * Indicates whether or not the two ID's point to the same ProcessGroup. If
     * either id is null, will return <code>false</code>.
     *
     * @param id1 group id
     * @param id2 other group id
     * @return true if same
     */
    public boolean areGroupsSame(final String id1, final String id2) {
        if (id1 == null || id2 == null) {
            return false;
        } else if (id1.equals(id2)) {
            return true;
        } else {
            final String comparable1 = id1.equals(ROOT_GROUP_ID_ALIAS) ? getRootGroupId() : id1;
            final String comparable2 = id2.equals(ROOT_GROUP_ID_ALIAS) ? getRootGroupId() : id2;
            return comparable1.equals(comparable2);
        }
    }

    public FlowFilePrioritizer createPrioritizer(final String type) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        FlowFilePrioritizer prioritizer;

        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final List<Bundle> prioritizerBundles = ExtensionManager.getBundles(type);
            if (prioritizerBundles.size() == 0) {
                throw new IllegalStateException(String.format("The specified class '%s' is not known to this nifi.", type));
            }
            if (prioritizerBundles.size() > 1) {
                throw new IllegalStateException(String.format("Multiple bundles found for the specified class '%s', only one is allowed.", type));
            }

            final Bundle bundle = prioritizerBundles.get(0);
            final ClassLoader detectedClassLoaderForType = bundle.getClassLoader();
            final Class<?> rawClass = Class.forName(type, true, detectedClassLoaderForType);

            Thread.currentThread().setContextClassLoader(detectedClassLoaderForType);
            final Class<? extends FlowFilePrioritizer> prioritizerClass = rawClass.asSubclass(FlowFilePrioritizer.class);
            final Object processorObj = prioritizerClass.newInstance();
            prioritizer = prioritizerClass.cast(processorObj);

            return prioritizer;
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }

    //
    // InputPort access
    //
    public PortDTO updateInputPort(final String parentGroupId, final PortDTO dto) {
        final ProcessGroup parentGroup = lookupGroup(parentGroupId);
        final Port port = parentGroup.getInputPort(dto.getId());
        if (port == null) {
            throw new IllegalStateException("No Input Port with ID " + dto.getId() + " is known as a child of ProcessGroup with ID " + parentGroupId);
        }

        final String name = dto.getName();
        if (dto.getPosition() != null) {
            port.setPosition(toPosition(dto.getPosition()));
        }

        if (name != null) {
            port.setName(name);
        }

        return createDTO(port);
    }

    private PortDTO createDTO(final Port port) {
        if (port == null) {
            return null;
        }

        final PortDTO dto = new PortDTO();
        dto.setId(port.getIdentifier());
        dto.setPosition(new PositionDTO(port.getPosition().getX(), port.getPosition().getY()));
        dto.setName(port.getName());
        dto.setParentGroupId(port.getProcessGroup().getIdentifier());

        return dto;
    }

    //
    // OutputPort access
    //
    public PortDTO updateOutputPort(final String parentGroupId, final PortDTO dto) {
        final ProcessGroup parentGroup = lookupGroup(parentGroupId);
        final Port port = parentGroup.getOutputPort(dto.getId());
        if (port == null) {
            throw new IllegalStateException("No Output Port with ID " + dto.getId() + " is known as a child of ProcessGroup with ID " + parentGroupId);
        }

        final String name = dto.getName();
        if (name != null) {
            port.setName(name);
        }

        if (dto.getPosition() != null) {
            port.setPosition(toPosition(dto.getPosition()));
        }

        return createDTO(port);
    }

    //
    // Processor/Prioritizer/Filter Class Access
    //
    @SuppressWarnings("rawtypes")
    public Set<Class> getFlowFileProcessorClasses() {
        return ExtensionManager.getExtensions(Processor.class);
    }

    @SuppressWarnings("rawtypes")
    public Set<Class> getFlowFileComparatorClasses() {
        return ExtensionManager.getExtensions(FlowFilePrioritizer.class);
    }

    /**
     * Returns the ProcessGroup with the given ID
     *
     * @param id group
     * @return the process group or null if not group is found
     */
    private ProcessGroup lookupGroup(final String id) {
        final ProcessGroup group = getGroup(id);
        if (group == null) {
            throw new IllegalStateException("No Group with ID " + id + " exists");
        }
        return group;
    }

    /**
     * Returns the ProcessGroup with the given ID
     *
     * @param id group id
     * @return the process group or null if not group is found
     */
    public ProcessGroup getGroup(final String id) {
        return allProcessGroups.get(requireNonNull(id));
    }

    public void onProcessGroupAdded(final ProcessGroup group) {
        allProcessGroups.put(group.getIdentifier(), group);
    }

    public void onProcessGroupRemoved(final ProcessGroup group) {
        allProcessGroups.remove(group.getIdentifier());
    }

    public void onProcessorAdded(final ProcessorNode procNode) {
        allProcessors.put(procNode.getIdentifier(), procNode);
    }

    public void onProcessorRemoved(final ProcessorNode procNode) {
        allProcessors.remove(procNode.getIdentifier());
    }

    public ProcessorNode getProcessorNode(final String id) {
        return allProcessors.get(id);
    }

    public void onConnectionAdded(final Connection connection) {
        allConnections.put(connection.getIdentifier(), connection);
    }

    public void onConnectionRemoved(final Connection connection) {
        allConnections.remove(connection.getIdentifier());
    }

    public Connection getConnection(final String id) {
        return allConnections.get(id);
    }

    public void onInputPortAdded(final Port inputPort) {
        allInputPorts.put(inputPort.getIdentifier(), inputPort);
    }

    public void onInputPortRemoved(final Port inputPort) {
        allInputPorts.remove(inputPort.getIdentifier());
    }

    public Port getInputPort(final String id) {
        return allInputPorts.get(id);
    }

    public void onOutputPortAdded(final Port outputPort) {
        allOutputPorts.put(outputPort.getIdentifier(), outputPort);
    }

    public void onOutputPortRemoved(final Port outputPort) {
        allOutputPorts.remove(outputPort.getIdentifier());
    }

    public Port getOutputPort(final String id) {
        return allOutputPorts.get(id);
    }

    public void onFunnelAdded(final Funnel funnel) {
        allFunnels.put(funnel.getIdentifier(), funnel);
    }

    public void onFunnelRemoved(final Funnel funnel) {
        allFunnels.remove(funnel.getIdentifier());
    }

    public Funnel getFunnel(final String id) {
        return allFunnels.get(id);
    }
    /**
     * Returns the status of all components in the controller. This request is
     * not in the context of a user so the results will be unfiltered.
     *
     * @return the component status
     */
    @Override
    public ProcessGroupStatus getControllerStatus() {
        return getGroupStatus(getRootGroupId());
    }

    /**
     * Returns the status of all components in the specified group. This request
     * is not in the context of a user so the results will be unfiltered.
     *
     * @param groupId group id
     * @return the component status
     */
    @Override
    public ProcessGroupStatus getGroupStatus(final String groupId) {
        return getGroupStatus(groupId, getProcessorStats());
    }

    /**
     * Returns the status for components in the specified group. This request is
     * made by the specified user so the results will be filtered accordingly.
     *
     * @param groupId group id
     * @param user user making request
     * @return the component status
     */
    public ProcessGroupStatus getGroupStatus(final String groupId, final NiFiUser user) {
        return getGroupStatus(groupId, getProcessorStats(), user);
    }

    /**
     * Returns the status for the components in the specified group with the
     * specified report. This request is not in the context of a user so the
     * results will be unfiltered.
     *
     * @param groupId group id
     * @param statusReport report
     * @return the component status
     */
    public ProcessGroupStatus getGroupStatus(final String groupId, final RepositoryStatusReport statusReport) {
        final ProcessGroup group = getGroup(groupId);

        // this was invoked with no user context so the results will be unfiltered... necessary for aggregating status history
        return getGroupStatus(group, statusReport, authorizable -> true);
    }

    /**
     * Returns the status for the components in the specified group with the
     * specified report. This request is made by the specified user so the
     * results will be filtered accordingly.
     *
     * @param groupId group id
     * @param statusReport report
     * @param user user making request
     * @return the component status
     */
    public ProcessGroupStatus getGroupStatus(final String groupId, final RepositoryStatusReport statusReport, final NiFiUser user) {
        final ProcessGroup group = getGroup(groupId);

        // on demand status request for a specific user... require authorization per component and filter results as appropriate
        return getGroupStatus(group, statusReport, authorizable -> authorizable.isAuthorized(authorizer, RequestAction.READ, user));
    }

    /**
     * Returns the status for the components in the specified group with the
     * specified report. The results will be filtered by executing the specified
     * predicate.
     *
     * @param group group id
     * @param statusReport report
     * @param isAuthorized is authorized check
     * @return the component status
     */
    public ProcessGroupStatus getGroupStatus(final ProcessGroup group, final RepositoryStatusReport statusReport, final Predicate<Authorizable> isAuthorized) {
        if (group == null) {
            return null;
        }

        final ProcessGroupStatus status = new ProcessGroupStatus();
        status.setId(group.getIdentifier());
        status.setName(isAuthorized.evaluate(group) ? group.getName() : group.getIdentifier());
        int activeGroupThreads = 0;
        long bytesRead = 0L;
        long bytesWritten = 0L;
        int queuedCount = 0;
        long queuedContentSize = 0L;
        int flowFilesIn = 0;
        long bytesIn = 0L;
        int flowFilesOut = 0;
        long bytesOut = 0L;
        int flowFilesReceived = 0;
        long bytesReceived = 0L;
        int flowFilesSent = 0;
        long bytesSent = 0L;
        int flowFilesTransferred = 0;
        long bytesTransferred = 0;

        // set status for processors
        final Collection<ProcessorStatus> processorStatusCollection = new ArrayList<>();
        status.setProcessorStatus(processorStatusCollection);
        for (final ProcessorNode procNode : group.getProcessors()) {
            final ProcessorStatus procStat = getProcessorStatus(statusReport, procNode, isAuthorized);
            processorStatusCollection.add(procStat);
            activeGroupThreads += procStat.getActiveThreadCount();
            bytesRead += procStat.getBytesRead();
            bytesWritten += procStat.getBytesWritten();

            flowFilesReceived += procStat.getFlowFilesReceived();
            bytesReceived += procStat.getBytesReceived();
            flowFilesSent += procStat.getFlowFilesSent();
            bytesSent += procStat.getBytesSent();
        }

        // set status for local child groups
        final Collection<ProcessGroupStatus> localChildGroupStatusCollection = new ArrayList<>();
        status.setProcessGroupStatus(localChildGroupStatusCollection);
        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            final ProcessGroupStatus childGroupStatus = getGroupStatus(childGroup, statusReport, isAuthorized);
            localChildGroupStatusCollection.add(childGroupStatus);
            activeGroupThreads += childGroupStatus.getActiveThreadCount();
            bytesRead += childGroupStatus.getBytesRead();
            bytesWritten += childGroupStatus.getBytesWritten();
            queuedCount += childGroupStatus.getQueuedCount();
            queuedContentSize += childGroupStatus.getQueuedContentSize();

            flowFilesReceived += childGroupStatus.getFlowFilesReceived();
            bytesReceived += childGroupStatus.getBytesReceived();
            flowFilesSent += childGroupStatus.getFlowFilesSent();
            bytesSent += childGroupStatus.getBytesSent();

            flowFilesTransferred += childGroupStatus.getFlowFilesTransferred();
            bytesTransferred += childGroupStatus.getBytesTransferred();
        }

        // set status for remote child groups
        final Collection<RemoteProcessGroupStatus> remoteProcessGroupStatusCollection = new ArrayList<>();
        status.setRemoteProcessGroupStatus(remoteProcessGroupStatusCollection);
        for (final RemoteProcessGroup remoteGroup : group.getRemoteProcessGroups()) {
            final RemoteProcessGroupStatus remoteStatus = createRemoteGroupStatus(remoteGroup, statusReport, isAuthorized);
            if (remoteStatus != null) {
                remoteProcessGroupStatusCollection.add(remoteStatus);

                flowFilesReceived += remoteStatus.getReceivedCount();
                bytesReceived += remoteStatus.getReceivedContentSize();
                flowFilesSent += remoteStatus.getSentCount();
                bytesSent += remoteStatus.getSentContentSize();
            }
        }

        // connection status
        final Collection<ConnectionStatus> connectionStatusCollection = new ArrayList<>();
        status.setConnectionStatus(connectionStatusCollection);

        // get the connection and remote port status
        for (final Connection conn : group.getConnections()) {
            final boolean isConnectionAuthorized = isAuthorized.evaluate(conn);
            final boolean isSourceAuthorized = isAuthorized.evaluate(conn.getSource());
            final boolean isDestinationAuthorized = isAuthorized.evaluate(conn.getDestination());

            final ConnectionStatus connStatus = new ConnectionStatus();
            connStatus.setId(conn.getIdentifier());
            connStatus.setGroupId(conn.getProcessGroup().getIdentifier());
            connStatus.setSourceId(conn.getSource().getIdentifier());
            connStatus.setSourceName(isSourceAuthorized ? conn.getSource().getName() : conn.getSource().getIdentifier());
            connStatus.setDestinationId(conn.getDestination().getIdentifier());
            connStatus.setDestinationName(isDestinationAuthorized ? conn.getDestination().getName() : conn.getDestination().getIdentifier());
            connStatus.setBackPressureDataSizeThreshold(conn.getFlowFileQueue().getBackPressureDataSizeThreshold());
            connStatus.setBackPressureObjectThreshold(conn.getFlowFileQueue().getBackPressureObjectThreshold());

            final FlowFileEvent connectionStatusReport = statusReport.getReportEntry(conn.getIdentifier());
            if (connectionStatusReport != null) {
                connStatus.setInputBytes(connectionStatusReport.getContentSizeIn());
                connStatus.setInputCount(connectionStatusReport.getFlowFilesIn());
                connStatus.setOutputBytes(connectionStatusReport.getContentSizeOut());
                connStatus.setOutputCount(connectionStatusReport.getFlowFilesOut());

                flowFilesTransferred += connectionStatusReport.getFlowFilesIn() + connectionStatusReport.getFlowFilesOut();
                bytesTransferred += connectionStatusReport.getContentSizeIn() + connectionStatusReport.getContentSizeOut();
            }

            if (isConnectionAuthorized) {
                if (StringUtils.isNotBlank(conn.getName())) {
                    connStatus.setName(conn.getName());
                } else if (conn.getRelationships() != null && !conn.getRelationships().isEmpty()) {
                    final Collection<String> relationships = new ArrayList<>(conn.getRelationships().size());
                    for (final Relationship relationship : conn.getRelationships()) {
                        relationships.add(relationship.getName());
                    }
                    connStatus.setName(StringUtils.join(relationships, ", "));
                }
            } else {
                connStatus.setName(conn.getIdentifier());
            }

            final QueueSize queueSize = conn.getFlowFileQueue().size();
            final int connectionQueuedCount = queueSize.getObjectCount();
            final long connectionQueuedBytes = queueSize.getByteCount();
            if (connectionQueuedCount > 0) {
                connStatus.setQueuedBytes(connectionQueuedBytes);
                connStatus.setQueuedCount(connectionQueuedCount);
            }
            connectionStatusCollection.add(connStatus);
            queuedCount += connectionQueuedCount;
            queuedContentSize += connectionQueuedBytes;

            final Connectable source = conn.getSource();
            if (ConnectableType.REMOTE_OUTPUT_PORT.equals(source.getConnectableType())) {
                final RemoteGroupPort remoteOutputPort = (RemoteGroupPort) source;
                activeGroupThreads += processScheduler.getActiveThreadCount(remoteOutputPort);
            }

            final Connectable destination = conn.getDestination();
            if (ConnectableType.REMOTE_INPUT_PORT.equals(destination.getConnectableType())) {
                final RemoteGroupPort remoteInputPort = (RemoteGroupPort) destination;
                activeGroupThreads += processScheduler.getActiveThreadCount(remoteInputPort);
            }
        }

        // status for input ports
        final Collection<PortStatus> inputPortStatusCollection = new ArrayList<>();
        status.setInputPortStatus(inputPortStatusCollection);

        final Set<Port> inputPorts = group.getInputPorts();
        for (final Port port : inputPorts) {
            final boolean isInputPortAuthorized = isAuthorized.evaluate(port);

            final PortStatus portStatus = new PortStatus();
            portStatus.setId(port.getIdentifier());
            portStatus.setGroupId(port.getProcessGroup().getIdentifier());
            portStatus.setName(isInputPortAuthorized ? port.getName() : port.getIdentifier());
            portStatus.setActiveThreadCount(processScheduler.getActiveThreadCount(port));

            // determine the run status
            if (ScheduledState.RUNNING.equals(port.getScheduledState())) {
                portStatus.setRunStatus(RunStatus.Running);
            } else if (ScheduledState.DISABLED.equals(port.getScheduledState())) {
                portStatus.setRunStatus(RunStatus.Disabled);
            } else if (!port.isValid()) {
                portStatus.setRunStatus(RunStatus.Invalid);
            } else {
                portStatus.setRunStatus(RunStatus.Stopped);
            }

            // special handling for root group ports
            if (port instanceof RootGroupPort) {
                final RootGroupPort rootGroupPort = (RootGroupPort) port;
                portStatus.setTransmitting(rootGroupPort.isTransmitting());
            }

            final FlowFileEvent entry = statusReport.getReportEntries().get(port.getIdentifier());
            if (entry == null) {
                portStatus.setInputBytes(0L);
                portStatus.setInputCount(0);
                portStatus.setOutputBytes(0L);
                portStatus.setOutputCount(0);
            } else {
                final int processedCount = entry.getFlowFilesOut();
                final long numProcessedBytes = entry.getContentSizeOut();
                portStatus.setOutputBytes(numProcessedBytes);
                portStatus.setOutputCount(processedCount);

                final int inputCount = entry.getFlowFilesIn();
                final long inputBytes = entry.getContentSizeIn();
                portStatus.setInputBytes(inputBytes);
                portStatus.setInputCount(inputCount);

                flowFilesIn += inputCount;
                bytesIn += inputBytes;
                bytesWritten += entry.getBytesWritten();

                flowFilesReceived += entry.getFlowFilesReceived();
                bytesReceived += entry.getBytesReceived();
            }

            inputPortStatusCollection.add(portStatus);
            activeGroupThreads += portStatus.getActiveThreadCount();
        }

        // status for output ports
        final Collection<PortStatus> outputPortStatusCollection = new ArrayList<>();
        status.setOutputPortStatus(outputPortStatusCollection);

        final Set<Port> outputPorts = group.getOutputPorts();
        for (final Port port : outputPorts) {
            final boolean isOutputPortAuthorized = isAuthorized.evaluate(port);

            final PortStatus portStatus = new PortStatus();
            portStatus.setId(port.getIdentifier());
            portStatus.setGroupId(port.getProcessGroup().getIdentifier());
            portStatus.setName(isOutputPortAuthorized ? port.getName() : port.getIdentifier());
            portStatus.setActiveThreadCount(processScheduler.getActiveThreadCount(port));

            // determine the run status
            if (ScheduledState.RUNNING.equals(port.getScheduledState())) {
                portStatus.setRunStatus(RunStatus.Running);
            } else if (ScheduledState.DISABLED.equals(port.getScheduledState())) {
                portStatus.setRunStatus(RunStatus.Disabled);
            } else if (!port.isValid()) {
                portStatus.setRunStatus(RunStatus.Invalid);
            } else {
                portStatus.setRunStatus(RunStatus.Stopped);
            }

            // special handling for root group ports
            if (port instanceof RootGroupPort) {
                final RootGroupPort rootGroupPort = (RootGroupPort) port;
                portStatus.setTransmitting(rootGroupPort.isTransmitting());
            }

            final FlowFileEvent entry = statusReport.getReportEntries().get(port.getIdentifier());
            if (entry == null) {
                portStatus.setInputBytes(0L);
                portStatus.setInputCount(0);
                portStatus.setOutputBytes(0L);
                portStatus.setOutputCount(0);
            } else {
                final int processedCount = entry.getFlowFilesOut();
                final long numProcessedBytes = entry.getContentSizeOut();
                portStatus.setOutputBytes(numProcessedBytes);
                portStatus.setOutputCount(processedCount);

                final int inputCount = entry.getFlowFilesIn();
                final long inputBytes = entry.getContentSizeIn();
                portStatus.setInputBytes(inputBytes);
                portStatus.setInputCount(inputCount);

                bytesRead += entry.getBytesRead();

                flowFilesOut += entry.getFlowFilesOut();
                bytesOut += entry.getContentSizeOut();

                flowFilesSent = entry.getFlowFilesSent();
                bytesSent += entry.getBytesSent();
            }

            outputPortStatusCollection.add(portStatus);
            activeGroupThreads += portStatus.getActiveThreadCount();
        }

        for (final Funnel funnel : group.getFunnels()) {
            activeGroupThreads += processScheduler.getActiveThreadCount(funnel);
        }

        status.setActiveThreadCount(activeGroupThreads);
        status.setBytesRead(bytesRead);
        status.setBytesWritten(bytesWritten);
        status.setQueuedCount(queuedCount);
        status.setQueuedContentSize(queuedContentSize);
        status.setInputContentSize(bytesIn);
        status.setInputCount(flowFilesIn);
        status.setOutputContentSize(bytesOut);
        status.setOutputCount(flowFilesOut);
        status.setFlowFilesReceived(flowFilesReceived);
        status.setBytesReceived(bytesReceived);
        status.setFlowFilesSent(flowFilesSent);
        status.setBytesSent(bytesSent);
        status.setFlowFilesTransferred(flowFilesTransferred);
        status.setBytesTransferred(bytesTransferred);

        return status;
    }

    private RemoteProcessGroupStatus createRemoteGroupStatus(final RemoteProcessGroup remoteGroup, final RepositoryStatusReport statusReport, final Predicate<Authorizable> isAuthorized) {
        final boolean isRemoteProcessGroupAuthorized = isAuthorized.evaluate(remoteGroup);

        int receivedCount = 0;
        long receivedContentSize = 0L;
        int sentCount = 0;
        long sentContentSize = 0L;
        int activeThreadCount = 0;
        int activePortCount = 0;
        int inactivePortCount = 0;

        final RemoteProcessGroupStatus status = new RemoteProcessGroupStatus();
        status.setGroupId(remoteGroup.getProcessGroup().getIdentifier());
        status.setName(isRemoteProcessGroupAuthorized ? remoteGroup.getName() : remoteGroup.getIdentifier());
        status.setTargetUri(isRemoteProcessGroupAuthorized ? remoteGroup.getTargetUri() : null);

        long lineageMillis = 0L;
        int flowFilesRemoved = 0;
        int flowFilesTransferred = 0;
        for (final Port port : remoteGroup.getInputPorts()) {
            // determine if this input port is connected
            final boolean isConnected = port.hasIncomingConnection();

            // we only want to consider remote ports that we are connected to
            if (isConnected) {
                if (port.isRunning()) {
                    activePortCount++;
                } else {
                    inactivePortCount++;
                }

                activeThreadCount += processScheduler.getActiveThreadCount(port);

                final FlowFileEvent portEvent = statusReport.getReportEntry(port.getIdentifier());
                if (portEvent != null) {
                    lineageMillis += portEvent.getAggregateLineageMillis();
                    flowFilesRemoved += portEvent.getFlowFilesRemoved();
                    flowFilesTransferred += portEvent.getFlowFilesOut();
                    sentCount += portEvent.getFlowFilesSent();
                    sentContentSize += portEvent.getBytesSent();
                }
            }
        }

        for (final Port port : remoteGroup.getOutputPorts()) {
            // determine if this output port is connected
            final boolean isConnected = !port.getConnections().isEmpty();

            // we only want to consider remote ports that we are connected from
            if (isConnected) {
                if (port.isRunning()) {
                    activePortCount++;
                } else {
                    inactivePortCount++;
                }

                activeThreadCount += processScheduler.getActiveThreadCount(port);

                final FlowFileEvent portEvent = statusReport.getReportEntry(port.getIdentifier());
                if (portEvent != null) {
                    receivedCount += portEvent.getFlowFilesReceived();
                    receivedContentSize += portEvent.getBytesReceived();
                }
            }
        }

        status.setId(remoteGroup.getIdentifier());
        status.setTransmissionStatus(remoteGroup.isTransmitting() ? TransmissionStatus.Transmitting : TransmissionStatus.NotTransmitting);
        status.setActiveThreadCount(activeThreadCount);
        status.setReceivedContentSize(receivedContentSize);
        status.setReceivedCount(receivedCount);
        status.setSentContentSize(sentContentSize);
        status.setSentCount(sentCount);
        status.setActiveRemotePortCount(activePortCount);
        status.setInactiveRemotePortCount(inactivePortCount);

        final int flowFilesOutOrRemoved = flowFilesTransferred + flowFilesRemoved;
        status.setAverageLineageDuration(flowFilesOutOrRemoved == 0 ? 0 : lineageMillis / flowFilesOutOrRemoved, TimeUnit.MILLISECONDS);

        return status;
    }

    private ProcessorStatus getProcessorStatus(final RepositoryStatusReport report, final ProcessorNode procNode, final Predicate<Authorizable> isAuthorized) {
        final boolean isProcessorAuthorized = isAuthorized.evaluate(procNode);

        final ProcessorStatus status = new ProcessorStatus();
        status.setId(procNode.getIdentifier());
        status.setGroupId(procNode.getProcessGroup().getIdentifier());
        status.setName(isProcessorAuthorized ? procNode.getName() : procNode.getIdentifier());
        status.setType(isProcessorAuthorized ? procNode.getComponentType() : "Processor");

        final FlowFileEvent entry = report.getReportEntries().get(procNode.getIdentifier());
        if (entry == null) {
            status.setInputBytes(0L);
            status.setInputCount(0);
            status.setOutputBytes(0L);
            status.setOutputCount(0);
            status.setBytesWritten(0L);
            status.setBytesRead(0L);
            status.setProcessingNanos(0);
            status.setInvocations(0);
            status.setAverageLineageDuration(0L);
            status.setFlowFilesRemoved(0);
        } else {
            final int processedCount = entry.getFlowFilesOut();
            final long numProcessedBytes = entry.getContentSizeOut();
            status.setOutputBytes(numProcessedBytes);
            status.setOutputCount(processedCount);

            final int inputCount = entry.getFlowFilesIn();
            final long inputBytes = entry.getContentSizeIn();
            status.setInputBytes(inputBytes);
            status.setInputCount(inputCount);

            final long readBytes = entry.getBytesRead();
            status.setBytesRead(readBytes);

            final long writtenBytes = entry.getBytesWritten();
            status.setBytesWritten(writtenBytes);

            status.setProcessingNanos(entry.getProcessingNanoseconds());
            status.setInvocations(entry.getInvocations());

            status.setAverageLineageDuration(entry.getAverageLineageMillis());

            status.setFlowFilesReceived(entry.getFlowFilesReceived());
            status.setBytesReceived(entry.getBytesReceived());
            status.setFlowFilesSent(entry.getFlowFilesSent());
            status.setBytesSent(entry.getBytesSent());
            status.setFlowFilesRemoved(entry.getFlowFilesRemoved());

            if (isProcessorAuthorized) {
                status.setCounters(entry.getCounters());
            }
        }

        // Determine the run status and get any validation error... only validating while STOPPED
        // is a trade-off we are willing to make, even though processor validity could change due to
        // environmental conditions (property configured with a file path and the file being externally
        // removed). This saves on validation costs that would be unnecessary most of the time.
        if (ScheduledState.DISABLED.equals(procNode.getScheduledState())) {
            status.setRunStatus(RunStatus.Disabled);
        } else if (ScheduledState.RUNNING.equals(procNode.getScheduledState())) {
            status.setRunStatus(RunStatus.Running);
        } else if (!procNode.isValid()) {
            status.setRunStatus(RunStatus.Invalid);
        } else {
            status.setRunStatus(RunStatus.Stopped);
        }

        status.setActiveThreadCount(processScheduler.getActiveThreadCount(procNode));

        return status;
    }

    public void startProcessor(final String parentGroupId, final String processorId) {
        final ProcessGroup group = lookupGroup(parentGroupId);
        final ProcessorNode node = group.getProcessor(processorId);
        if (node == null) {
            throw new IllegalStateException("Cannot find ProcessorNode with ID " + processorId + " within ProcessGroup with ID " + parentGroupId);
        }

        writeLock.lock();
        try {
            if (initialized.get()) {
                group.startProcessor(node);
            } else {
                startConnectablesAfterInitialization.add(node);
            }
        } finally {
            writeLock.unlock();
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
            writeLock.unlock();
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
            writeLock.unlock();
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
            writeLock.unlock();
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

    public void stopAllProcessors() {
        stopProcessGroup(getRootGroupId());
    }

    public void startProcessGroup(final String groupId) {
        lookupGroup(groupId).startProcessing();
    }

    public void stopProcessGroup(final String groupId) {
        lookupGroup(groupId).stopProcessing();
    }

    public ReportingTaskNode createReportingTask(final String type, final BundleCoordinate bundleCoordinate) throws ReportingTaskInstantiationException {
        return createReportingTask(type, bundleCoordinate, true);
    }

    public ReportingTaskNode createReportingTask(final String type, final BundleCoordinate bundleCoordinate, final boolean firstTimeAdded) throws ReportingTaskInstantiationException {
        return createReportingTask(type, UUID.randomUUID().toString(), bundleCoordinate, firstTimeAdded);
    }

    @Override
    public ReportingTaskNode createReportingTask(final String type, final String id, final BundleCoordinate bundleCoordinate,final boolean firstTimeAdded)
            throws ReportingTaskInstantiationException {
        return createReportingTask(type, id, bundleCoordinate, Collections.emptySet(), firstTimeAdded, true);
    }

    public ReportingTaskNode createReportingTask(final String type, final String id, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls,
                                                 final boolean firstTimeAdded, final boolean register) throws ReportingTaskInstantiationException {
        if (type == null || id == null || bundleCoordinate == null) {
            throw new NullPointerException();
        }

        LoggableComponent<ReportingTask> task = null;
        boolean creationSuccessful = true;
        try {
            task = instantiateReportingTask(type, id, bundleCoordinate, additionalUrls);
        } catch (final Exception e) {
            LOG.error("Could not create Reporting Task of type " + type + " for ID " + id + "; creating \"Ghost\" implementation", e);
            final GhostReportingTask ghostTask = new GhostReportingTask();
            ghostTask.setIdentifier(id);
            ghostTask.setCanonicalClassName(type);
            task = new LoggableComponent<>(ghostTask, bundleCoordinate, null);
            creationSuccessful = false;
        }

        final ComponentVariableRegistry componentVarRegistry = new StandardComponentVariableRegistry(this.variableRegistry);
        final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(controllerServiceProvider, componentVarRegistry);
        final ReportingTaskNode taskNode;
        if (creationSuccessful) {
            taskNode = new StandardReportingTaskNode(task, id, this, processScheduler, validationContextFactory, componentVarRegistry, this);
        } else {
            final String simpleClassName = type.contains(".") ? StringUtils.substringAfterLast(type, ".") : type;
            final String componentType = "(Missing) " + simpleClassName;

            taskNode = new StandardReportingTaskNode(task, id, this, processScheduler, validationContextFactory, componentType, type, componentVarRegistry, this, true);
        }

        taskNode.setName(taskNode.getReportingTask().getClass().getSimpleName());

        if (firstTimeAdded) {
            final ReportingInitializationContext config = new StandardReportingInitializationContext(id, taskNode.getName(),
                    SchedulingStrategy.TIMER_DRIVEN, "1 min", taskNode.getLogger(), this, nifiProperties);

            try {
                taskNode.getReportingTask().initialize(config);
            } catch (final InitializationException ie) {
                throw new ReportingTaskInstantiationException("Failed to initialize reporting task of type " + type, ie);
            }

            try (final NarCloseable x = NarCloseable.withComponentNarLoader(taskNode.getReportingTask().getClass(), taskNode.getReportingTask().getIdentifier())) {
                ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, taskNode.getReportingTask());
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, taskNode.getReportingTask());
            } catch (final Exception e) {
                throw new ComponentLifeCycleException("Failed to invoke On-Added Lifecycle methods of " + taskNode.getReportingTask(), e);
            }
        }

        if (register) {
            reportingTasks.put(id, taskNode);

            // Register log observer to provide bulletins when reporting task logs anything at WARN level or above
            final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
            logRepository.addObserver(StandardProcessorNode.BULLETIN_OBSERVER_ID, LogLevel.WARN,
                    new ReportingTaskLogObserver(getBulletinRepository(), taskNode));
        }

        return taskNode;
    }

    private LoggableComponent<ReportingTask> instantiateReportingTask(final String type, final String id, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls)
            throws ReportingTaskInstantiationException {

        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final Bundle reportingTaskBundle = ExtensionManager.getBundle(bundleCoordinate);
            if (reportingTaskBundle == null) {
                throw new IllegalStateException("Unable to find bundle for coordinate " + bundleCoordinate.getCoordinate());
            }

            final ClassLoader detectedClassLoader = ExtensionManager.createInstanceClassLoader(type, id, reportingTaskBundle, additionalUrls);
            final Class<?> rawClass = Class.forName(type, false, detectedClassLoader);
            Thread.currentThread().setContextClassLoader(detectedClassLoader);

            final Class<? extends ReportingTask> reportingTaskClass = rawClass.asSubclass(ReportingTask.class);
            final Object reportingTaskObj = reportingTaskClass.newInstance();

            final ReportingTask reportingTask = reportingTaskClass.cast(reportingTaskObj);
            final ComponentLog componentLog = new SimpleProcessLogger(id, reportingTask);

            return new LoggableComponent<>(reportingTask, bundleCoordinate, componentLog);
        } catch (final Exception e) {
            throw new ReportingTaskInstantiationException(type, e);
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }

    @Override
    public void reload(final ReportingTaskNode existingNode, final String newType, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls)
            throws ReportingTaskInstantiationException {
        if (existingNode == null) {
            throw new IllegalStateException("Existing ReportingTaskNode cannot be null");
        }

        final String id = existingNode.getReportingTask().getIdentifier();

        // ghost components will have a null logger
        if (existingNode.getLogger() != null) {
            existingNode.getLogger().debug("Reloading component {} to type {} from bundle {}", new Object[]{id, newType, bundleCoordinate});
        }

        // createReportingTask will create a new instance class loader for the same id so
        // save the instance class loader to use it for calling OnRemoved on the existing processor
        final ClassLoader existingInstanceClassLoader = ExtensionManager.getInstanceClassLoader(id);

        // set firstTimeAdded to true so lifecycle annotations get fired, but don't register this node
        // attempt the creation to make sure it works before firing the OnRemoved methods below
        final ReportingTaskNode newNode = createReportingTask(newType, id, bundleCoordinate, additionalUrls, true, false);

        // call OnRemoved for the existing reporting task using the previous instance class loader
        try (final NarCloseable x = NarCloseable.withComponentNarLoader(existingInstanceClassLoader)) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, existingNode.getReportingTask(), existingNode.getConfigurationContext());
        } finally {
            ExtensionManager.closeURLClassLoader(id, existingInstanceClassLoader);
        }

        // set the new reporting task into the existing node
        final LoggableComponent<ReportingTask> newReportingTask = new LoggableComponent<>(newNode.getReportingTask(), newNode.getBundleCoordinate(), newNode.getLogger());
        existingNode.setReportingTask(newReportingTask);
        existingNode.setExtensionMissing(newNode.isExtensionMissing());

        // need to refresh the properties in case we are changing from ghost component to real component
        existingNode.refreshProperties();
    }

    @Override
    public ReportingTaskNode getReportingTaskNode(final String taskId) {
        return reportingTasks.get(taskId);
    }

    @Override
    public void startReportingTask(final ReportingTaskNode reportingTaskNode) {
        if (isTerminated()) {
            throw new IllegalStateException("Cannot start reporting task " + reportingTaskNode.getIdentifier() + " because the controller is terminated");
        }

        reportingTaskNode.verifyCanStart();
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

    @Override
    public void removeReportingTask(final ReportingTaskNode reportingTaskNode) {
        final ReportingTaskNode existing = reportingTasks.get(reportingTaskNode.getIdentifier());
        if (existing == null || existing != reportingTaskNode) {
            throw new IllegalStateException("Reporting Task " + reportingTaskNode + " does not exist in this Flow");
        }

        reportingTaskNode.verifyCanDelete();

        try (final NarCloseable x = NarCloseable.withComponentNarLoader(reportingTaskNode.getReportingTask().getClass(), reportingTaskNode.getReportingTask().getIdentifier())) {
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
        ExtensionManager.removeInstanceClassLoader(reportingTaskNode.getIdentifier());
    }

    @Override
    public Set<ReportingTaskNode> getAllReportingTasks() {
        return new HashSet<>(reportingTasks.values());
    }

    @Override
    public ControllerServiceNode createControllerService(final String type, final String id, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls, final boolean firstTimeAdded) {
        final ControllerServiceNode serviceNode = controllerServiceProvider.createControllerService(type, id, bundleCoordinate, additionalUrls, firstTimeAdded);

        // Register log observer to provide bulletins when reporting task logs anything at WARN level or above
        final LogRepository logRepository = LogRepositoryFactory.getRepository(id);
        logRepository.addObserver(StandardProcessorNode.BULLETIN_OBSERVER_ID, LogLevel.WARN,
                new ControllerServiceLogObserver(getBulletinRepository(), serviceNode));

        if (firstTimeAdded) {
            final ControllerService service = serviceNode.getControllerServiceImplementation();

            try (final NarCloseable nc = NarCloseable.withComponentNarLoader(service.getClass(), service.getIdentifier())) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, service);
            }
        }

        return serviceNode;
    }

    @Override
    public void reload(final ControllerServiceNode existingNode, final String newType, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls)
            throws ControllerServiceInstantiationException {
        if (existingNode == null) {
            throw new IllegalStateException("Existing ControllerServiceNode cannot be null");
        }

        final String id = existingNode.getIdentifier();

        // ghost components will have a null logger
        if (existingNode.getLogger() != null) {
            existingNode.getLogger().debug("Reloading component {} to type {} from bundle {}", new Object[]{id, newType, bundleCoordinate});
        }

        // createControllerService will create a new instance class loader for the same id so
        // save the instance class loader to use it for calling OnRemoved on the existing service
        final ClassLoader existingInstanceClassLoader = ExtensionManager.getInstanceClassLoader(id);

        // create a new node with firstTimeAdded as true so lifecycle methods get called
        // attempt the creation to make sure it works before firing the OnRemoved methods below
        final ControllerServiceNode newNode = controllerServiceProvider.createControllerService(newType, id, bundleCoordinate, additionalUrls, true);

        // call OnRemoved for the existing service using the previous instance class loader
        try (final NarCloseable x = NarCloseable.withComponentNarLoader(existingInstanceClassLoader)) {
            final ConfigurationContext configurationContext = new StandardConfigurationContext(existingNode, controllerServiceProvider, null, variableRegistry);
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, existingNode.getControllerServiceImplementation(), configurationContext);
        } finally {
            ExtensionManager.closeURLClassLoader(id, existingInstanceClassLoader);
        }

        // take the invocation handler that was created for new proxy and is set to look at the new node,
        // and set it to look at the existing node
        final ControllerServiceInvocationHandler invocationHandler = newNode.getInvocationHandler();
        invocationHandler.setServiceNode(existingNode);

        // create LoggableComponents for the proxy and implementation
        final LoggableComponent<ControllerService> loggableProxy = new LoggableComponent<>(newNode.getProxiedControllerService(), bundleCoordinate, newNode.getLogger());
        final LoggableComponent<ControllerService> loggableImplementation = new LoggableComponent<>(newNode.getControllerServiceImplementation(), bundleCoordinate, newNode.getLogger());

        // set the new impl, proxy, and invocation handler into the existing node
        existingNode.setControllerServiceAndProxy(loggableImplementation, loggableProxy, invocationHandler);
        existingNode.setExtensionMissing(newNode.isExtensionMissing());

        // need to refresh the properties in case we are changing from ghost component to real component
        existingNode.refreshProperties();
    }

    @Override
    public void enableReportingTask(final ReportingTaskNode reportingTaskNode) {
        reportingTaskNode.verifyCanEnable();
        processScheduler.enableReportingTask(reportingTaskNode);
    }

    @Override
    public void disableReportingTask(final ReportingTaskNode reportingTaskNode) {
        reportingTaskNode.verifyCanDisable();
        processScheduler.disableReportingTask(reportingTaskNode);
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
    public CompletableFuture<Void> enableControllerService(final ControllerServiceNode serviceNode) {
        return controllerServiceProvider.enableControllerService(serviceNode);
    }

    @Override
    public void enableControllerServices(final Collection<ControllerServiceNode> serviceNodes) {
        controllerServiceProvider.enableControllerServices(serviceNodes);
    }

    @Override
    public CompletableFuture<Void> disableControllerService(final ControllerServiceNode serviceNode) {
        serviceNode.verifyCanDisable();
        return controllerServiceProvider.disableControllerService(serviceNode);
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

    @Override
    public ControllerService getControllerService(final String serviceIdentifier) {
        return controllerServiceProvider.getControllerService(serviceIdentifier);
    }

    @Override
    public ControllerService getControllerServiceForComponent(final String serviceIdentifier, final String componentId) {
        return controllerServiceProvider.getControllerServiceForComponent(serviceIdentifier, componentId);
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) throws IllegalArgumentException {
        return controllerServiceProvider.getControllerServiceIdentifiers(serviceType);
    }

    @Override
    public ControllerServiceNode getControllerServiceNode(final String serviceIdentifier) {
        return controllerServiceProvider.getControllerServiceNode(serviceIdentifier);
    }

    public Set<ControllerServiceNode> getRootControllerServices() {
        return new HashSet<>(rootControllerServices.values());
    }

    public void addRootControllerService(final ControllerServiceNode serviceNode) {
        final ControllerServiceNode existing = rootControllerServices.putIfAbsent(serviceNode.getIdentifier(), serviceNode);
        if (existing != null) {
            throw new IllegalStateException("Controller Service with ID " + serviceNode.getIdentifier() + " already exists at the Controller level");
        }
    }

    public ControllerServiceNode getRootControllerService(final String serviceIdentifier) {
        return rootControllerServices.get(serviceIdentifier);
    }

    public void removeRootControllerService(final ControllerServiceNode service) {
        final ControllerServiceNode existing = rootControllerServices.get(requireNonNull(service).getIdentifier());
        if (existing == null) {
            throw new IllegalStateException(service + " is not a member of this Process Group");
        }

        service.verifyCanDelete();

        try (final NarCloseable x = NarCloseable.withComponentNarLoader(service.getControllerServiceImplementation().getClass(), service.getIdentifier())) {
            final ConfigurationContext configurationContext = new StandardConfigurationContext(service, controllerServiceProvider, null, variableRegistry);
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, service.getControllerServiceImplementation(), configurationContext);
        }

        for (final Map.Entry<PropertyDescriptor, String> entry : service.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.getControllerServiceDefinition() != null) {
                final String value = entry.getValue() == null ? descriptor.getDefaultValue() : entry.getValue();
                if (value != null) {
                    final ControllerServiceNode referencedNode = getRootControllerService(value);
                    if (referencedNode != null) {
                        referencedNode.removeReference(service);
                    }
                }
            }
        }

        rootControllerServices.remove(service.getIdentifier());
        getStateManagerProvider().onComponentRemoved(service.getIdentifier());

        ExtensionManager.removeInstanceClassLoader(service.getIdentifier());

        LOG.info("{} removed from Flow Controller", service, this);
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
    public Set<ControllerServiceNode> getAllControllerServices() {
        return controllerServiceProvider.getAllControllerServices();
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

    public int getActiveThreadCount() {
        final int timerDrivenCount = timerDrivenEngineRef.get().getActiveCount();
        final int eventDrivenCount = eventDrivenSchedulingAgent.getActiveThreadCount();
        return timerDrivenCount + eventDrivenCount;
    }

    private RepositoryStatusReport getProcessorStats() {
        // processed in last 5 minutes
        return getProcessorStats(System.currentTimeMillis() - 300000);
    }

    private RepositoryStatusReport getProcessorStats(final long since) {
        return flowFileEventRepository.reportTransferEvents(since);
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
            writeLock.unlock();
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
            writeLock.unlock();
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
            readLock.unlock();
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
            readLock.unlock();
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
            readLock.unlock();
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

                    heartbeat();
                } else {
                    stateManagerProvider.disableClusterProvider();

                    setPrimary(false);
                }

                final List<RemoteProcessGroup> remoteGroups = getGroup(getRootGroupId()).findAllRemoteProcessGroups();
                for (final RemoteProcessGroup remoteGroup : remoteGroups) {
                    remoteGroup.reinitialize(clustered);
                }
            }

            if (!clustered) {
                leaderElectionManager.unregister(ClusterRoles.PRIMARY_NODE);
                leaderElectionManager.unregister(ClusterRoles.CLUSTER_COORDINATOR);
            }

            // update the heartbeat bean
            this.heartbeatBeanRef.set(new HeartbeatBean(getRootGroup(), isPrimary()));
        } finally {
            writeLock.unlock();
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

    public void setPrimary(final boolean primary) {
        final PrimaryNodeState nodeState = primary ? PrimaryNodeState.ELECTED_PRIMARY_NODE : PrimaryNodeState.PRIMARY_NODE_REVOKED;
        final ProcessGroup rootGroup = getGroup(getRootGroupId());
        for (final ProcessorNode procNode : rootGroup.findAllProcessors()) {
            try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(procNode.getProcessor().getClass(), procNode.getIdentifier())) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnPrimaryNodeStateChange.class, procNode.getProcessor(), nodeState);
            }
        }
        for (final ControllerServiceNode serviceNode : getAllControllerServices()) {
            try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(serviceNode.getControllerServiceImplementation().getClass(), serviceNode.getIdentifier())) {
                ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnPrimaryNodeStateChange.class, serviceNode.getControllerServiceImplementation(), nodeState);
            }
        }
        for (final ReportingTaskNode reportingTaskNode : getAllReportingTasks()) {
            try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(reportingTaskNode.getReportingTask().getClass(), reportingTaskNode.getIdentifier())) {
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
                .setComponentType(getName())
                .setComponentId(getRootGroupId())
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
                .setComponentType(getName())
                .setComponentId(getRootGroupId())
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

        final List<Connection> connections = getGroup(getRootGroupId()).findAllConnections();
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

        final List<Connection> connections = getGroup(getRootGroupId()).findAllConnections();
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

    @Override
    public List<String> getComponentIdentifiers() {
        final List<String> componentIds = new ArrayList<>();
        getGroup(getRootGroupId()).findAllProcessors().stream()
            .forEach(proc -> componentIds.add(proc.getIdentifier()));
        getGroup(getRootGroupId()).getInputPorts().stream()
            .forEach(port -> componentIds.add(port.getIdentifier()));
        getGroup(getRootGroupId()).getOutputPorts().stream()
            .forEach(port -> componentIds.add(port.getIdentifier()));

        return componentIds;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public List<String> getComponentTypes() {
        final Set<Class> procClasses = ExtensionManager.getExtensions(Processor.class);
        final List<String> componentTypes = new ArrayList<>(procClasses.size() + 2);
        componentTypes.add(ProvenanceEventRecord.REMOTE_INPUT_PORT_TYPE);
        componentTypes.add(ProvenanceEventRecord.REMOTE_OUTPUT_PORT_TYPE);
        procClasses.stream()
            .map(procClass -> procClass.getSimpleName())
            .forEach(componentType -> componentTypes.add(componentType));
        return componentTypes;
    }

    @Override
    public List<String> getQueueIdentifiers() {
        return getAllQueues().stream()
            .map(q -> q.getIdentifier())
            .collect(Collectors.toList());
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
            this.heartbeatBeanRef.set(new HeartbeatBean(getRootGroup(), isPrimary()));
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
                    bean = new HeartbeatBean(getGroup(getRootGroupId()), isPrimary());
                } finally {
                    readLock.unlock();
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
        final List<RemoteProcessGroup> remoteGroups = getGroup(getRootGroupId()).findAllRemoteProcessGroups();
        for (final RemoteProcessGroup remoteGroup : remoteGroups) {
            try {
                remoteGroup.refreshFlowContents();
            } catch (final CommunicationsException | ClientHandlerException e) {
                LOG.warn("Unable to communicate with remote instance {} due to {}", remoteGroup, e.toString());
                if (LOG.isDebugEnabled()) {
                    LOG.warn("", e);
                }
            }
        }
    }

    @Override
    public List<ProvenanceEventRecord> getProvenanceEvents(final long firstEventId, final int maxRecords) throws IOException {
        return new ArrayList<>(provenanceRepository.getEvents(firstEventId, maxRecords));
    }

    @Override
    public Authorizable createLocalDataAuthorizable(final String componentId) {
        final String rootGroupId = getRootGroupId();

        // Provenance Events are generated only by connectable components, with the exception of DOWNLOAD events,
        // which have the root process group's identifier assigned as the component ID, and DROP events, which
        // could have the connection identifier assigned as the component ID. So, we check if the component ID
        // is set to the root group and otherwise assume that the ID is that of a connectable or connection.
        final DataAuthorizable authorizable;
        if (rootGroupId.equals(componentId)) {
            authorizable = new DataAuthorizable(getRootGroup());
        } else {
            // check if the component is a connectable, this should be the case most often
            final Connectable connectable = getRootGroup().findLocalConnectable(componentId);
            if (connectable == null) {
                // if the component id is not a connectable then consider a connection
                final Connection connection = getRootGroup().findConnection(componentId);

                if (connection == null) {
                    throw new ResourceNotFoundException("The component that generated this event is no longer part of the data flow.");
                } else {
                    // authorizable for connection data is associated with the source connectable
                    authorizable = new DataAuthorizable(connection.getSource());
                }
            } else {
                authorizable = new DataAuthorizable(connectable);
            }
        }

        return authorizable;
    }

    @Override
    public Authorizable createRemoteDataAuthorizable(String remoteGroupPortId) {
        final DataAuthorizable authorizable;

        final RemoteGroupPort remoteGroupPort = getRootGroup().findRemoteGroupPort(remoteGroupPortId);
        if (remoteGroupPort == null) {
            throw new ResourceNotFoundException("The component that generated this event is no longer part of the data flow.");
        } else {
            // authorizable for remote group ports should be the remote process group
            authorizable = new DataAuthorizable(remoteGroupPort.getRemoteProcessGroup());
        }

        return authorizable;
    }

    @Override
    public List<Action> getFlowChanges(final int firstActionId, final int maxActions) {
        final History history = auditService.getActions(firstActionId, maxActions);
        return new ArrayList<>(history.getActions());
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

    public ProcessScheduler getProcessScheduler() {
        return processScheduler;
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType, final String groupId) {
        return controllerServiceProvider.getControllerServiceIdentifiers(serviceType, groupId);
    }

    @Override
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

    @Override
    public Collection<FlowFileQueue> getAllQueues() {
        final Collection<Connection> connections = getGroup(getRootGroupId()).findAllConnections();
        final List<FlowFileQueue> queues = new ArrayList<>(connections.size());
        for (final Connection connection : connections) {
            queues.add(connection.getFlowFileQueue());
        }

        return queues;
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
