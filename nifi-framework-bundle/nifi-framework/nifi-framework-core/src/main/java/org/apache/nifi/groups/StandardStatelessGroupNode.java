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

package org.apache.nifi.groups;

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.components.PortFunction;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.state.StatelessStateManagerProvider;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.BackoffMechanism;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.SchedulingAgentCallback;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.scheduling.LifecycleState;
import org.apache.nifi.controller.scheduling.LifecycleStateManager;
import org.apache.nifi.controller.scheduling.SchedulingAgent;
import org.apache.nifi.controller.scheduling.StatelessProcessScheduler;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.tasks.StatelessFlowTask;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.stateless.engine.ProcessContextFactory;
import org.apache.nifi.stateless.engine.StatelessProcessContextFactory;
import org.apache.nifi.stateless.flow.StandardDataflowDefinition;
import org.apache.nifi.stateless.flow.StandardStatelessFlow;
import org.apache.nifi.stateless.flow.StatelessDataflowInitializationContext;
import org.apache.nifi.stateless.flow.TransactionThresholds;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;
import org.apache.nifi.util.FormatUtils;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class StandardStatelessGroupNode implements StatelessGroupNode {
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock writeLock = rwLock.writeLock();
    private volatile ScheduledState currentState = ScheduledState.STOPPED;
    private volatile ScheduledState desiredState = ScheduledState.STOPPED;
    private volatile int maxConcurrentTasks = 1;
    private final AtomicLong yieldExpiration = new AtomicLong(0L);

    private final ProcessGroup processGroup;
    private final ControllerServiceProvider controllerServiceProvider;
    private final StateManagerProvider stateManagerProvider;
    private final ExtensionManager extensionManager;
    private final RepositoryContextFactory statelessRepoContextFactory;
    private final FlowFileRepository nifiFlowFileRepository;
    private final ContentRepository nifiContentRepository;
    private final ProvenanceEventRepository nifiProvenanceRepo;
    private final BulletinRepository bulletinRepository;
    private final StatelessGroupFactory statelessGroupFactory;
    private final LifecycleStateManager lifecycleStateManager;
    private final FlowFileEventRepository flowFileEventRepository;
    private final long boredYieldMillis;

    private volatile List<Connection> incomingConnections;
    private volatile Set<Connection> allOutgoingConnections;
    private volatile Map<String, Set<Connection>> outgoingConnectionsByPort;
    private volatile Map<String, Relationship> relationships;

    private final BlockingQueue<StatelessFlowTask> flowTaskQueue = new LinkedBlockingDeque<>();
    private volatile List<StatelessFlowTask> initializedFlowTasks;
    private volatile boolean primaryNodeOnly = false;
    private volatile long schedulingNanos = 1L;
    private ComponentLog logger;


    private StandardStatelessGroupNode(final Builder builder) {
        this.processGroup = builder.rootGroup;
        this.controllerServiceProvider = builder.controllerServiceProvider;
        this.stateManagerProvider = builder.stateManagerProvider;
        this.extensionManager = builder.extensionManager;
        this.statelessRepoContextFactory = builder.statelessRepoContextFactory;
        this.nifiFlowFileRepository = builder.nifiFlowFileRepository;
        this.nifiContentRepository = builder.nifiContentRepository;
        this.nifiProvenanceRepo = builder.nifiProvenanceRepo;
        this.bulletinRepository = builder.bulletinRepository;
        this.statelessGroupFactory = builder.statelessGroupFactory;
        this.lifecycleStateManager = builder.lifecycleStateManager;
        this.flowFileEventRepository = builder.flowFileEventRepository;
        this.boredYieldMillis = builder.boredYieldMillis;
    }

    @Override
    public void initialize(final StatelessGroupNodeInitializationContext initializationContext) {
        logger = initializationContext.getLogger();
    }

    @Override
    public void start(final ScheduledExecutorService executor, final SchedulingAgentCallback schedulingAgentCallback, final LifecycleState lifecycleState) {
        writeLock.lock();
        try {
            if (currentState != ScheduledState.STOPPED) {
                logger.warn("Cannot start {} because it is not currently stopped; its current state is {}", this, currentState);
                return;
            }

            desiredState = ScheduledState.RUNNING;
            currentState = ScheduledState.STARTING;
            logger.info("Starting {}", this);
        } finally {
            writeLock.unlock();
        }

        getProcessGroup().findAllInputPorts().forEach(Port::onSchedulingStart);
        getProcessGroup().findAllOutputPorts().forEach(Port::onSchedulingStart);

        executor.submit(() -> initialize(executor, schedulingAgentCallback, lifecycleState) );
    }

    private void initialize(final ScheduledExecutorService executor, final SchedulingAgentCallback schedulingAgentCallback, final LifecycleState lifecycleState) {
        if (getDesiredState() != ScheduledState.RUNNING) {
            shutdown();
            return;
        }

        if (!lifecycleState.isScheduled()) {
            logger.debug("Triggered to initialize {} but Lifecycle State indicates that the component is not scheduled. Will not initialize", this);
            return;
        }

        lifecycleState.incrementActiveThreadCount(null);
        try {
            logger.info("Initializing {}", this);
            flowTaskQueue.clear();

            // Determine scheduling information
            boolean runOnPrimaryNodeOnly = false;
            final List<ProcessorNode> triggerSeriallyProcessors = new ArrayList<>();
            long schedulingNanos = 1L;
            for (final ProcessorNode processorNode : processGroup.findAllProcessors()) {
                if (processorNode.isIsolated()) {
                    runOnPrimaryNodeOnly = true;
                    triggerSeriallyProcessors.add(processorNode);
                } else if (processorNode.isTriggeredSerially()) {
                    triggerSeriallyProcessors.add(processorNode);
                }

                if (isSourceProcessor(processorNode)) {
                    final long processorNanos = processorNode.getSchedulingPeriod(TimeUnit.NANOSECONDS);
                    schedulingNanos = Math.max(schedulingNanos, processorNanos);
                }
            }
            this.primaryNodeOnly = runOnPrimaryNodeOnly;
            this.schedulingNanos = schedulingNanos;

            final boolean triggerSerially = !triggerSeriallyProcessors.isEmpty();
            if (maxConcurrentTasks > 1 && triggerSerially) {
                logger.warn("Flow is configured to run using the Stateless Engine with {} max concurrent tasks. However, {} only allows a single concurrent task or allows running only on " +
                    "the Primary Node, so the flow will run with a single concurrent task", maxConcurrentTasks, triggerSeriallyProcessors);
            }

            // Perform setup in background thread outside of lock
            final List<StandardStatelessFlow> createdFlows = new ArrayList<>();
            initializedFlowTasks = new ArrayList<>();
            try {
                final VersionedExternalFlow versionedExternalFlow = statelessGroupFactory.createVersionedExternalFlow(processGroup);
                initializeInputsAndOutputs();

                final String timeout = processGroup.getStatelessFlowTimeout();
                final long timeoutMillis = (long) FormatUtils.getPreciseTimeDuration(timeout, TimeUnit.MILLISECONDS);

                final int concurrentTasks = triggerSerially ? 1 : maxConcurrentTasks;
                for (int i = 0; i < concurrentTasks; i++) {
                    final StandardStatelessFlow statelessFlow = createStatelessFlow(processGroup, versionedExternalFlow);
                    createdFlows.add(statelessFlow);

                    final StatelessFlowTask task = new StatelessFlowTask.Builder()
                        .statelessGroupNode(this)
                        .nifiFlowFileRepository(nifiFlowFileRepository)
                        .nifiContentRepository(nifiContentRepository)
                        .nifiProvenanceRepository(nifiProvenanceRepo)
                        .statelessFlow(statelessFlow)
                        .flowFileEventRepository(flowFileEventRepository)
                        .timeout(Math.max(0, timeoutMillis), TimeUnit.MILLISECONDS)
                        .logger(logger)
                        .build();

                    flowTaskQueue.offer(task);
                    initializedFlowTasks.add(task);
                }
            } catch (final Exception e) {
                for (final StandardStatelessFlow flow : createdFlows) {
                    try {
                        flow.shutdown(false, true);
                    } catch (final Exception ex) {
                        logger.error("Failed to shutdown Stateless Flow {}", flow, ex);
                    }
                }

                flowTaskQueue.clear();
                logger.error("Failed to start {}; will try again in 10 seconds", this, e);
                executor.schedule(() -> initialize(executor, schedulingAgentCallback, lifecycleState), 10, TimeUnit.SECONDS);
                return;
            }

            initializeInputsAndOutputs();

            // Ensure that group is still scheduled to run; if so, start scheduling tasks to run. Otherwise, note the fact by setting teardown = true;
            // we do not perform teardown within the write lock because it can be expensive.
            boolean shutdown = false;
            writeLock.lock();
            try {
                if (desiredState == ScheduledState.RUNNING) {
                    logger.info("{} has been started", this);
                    currentState = ScheduledState.RUNNING;
                    schedulingAgentCallback.trigger();
                } else {
                    logger.info("{} completed setup but is no longer scheduled to run; desired state is now {}; will shutdown", this, desiredState);
                    shutdown = true;
                }
            } finally {
                writeLock.unlock();
            }

            // If there is a need to perform teardown, do so now.
            if (shutdown) {
                shutdown();
            }
        } finally {
            lifecycleState.decrementActiveThreadCount();
        }
    }

    private void initializeInputsAndOutputs() {
        final List<Connection> incomingConnections = new ArrayList<>();
        for (final Port port : processGroup.getInputPorts()) {
            incomingConnections.addAll(port.getIncomingConnections());
        }
        this.incomingConnections = incomingConnections;

        final Map<String, Relationship> relationships = new HashMap<>();
        final Map<String, Set<Connection>> outgoingConnections = new HashMap<>();
        final Set<Connection> allOutgoingConnections = new HashSet<>();
        for (final Port port : processGroup.getOutputPorts()) {
            allOutgoingConnections.addAll(port.getConnections());
            outgoingConnections.put(port.getName(), port.getConnections());
            relationships.put(port.getName(), new Relationship.Builder().name(port.getName()).build());
        }
        this.outgoingConnectionsByPort = Collections.unmodifiableMap(outgoingConnections);
        this.allOutgoingConnections = Collections.unmodifiableSet(allOutgoingConnections);
        this.relationships = Collections.unmodifiableMap(relationships);
    }

    private void shutdown() {
        logger.info("{} has been stopped", this);

        writeLock.lock();
        try {
            shutdownFlows();
            processGroup.stopComponents();  // Stop components, in case there were no flows that had been successfully created.
            flowTaskQueue.clear();

            currentState = ScheduledState.STOPPED;
        } finally {
            writeLock.unlock();
        }
    }

    private void shutdownFlows() {
        final List<StatelessFlowTask> tasks = initializedFlowTasks;
        if (tasks != null) {
            tasks.forEach(StatelessFlowTask::shutdown);
        }
    }


    private StandardStatelessFlow createStatelessFlow(final ProcessGroup group, final VersionedExternalFlow versionedExternalFlow) {
        final Set<String> failurePortNames = group.getOutputPorts().stream()
            .filter(port -> port.getPortFunction() == PortFunction.FAILURE)
            .map(Port::getName)
            .collect(Collectors.toSet());

        final StandardDataflowDefinition dataflowDefinition = new StandardDataflowDefinition.Builder()
            .versionedExternalFlow(versionedExternalFlow)
            .failurePortNames(failurePortNames)
            .transactionThresholds(TransactionThresholds.UNLIMITED)
            .build();

        final ProcessScheduler processScheduler = new StatelessProcessScheduler(extensionManager, Duration.of(10, ChronoUnit.SECONDS)) {
            @Override
            public void yield(final ProcessorNode procNode) {
                if (isSourceProcessor(procNode)) {
                    StandardStatelessGroupNode.this.yield(procNode.getYieldPeriod(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
                }
            }
        };

        final StatelessStateManagerProvider statelessStateManagerProvider = new StatelessStateManagerProvider();
        final ProcessContextFactory processContextFactory = new StatelessProcessContextFactory(controllerServiceProvider, stateManagerProvider);

        final ProcessGroup rootGroup = statelessGroupFactory.createStatelessProcessGroup(group, versionedExternalFlow);
        final StandardStatelessFlow dataflow = new StandardStatelessFlow(
            rootGroup,
            Collections.emptyList(),
            controllerServiceProvider,
            processContextFactory,
            statelessRepoContextFactory,
            dataflowDefinition,
            statelessStateManagerProvider,
            processScheduler,
            bulletinRepository,
            lifecycleStateManager,
            Duration.of(10, ChronoUnit.SECONDS));

        // We don't want to enable Controller Services because we want to use the actual Controller Services that exist within the
        // Standard NiFi instance, not the ephemeral ones that created during the initialization of the Stateless Group.
        // This may not matter for Controller Services such as Record Readers and Writers, but it can matter for other types
        // of Controller Services, such as connection pooling services. We don't want to create a new instance of a Connection Pool
        // for each Concurrent Task in a Stateless Group, for example.
        // Since we will not be using the ephemeral Controller Services, we also do not want to enable them.
        final StatelessDataflowInitializationContext initializationContext = new StatelessDataflowInitializationContext() {
            @Override
            public boolean isEnableControllerServices() {
                return false;
            }
        };

        dataflow.initialize(initializationContext);

        return dataflow;
    }

    private boolean isSourceProcessor(final ProcessorNode procNode) {
        final List<Connection> incomingConnections = procNode.getIncomingConnections();
        if (incomingConnections.isEmpty()) {
            return true;
        }

        for (final Connection connection : incomingConnections) {
            final boolean selfLoop = connection.getSource() == procNode;
            if (!selfLoop) {
                return false;
            }
        }

        return true;
    }

    @Override
    public CompletableFuture<Void> stop(final ProcessScheduler processScheduler, final ScheduledExecutorService executor, final SchedulingAgent schedulingAgent, final LifecycleState lifecycleState) {
        logger.info("Stopping {}", this);
        lifecycleState.incrementActiveThreadCount(null);

        writeLock.lock();
        try {
            schedulingAgent.unschedule(this, lifecycleState);

            if (currentState == ScheduledState.STOPPED || currentState == ScheduledState.STOPPING) {
                logger.debug("Attempt to stop {} but state is already {}", this, currentState);
                lifecycleState.decrementActiveThreadCount();
                return CompletableFuture.completedFuture(null);
            }

            if (currentState != ScheduledState.STARTING && currentState != ScheduledState.RUNNING) {
                logger.warn("Cannot stop {} because it is not currently running or starting; its current state is {}", this, currentState);
                lifecycleState.decrementActiveThreadCount();
                return CompletableFuture.completedFuture(null);
            }

            desiredState = ScheduledState.STOPPED;
            currentState = ScheduledState.STOPPING;

            final CompletableFuture<Void> future = new CompletableFuture<>();
            processScheduler.submitFrameworkTask(() -> {
                shutdownFlows();
                stopPorts();
                stopCanvasComponents(processScheduler).join();

                lifecycleState.decrementActiveThreadCount();
                currentState = ScheduledState.STOPPED;
                logger.info("{} is now fully stopped.", this);
                flowTaskQueue.clear();

                future.complete(null);
            });

            return future;
        } finally {
            writeLock.unlock();
        }
    }


    /**
     * When a Stateless Group is stopped, we need to stop all of the Processors within the group, as well as all of the
     * processors that are on the canvas. When the Stateless Group is started, the processors on the canvas are transitioned to a RUNNING
     * state but their lifecycle methods are not called, and they are not scheduled to run. However, this must be done in order to ensure that
     * the state reflected in the UI (and REST API) shows the components running. Additionally, the Controller Services are referenced by the
     * components of the Stateless Group.
     *
     * When we stop the Stateless Group, we need to then stop all of these components. However, it is important that we stop these components
     * after stopping Processors in the Stateless Group (because we don't want to disable services until the Processors have been stopped) and
     * before the Stateless Group's state transitions to STOPPED (because we don't want to return a state of STOPPED if the processor on the canvas
     * is running, as that will prevent things like parameter updates, etc. from occurring).
     *
     * As a result, we have this method that is responsible for stopping these components and which may be called from a callback within the Stateless Group Node.
     *
     * @param scheduler the process scheduler to use for stopping the components
     *
     * @return a CompletableFuture that can be used to wait until the process has completed
     */
    private CompletableFuture<Void> stopCanvasComponents(final ProcessScheduler scheduler) {
        final List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (final ProcessorNode procNode : getProcessGroup().findAllProcessors()) {
            final CompletableFuture<Void> stopProcessorFuture = scheduler.stopProcessor(procNode);
            futures.add(stopProcessorFuture);
        }

        for (final ProcessGroup innerGroup : getProcessGroup().findAllProcessGroups()) {
            innerGroup.getInputPorts().forEach(scheduler::stopPort);
            innerGroup.getOutputPorts().forEach(scheduler::stopPort);
        }

        final Set<ControllerServiceNode> controllerServices = getProcessGroup().findAllControllerServices();

        final CompletableFuture<?>[] processorFutureArray = futures.toArray(new CompletableFuture[0]);
        final CompletableFuture<Void> processorsStoppedFuture = CompletableFuture.allOf(processorFutureArray);
        final CompletableFuture<Void> allStoppedFuture = processorsStoppedFuture.thenAccept(completion -> {
            controllerServiceProvider.disableControllerServicesAsync(controllerServices).join();
        });

        return allStoppedFuture;
    }



    private void stopPorts() {
        final List<Port> allPorts = new ArrayList<>();
        allPorts.addAll(getProcessGroup().findAllInputPorts());
        allPorts.addAll(getProcessGroup().findAllOutputPorts());

        allPorts.forEach(Port::shutdown);

        // Local Ports finish very quickly. We should consider refactoring the shutdown() method so that
        // it returns a Future that is completed only when all of the active threads have finished. But for now,
        // we'll just sleep for a very short period of time and keep checking. We do this only because we expect
        // the ports to finish very quickly.
        while (!isStopped(allPorts)) {
            try {
                Thread.sleep(2L);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private boolean isStopped(final Collection<Port> ports) {
        for (final Port port : ports) {
            if (port.isRunning()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public ScheduledState getCurrentState() {
        return currentState;
    }

    @Override
    public ScheduledState getDesiredState() {
        return desiredState;
    }

    @Override
    public String toString() {
        return "StandardStatelessGroupNode[group=" + processGroup + "]";
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final StandardStatelessGroupNode that = (StandardStatelessGroupNode) other;
        return Objects.equals(processGroup, that.processGroup);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processGroup);
    }

    @Override
    public Optional<String> getVersionedComponentId() {
        return processGroup.getVersionedComponentId();
    }

    @Override
    public void setVersionedComponentId(final String versionedComponentId) {
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return processGroup;
    }

    @Override
    public Resource getResource() {
        return processGroup.getResource();
    }

    @Override
    public String getProcessGroupIdentifier() {
        return processGroup.getIdentifier();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final StatelessFlowTask task = flowTaskQueue.poll();
        if (task == null) {
            return;
        }

        try {
            task.trigger();
        } catch (final ProcessException pe) {
            throw pe;
        } catch (final Exception e) {
            throw new ProcessException("Failed while running Stateless Flow", e);
        } finally {
            // If still running, offer the task back onto the queue
            if (desiredState == ScheduledState.RUNNING) {
                flowTaskQueue.offer(task);
            }
        }
    }

    @Override
    public void setMaxConcurrentTasks(final int taskCount) {
        maxConcurrentTasks = taskCount;
    }

    @Override
    public int getMaxConcurrentTasks() {
        final boolean triggerSerially = processGroup.findAllProcessors().stream().anyMatch(ProcessorNode::isTriggeredSerially);
        return triggerSerially ? 1 : maxConcurrentTasks;
    }

    @Override
    public ScheduledState getScheduledState() {
        return getCurrentState();
    }

    @Override
    public boolean isRunning() {
        final ScheduledState state = getCurrentState();
        return state == ScheduledState.RUNNING || state == ScheduledState.RUN_ONCE || state == ScheduledState.STARTING;
    }

    @Override
    public boolean isIsolated() {
        return primaryNodeOnly;
    }

    @Override
    public long getSchedulingPeriod(final TimeUnit timeUnit) {
        return timeUnit.convert(schedulingNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public String getSchedulingPeriod() {
        return schedulingNanos + " nanos";
    }

    @Override
    public void setSchedulingPeriod(final String schedulingPeriod) {
    }

    @Override
    public Position getPosition() {
        return processGroup.getPosition();
    }

    @Override
    public void setPosition(final Position position) {
        processGroup.setPosition(position);
    }

    @Override
    public String getIdentifier() {
        return processGroup.getIdentifier();
    }

    @Override
    public Collection<Relationship> getRelationships() {
        return relationships.values();
    }

    @Override
    public Relationship getRelationship(final String relationshipName) {
        return relationships.get(relationshipName);
    }

    @Override
    public void addConnection(final Connection connection) throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasIncomingConnection() {
        return !incomingConnections.isEmpty();
    }

    @Override
    public void removeConnection(final Connection connection) throws IllegalStateException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateConnection(final Connection newConnection) throws IllegalStateException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Connection> getIncomingConnections() {
        return incomingConnections;
    }

    @Override
    public Set<Connection> getConnections() {
        return allOutgoingConnections;
    }

    @Override
    public Set<Connection> getConnections(final Relationship relationship) {
        final Set<Connection> connections = outgoingConnectionsByPort.get(relationship.getName());
        return connections == null ? Collections.emptySet() : connections;
    }

    @Override
    public String getName() {
        return processGroup.getName();
    }

    @Override
    public void setName(final String name) {

    }

    @Override
    public String getComments() {
        return null;
    }

    @Override
    public void setComments(final String comments) {
    }

    @Override
    public boolean isTriggerWhenEmpty() {
        return false;
    }

    @Override
    public ProcessGroup getProcessGroup() {
        return processGroup;
    }

    @Override
    public void setProcessGroup(final ProcessGroup group) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAutoTerminated(final Relationship relationship) {
        return false;
    }

    @Override
    public boolean isLossTolerant() {
        return false;
    }

    @Override
    public void setLossTolerant(final boolean lossTolerant) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectableType getConnectableType() {
        return ConnectableType.STATELESS_GROUP;
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        return Collections.emptyList();
    }

    @Override
    public long getPenalizationPeriod(final TimeUnit timeUnit) {
        return 0;
    }

    @Override
    public String getPenalizationPeriod() {
        return "0 secs";
    }

    @Override
    public long getYieldPeriod(final TimeUnit timeUnit) {
        return 0;
    }

    @Override
    public String getYieldPeriod() {
        return "0 secs";
    }

    @Override
    public void setYieldPeriod(final String yieldPeriod) {
    }

    @Override
    public void setPenalizationPeriod(final String penalizationPeriod) {
    }

    @Override
    public void yield() {
    }

    @Override
    public void yield(final long yieldDuration, final TimeUnit timeUnit) {
        final long newExpiration = System.currentTimeMillis() + timeUnit.toMillis(yieldDuration);

        boolean updated = false;
        while (!updated) {
            final long currentExpiration = this.yieldExpiration.get();
            if (newExpiration <= currentExpiration) {
                return;
            }

            updated = yieldExpiration.compareAndSet(currentExpiration, newExpiration);
        }
    }

    @Override
    public long getYieldExpiration() {
        return yieldExpiration.get();
    }

    @Override
    public long getBoredYieldDuration(final TimeUnit timeUnit) {
        return TimeUnit.MILLISECONDS.convert(boredYieldMillis, timeUnit);
    }

    @Override
    public boolean isSideEffectFree() {
        return false;
    }

    @Override
    public void verifyCanDelete() throws IllegalStateException {
    }

    @Override
    public void verifyCanDelete(final boolean ignoreConnections) throws IllegalStateException {
    }

    @Override
    public void verifyCanStart() throws IllegalStateException {
    }

    @Override
    public void verifyCanStop() throws IllegalStateException {
    }

    @Override
    public void verifyCanUpdate() throws IllegalStateException {
    }

    @Override
    public void verifyCanEnable() throws IllegalStateException {
    }

    @Override
    public void verifyCanDisable() throws IllegalStateException {
    }

    @Override
    public void verifyCanClearState() throws IllegalStateException {
    }

    @Override
    public SchedulingStrategy getSchedulingStrategy() {
        return SchedulingStrategy.TIMER_DRIVEN;
    }

    @Override
    public String getComponentType() {
        return "StatelessGroupNode";
    }

    @Override
    public int getRetryCount() {
        return 0;
    }

    @Override
    public void setRetryCount(final Integer retryCount) {
    }

    @Override
    public Set<String> getRetriedRelationships() {
        return Collections.emptySet();
    }

    @Override
    public void setRetriedRelationships(final Set<String> retriedRelationships) {
    }

    @Override
    public boolean isRelationshipRetried(final Relationship relationship) {
        return false;
    }

    @Override
    public BackoffMechanism getBackoffMechanism() {
        return BackoffMechanism.PENALIZE_FLOWFILE;
    }

    @Override
    public void setBackoffMechanism(final BackoffMechanism backoffMechanism) {
    }

    @Override
    public String getMaxBackoffPeriod() {
        return "0 sec";
    }

    @Override
    public void setMaxBackoffPeriod(final String maxBackoffPeriod) {
    }

    @Override
    public String evaluateParameters(final String value) {
        return value;
    }


    public static class Builder {
        private ProcessGroup rootGroup;
        private ControllerServiceProvider controllerServiceProvider;
        private ExtensionManager extensionManager;
        private RepositoryContextFactory statelessRepoContextFactory;
        private FlowFileRepository nifiFlowFileRepository;
        private ContentRepository nifiContentRepository;
        private ProvenanceEventRepository nifiProvenanceRepo;
        private StateManagerProvider stateManagerProvider;
        private BulletinRepository bulletinRepository;
        private StatelessGroupFactory statelessGroupFactory;
        private LifecycleStateManager lifecycleStateManager;
        private FlowFileEventRepository flowFileEventRepository;
        private long boredYieldMillis = 10L;

        public Builder rootGroup(final ProcessGroup rootGroup) {
            this.rootGroup = rootGroup;
            return this;
        }

        public Builder controllerServiceProvider(final ControllerServiceProvider controllerServiceProvider) {
            this.controllerServiceProvider = controllerServiceProvider;
            return this;
        }

        public Builder extensionManager(final ExtensionManager extensionManager) {
            this.extensionManager = extensionManager;
            return this;
        }

        public Builder statelessRepositoryContextFactory(final RepositoryContextFactory statelessRepoContextFactory) {
            this.statelessRepoContextFactory = statelessRepoContextFactory;
            return this;
        }

        public Builder nifiFlowFileRepository(final FlowFileRepository nifiFlowFileRepository) {
            this.nifiFlowFileRepository = nifiFlowFileRepository;
            return this;
        }

        public Builder nifiContentRepository(final ContentRepository nifiContentRepository) {
            this.nifiContentRepository = nifiContentRepository;
            return this;
        }

        public Builder nifiProvenanceRepository(final ProvenanceEventRepository nifiProvenanceRepo) {
            this.nifiProvenanceRepo = nifiProvenanceRepo;
            return this;
        }

        public Builder stateManagerProvider(final StateManagerProvider stateManagerProvider) {
            this.stateManagerProvider = stateManagerProvider;
            return this;
        }

        public Builder bulletinRepository(final BulletinRepository bulletinRepository) {
            this.bulletinRepository = bulletinRepository;
            return this;
        }

        public Builder statelessGroupFactory(final StatelessGroupFactory statelessGroupFactory) {
            this.statelessGroupFactory = statelessGroupFactory;
            return this;
        }

        public Builder lifecycleStateManager(final LifecycleStateManager lifecycleStateManager) {
            this.lifecycleStateManager = lifecycleStateManager;
            return this;
        }

        public Builder flowFileEventRepository(final FlowFileEventRepository flowFileEventRepository) {
            this.flowFileEventRepository = flowFileEventRepository;
            return this;
        }

        public Builder boredYieldDuration(final long period, final TimeUnit timeUnit) {
            this.boredYieldMillis = TimeUnit.MILLISECONDS.convert(period, timeUnit);
            return this;
        }

        public StatelessGroupNode build() {
            if (rootGroup == null) {
                throw new IllegalStateException("Root ProcessGroup must be provided");
            }
            if (controllerServiceProvider == null) {
                throw new IllegalStateException("Controller Service Provider must be provided");
            }
            if (extensionManager == null) {
                throw new IllegalStateException("Extension Manager must be provided");
            }
            if (statelessRepoContextFactory == null) {
                throw new IllegalStateException("Stateless Repository Context Factory must be provided");
            }
            if (nifiFlowFileRepository == null) {
                throw new IllegalStateException("FlowFile Repository must be provided");
            }
            if (nifiContentRepository == null) {
                throw new IllegalStateException("Content Repository must be provided");
            }
            if (nifiProvenanceRepo == null) {
                throw new IllegalStateException("Provenance Repository must be provided");
            }
            if (stateManagerProvider == null) {
                throw new IllegalStateException("State Manager Provider must be provided");
            }
            if (bulletinRepository == null) {
                throw new IllegalStateException("Bulletin Repository must be provided");
            }
            if (statelessGroupFactory == null) {
                throw new IllegalStateException("Stateless Group Factory must be provided");
            }
            if (lifecycleStateManager == null) {
                throw new IllegalStateException("Lifecycle State Manager must be provided");
            }
            if (flowFileEventRepository == null) {
                throw new IllegalStateException("FlowFile Event Repository must be provided");
            }

            return new StandardStatelessGroupNode(this);
        }

    }
}
