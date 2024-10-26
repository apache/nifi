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

package org.apache.nifi.stateless.flow;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.StatelessStateManagerProvider;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.LocalPort;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.RepositoryContext;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.StandardProcessSessionFactory;
import org.apache.nifi.controller.repository.StandardRepositoryRecord;
import org.apache.nifi.controller.repository.metrics.NopPerformanceTracker;
import org.apache.nifi.controller.scheduling.LifecycleStateManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.controller.state.StandardStateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.TerminatedTaskException;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.stateless.engine.ExecutionProgress;
import org.apache.nifi.stateless.engine.ProcessContextFactory;
import org.apache.nifi.stateless.engine.StandardExecutionProgress;
import org.apache.nifi.stateless.queue.DrainableFlowFileQueue;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;
import org.apache.nifi.stateless.repository.StatelessProvenanceRepository;
import org.apache.nifi.stateless.session.AsynchronousCommitTracker;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.Connectables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.NumberFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StandardStatelessFlow implements StatelessDataflow {
    private static final Logger logger = LoggerFactory.getLogger(StandardStatelessFlow.class);
    private static final long TEN_MILLIS_IN_NANOS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final String PARENT_FLOW_GROUP_ID = "stateless-flow";

    private final ProcessGroup rootGroup;
    private final List<Connection> allConnections;
    private final List<ReportingTaskNode> reportingTasks;
    private final Set<Connectable> rootConnectables;
    private final Map<String, Port> inputPortsByName;
    private final ControllerServiceProvider controllerServiceProvider;
    private final ProcessContextFactory processContextFactory;
    private final RepositoryContextFactory repositoryContextFactory;
    private final List<FlowFileQueue> internalFlowFileQueues;
    private final DataflowDefinition dataflowDefinition;
    private final StatelessStateManagerProvider stateManagerProvider;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ProcessScheduler processScheduler;
    private final AsynchronousCommitTracker tracker;
    private final TransactionThresholdMeter transactionThresholdMeter;
    private final List<BackgroundTask> backgroundTasks = new ArrayList<>();
    private final BulletinRepository bulletinRepository;
    private final LifecycleStateManager lifecycleStateManager;
    private final long componentEnableTimeoutMillis;
    private final List<Port> inputPorts;

    private volatile ExecutorService runDataflowExecutor;
    private volatile ScheduledExecutorService backgroundTaskExecutor;
    private volatile boolean initialized = false;
    private volatile Boolean stateful = null;
    private volatile boolean shutdown = false;

    public StandardStatelessFlow(final ProcessGroup rootGroup, final List<ReportingTaskNode> reportingTasks, final ControllerServiceProvider controllerServiceProvider,
                                 final ProcessContextFactory processContextFactory, final RepositoryContextFactory repositoryContextFactory, final DataflowDefinition dataflowDefinition,
                                 final StatelessStateManagerProvider stateManagerProvider, final ProcessScheduler processScheduler, final BulletinRepository bulletinRepository,
                                 final LifecycleStateManager lifecycleStateManager, final Duration componentEnableTimeout) {
        this.rootGroup = rootGroup;
        this.allConnections = rootGroup.findAllConnections();
        this.reportingTasks = reportingTasks;
        this.controllerServiceProvider = controllerServiceProvider;
        this.processContextFactory = new CachingProcessContextFactory(processContextFactory);
        this.repositoryContextFactory = repositoryContextFactory;
        this.dataflowDefinition = dataflowDefinition;
        this.stateManagerProvider = stateManagerProvider;
        this.processScheduler = processScheduler;
        this.transactionThresholdMeter = new TransactionThresholdMeter(dataflowDefinition.getTransactionThresholds());
        this.bulletinRepository = bulletinRepository;
        this.componentEnableTimeoutMillis = componentEnableTimeout.toMillis();
        this.tracker = new AsynchronousCommitTracker(rootGroup);
        this.lifecycleStateManager = lifecycleStateManager;
        this.inputPorts = new ArrayList<>(rootGroup.getInputPorts());

        rootConnectables = new HashSet<>();
        inputPortsByName = mapInputPortsToName(rootGroup);

        discoverRootProcessors(rootGroup, rootConnectables);
        discoverRootRemoteGroupPorts(rootGroup, rootConnectables);
        discoverRootInputPorts(rootGroup, rootConnectables);

        internalFlowFileQueues = discoverInternalFlowFileQueues(rootGroup);
    }

    private List<FlowFileQueue> discoverInternalFlowFileQueues(final ProcessGroup group) {
        final Set<Port> rootGroupInputPorts = rootGroup.getInputPorts();
        final Set<Port> rootGroupOutputPorts = rootGroup.getOutputPorts();

        //noinspection SuspiciousMethodCalls
        return group.findAllConnections().stream()
            .filter(connection -> !rootGroupInputPorts.contains(connection.getSource()))
            .filter(connection -> !rootGroupOutputPorts.contains(connection.getDestination()))
            .map(Connection::getFlowFileQueue)
            .distinct()
            .collect(Collectors.toCollection(ArrayList::new));
    }

    private Map<String, Port> mapInputPortsToName(final ProcessGroup group) {
        final Map<String, Port> inputPortsByName = new HashMap<>();
        for (final Port port : group.getInputPorts()) {
            inputPortsByName.put(port.getName(), port);
        }

        return inputPortsByName;
    }

    private void discoverRootInputPorts(final ProcessGroup processGroup, final Set<Connectable> rootComponents) {
        for (final Port port : processGroup.getInputPorts()) {
            for (final Connection connection : port.getConnections()) {
                final Connectable connectable = connection.getDestination();
                if (!isTerminalPort(connectable)) {
                    rootComponents.add(connectable);
                }
            }
        }
    }

    private void discoverRootProcessors(final ProcessGroup processGroup, final Set<Connectable> rootComponents) {
        for (final ProcessorNode processor : processGroup.findAllProcessors()) {
            // If no incoming connections (other than self-loops) then consider Processor a "root" processor.
            if (!Connectables.hasNonLoopConnection(processor)) {
                rootComponents.add(processor);
            }
        }
    }

    private void discoverRootRemoteGroupPorts(final ProcessGroup processGroup, final Set<Connectable> rootComponents) {
        final List<RemoteProcessGroup> rpgs = processGroup.findAllRemoteProcessGroups();
        for (final RemoteProcessGroup rpg : rpgs) {
            final Set<RemoteGroupPort> remoteGroupPorts = rpg.getOutputPorts();
            for (final RemoteGroupPort remoteGroupPort : remoteGroupPorts) {
                if (!remoteGroupPort.getConnections().isEmpty()) {
                    rootComponents.add(remoteGroupPort);
                }
            }
        }
    }

    public static boolean isTerminalPort(final Connectable connectable) {
        final ConnectableType connectableType = connectable.getConnectableType();
        if (connectableType != ConnectableType.OUTPUT_PORT) {
            return false;
        }

        final ProcessGroup portGroup = connectable.getProcessGroup();
        if (PARENT_FLOW_GROUP_ID.equals(portGroup.getIdentifier())) {
            logger.debug("FlowFiles queued for {} but this is a Terminal Port. Will not trigger Port to run.", connectable);
            return true;
        }

        return false;
    }

    @Override
    public void initialize(final StatelessDataflowInitializationContext initializationContext) {
        if (initialized) {
            logger.debug("{} initialize() was called, but dataflow has already been initialized. Returning without doing anything.", this);
            return;
        }

        initialized = true;

        // Trigger validation to occur so that components can be enabled/started.
        performValidation();

        // Enable Controller Services and start processors in the flow.
        // This is different than the calling ProcessGroup.startProcessing() because
        // that method triggers the behavior to happen in the background and provides no way of knowing
        // whether or not the activity succeeded. We want to block and wait for everything to start/enable
        // before proceeding any further.
        try {
            final long serviceEnableStart = System.currentTimeMillis();

            if (initializationContext.isEnableControllerServices()) {
                enableControllerServices(rootGroup);
                waitForServicesEnabled(rootGroup);
            } else {
                logger.debug("Skipping Controller Service enablement because initializationContext.isEnableControllerServices() returned false");
            }

            final long serviceEnableMillis = System.currentTimeMillis() - serviceEnableStart;

            // Perform validation again so that any processors that reference controller services that were just
            // enabled now can be started
            final long validationStart = System.currentTimeMillis();
            final StatelessDataflowValidation validationResult = performValidation();
            final long validationMillis = System.currentTimeMillis() - validationStart;

            if (!validationResult.isValid()) {
                logger.warn("{} Attempting to initialize dataflow but found at least one invalid component: {}", this, validationResult);
            }

            startProcessors(rootGroup);
            startRemoteGroups(rootGroup);

            startReportingTasks();

            final long initializationMillis = System.currentTimeMillis() - validationStart;

            logger.info("Successfully initialized components in {} millis ({} millis to perform validation, {} millis for services to enable)",
                initializationMillis, validationMillis, serviceEnableMillis);

            // Create executor for dataflow
            final String flowName = dataflowDefinition.getFlowName();
            final String threadName = (flowName == null || flowName.trim().isEmpty()) ? "Run Dataflow" : "Run Dataflow " + flowName;
            runDataflowExecutor = Executors.newFixedThreadPool(1, createNamedThreadFactory(threadName, false));

            // Periodically log component statuses
            backgroundTaskExecutor = Executors.newScheduledThreadPool(1, createNamedThreadFactory("Background Tasks", true));
            backgroundTasks.forEach(task -> backgroundTaskExecutor.scheduleWithFixedDelay(task.getTask(), task.getSchedulingPeriod(), task.getSchedulingPeriod(), task.getSchedulingUnit()));
        } catch (final Throwable t) {
            processScheduler.shutdown();

            if (runDataflowExecutor != null) {
                runDataflowExecutor.shutdownNow();
            }

            if (backgroundTaskExecutor != null) {
                backgroundTaskExecutor.shutdownNow();
            }

            throw t;
        }
    }

    private ThreadFactory createNamedThreadFactory(final String name, final boolean daemon) {
        return (Runnable r) -> {
            final Thread thread = Executors.defaultThreadFactory().newThread(r);
            thread.setName(name);
            thread.setDaemon(daemon);
            return thread;
        };
    }

    /**
     * Schedules the given background task to run periodically after the dataflow has been initialized until it has been shutdown
     * @param task the task to run
     * @param period how often to run it
     * @param unit the unit for the time period
     */
    public void scheduleBackgroundTask(final Runnable task, final long period, final TimeUnit unit) {
        backgroundTasks.add(new BackgroundTask(task, period, unit));
    }

    private void waitForServicesEnabled(final ProcessGroup group) {
        final long startTime = System.currentTimeMillis();
        final long cutoff = startTime + this.componentEnableTimeoutMillis;

        final Set<ControllerServiceNode> serviceNodes = group.findAllControllerServices();
        for (final ControllerServiceNode serviceNode : serviceNodes) {
            final boolean enabled;
            try {
                enabled = serviceNode.awaitEnabled(cutoff - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for Controller Services to enable", ie);
            }

            if (enabled) {
                continue;
            }

            if (System.currentTimeMillis() > cutoff) {
                final String validationErrors = performValidation().toString();
                throw new IllegalStateException("At least one Controller Service never finished enabling. All validation errors: " + validationErrors);
            }
        }
    }


    private void startReportingTasks() {
        reportingTasks.forEach(this::startReportingTask);
    }

    private void startReportingTask(final ReportingTaskNode taskNode) {
        processScheduler.schedule(taskNode);
    }

    @Override
    public void shutdown(final boolean triggerComponentShutdown, final boolean interruptProcessors) {
        if (shutdown) {
            return;
        }

        shutdown = true;
        logger.info("Shutting down dataflow {}", rootGroup.getName());

        if (runDataflowExecutor != null) {
            if (interruptProcessors) {
                runDataflowExecutor.shutdownNow();
            } else {
                runDataflowExecutor.shutdown();
            }
        }
        if (backgroundTaskExecutor != null) {
            backgroundTaskExecutor.shutdown();
        }

        logger.info("Stopping all components");
        rootGroup.stopComponents().join();
        rootGroup.findAllRemoteProcessGroups().forEach(RemoteProcessGroup::shutdown);

        if (triggerComponentShutdown) {
            rootGroup.shutdown();
        }

        reportingTasks.forEach(processScheduler::unschedule);

        final Set<ControllerServiceNode> allControllerServices = rootGroup.findAllControllerServices();
        logger.info("Disabling {} Controller Services", allControllerServices.size());

        try {
            controllerServiceProvider.disableControllerServicesAsync(allControllerServices).get();
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            logger.error("Failed to properly disable one or more of the following Controller Services: {} due to being interrupted while waiting for them to disable", allControllerServices, ie);
        } catch (final Exception e) {
            logger.error("Failed to properly disable one or more of the following Controller Services: {}", allControllerServices, e);
        }

        logger.info("Finished disabling all Controller Services");
        stateManagerProvider.shutdown();

        // invoke any methods annotated with @OnShutdown on Controller Services
        if (triggerComponentShutdown) {
            allControllerServices.forEach(cs -> processScheduler.shutdownControllerService(cs, controllerServiceProvider));

            // invoke any methods annotated with @OnShutdown on Reporting Tasks
            reportingTasks.forEach(processScheduler::shutdownReportingTask);
        }

        processScheduler.shutdown();
        repositoryContextFactory.shutdown();

        logger.info("Finished shutting down dataflow");
    }

    @Override
    public StatelessDataflowValidation performValidation() {
        final Map<ComponentNode, List<ValidationResult>> resultsMap = new HashMap<>();

        for (final ControllerServiceNode serviceNode : rootGroup.findAllControllerServices()) {
            performValidation(serviceNode, resultsMap);
        }

        for (final ProcessorNode procNode : rootGroup.findAllProcessors()) {
            performValidation(procNode, resultsMap);
        }

        return new StandardStatelessDataflowValidation(resultsMap);
    }

    private void performValidation(final ComponentNode componentNode, final Map<ComponentNode, List<ValidationResult>> resultsMap) {
        final ValidationStatus validationStatus = componentNode.performValidation();
        if (validationStatus == ValidationStatus.VALID) {
            return;
        }

        final Collection<ValidationResult> validationResults = componentNode.getValidationErrors();

        final List<ValidationResult> invalidResults = new ArrayList<>();
        for (final ValidationResult result : validationResults) {
            if (!result.isValid()) {
                invalidResults.add(result);
            }
        }

        resultsMap.put(componentNode, invalidResults);
    }

    private void enableControllerServices(final ProcessGroup processGroup) {
        final Set<ControllerServiceNode> services = processGroup.getControllerServices(false);
        for (final ControllerServiceNode serviceNode : services) {
            final Future<?> future = controllerServiceProvider.enableControllerServiceAndDependencies(serviceNode);

            try {
                future.get(this.componentEnableTimeoutMillis, TimeUnit.MILLISECONDS);
            } catch (final Exception e) {
                throw new IllegalStateException("Controller Service " + serviceNode + " has not fully enabled. Current Validation Status is "
                    + serviceNode.getValidationStatus() + " with validation Errors: " + serviceNode.getValidationErrors(), e);
            }
        }

        processGroup.getProcessGroups().forEach(this::enableControllerServices);
    }

    private void startProcessors(final ProcessGroup processGroup) {
        final Collection<ProcessorNode> processors = processGroup.getProcessors();
        final Map<ProcessorNode, Future<?>> futures = new HashMap<>(processors.size());

        for (final ProcessorNode processor : processors) {
            final Future<?> future = processGroup.startProcessor(processor, true);
            futures.put(processor, future);
        }

        for (final Map.Entry<ProcessorNode, Future<?>> entry : futures.entrySet()) {
            final ProcessorNode processor = entry.getKey();
            final Future<?> future = entry.getValue();

            final long start = System.currentTimeMillis();
            try {
                future.get(this.componentEnableTimeoutMillis, TimeUnit.MILLISECONDS);
            } catch (final Exception e) {
                final StatelessDataflowValidation validation = performValidation();
                if (validation.isValid()) {
                    throw new IllegalStateException("Processor " + processor + " is valid but has not fully started", e);
                } else {
                    final String validationErrors = performValidation().toString();
                    throw new IllegalStateException("Processor " + processor + " has not fully started. Current Validation Status is "
                                                    + processor.getValidationStatus() + ". All validation errors: " + validationErrors);
                }
            }

            final long millis = System.currentTimeMillis() - start;
            logger.debug("Waited {} millis for {} to start", millis, processor);
        }

        processGroup.getProcessGroups().forEach(this::startProcessors);
    }

    private void startRemoteGroups(final ProcessGroup processGroup) {
        final List<RemoteProcessGroup> rpgs = processGroup.findAllRemoteProcessGroups();
        rpgs.forEach(RemoteProcessGroup::initialize);
        rpgs.forEach(RemoteProcessGroup::startTransmitting);
    }

    @Override
    public DataflowTrigger trigger(final DataflowTriggerContext triggerContext) {
        if (!initialized) {
            throw new IllegalStateException("Must initialize dataflow before triggering it");
        }

        final BlockingQueue<TriggerResult> resultQueue = new LinkedBlockingQueue<>();

        final ExecutionProgress executionProgress = new StandardExecutionProgress(rootGroup, internalFlowFileQueues, resultQueue,
            repositoryContextFactory, dataflowDefinition.getFailurePortNames(), tracker, stateManagerProvider, triggerContext, this::purge);

        final Future<?> future = runDataflowExecutor.submit(
            () -> executeDataflow(resultQueue, executionProgress, tracker, triggerContext));

        final DataflowTrigger trigger = new DataflowTrigger() {
            @Override
            public void cancel() {
                executionProgress.notifyExecutionCanceled();
                future.cancel(true);
            }

            @Override
            public Optional<TriggerResult> getResultNow() {
                final TriggerResult result = resultQueue.poll();
                return Optional.ofNullable(result);
            }

            @Override
            public Optional<TriggerResult> getResult(final long maxWaitTime, final TimeUnit timeUnit) throws InterruptedException {
                final TriggerResult result = resultQueue.poll(maxWaitTime, timeUnit);
                return Optional.ofNullable(result);
            }

            @Override
            public TriggerResult getResult() throws InterruptedException {
                final TriggerResult result = resultQueue.take();
                return result;
            }
        };

        return trigger;
    }


    private void executeDataflow(final BlockingQueue<TriggerResult> resultQueue, final ExecutionProgress executionProgress, final AsynchronousCommitTracker tracker,
                                 final DataflowTriggerContext triggerContext) {
        final long startNanos = System.nanoTime();
        transactionThresholdMeter.reset();

        final StatelessFlowCurrent current = new StandardStatelessFlowCurrent.Builder()
            .commitTracker(tracker)
            .executionProgress(executionProgress)
            .processContextFactory(processContextFactory)
            .repositoryContextFactory(repositoryContextFactory)
            .rootConnectables(rootConnectables)
            .flowFileSupplier(triggerContext.getFlowFileSupplier())
            .provenanceEventRepository(triggerContext.getProvenanceEventRepository())
            .inputPorts(inputPorts)
            .transactionThresholdMeter(transactionThresholdMeter)
            .lifecycleStateManager(lifecycleStateManager)
            .build();

        final Runnable logCompletion = () -> {
            if (logger.isDebugEnabled()) {
                final long nanos = System.nanoTime() - startNanos;
                final String prettyPrinted = (nanos > TEN_MILLIS_IN_NANOS) ? (TimeUnit.NANOSECONDS.toMillis(nanos) + " millis") : NumberFormat.getInstance().format(nanos) + " nanos";
                logger.debug("Ran dataflow in {}", prettyPrinted);
            }
        };

        try {
            current.triggerFlow();

            executionProgress.enqueueTriggerResult(logCompletion, cause -> {
                logger.error("Failed to execute dataflow", cause);
            });

            logger.debug("Completed triggering of components in dataflow. Will not wait for acknowledgment as the invocation is asynchronous.");
        } catch (final TerminatedTaskException tte) {
            // This occurs when the caller invokes the cancel() method of DataflowTrigger.
            logger.debug("Caught a TerminatedTaskException", tte);
            executionProgress.notifyExecutionFailed(tte);
            resultQueue.offer(new CanceledTriggerResult());
        } catch (final Throwable t) {
            logger.error("Failed to execute dataflow", t);
            executionProgress.notifyExecutionFailed(t);
            resultQueue.offer(new ExceptionalTriggerResult(t));
        }
    }

    @Override
    public boolean isStateful() {
        if (stateful == null) {
            final boolean hasStatefulReportingTask = reportingTasks.stream().anyMatch(this::isStateful);
            if (hasStatefulReportingTask) {
                return true;
            }
            stateful = isStateful(rootGroup);
        }
        return stateful;
    }

    private boolean isStateful(final ProcessGroup processGroup) {
        final boolean hasStatefulProcessor = processGroup.getProcessors().stream().anyMatch(this::isStateful);

        if (hasStatefulProcessor) {
            return true;
        }
        final boolean hasStatefulControllerService = processGroup.getControllerServices(false).stream().anyMatch(this::isStateful);
        if (hasStatefulControllerService) {
            return true;
        }

        return processGroup.getProcessGroups().stream().anyMatch(this::isStateful);
    }

    private boolean isStateful(final ProcessorNode processorNode) {
        final Processor processor = processorNode.getProcessor();
        final ProcessContext context = processContextFactory.createProcessContext(processorNode);
        return processor.isStateful(context);
    }

    private boolean isStateful(final ControllerServiceNode controllerServiceNode) {
        final ControllerService controllerService = controllerServiceNode.getControllerServiceImplementation();
        final ConfigurationContext context = new StandardConfigurationContext(controllerServiceNode, controllerServiceProvider, null);
        return controllerService.isStateful(context);
    }

    private boolean isStateful(final ReportingTaskNode reportingTaskNode) {
        final ReportingTask reportingTask = reportingTaskNode.getReportingTask();
        return reportingTask.isStateful(reportingTaskNode.getReportingContext());
    }

    @Override
    public Set<String> getInputPortNames() {
        return inputPortsByName.keySet();
    }

    @Override
    public Set<String> getOutputPortNames() {
        return rootGroup.getOutputPorts().stream()
            .map(Port::getName)
            .collect(Collectors.toSet());
    }

    @Override
    public QueueSize enqueue(final byte[] flowFileContents, final Map<String, String> attributes, final String portName) {
        try {
            try (final InputStream bais = new ByteArrayInputStream(flowFileContents)) {
                return enqueue(bais, attributes, portName);
            }
        } catch (final IOException e) {
            throw new FlowFileAccessException("Failed to enqueue FlowFile", e);
        }
    }

    @Override
    public QueueSize enqueue(final InputStream flowFileContents, final Map<String, String> attributes, final String portName) {
        final Port inputPort = rootGroup.getInputPortByName(portName);
        if (inputPort == null) {
            throw new IllegalArgumentException("No Input Port exists with name <" + portName + ">. Valid Port names are " + getInputPortNames());
        }

        final RepositoryContext repositoryContext = repositoryContextFactory.createRepositoryContext(inputPort, new StatelessProvenanceRepository(10));
        final ProcessSessionFactory sessionFactory = new StandardProcessSessionFactory(repositoryContext, () -> false, new NopPerformanceTracker());
        final ProcessSession session = sessionFactory.createSession();
        try {
            // Get one of the outgoing connections for the Input Port so that we can return QueueSize for it.
            // It doesn't really matter which connection we get because all will have the same data queued up.
            final Set<Connection> portConnections = inputPort.getConnections();
            if (portConnections.isEmpty()) {
                throw new IllegalStateException("Cannot enqueue data for Input Port <" + portName + "> because it has no outgoing connections");
            }

            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, out -> StreamUtils.copy(flowFileContents, out));
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, LocalPort.PORT_RELATIONSHIP);
            session.commitAsync();

            final Connection firstConnection = portConnections.iterator().next();
            return firstConnection.getFlowFileQueue().size();
        } catch (final Throwable t) {
            session.rollback();
            throw t;
        }
    }


    @Override
    public boolean isFlowFileQueued() {
        for (final Connection connection : allConnections) {
            if (!connection.getFlowFileQueue().isActiveQueueEmpty()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void purge() {
        final List<FlowFileRecord> flowFiles = new ArrayList<>();
        for (final Connection connection : allConnections) {
            try {
                final FlowFileQueue queue = connection.getFlowFileQueue();
                ((DrainableFlowFileQueue) queue).drainTo(flowFiles);

                final List<RepositoryRecord> repositoryRecords = new ArrayList<>();
                for (final FlowFileRecord flowFile : flowFiles) {
                    final StandardRepositoryRecord record = new StandardRepositoryRecord(queue, flowFile);
                    record.markForDelete();
                    repositoryRecords.add(record);
                }

                repositoryContextFactory.getFlowFileRepository().updateRepository(repositoryRecords);
            } catch (final Exception e) {
                logger.warn("Failed to update FlowFile Repository in order to notify it of transient claims. Some content in the Content Repository may not be cleaned up until restart", e);
            }

            flowFiles.clear();
        }

        repositoryContextFactory.getContentRepository().purge();
    }

    @Override
    public Map<String, String> getComponentStates(final Scope scope) {
        final Map<String, StateMap> stateMaps = stateManagerProvider.getAllComponentStates(scope);
        final Map<String, String> componentStates = serializeStateMaps(stateMaps);
        return componentStates;
    }

    private Map<String, String> serializeStateMaps(final Map<String, StateMap> stateMaps) {
        if (stateMaps == null) {
            return Collections.emptyMap();
        }

        final Map<String, String> serializedStateMaps = new HashMap<>();
        for (final Map.Entry<String, StateMap> entry : stateMaps.entrySet()) {
            final String componentId = entry.getKey();
            final StateMap stateMap = entry.getValue();
            if (!stateMap.getStateVersion().isPresent()) {
                continue;
            }

            final SerializableStateMap serializableStateMap = new SerializableStateMap();
            serializableStateMap.setStateValues(stateMap.toMap());
            serializableStateMap.setVersion(stateMap.getStateVersion().orElse(null));

            final String serialized;
            try {
                serialized = objectMapper.writeValueAsString(serializableStateMap);
            } catch (final Exception e) {
                throw new RuntimeException("Failed to serialize components' state maps as Strings", e);
            }

            serializedStateMaps.put(componentId, serialized);
        }

        return serializedStateMaps;
    }

    @Override
    public void setComponentStates(final Map<String, String> componentStates, final Scope scope) {
        final Map<String, StateMap> stateMaps = deserializeStateMaps(componentStates);
        stateManagerProvider.updateComponentsStates(stateMaps, scope);
    }

    private Map<String, StateMap> deserializeStateMaps(final Map<String, String> componentStates) {
        if (componentStates == null) {
            return Collections.emptyMap();
        }

        final Map<String, StateMap> deserializedStateMaps = new HashMap<>();

        for (final Map.Entry<String, String> entry : componentStates.entrySet()) {
            final String componentId = entry.getKey();
            final String serialized = entry.getValue();

            final SerializableStateMap deserialized;
            try {
                deserialized = objectMapper.readValue(serialized, SerializableStateMap.class);
            } catch (final Exception e) {
                // We want to avoid throwing an Exception here because if we do, we may never be able to run the flow again, at least not without
                // destroying all state that exists for the component. Would be better to simply skip the state for this component
                logger.error("Failed to deserialized components' state for component with ID {}. State will be reset to empty", componentId, e);
                continue;
            }

            final StateMap stateMap = new StandardStateMap(deserialized.getStateValues(), Optional.ofNullable(deserialized.getVersion()));
            deserializedStateMaps.put(componentId, stateMap);
        }

        return deserializedStateMaps;
    }

    @Override
    public boolean isSourcePrimaryNodeOnly() {
        for (final Connectable connectable : rootConnectables) {
            if (connectable.isIsolated()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public long getSourceYieldExpiration() {
        long latest = 0L;
        for (final Connectable connectable : rootConnectables) {
            latest = Math.max(latest, connectable.getYieldExpiration());
        }

        return latest;
    }

    @Override
    public BulletinRepository getBulletinRepository() {
        return bulletinRepository;
    }

    @SuppressWarnings("unused")
    public Set<Processor> findAllProcessors() {
        return rootGroup.findAllProcessors().stream()
            .map(ProcessorNode::getProcessor)
            .collect(Collectors.toSet());
    }

    private static class SerializableStateMap {
        private String version;
        private Map<String, String> stateValues;

        public String getVersion() {
            return version;
        }

        public void setVersion(final String version) {
            this.version = version;
        }

        public Map<String, String> getStateValues() {
            return stateValues;
        }

        public void setStateValues(final Map<String, String> stateValues) {
            this.stateValues = stateValues;
        }
    }

    private static class BackgroundTask {
        private final Runnable task;
        private final long schedulingPeriod;
        private final TimeUnit schedulingUnit;

        public BackgroundTask(final Runnable task, final long schedulingPeriod, final TimeUnit schedulingUnit) {
            this.task = task;
            this.schedulingPeriod = schedulingPeriod;
            this.schedulingUnit = schedulingUnit;
        }

        public Runnable getTask() {
            return task;
        }

        public long getSchedulingPeriod() {
            return schedulingPeriod;
        }

        public TimeUnit getSchedulingUnit() {
            return schedulingUnit;
        }
    }

    public ContentRepository getContentRepository() {
        return repositoryContextFactory.getContentRepository();
    }
}
