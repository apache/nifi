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
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.LocalPort;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.RepositoryContext;
import org.apache.nifi.controller.repository.StandardProcessSessionFactory;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.state.StandardStateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.TerminatedTaskException;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.stateless.engine.ExecutionProgress;
import org.apache.nifi.stateless.engine.ExecutionProgress.CompletionAction;
import org.apache.nifi.stateless.engine.ProcessContextFactory;
import org.apache.nifi.stateless.engine.StandardExecutionProgress;
import org.apache.nifi.stateless.queue.DrainableFlowFileQueue;
import org.apache.nifi.stateless.repository.ByteArrayContentRepository;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;
import org.apache.nifi.stateless.session.AsynchronousCommitTracker;
import org.apache.nifi.util.Connectables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class StandardStatelessFlow implements StatelessDataflow {
    private static final Logger logger = LoggerFactory.getLogger(StandardStatelessFlow.class);
    private static final long COMPONENT_ENABLE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);
    private static final long TEN_MILLIS_IN_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    private final ProcessGroup rootGroup;
    private final List<Connection> allConnections;
    private final List<ReportingTaskNode> reportingTasks;
    private final Set<Connectable> rootConnectables;
    private final ControllerServiceProvider controllerServiceProvider;
    private final ProcessContextFactory processContextFactory;
    private final RepositoryContextFactory repositoryContextFactory;
    private final List<FlowFileQueue> internalFlowFileQueues;
    private final DataflowDefinition<?> dataflowDefinition;
    private final StatelessStateManagerProvider stateManagerProvider;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ProcessScheduler processScheduler;
    private final AsynchronousCommitTracker tracker = new AsynchronousCommitTracker();
    private final TransactionThresholdMeter transactionThresholdMeter;
    private final List<BackgroundTask> backgroundTasks = new ArrayList<>();

    private volatile ExecutorService runDataflowExecutor;
    private volatile ScheduledExecutorService backgroundTaskExecutor;
    private volatile boolean initialized = false;

    public StandardStatelessFlow(final ProcessGroup rootGroup, final List<ReportingTaskNode> reportingTasks, final ControllerServiceProvider controllerServiceProvider,
                                 final ProcessContextFactory processContextFactory, final RepositoryContextFactory repositoryContextFactory, final DataflowDefinition<?> dataflowDefinition,
                                 final StatelessStateManagerProvider stateManagerProvider, final ProcessScheduler processScheduler) {
        this.rootGroup = rootGroup;
        this.allConnections = rootGroup.findAllConnections();
        this.reportingTasks = reportingTasks;
        this.controllerServiceProvider = controllerServiceProvider;
        this.processContextFactory = processContextFactory;
        this.repositoryContextFactory = repositoryContextFactory;
        this.dataflowDefinition = dataflowDefinition;
        this.stateManagerProvider = stateManagerProvider;
        this.processScheduler = processScheduler;
        this.transactionThresholdMeter = new TransactionThresholdMeter(dataflowDefinition.getTransactionThresholds());

        rootConnectables = new HashSet<>();

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

    private void discoverRootInputPorts(final ProcessGroup processGroup, final Set<Connectable> rootComponents) {
        for (final Port port : processGroup.getInputPorts()) {
            for (final Connection connection : port.getConnections()) {
                final Connectable connectable = connection.getDestination();
                rootComponents.add(connectable);
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

    @Override
    public void initialize() {
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
            enableControllerServices(rootGroup);

            waitForServicesEnabled(rootGroup);
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
        final long cutoff = startTime + COMPONENT_ENABLE_TIMEOUT_MILLIS;

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
    public void shutdown() {
        if (runDataflowExecutor != null) {
            runDataflowExecutor.shutdown();
        }
        if (backgroundTaskExecutor != null) {
            backgroundTaskExecutor.shutdown();
        }

        rootGroup.stopProcessing();
        rootGroup.findAllRemoteProcessGroups().forEach(RemoteProcessGroup::shutdown);
        rootGroup.shutdown();

        final Set<ControllerServiceNode> allControllerServices = rootGroup.findAllControllerServices();
        controllerServiceProvider.disableControllerServicesAsync(allControllerServices);
        reportingTasks.forEach(processScheduler::unschedule);

        stateManagerProvider.shutdown();

        // invoke any methods annotated with @OnShutdown on Controller Services
        allControllerServices.forEach(cs -> processScheduler.shutdownControllerService(cs, controllerServiceProvider));

        // invoke any methods annotated with @OnShutdown on Reporting Tasks
        reportingTasks.forEach(processScheduler::shutdownReportingTask);

        processScheduler.shutdown();
        repositoryContextFactory.shutdown();
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
                future.get(COMPONENT_ENABLE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
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
                future.get(COMPONENT_ENABLE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (final Exception e) {
                final String validationErrors = performValidation().toString();
                throw new IllegalStateException("Processor " + processor + " has not fully enabled. Current Validation Status is "
                    + processor.getValidationStatus() + ". All validation errors: " + validationErrors);
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
    public DataflowTrigger trigger() {
        if (!initialized) {
            throw new IllegalStateException("Must initialize dataflow before triggering it");
        }

        final BlockingQueue<TriggerResult> resultQueue = new LinkedBlockingQueue<>();

        final ExecutionProgress executionProgress = new StandardExecutionProgress(rootGroup, internalFlowFileQueues, resultQueue,
            (ByteArrayContentRepository) repositoryContextFactory.getContentRepository(), dataflowDefinition.getFailurePortNames(), tracker,
            stateManagerProvider);

        final AtomicReference<Future<?>> processFuture = new AtomicReference<>();
        final DataflowTrigger trigger = new DataflowTrigger() {
            @Override
            public void cancel() {
                executionProgress.notifyExecutionCanceled();

                final Future<?> future = processFuture.get();
                if (future != null) {
                    future.cancel(true);
                }
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

        final Future<?> future = runDataflowExecutor.submit(() -> executeDataflow(resultQueue, executionProgress, tracker));
        processFuture.set(future);

        return trigger;
    }


    private void executeDataflow(final BlockingQueue<TriggerResult> resultQueue, final ExecutionProgress executionProgress, final AsynchronousCommitTracker tracker) {
        final long startNanos = System.nanoTime();
        transactionThresholdMeter.reset();

        final StatelessFlowCurrent current = new StandardStatelessFlowCurrent.Builder()
            .commitTracker(tracker)
            .executionProgress(executionProgress)
            .processContextFactory(processContextFactory)
            .repositoryContextFactory(repositoryContextFactory)
            .rootConnectables(rootConnectables)
            .transactionThresholdMeter(transactionThresholdMeter)
            .build();

        try {
            current.triggerFlow();

            logger.debug("Completed triggering of components in dataflow. Will now wait for acknowledgment");
            final CompletionAction completionAction = executionProgress.awaitCompletionAction();

            switch (completionAction) {
                case CANCEL:
                    logger.debug("Dataflow was canceled");
                    purge();
                    break;
                case COMPLETE:
                default:
                    final long nanos = System.nanoTime() - startNanos;
                    final String prettyPrinted = (nanos > TEN_MILLIS_IN_NANOS) ? (TimeUnit.NANOSECONDS.toMillis(nanos) + " millis") : NumberFormat.getInstance().format(nanos) + " nanos";
                    logger.info("Ran dataflow in {}", prettyPrinted);
                    break;
            }
        } catch (final TerminatedTaskException tte) {
            // This occurs when the caller invokes the cancel() method of DataflowTrigger.
            logger.debug("Caught a TerminatedTaskException", tte);
            purge();
            tracker.triggerFailureCallbacks(tte);
            stateManagerProvider.rollbackUpdates();
            resultQueue.offer(new CanceledTriggerResult());
        } catch (final Throwable t) {
            logger.error("Failed to execute dataflow", t);
            purge();
            tracker.triggerFailureCallbacks(t);
            stateManagerProvider.rollbackUpdates();
            resultQueue.offer(new ExceptionalTriggerResult(t));
        }
    }


    @Override
    public Set<String> getInputPortNames() {
        return rootGroup.getInputPorts().stream()
            .map(Port::getName)
            .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getOutputPortNames() {
        return rootGroup.getOutputPorts().stream()
            .map(Port::getName)
            .collect(Collectors.toSet());
    }

    @Override
    public QueueSize enqueue(final byte[] flowFileContents, final Map<String, String> attributes, final String portName) {
        final Port inputPort = rootGroup.getInputPortByName(portName);
        if (inputPort == null) {
            throw new IllegalArgumentException("No Input Port exists with name <" + portName + ">. Valid Port names are " + getInputPortNames());
        }

        final RepositoryContext repositoryContext = repositoryContextFactory.createRepositoryContext(inputPort);
        final ProcessSessionFactory sessionFactory = new StandardProcessSessionFactory(repositoryContext, () -> false);
        final ProcessSession session = sessionFactory.createSession();
        try {
            // Get one of the outgoing connections for the Input Port so that we can return QueueSize for it.
            // It doesn't really matter which connection we get because all will have the same data queued up.
            final Set<Connection> portConnections = inputPort.getConnections();
            if (portConnections.isEmpty()) {
                throw new IllegalStateException("Cannot enqueue data for Input Port <" + portName + "> because it has no outgoing connections");
            }

            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, out -> out.write(flowFileContents));
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
            ((DrainableFlowFileQueue) connection.getFlowFileQueue()).drainTo(flowFiles);
            flowFiles.clear();
        }
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
            if (stateMap.getVersion() == -1) {
                // Version of -1 indicates no state has been stored.
                continue;
            }

            final SerializableStateMap serializableStateMap = new SerializableStateMap();
            serializableStateMap.setStateValues(stateMap.toMap());
            serializableStateMap.setVersion(stateMap.getVersion());

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

            final StateMap stateMap = new StandardStateMap(deserialized.getStateValues(), deserialized.getVersion());
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

    @SuppressWarnings("unused")
    public Set<Processor> findAllProcessors() {
        return rootGroup.findAllProcessors().stream()
            .map(ProcessorNode::getProcessor)
            .collect(Collectors.toSet());
    }

    private static class SerializableStateMap {
        private long version;
        private Map<String, String> stateValues;

        public long getVersion() {
            return version;
        }

        public void setVersion(final long version) {
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
}
