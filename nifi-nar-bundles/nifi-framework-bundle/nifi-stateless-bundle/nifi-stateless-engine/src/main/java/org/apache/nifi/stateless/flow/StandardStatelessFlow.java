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

import org.apache.nifi.components.ValidationResult;
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
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.RepositoryContext;
import org.apache.nifi.controller.repository.StandardProcessSessionFactory;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.metrics.StandardFlowFileEvent;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.stateless.engine.ProcessContextFactory;
import org.apache.nifi.stateless.queue.DrainableFlowFileQueue;
import org.apache.nifi.stateless.repository.ByteArrayContentRepository;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;
import org.apache.nifi.stateless.session.StatelessProcessSessionFactory;
import org.apache.nifi.util.Connectables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StandardStatelessFlow implements StatelessDataflow {
    private static final Logger logger = LoggerFactory.getLogger(StandardStatelessFlow.class);
    private static final long COMPONENT_ENABLE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);

    private final ProcessGroup rootGroup;
    private final List<ReportingTaskNode> reportingTasks;
    private final Set<Connectable> rootConnectables;
    private final ControllerServiceProvider controllerServiceProvider;
    private final ProcessContextFactory processContextFactory;
    private final RepositoryContextFactory repositoryContextFactory;
    private final DataflowDefinition<VersionedFlowSnapshot> dataflowDefinition;

    private volatile ProcessScheduler processScheduler;

    public StandardStatelessFlow(final ProcessGroup rootGroup, final List<ReportingTaskNode> reportingTasks, final ControllerServiceProvider controllerServiceProvider,
                                 final ProcessContextFactory processContextFactory, final RepositoryContextFactory repositoryContextFactory,
                                 final DataflowDefinition<VersionedFlowSnapshot> dataflowDefinition) {
        this.rootGroup = rootGroup;
        this.reportingTasks = reportingTasks;
        this.controllerServiceProvider = controllerServiceProvider;
        this.processContextFactory = processContextFactory;
        this.repositoryContextFactory = repositoryContextFactory;
        this.dataflowDefinition = dataflowDefinition;

        rootConnectables = new HashSet<>();

        discoverRootProcessors(rootGroup, rootConnectables);
        discoverRootRemoteGroupPorts(rootGroup, rootConnectables);
        discoverRootInputPorts(rootGroup, rootConnectables);
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

    public void initialize(final ProcessScheduler processScheduler) {
        this.processScheduler = processScheduler;

        // Trigger validation to occur so that components can be enabled/started.
        final long validationStart = System.currentTimeMillis();
        performValidation();
        final long validationMillis = System.currentTimeMillis() - validationStart;

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
            performValidation();

            startProcessors(rootGroup);
            startRemoteGroups(rootGroup);

            startReportingTasks();

            final long initializationMillis = System.currentTimeMillis() - validationStart;

            logger.info("Successfully initialized components in {} millis ({} millis to perform validation, {} millis for services to enable)",
                initializationMillis, validationMillis, serviceEnableMillis);
        } catch (final Throwable t) {
            processScheduler.shutdown();
            throw t;
        }
    }

    private void waitForServicesEnabled(final ProcessGroup group) {
        final long startTime = System.currentTimeMillis();
        final long cutoff = startTime + COMPONENT_ENABLE_TIMEOUT_MILLIS;

        while (isAnyServiceEnabling(group)) {
            if (System.currentTimeMillis() > cutoff) {
                final String validationErrors = performValidation().toString();
                throw new IllegalStateException("At least one Controller Service never finished enabling. All validation errors: " + validationErrors);
            }

            logger.debug("At least one Controller Service in group {} is still enabling. Will wait 5 milliseconds and check again", group);

            try {
                Thread.sleep(5L);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for Controller Services to enable", ie);
            }
        }

        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            waitForServicesEnabled(childGroup);
        }
    }

    private boolean isAnyServiceEnabling(final ProcessGroup group) {
        for (final ControllerServiceNode serviceNode : group.getControllerServices(false)) {
            final ControllerServiceState state = serviceNode.getState();
            if (state == ControllerServiceState.ENABLING) {
                return true;
            }
        }

        return false;
    }

    private void startReportingTasks() {
        reportingTasks.forEach(this::startReportingTask);
    }

    private void startReportingTask(final ReportingTaskNode taskNode) {
        processScheduler.schedule(taskNode);
    }

    @Override
    public void shutdown() {
        if (processScheduler != null) {
            processScheduler.shutdown();
        }

        rootGroup.findAllRemoteProcessGroups().forEach(RemoteProcessGroup::shutdown);

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
                    + serviceNode.getValidationStatus() + " with validation Errors: " + serviceNode.getValidationErrors());
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
    public void trigger() {
        for (final Connectable connectable : rootConnectables) {
            final ProcessContext processContext = processContextFactory.createProcessContext(connectable);
            final StatelessProcessSessionFactory sessionFactory = new StatelessProcessSessionFactory(connectable, repositoryContextFactory,
                processContextFactory, dataflowDefinition.getFailurePortNames());

            final long start = System.nanoTime();
            final long processingNanos;
            int invocations = 0;

            final List<Connection> incommingConnections = connectable.getIncomingConnections();
            if (incommingConnections.isEmpty()) {
                // If there is no incoming connection, trigger once.
                logger.debug("Triggering {}", connectable);
                connectable.onTrigger(processContext, sessionFactory);
                invocations = 1;
            } else {
                // If there is an incoming connection, trigger until all incoming connections are empty.
                for (final Connection incomingConnection : incommingConnections) {
                    while (!incomingConnection.getFlowFileQueue().isEmpty()) {
                        logger.debug("Triggering {}", connectable);
                        connectable.onTrigger(processContext, sessionFactory);
                        invocations++;
                    }
                }
            }

            processingNanos = System.nanoTime() - start;
            registerProcessEvent(connectable, invocations, processingNanos);
        }
    }

    private void registerProcessEvent(final Connectable connectable, final int invocations, final long processingNanos) {
        try {
            final StandardFlowFileEvent procEvent = new StandardFlowFileEvent();
            procEvent.setProcessingNanos(processingNanos);
            procEvent.setInvocations(invocations);
            repositoryContextFactory.getFlowFileEventRepository().updateRepository(procEvent, connectable.getIdentifier());
        } catch (final IOException e) {
            logger.error("Unable to update FlowFileEvent Repository for {}; statistics may be inaccurate. Reason for failure: {}", connectable.getRunnableComponent(), e.toString(), e);
        }
    }

    public Map<String, List<FlowFile>> drainOutputQueues() {
        final Map<String, List<FlowFile>> flowFileMap = new HashMap<>();

        for (final Port port : rootGroup.getOutputPorts()) {
            final List<FlowFile> flowFiles = drainOutputQueues(port);
            flowFileMap.put(port.getName(), flowFiles);
        }

        return flowFileMap;
    }

    @Override
    public List<FlowFile> drainOutputQueues(final String portName) {
        final Port port = rootGroup.getOutputPortByName(portName);
        if (port == null) {
            throw new IllegalArgumentException("No Output Port exists with name <" + portName + ">. Valid Port names are " + getOutputPortNames());
        }

        return drainOutputQueues(port);
    }

    private List<FlowFile> drainOutputQueues(final Port port) {
        final List<Connection> incomingConnections = port.getIncomingConnections();
        if (incomingConnections.isEmpty()) {
            return Collections.emptyList();
        }

        final List<FlowFile> portFlowFiles = new ArrayList<>();
        for (final Connection connection : incomingConnections) {
            final DrainableFlowFileQueue flowFileQueue = (DrainableFlowFileQueue) connection.getFlowFileQueue();
            final List<FlowFileRecord> flowFileRecords = new ArrayList<>(flowFileQueue.size().getObjectCount());
            flowFileQueue.drainTo(flowFileRecords);
            portFlowFiles.addAll(flowFileRecords);

            for (final FlowFileRecord flowFileRecord : flowFileRecords) {
                repositoryContextFactory.getContentRepository().decrementClaimantCount(flowFileRecord.getContentClaim());
            }
        }

        return portFlowFiles;
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
    public void enqueue(final byte[] flowFileContents, final Map<String, String> attributes, final String portName) {
        final Port inputPort = rootGroup.getInputPortByName(portName);
        if (inputPort == null) {
            throw new IllegalArgumentException("No Input Port exists with name <" + portName + ">. Valid Port names are " + getInputPortNames());
        }

        final RepositoryContext repositoryContext = repositoryContextFactory.createRepositoryContext(inputPort);
        final ProcessSessionFactory sessionFactory = new StandardProcessSessionFactory(repositoryContext, () -> false);
        final ProcessSession session = sessionFactory.createSession();
        try {
            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, out -> out.write(flowFileContents));
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, LocalPort.PORT_RELATIONSHIP);
            session.commit();
        } catch (final Throwable t) {
            session.rollback();
            throw t;
        }
    }

    @Override
    public byte[] getFlowFileContents(final FlowFile flowFile) {
        if (!(flowFile instanceof FlowFileRecord)) {
            throw new IllegalArgumentException("FlowFile was not created by this flow");
        }

        final FlowFileRecord flowFileRecord = (FlowFileRecord) flowFile;
        final ContentClaim contentClaim = flowFileRecord.getContentClaim();
        final ContentRepository contentRepository = repositoryContextFactory.getContentRepository();
        return ((ByteArrayContentRepository) contentRepository).getBytes(contentClaim);
    }

    @Override
    public int getFlowFilesQueued() {
        return rootGroup.findAllConnections().stream()
            .map(Connection::getFlowFileQueue)
            .map(FlowFileQueue::size)
            .mapToInt(QueueSize::getObjectCount)
            .sum();
    }

    @Override
    public long getBytesQueued() {
        return rootGroup.findAllConnections().stream()
            .map(Connection::getFlowFileQueue)
            .map(FlowFileQueue::size)
            .mapToLong(QueueSize::getByteCount)
            .sum();
    }
}
