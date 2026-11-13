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

import org.apache.nifi.annotation.lifecycle.OnDrainCompleted;
import org.apache.nifi.annotation.lifecycle.OnDrainInitiated;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.util.Connectables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Creating the drain asks the sources of the Process Group to stop. {@link #start()} then enables the referenced Controller Services, starts the other
 * components, and calls the {@link OnDrainInitiated} methods in the background. Processors inside Stateless groups receive no drain lifecycle calls.
 */
public class StandardProcessGroupDrain implements ProcessGroupDrain {
    private static final Logger logger = LoggerFactory.getLogger(StandardProcessGroupDrain.class);

    private final String identifier;
    private final ProcessGroup processGroup;
    private final ControllerServiceProvider controllerServiceProvider;
    private final Function<ProcessorNode, ProcessContext> processContextFactory;
    private final Executor backgroundExecutor;
    private final CompletableFuture<Void> sourcesStopped;
    private final CompletableFuture<Void> preparation = new CompletableFuture<>();

    private volatile QueueSize originalQueueSize;
    private volatile ProcessGroupDrainState state = ProcessGroupDrainState.DRAINING;
    private volatile String failureReason;

    // Guarded by synchronizing on this object
    private boolean initiated;
    private boolean ended;

    public StandardProcessGroupDrain(final String identifier, final ProcessGroup processGroup, final ControllerServiceProvider controllerServiceProvider,
                                     final Function<ProcessorNode, ProcessContext> processContextFactory, final Executor backgroundExecutor) {
        this.identifier = identifier;
        this.processGroup = processGroup;
        this.controllerServiceProvider = controllerServiceProvider;
        this.processContextFactory = processContextFactory;
        this.backgroundExecutor = backgroundExecutor;

        final List<CompletableFuture<Void>> stopFutures = stopSources();
        this.sourcesStopped = CompletableFuture.allOf(stopFutures.toArray(new CompletableFuture<?>[0]))
            .thenRun(() -> originalQueueSize = processGroup.getQueueSize());
    }

    /**
     * The components are started on a separate background task so that they are never started on a thread that enables a Controller Service.
     */
    public void start() {
        final CompletableFuture<List<CompletableFuture<Void>>> serviceEnableFutures = CompletableFuture.supplyAsync(() -> {
            stopRemoteOutputPorts();
            return enableReferencedServices();
        }, backgroundExecutor);

        serviceEnableFutures.thenCompose(enableFutures -> CompletableFuture.allOf(enableFutures.toArray(new CompletableFuture<?>[0])))
            .thenRunAsync(this::startComponentsAndInitiate, backgroundExecutor)
            .thenCompose(ignored -> sourcesStopped)
            .whenComplete((result, failure) -> {
                if (failure != null) {
                    final Throwable cause = (failure instanceof CompletionException && failure.getCause() != null) ? failure.getCause() : failure;
                    markFailed("Failed to prepare the components of the Process Group: " + cause.getMessage(), cause);
                }

                preparation.complete(null);
            });
    }

    /**
     * A drain that failed remains failed so that its failure can still be reported.
     */
    public synchronized void end() {
        if (ended) {
            return;
        }

        ended = true;
        if (state == ProcessGroupDrainState.DRAINING) {
            state = ProcessGroupDrainState.ENDED;
        }

        if (initiated) {
            invokeLifecycleMethods(OnDrainCompleted.class);
            logger.info("Drain {} of {} ended; {} FlowFiles remain queued", identifier, processGroup, processGroup.getQueueSize().getObjectCount());
        } else {
            logger.info("Drain {} of {} ended before it was initiated", identifier, processGroup);
        }
    }

    private synchronized void startComponentsAndInitiate() {
        if (state != ProcessGroupDrainState.DRAINING) {
            return;
        }

        startOtherComponents(processGroup);
        invokeLifecycleMethods(OnDrainInitiated.class);
        initiated = true;
        logger.info("Drain {} of {} initiated", identifier, processGroup);
    }

    private synchronized void markFailed(final String reason, final Throwable cause) {
        if (state != ProcessGroupDrainState.DRAINING) {
            return;
        }

        logger.error("Drain {} of {} failed: {}", identifier, processGroup, reason, cause);
        failureReason = reason;
        state = ProcessGroupDrainState.FAILED;
    }

    /**
     * Remote Process Group Output Ports are not stopped here because stopping one blocks until it has stopped.
     */
    private List<CompletableFuture<Void>> stopSources() {
        final List<CompletableFuture<Void>> stopFutures = new ArrayList<>();
        for (final ProcessGroup group : findStandardGroups()) {
            for (final ProcessorNode processor : group.getProcessors()) {
                if (ProcessGroup.STOP_PROCESSORS_FILTER.test(processor) && isSource(processor)) {
                    stopFutures.add(group.stopProcessor(processor));
                }
            }

            for (final ProcessGroup childGroup : group.getProcessGroups()) {
                if (childGroup.resolveExecutionEngine() == ExecutionEngine.STATELESS && containsSourceProcessor(childGroup)
                        && childGroup.getStatelessScheduledState() == StatelessGroupScheduledState.RUNNING) {
                    stopFutures.add(childGroup.stopProcessing());
                }
            }
        }

        for (final Port inputPort : processGroup.getInputPorts()) {
            if (ProcessGroup.STOP_PORTS_FILTER.test(inputPort)) {
                processGroup.stopInputPort(inputPort);
            }
        }

        return stopFutures;
    }

    private void stopRemoteOutputPorts() {
        for (final ProcessGroup group : findStandardGroups()) {
            for (final RemoteProcessGroup remoteProcessGroup : group.getRemoteProcessGroups()) {
                for (final RemoteGroupPort remoteOutputPort : remoteProcessGroup.getOutputPorts()) {
                    if (remoteOutputPort.isRunning()) {
                        remoteProcessGroup.stopTransmitting(remoteOutputPort);
                    }
                }
            }
        }
    }

    /**
     * An invalid service does not finish enabling until it becomes valid, so only the futures of valid services are returned.
     */
    private List<CompletableFuture<Void>> enableReferencedServices() {
        final List<CompletableFuture<Void>> enableFutures = new ArrayList<>();
        for (final ControllerServiceNode service : processGroup.findControllerServicesForDrain()) {
            if (service.isActive()) {
                continue;
            }

            if (service.getValidationStatus() != ValidationStatus.VALID) {
                service.performValidation();
            }

            final CompletableFuture<Void> enableFuture = controllerServiceProvider.enableControllerServiceAndDependencies(service);
            if (service.getValidationStatus() == ValidationStatus.VALID) {
                enableFutures.add(enableFuture);
            }
        }

        return enableFutures;
    }

    private void startOtherComponents(final ProcessGroup group) {
        for (final ProcessorNode processor : group.getProcessors()) {
            if (processor.getScheduledState() == ScheduledState.DISABLED || processor.isRunning() || isSource(processor)) {
                continue;
            }

            if (processor.performValidation() == ValidationStatus.INVALID) {
                logger.debug("Not starting {} for drain {} because it is invalid", processor, identifier);
                continue;
            }

            group.startProcessor(processor, false);
        }

        for (final Port outputPort : group.getOutputPorts()) {
            if (ProcessGroup.START_PORTS_FILTER.test(outputPort)) {
                group.startOutputPort(outputPort);
            }
        }

        if (group != processGroup) {
            for (final Port inputPort : group.getInputPorts()) {
                if (ProcessGroup.START_PORTS_FILTER.test(inputPort)) {
                    group.startInputPort(inputPort);
                }
            }
        }

        for (final RemoteProcessGroup remoteProcessGroup : group.getRemoteProcessGroups()) {
            for (final RemoteGroupPort remoteInputPort : remoteProcessGroup.getInputPorts()) {
                if (remoteInputPort.isRunning() || !remoteInputPort.hasIncomingConnection()) {
                    continue;
                }

                if (remoteInputPort.getScheduledState() != ScheduledState.STOPPED) {
                    logger.debug("Not starting {} for drain {} because it is {}", remoteInputPort, identifier, remoteInputPort.getScheduledState());
                    continue;
                }

                if (!remoteInputPort.isValid()) {
                    logger.debug("Not starting {} for drain {} because it is invalid", remoteInputPort, identifier);
                    continue;
                }

                remoteProcessGroup.startTransmitting(remoteInputPort);
            }
        }

        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            if (childGroup.resolveExecutionEngine() == ExecutionEngine.STATELESS) {
                if (keepsRunning(childGroup) && childGroup.getStatelessScheduledState() != StatelessGroupScheduledState.RUNNING) {
                    childGroup.startProcessing();
                }
            } else {
                startOtherComponents(childGroup);
            }
        }
    }

    private void invokeLifecycleMethods(final Class<? extends Annotation> lifecycleAnnotation) {
        for (final ControllerServiceNode service : processGroup.findControllerServicesForDrain()) {
            try {
                service.invokeDrainLifecycleMethods(lifecycleAnnotation);
            } catch (final Exception e) {
                logger.error("Failed to call @{} methods of {} for drain {}", lifecycleAnnotation.getSimpleName(), service, identifier, e);
            }
        }

        for (final ProcessGroup group : findStandardGroups()) {
            for (final ProcessorNode processor : group.getProcessors()) {
                try {
                    processor.invokeDrainLifecycleMethods(lifecycleAnnotation, processContextFactory.apply(processor));
                } catch (final Exception e) {
                    logger.error("Failed to call @{} methods of {} for drain {}", lifecycleAnnotation.getSimpleName(), processor, identifier, e);
                }
            }
        }
    }

    private List<ProcessGroup> findStandardGroups() {
        final List<ProcessGroup> groups = new ArrayList<>();
        addStandardGroups(processGroup, groups);
        return groups;
    }

    private void addStandardGroups(final ProcessGroup group, final List<ProcessGroup> groups) {
        groups.add(group);
        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            if (childGroup.resolveExecutionEngine() != ExecutionEngine.STATELESS) {
                addStandardGroups(childGroup, groups);
            }
        }
    }

    @Override
    public List<DrainBlockingComponent> getBlockingComponents() {
        final List<DrainBlockingComponent> blockingComponents = new ArrayList<>();
        for (final ProcessGroup group : findStandardGroups()) {
            for (final ProcessorNode processor : group.getProcessors()) {
                addBlockingComponent(processor, processor.getValidationStatus() != ValidationStatus.INVALID, null, blockingComponents);
            }

            for (final Port inputPort : group.getInputPorts()) {
                addBlockingComponent(inputPort, inputPort.isValid(), null, blockingComponents);
            }

            for (final Port outputPort : group.getOutputPorts()) {
                addBlockingComponent(outputPort, outputPort.isValid(), null, blockingComponents);
            }

            for (final RemoteProcessGroup remoteProcessGroup : group.getRemoteProcessGroups()) {
                for (final RemoteGroupPort remoteInputPort : remoteProcessGroup.getInputPorts()) {
                    addBlockingComponent(remoteInputPort, remoteInputPort.isValid(), remoteProcessGroup, blockingComponents);
                }
            }
        }

        return blockingComponents;
    }

    /**
     * A Remote Process Group port is invalid when its Remote Process Group is invalid, so the errors of the Remote Process Group are included.
     */
    private void addBlockingComponent(final Connectable connectable, final boolean valid, final RemoteProcessGroup remoteProcessGroup,
                                      final List<DrainBlockingComponent> blockingComponents) {
        final ScheduledState scheduledState = connectable.getScheduledState();
        final DrainBlockingComponent.Reason reason;
        if (scheduledState == ScheduledState.DISABLED) {
            reason = DrainBlockingComponent.Reason.DISABLED;
        } else if (scheduledState == ScheduledState.STOPPED && !valid) {
            reason = DrainBlockingComponent.Reason.INVALID;
        } else {
            return;
        }

        int queuedCount = 0;
        for (final Connection connection : connectable.getIncomingConnections()) {
            queuedCount += connection.getFlowFileQueue().size().getObjectCount();
        }

        if (queuedCount == 0) {
            return;
        }

        final List<String> validationErrors = new ArrayList<>();
        for (final ValidationResult validationResult : connectable.getValidationErrors()) {
            validationErrors.add(validationResult.toString());
        }

        String remoteProcessGroupId = null;
        if (remoteProcessGroup != null) {
            remoteProcessGroupId = remoteProcessGroup.getIdentifier();
            for (final ValidationResult validationResult : remoteProcessGroup.validate()) {
                if (!validationResult.isValid()) {
                    validationErrors.add(validationResult.toString());
                }
            }
        }

        blockingComponents.add(new DrainBlockingComponent(connectable.getIdentifier(), connectable.getName(), connectable.getConnectableType(),
            connectable.getProcessGroup().getIdentifier(), remoteProcessGroupId, reason, validationErrors, queuedCount));
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public ProcessGroup getProcessGroup() {
        return processGroup;
    }

    @Override
    public ProcessGroupDrainState getState() {
        return state;
    }

    @Override
    public Optional<String> getFailureReason() {
        return Optional.ofNullable(failureReason);
    }

    @Override
    public boolean isPrepared() {
        return preparation.isDone();
    }

    @Override
    public CompletableFuture<Void> getPreparationFuture() {
        return preparation;
    }

    @Override
    public QueueSize getOriginalQueueSize() {
        final QueueSize recordedQueueSize = originalQueueSize;
        return recordedQueueSize == null ? processGroup.getQueueSize() : recordedQueueSize;
    }

    @Override
    public QueueSize getCurrentQueueSize() {
        return processGroup.getQueueSize();
    }

    @Override
    public boolean keepsStopped(final Connectable connectable) {
        if (connectable.getConnectableType() == ConnectableType.STATELESS_GROUP) {
            return keepsStopped(connectable.getProcessGroup());
        }

        return isControlledByDrain(connectable) && isSource(connectable);
    }

    @Override
    public boolean keepsRunning(final Connectable connectable) {
        if (connectable.getConnectableType() == ConnectableType.STATELESS_GROUP) {
            return keepsRunning(connectable.getProcessGroup());
        }

        return isControlledByDrain(connectable) && !isSource(connectable);
    }

    @Override
    public boolean keepsStopped(final ProcessGroup statelessGroup) {
        return state == ProcessGroupDrainState.DRAINING && isWithinDrainedGroup(statelessGroup) && containsSourceProcessor(statelessGroup);
    }

    @Override
    public boolean keepsRunning(final ProcessGroup statelessGroup) {
        return state == ProcessGroupDrainState.DRAINING && isWithinDrainedGroup(statelessGroup) && !containsSourceProcessor(statelessGroup) && canReceiveData(statelessGroup);
    }

    @Override
    public void verifyCanStart(final Connectable connectable) {
        if (keepsStopped(connectable)) {
            verifyNothingQueued(connectable + " cannot be started because it brings data into " + processGroup + ", which is being drained");
        }
    }

    @Override
    public void verifyCanStop(final Connectable connectable) {
        if (keepsRunning(connectable)) {
            verifyNothingQueued(connectable + " cannot be stopped because " + processGroup + " is being drained");
        }
    }

    @Override
    public void verifyCanStart(final ProcessGroup statelessGroup) {
        if (keepsStopped(statelessGroup)) {
            verifyNothingQueued(statelessGroup + " cannot be started because it brings data into " + processGroup + ", which is being drained");
        }
    }

    @Override
    public void verifyCanStop(final ProcessGroup statelessGroup) {
        if (keepsRunning(statelessGroup)) {
            verifyNothingQueued(statelessGroup + " cannot be stopped because " + processGroup + " is being drained");
        }
    }

    private void verifyNothingQueued(final String reason) {
        final int queuedCount = processGroup.getQueueSize().getObjectCount();
        if (queuedCount > 0) {
            throw new IllegalStateException(reason + " and still has " + queuedCount + " FlowFiles queued");
        }
    }

    private boolean isControlledByDrain(final Connectable connectable) {
        if (state != ProcessGroupDrainState.DRAINING) {
            return false;
        }

        final ProcessGroup group = connectable.getProcessGroup();
        return isWithinDrainedGroup(group) && group.resolveExecutionEngine() != ExecutionEngine.STATELESS;
    }

    private boolean isWithinDrainedGroup(final ProcessGroup group) {
        ProcessGroup current = group;
        while (current != null) {
            if (current.getIdentifier().equals(processGroup.getIdentifier())) {
                return true;
            }

            current = current.getParent();
        }

        return false;
    }

    private boolean isSource(final Connectable connectable) {
        return switch (connectable.getConnectableType()) {
            case PROCESSOR -> !Connectables.hasNonLoopConnection(connectable);
            case INPUT_PORT -> processGroup.getIdentifier().equals(connectable.getProcessGroup().getIdentifier());
            case REMOTE_OUTPUT_PORT -> true;
            default -> false;
        };
    }

    private boolean containsSourceProcessor(final ProcessGroup statelessGroup) {
        for (final ProcessorNode processor : statelessGroup.findAllProcessors()) {
            if (!Connectables.hasNonLoopConnection(processor)) {
                return true;
            }
        }

        return false;
    }

    /**
     * A running Stateless group that cannot receive data keeps a thread busy because it never waits for data to arrive.
     */
    private boolean canReceiveData(final ProcessGroup statelessGroup) {
        for (final Port inputPort : statelessGroup.getInputPorts()) {
            if (inputPort.hasIncomingConnection()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return "StandardProcessGroupDrain[identifier=" + identifier + ", processGroup=" + processGroup.getIdentifier() + ", state=" + state + "]";
    }
}
