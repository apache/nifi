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
package org.apache.nifi.web;

import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.util.CancellableTimedPause;
import org.apache.nifi.web.util.ComponentLifecycle;
import org.apache.nifi.web.util.InvalidComponentAction;
import org.apache.nifi.web.util.LifecycleManagementException;
import org.apache.nifi.web.util.Pause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public final class RemovedConnectionDrainCoordinator {
    private static final Logger logger = LoggerFactory.getLogger(RemovedConnectionDrainCoordinator.class);
    static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(250);

    private final RemovedConnectionDrainClassifier classifier;
    private final PauseFactory pauseFactory;
    private final Duration drainTimeout;

    public RemovedConnectionDrainCoordinator() {
        this(new RemovedConnectionDrainClassifier(), new MonotonicPauseFactory(DEFAULT_POLL_INTERVAL, System::nanoTime), DEFAULT_DRAIN_TIMEOUT);
    }

    RemovedConnectionDrainCoordinator(final RemovedConnectionDrainClassifier classifier, final PauseFactory pauseFactory, final Duration drainTimeout) {
        this.classifier = Objects.requireNonNull(classifier, "Removed Connection Drain Classifier required");
        this.pauseFactory = Objects.requireNonNull(pauseFactory, "Pause Factory required");
        this.drainTimeout = Objects.requireNonNull(drainTimeout, "Drain Timeout required");
    }

    public DrainResult coordinateDrain(final FlowUpdateImpact flowUpdateImpact, final RemovedConnectionDrainClassifier.Context context,
                                       final ComponentLifecycle componentLifecycle, final URI requestUri, final String groupId,
                                       final CancellationHandle cancellationHandle) throws LifecycleManagementException {
        Objects.requireNonNull(flowUpdateImpact, "Flow Update Impact required");
        Objects.requireNonNull(context, "Removed Connection Drain Context required");
        Objects.requireNonNull(componentLifecycle, "Component Lifecycle required");
        Objects.requireNonNull(requestUri, "Request URI required");
        Objects.requireNonNull(groupId, "Group ID required");
        Objects.requireNonNull(cancellationHandle, "Cancellation Handle required");

        final RemovedConnectionDrainClassifier.Context queueAwareContext = createQueueAwareContext(flowUpdateImpact, context, componentLifecycle, requestUri);
        final RemovedConnectionDrainClassifier.BatchResult batchResult = classifier.classify(flowUpdateImpact, queueAwareContext);
        if (!batchResult.isSupported()) {
            throw new LifecycleManagementException(buildClassificationFailureMessage(batchResult));
        }

        final Set<String> candidateConnectionIds = batchResult.connectionResults().stream()
                .filter(result -> result.classification() == RemovedConnectionDrainClassifier.Classification.CANDIDATE)
                .map(result -> result.connection().getConnectionInstanceId())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        if (candidateConnectionIds.isEmpty()) {
            return DrainResult.success(Collections.emptySet(), Collections.emptySet());
        }

        final Map<String, AffectedComponentEntity> affectedComponentsById = flowUpdateImpact.getAffectedComponents().stream()
                .collect(Collectors.toMap(AffectedComponentEntity::getId, entity -> entity, (left, right) -> left, LinkedHashMap::new));
        final Set<AffectedComponentEntity> componentsToStop = new LinkedHashSet<>();
        for (final String producerBarrierComponentId : batchResult.producerBarrierComponentIds()) {
            final AffectedComponentEntity entity = getProducerBarrierEntity(affectedComponentsById, queueAwareContext, producerBarrierComponentId);
            if (entity == null || entity.getComponent() == null) {
                continue;
            }

            if (isActive(entity.getComponent())) {
                componentsToStop.add(entity);
            }
        }

        final List<String> orderedCandidateConnectionIds = candidateConnectionIds.stream().sorted().toList();
        final List<String> orderedProducerBarrierIds = componentsToStop.stream().map(AffectedComponentEntity::getId).sorted().toList();
        logger.info("Starting drain of removed connections {} with producer barriers {}", orderedCandidateConnectionIds, orderedProducerBarrierIds);

        final DeadlinePause drainPause = pauseFactory.createDrainPause(drainTimeout);
        cancellationHandle.setCancelCallback(drainPause::cancel);

        final Set<AffectedComponentEntity> drainStoppedComponents = new LinkedHashSet<>();
        try {
            if (!componentsToStop.isEmpty()) {
                final Set<AffectedComponentEntity> updatedStoppedComponents = componentLifecycle.scheduleComponents(
                        requestUri, groupId, componentsToStop, ScheduledState.STOPPED, drainPause, InvalidComponentAction.SKIP);
                drainStoppedComponents.addAll(getStoppedComponents(componentsToStop, updatedStoppedComponents));

                if (!allComponentsStopped(componentsToStop, updatedStoppedComponents)) {
                    if (cancellationHandle.isCancelled()) {
                        return restoreAfterCancellation(componentLifecycle, requestUri, groupId, candidateConnectionIds, drainStoppedComponents);
                    }

                    final Set<String> producerBarrierIds = componentsToStop.stream()
                            .map(AffectedComponentEntity::getId)
                            .collect(Collectors.toCollection(LinkedHashSet::new));
                    throw new LifecycleManagementException(buildStopTimeoutMessage(producerBarrierIds));
                }
            }

            if (cancellationHandle.isCancelled()) {
                return restoreAfterCancellation(componentLifecycle, requestUri, groupId, candidateConnectionIds, drainStoppedComponents);
            }

            final boolean queuesDrained = componentLifecycle.waitForConnectionQueuesEmpty(requestUri, candidateConnectionIds, drainPause);
            if (queuesDrained) {
                if (cancellationHandle.isCancelled()) {
                    return restoreAfterCancellation(componentLifecycle, requestUri, groupId, candidateConnectionIds, drainStoppedComponents);
                }

                logger.info("Completed draining removed connections {}", orderedCandidateConnectionIds);
                return DrainResult.success(candidateConnectionIds, drainStoppedComponents);
            }

            if (cancellationHandle.isCancelled()) {
                return restoreAfterCancellation(componentLifecycle, requestUri, groupId, candidateConnectionIds, drainStoppedComponents);
            }

            throw new LifecycleManagementException(buildQueueTimeoutMessage(candidateConnectionIds));
        } catch (final LifecycleManagementException e) {
            final Set<AffectedComponentEntity> stoppedComponents = getStoppedComponentsToRestore(queueAwareContext, componentsToStop, drainStoppedComponents);
            if (cancellationHandle.isCancelled()) {
                return restoreAfterCancellation(componentLifecycle, requestUri, groupId, candidateConnectionIds, stoppedComponents);
            }

            final LifecycleManagementException failure = decorateFailure(e, componentsToStop, candidateConnectionIds);
            logger.warn("Removed connection drain failed for connections {}", orderedCandidateConnectionIds, failure);
            restoreOrSuppress(componentLifecycle, requestUri, groupId, stoppedComponents, failure);
            throw failure;
        } catch (final RuntimeException e) {
            final Set<AffectedComponentEntity> stoppedComponents = getStoppedComponentsToRestore(queueAwareContext, componentsToStop, drainStoppedComponents);
            if (cancellationHandle.isCancelled()) {
                return restoreAfterCancellation(componentLifecycle, requestUri, groupId, candidateConnectionIds, stoppedComponents);
            }

            final LifecycleManagementException failure = new LifecycleManagementException(
                    "Removed connection drain failed for connections " + candidateConnectionIds.stream().sorted().toList(), e);
            logger.warn("Removed connection drain failed for connections {}", orderedCandidateConnectionIds, failure);
            restoreOrSuppress(componentLifecycle, requestUri, groupId, stoppedComponents, failure);
            throw failure;
        } finally {
            cancellationHandle.setCancelCallback(null);
        }
    }

    private DrainResult restoreAfterCancellation(final ComponentLifecycle componentLifecycle, final URI requestUri, final String groupId,
                                                 final Set<String> candidateConnectionIds,
                                                 final Set<AffectedComponentEntity> drainStoppedComponents) {
        LifecycleManagementException restorationFailure = null;
        try {
            restoreStoppedComponents(componentLifecycle, requestUri, groupId, drainStoppedComponents);
        } catch (final LifecycleManagementException e) {
            restorationFailure = e;
            logger.warn("Failed to restore producer barriers {} after removed connection drain cancellation",
                    drainStoppedComponents.stream().map(AffectedComponentEntity::getId).sorted().toList(), e);
        }

        return DrainResult.cancelled(candidateConnectionIds, drainStoppedComponents, restorationFailure);
    }

    private String buildClassificationFailureMessage(final RemovedConnectionDrainClassifier.BatchResult batchResult) {
        final List<String> unsupportedConnections = batchResult.connectionResults().stream()
                .filter(result -> result.classification() == RemovedConnectionDrainClassifier.Classification.UNSUPPORTED)
                .map(result -> result.connection().getConnectionInstanceId() + "[reason=" + result.unsupportedReason().name() + "]")
                .toList();
        return "Removed connection drain preflight failed: " + unsupportedConnections;
    }

    private String buildStopTimeoutMessage(final Set<String> producerBarrierIds) {
        return "Removed connection drain timed out [timedOut=true, cancelled=false, phase=stopping-producer-barriers, timeout="
                + drainTimeout.toSeconds() + "s, componentIds=" + producerBarrierIds.stream().sorted().toList() + "]";
    }

    private String buildQueueTimeoutMessage(final Set<String> connectionIds) {
        return "Removed connection drain timed out [timedOut=true, cancelled=false, phase=waiting-for-queues, timeout="
                + drainTimeout.toSeconds() + "s, connectionIds=" + connectionIds.stream().sorted().toList() + "]";
    }

    private AffectedComponentEntity getProducerBarrierEntity(final Map<String, AffectedComponentEntity> affectedComponentsById,
                                                             final RemovedConnectionDrainClassifier.Context context,
                                                             final String producerBarrierComponentId) {
        final AffectedComponentEntity affectedComponentEntity = affectedComponentsById.get(producerBarrierComponentId);
        if (affectedComponentEntity != null && affectedComponentEntity.getComponent() != null) {
            return affectedComponentEntity;
        }

        final RemovedConnectionDrainClassifier.LiveConnectable liveConnectable = context.getConnectable(producerBarrierComponentId);
        if (liveConnectable == null) {
            return null;
        }

        final String referenceType = getReferenceType(liveConnectable.type());
        if (referenceType == null) {
            return null;
        }

        final AffectedComponentDTO componentDto = new AffectedComponentDTO();
        componentDto.setId(liveConnectable.id());
        componentDto.setName(liveConnectable.id());
        componentDto.setProcessGroupId(liveConnectable.processGroupId());
        componentDto.setReferenceType(referenceType);
        componentDto.setState(getState(liveConnectable));

        final AffectedComponentEntity componentEntity = new AffectedComponentEntity();
        componentEntity.setId(liveConnectable.id());
        componentEntity.setReferenceType(referenceType);
        componentEntity.setComponent(componentDto);
        return componentEntity;
    }

    private String getReferenceType(final ConnectableType connectableType) {
        if (connectableType == ConnectableType.PROCESSOR) {
            return AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR;
        }
        if (connectableType == ConnectableType.INPUT_PORT) {
            return AffectedComponentDTO.COMPONENT_TYPE_INPUT_PORT;
        }
        if (connectableType == ConnectableType.OUTPUT_PORT) {
            return AffectedComponentDTO.COMPONENT_TYPE_OUTPUT_PORT;
        }

        return null;
    }

    private String getState(final RemovedConnectionDrainClassifier.LiveConnectable liveConnectable) {
        if (liveConnectable.type() == ConnectableType.PROCESSOR) {
            return liveConnectable.physicalScheduledState() == null ? null : liveConnectable.physicalScheduledState().name();
        }

        return liveConnectable.running() ? "RUNNING" : "STOPPED";
    }

    private LifecycleManagementException decorateFailure(final LifecycleManagementException failure,
                                                         final Set<AffectedComponentEntity> componentsToStop,
                                                         final Set<String> candidateConnectionIds) {
        final String message = failure.getMessage();
        if (message != null && message.startsWith("Removed connection drain")) {
            return failure;
        }

        final String decoratedMessage;
        if (message != null && message.contains("waiting for connection queues")) {
            decoratedMessage = "Removed connection drain failed while waiting for connections "
                    + candidateConnectionIds.stream().sorted().toList() + ": " + message;
        } else {
            final List<String> componentIds = componentsToStop.stream().map(AffectedComponentEntity::getId).sorted().toList();
            decoratedMessage = "Removed connection drain failed while stopping producer barriers " + componentIds + ": " + message;
        }

        return new LifecycleManagementException(decoratedMessage, failure);
    }

    private void restoreOrSuppress(final ComponentLifecycle componentLifecycle, final URI requestUri, final String groupId,
                                   final Set<AffectedComponentEntity> stoppedComponents, final LifecycleManagementException failure) {
        try {
            restoreStoppedComponents(componentLifecycle, requestUri, groupId, stoppedComponents);
        } catch (final LifecycleManagementException restorationFailure) {
            logger.warn("Failed to restore producer barriers {} after removed connection drain failure",
                    stoppedComponents.stream().map(AffectedComponentEntity::getId).sorted().toList(), restorationFailure);
            failure.addSuppressed(restorationFailure);
        }
    }

    private void restoreStoppedComponents(final ComponentLifecycle componentLifecycle, final URI requestUri, final String groupId,
                                          final Set<AffectedComponentEntity> stoppedComponents) throws LifecycleManagementException {
        if (stoppedComponents.isEmpty()) {
            return;
        }

        componentLifecycle.scheduleComponents(requestUri, groupId, stoppedComponents, ScheduledState.RUNNING,
                pauseFactory.createRestorationPause(), InvalidComponentAction.SKIP);
    }

    private RemovedConnectionDrainClassifier.Context createQueueAwareContext(final FlowUpdateImpact flowUpdateImpact,
                                                                             final RemovedConnectionDrainClassifier.Context context,
                                                                             final ComponentLifecycle componentLifecycle,
                                                                             final URI requestUri) throws LifecycleManagementException {
        final Map<String, Boolean> knownQueueEmptyByConnectionId = new LinkedHashMap<>();
        final Pause noWaitPause = NoWaitPause.INSTANCE;
        for (final RemovedConnectionDescriptor removedConnection : flowUpdateImpact.getRemovedConnections()) {
            final String connectionId = removedConnection.getConnectionInstanceId();
            if (connectionId == null) {
                continue;
            }

            final boolean knownQueueEmpty = componentLifecycle.waitForConnectionQueuesEmpty(requestUri, Set.of(connectionId), noWaitPause);
            knownQueueEmptyByConnectionId.put(connectionId, knownQueueEmpty);
        }

        return new QueueAwareContext(context, knownQueueEmptyByConnectionId);
    }

    private boolean allComponentsStopped(final Set<AffectedComponentEntity> componentsToStop, final Set<AffectedComponentEntity> updatedStoppedComponents) {
        if (componentsToStop.isEmpty()) {
            return true;
        }

        final Map<String, AffectedComponentEntity> updatedComponentsById = toOrderedMap(updatedStoppedComponents);
        for (final AffectedComponentEntity componentToStop : componentsToStop) {
            final AffectedComponentEntity updatedComponent = updatedComponentsById.getOrDefault(componentToStop.getId(), componentToStop);
            if (updatedComponent.getComponent() == null || isActive(updatedComponent.getComponent())) {
                return false;
            }
        }

        return true;
    }

    private Set<AffectedComponentEntity> getStoppedComponents(final Collection<AffectedComponentEntity> componentsToStop,
                                                              final Collection<AffectedComponentEntity> updatedComponents) {
        if (componentsToStop == null || componentsToStop.isEmpty()) {
            return Collections.emptySet();
        }

        final Map<String, AffectedComponentEntity> updatedComponentsById = toOrderedMap(updatedComponents);
        final Set<AffectedComponentEntity> stoppedComponents = new LinkedHashSet<>();
        for (final AffectedComponentEntity componentToStop : componentsToStop) {
            final AffectedComponentEntity updatedComponent = updatedComponentsById.getOrDefault(componentToStop.getId(), componentToStop);
            if (updatedComponent.getComponent() != null && !isActive(updatedComponent.getComponent())) {
                stoppedComponents.add(updatedComponent);
            }
        }

        return stoppedComponents;
    }

    private Set<AffectedComponentEntity> getStoppedComponentsToRestore(final RemovedConnectionDrainClassifier.Context context,
                                                                       final Collection<AffectedComponentEntity> componentsToStop,
                                                                       final Collection<AffectedComponentEntity> updatedComponents) {
        if (componentsToStop == null || componentsToStop.isEmpty()) {
            return Collections.emptySet();
        }

        final Map<String, AffectedComponentEntity> updatedComponentsById = toOrderedMap(updatedComponents);
        final Set<AffectedComponentEntity> stoppedComponents = new LinkedHashSet<>();
        for (final AffectedComponentEntity componentToStop : componentsToStop) {
            final AffectedComponentEntity updatedComponent = updatedComponentsById.get(componentToStop.getId());
            if (updatedComponent != null) {
                if (updatedComponent.getComponent() != null && !isActive(updatedComponent.getComponent())) {
                    stoppedComponents.add(updatedComponent);
                }
                continue;
            }

            final RemovedConnectionDrainClassifier.LiveConnectable liveConnectable = context.getConnectable(componentToStop.getId());
            if (liveConnectable == null || isActive(liveConnectable)) {
                continue;
            }

            final AffectedComponentEntity liveComponent = getProducerBarrierEntity(Collections.emptyMap(), context, componentToStop.getId());
            if (liveComponent != null && liveComponent.getComponent() != null) {
                stoppedComponents.add(liveComponent);
            }
        }

        return stoppedComponents;
    }

    private Map<String, AffectedComponentEntity> toOrderedMap(final Collection<AffectedComponentEntity> components) {
        if (components == null || components.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, AffectedComponentEntity> orderedMap = new LinkedHashMap<>();
        for (final AffectedComponentEntity component : components) {
            orderedMap.put(component.getId(), component);
        }
        return orderedMap;
    }

    private boolean isActive(final AffectedComponentDTO affectedComponentDto) {
        final String state = affectedComponentDto.getState();
        if ("Running".equalsIgnoreCase(state) || "Starting".equalsIgnoreCase(state)) {
            return true;
        }

        final Integer activeThreadCount = affectedComponentDto.getActiveThreadCount();
        return activeThreadCount != null && activeThreadCount > 0;
    }

    private boolean isActive(final RemovedConnectionDrainClassifier.LiveConnectable liveConnectable) {
        if (liveConnectable == null) {
            return false;
        }

        if (liveConnectable.type() == ConnectableType.PROCESSOR) {
            return liveConnectable.physicalScheduledState() == ScheduledState.RUNNING
                    || liveConnectable.physicalScheduledState() == ScheduledState.STARTING;
        }

        return liveConnectable.running();
    }

    public interface CancellationHandle {
        boolean isCancelled();

        void setCancelCallback(Runnable runnable);
    }

    interface PauseFactory {
        DeadlinePause createDrainPause(Duration timeout);

        Pause createRestorationPause();
    }

    interface DeadlinePause extends Pause {
        void cancel();
    }

    public record DrainResult(Set<String> candidateConnectionIds, Set<AffectedComponentEntity> drainStoppedComponents, boolean cancelled,
                              LifecycleManagementException restorationFailure) {
        public DrainResult {
            candidateConnectionIds = copyOrderedSet(candidateConnectionIds);
            drainStoppedComponents = copyOrderedSet(drainStoppedComponents);
        }

        static DrainResult success(final Set<String> candidateConnectionIds, final Set<AffectedComponentEntity> drainStoppedComponents) {
            return new DrainResult(candidateConnectionIds, drainStoppedComponents, false, null);
        }

        static DrainResult cancelled(final Set<String> candidateConnectionIds, final Set<AffectedComponentEntity> drainStoppedComponents) {
            return cancelled(candidateConnectionIds, drainStoppedComponents, null);
        }

        static DrainResult cancelled(final Set<String> candidateConnectionIds, final Set<AffectedComponentEntity> drainStoppedComponents,
                                     final LifecycleManagementException restorationFailure) {
            return new DrainResult(candidateConnectionIds, drainStoppedComponents, true, restorationFailure);
        }
    }

    private static final class MonotonicPauseFactory implements PauseFactory {
        private final Duration pollInterval;
        private final LongSupplier nanoTimeSupplier;

        private MonotonicPauseFactory(final Duration pollInterval, final LongSupplier nanoTimeSupplier) {
            this.pollInterval = pollInterval;
            this.nanoTimeSupplier = nanoTimeSupplier;
        }

        @Override
        public DeadlinePause createDrainPause(final Duration timeout) {
            return new TimedDeadlinePause(pollInterval, timeout, nanoTimeSupplier);
        }

        @Override
        public Pause createRestorationPause() {
            return new CancellableTimedPause(pollInterval.toMillis(), Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
    }

    private static final class TimedDeadlinePause implements DeadlinePause {
        private final long pauseNanos;
        private final long deadlineNanos;
        private final LongSupplier nanoTimeSupplier;
        private volatile boolean cancelled;

        private TimedDeadlinePause(final Duration pollInterval, final Duration timeout, final LongSupplier nanoTimeSupplier) {
            this.pauseNanos = Math.max(1L, pollInterval.toNanos());
            this.nanoTimeSupplier = nanoTimeSupplier;
            this.deadlineNanos = nanoTimeSupplier.getAsLong() + timeout.toNanos();
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        @Override
        public boolean pause() {
            if (cancelled) {
                return false;
            }

            final long now = nanoTimeSupplier.getAsLong();
            if (now >= deadlineNanos) {
                return false;
            }

            try {
                TimeUnit.NANOSECONDS.sleep(Math.min(pauseNanos, Math.max(1L, deadlineNanos - now)));
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }

            return !cancelled && nanoTimeSupplier.getAsLong() < deadlineNanos;
        }
    }

    private enum NoWaitPause implements Pause {
        INSTANCE;

        @Override
        public boolean pause() {
            return false;
        }
    }

    private record QueueAwareContext(RemovedConnectionDrainClassifier.Context delegate,
                                     Map<String, Boolean> knownQueueEmptyByConnectionId) implements RemovedConnectionDrainClassifier.Context {
        private QueueAwareContext {
            delegate = Objects.requireNonNull(delegate, "Removed Connection Drain Context required");
            knownQueueEmptyByConnectionId = Collections.unmodifiableMap(new LinkedHashMap<>(knownQueueEmptyByConnectionId));
        }

        @Override
        public RemovedConnectionDrainClassifier.LiveConnection getConnection(final String connectionId) {
            final RemovedConnectionDrainClassifier.LiveConnection connection = delegate.getConnection(connectionId);
            if (connection == null) {
                return null;
            }

            final Boolean knownQueueEmpty = knownQueueEmptyByConnectionId.get(connectionId);
            if (knownQueueEmpty == null) {
                return connection;
            }

            return new RemovedConnectionDrainClassifier.LiveConnection(connection.id(), connection.sourceId(), connection.destinationId(), knownQueueEmpty);
        }

        @Override
        public RemovedConnectionDrainClassifier.LiveConnectable getConnectable(final String connectableId) {
            return delegate.getConnectable(connectableId);
        }

        @Override
        public RemovedConnectionDrainClassifier.LiveProcessGroup getProcessGroup(final String processGroupId) {
            return delegate.getProcessGroup(processGroupId);
        }
    }

    private static <T> Set<T> copyOrderedSet(final Collection<T> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }

        return Collections.unmodifiableSet(new LinkedHashSet<>(values));
    }
}
