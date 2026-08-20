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

import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.groups.ProcessGroup;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public final class RemovedConnectionDrainClassifier {
    private static final Set<ConnectableType> SUPPORTED_SOURCE_TYPES = Set.of(
            ConnectableType.PROCESSOR,
            ConnectableType.INPUT_PORT,
            ConnectableType.OUTPUT_PORT,
            ConnectableType.FUNNEL);
    private static final Set<ConnectableType> SUPPORTED_PRODUCER_BARRIER_TYPES = Set.of(
            ConnectableType.PROCESSOR,
            ConnectableType.INPUT_PORT,
            ConnectableType.OUTPUT_PORT);
    private static final Set<ConnectableType> SUPPORTED_DESTINATION_TYPES = Set.of(
            ConnectableType.PROCESSOR,
            ConnectableType.INPUT_PORT,
            ConnectableType.OUTPUT_PORT);
    private static final Comparator<RemovedConnectionDescriptor> CONNECTION_ORDER =
            Comparator.comparing(RemovedConnectionDescriptor::getConnectionInstanceId, Comparator.nullsLast(String::compareTo))
                    .thenComparing(RemovedConnectionDescriptor::getConnectionVersionedId, Comparator.nullsLast(String::compareTo));

    BatchResult classify(final FlowUpdateImpact flowUpdateImpact, final Context context) {
        Objects.requireNonNull(flowUpdateImpact, "Flow Update Impact required");
        Objects.requireNonNull(context, "Removed Connection Drain Context required");

        final List<RemovedConnectionDescriptor> orderedConnections = flowUpdateImpact.getRemovedConnections().stream()
                .sorted(CONNECTION_ORDER)
                .toList();

        final Set<String> removedConnectionIds = orderedConnections.stream()
                .map(RemovedConnectionDescriptor::getConnectionInstanceId)
                .filter(Objects::nonNull)
                .collect(toOrderedSet());

        final Set<String> nonEmptyRemovedDestinationIds = new LinkedHashSet<>();
        final Map<String, ConnectionResult> initialResults = new LinkedHashMap<>();

        for (final RemovedConnectionDescriptor descriptor : orderedConnections) {
            final LiveConnection liveConnection = context.getConnection(descriptor.getConnectionInstanceId());
            if (liveConnection == null) {
                initialResults.put(descriptor.getConnectionInstanceId(), ConnectionResult.unsupported(descriptor, UnsupportedReason.CONNECTION_NOT_FOUND));
                continue;
            }

            if (liveConnection.knownQueueEmpty()) {
                initialResults.put(descriptor.getConnectionInstanceId(), ConnectionResult.noDrain(descriptor));
                continue;
            }

            if (descriptor.getDestinationInstanceId() != null) {
                nonEmptyRemovedDestinationIds.add(descriptor.getDestinationInstanceId());
            }
        }

        final Map<String, ConnectionResult> classifiedResults = new LinkedHashMap<>(initialResults);
        for (final RemovedConnectionDescriptor descriptor : orderedConnections) {
            if (classifiedResults.containsKey(descriptor.getConnectionInstanceId())) {
                continue;
            }

            classifiedResults.put(descriptor.getConnectionInstanceId(), classifyNonEmpty(descriptor, flowUpdateImpact, context,
                    nonEmptyRemovedDestinationIds, removedConnectionIds));
        }

        final Set<String> candidateProducerBarrierIds = getCandidateProducerBarrierIds(orderedConnections, classifiedResults);

        for (final RemovedConnectionDescriptor descriptor : orderedConnections) {
            final ConnectionResult connectionResult = classifiedResults.get(descriptor.getConnectionInstanceId());
            if (connectionResult.classification() != Classification.CANDIDATE) {
                continue;
            }

            if (hasRetainedFeedbackPath(descriptor.getDestinationInstanceId(), candidateProducerBarrierIds, removedConnectionIds, context)) {
                classifiedResults.put(descriptor.getConnectionInstanceId(), ConnectionResult.unsupported(descriptor, UnsupportedReason.RETAINED_FEEDBACK_PATH));
            }
        }

        final List<ConnectionResult> connectionResults = new ArrayList<>(orderedConnections.size());
        for (final RemovedConnectionDescriptor descriptor : orderedConnections) {
            connectionResults.add(classifiedResults.get(descriptor.getConnectionInstanceId()));
        }

        return new BatchResult(connectionResults, getCandidateProducerBarrierIds(orderedConnections, classifiedResults));
    }

    private ConnectionResult classifyNonEmpty(final RemovedConnectionDescriptor descriptor, final FlowUpdateImpact flowUpdateImpact,
                                              final Context context, final Set<String> nonEmptyRemovedDestinationIds,
                                              final Set<String> removedConnectionIds) {
        if (descriptor.getRemovalReason() == RemovalReason.SOURCE_CHANGED) {
            return ConnectionResult.unsupported(descriptor, UnsupportedReason.SOURCE_CHANGED_REMOVAL);
        }

        if (!isRetainedGroupHierarchy(descriptor.getContainingProcessGroupId(), flowUpdateImpact.getRemovedProcessGroupIds(), context)) {
            return ConnectionResult.unsupported(descriptor, UnsupportedReason.CONNECTION_IN_REMOVED_GROUP);
        }

        if (isRemovedEndpoint(flowUpdateImpact, descriptor.getSourceInstanceId())) {
            return ConnectionResult.unsupported(descriptor, UnsupportedReason.SOURCE_COMPONENT_REMOVED);
        }

        if (isRemovedEndpoint(flowUpdateImpact, descriptor.getDestinationInstanceId())) {
            return ConnectionResult.unsupported(descriptor, UnsupportedReason.DESTINATION_COMPONENT_REMOVED);
        }

        if (!SUPPORTED_SOURCE_TYPES.contains(descriptor.getSourceType())) {
            return ConnectionResult.unsupported(descriptor, UnsupportedReason.UNSUPPORTED_SOURCE_TYPE);
        }

        if (!SUPPORTED_DESTINATION_TYPES.contains(descriptor.getDestinationType())) {
            return ConnectionResult.unsupported(descriptor, UnsupportedReason.UNSUPPORTED_DESTINATION_TYPE);
        }

        final LiveConnectable destination = context.getConnectable(descriptor.getDestinationInstanceId());
        if (destination == null) {
            return ConnectionResult.unsupported(descriptor, UnsupportedReason.DESTINATION_COMPONENT_NOT_FOUND);
        }

        final Optional<UnsupportedReason> destinationValidationFailure = validateDestination(destination);
        if (destinationValidationFailure.isPresent()) {
            return ConnectionResult.unsupported(descriptor, destinationValidationFailure.get());
        }

        final Set<String> producerBarrierIds = resolveProducerBarrierIds(descriptor, flowUpdateImpact, context, nonEmptyRemovedDestinationIds);
        if (producerBarrierIds.isEmpty()) {
            return ConnectionResult.unsupported(descriptor, UnsupportedReason.FUNNEL_SOURCE_WITHOUT_SUPPORTED_PRODUCER);
        }

        for (final String producerBarrierId : producerBarrierIds) {
            if (Objects.equals(producerBarrierId, descriptor.getDestinationInstanceId())) {
                return ConnectionResult.unsupported(descriptor, UnsupportedReason.SELF_LOOP);
            }

            if (nonEmptyRemovedDestinationIds.contains(producerBarrierId)) {
                return ConnectionResult.unsupported(descriptor, UnsupportedReason.PRODUCER_BARRIER_IS_REMOVED_DESTINATION);
            }

            final LiveConnectable producerBarrier = context.getConnectable(producerBarrierId);
            if (producerBarrier == null) {
                return ConnectionResult.unsupported(descriptor, UnsupportedReason.SOURCE_COMPONENT_NOT_FOUND);
            }

            if (!SUPPORTED_PRODUCER_BARRIER_TYPES.contains(producerBarrier.type())) {
                return ConnectionResult.unsupported(descriptor, UnsupportedReason.UNSUPPORTED_SOURCE_TYPE);
            }

            if (!isRetainedGroupHierarchy(producerBarrier.processGroupId(), flowUpdateImpact.getRemovedProcessGroupIds(), context)) {
                return ConnectionResult.unsupported(descriptor, UnsupportedReason.SOURCE_COMPONENT_REMOVED);
            }

            if (isRemovedEndpoint(flowUpdateImpact, producerBarrierId)) {
                return ConnectionResult.unsupported(descriptor, UnsupportedReason.SOURCE_COMPONENT_REMOVED);
            }
        }

        return ConnectionResult.candidate(descriptor, producerBarrierIds);
    }

    private Set<String> resolveProducerBarrierIds(final RemovedConnectionDescriptor descriptor, final FlowUpdateImpact flowUpdateImpact,
                                                  final Context context, final Set<String> nonEmptyRemovedDestinationIds) {
        if (descriptor.getSourceType() != ConnectableType.FUNNEL) {
            if (descriptor.getSourceInstanceId() == null) {
                return Collections.emptySet();
            }

            final LiveConnectable source = context.getConnectable(descriptor.getSourceInstanceId());
            if (source == null) {
                return Collections.emptySet();
            }

            if (!isRetainedGroupHierarchy(source.processGroupId(), flowUpdateImpact.getRemovedProcessGroupIds(), context)) {
                return Collections.emptySet();
            }

            return Set.of(source.id());
        }

        final LiveConnectable funnel = context.getConnectable(descriptor.getSourceInstanceId());
        if (funnel == null) {
            return Collections.emptySet();
        }

        final Set<String> producerBarrierIds = new LinkedHashSet<>();
        collectUpstreamProducerBarrierIds(funnel, context, producerBarrierIds, new HashSet<>());
        return producerBarrierIds;
    }

    private void collectUpstreamProducerBarrierIds(final LiveConnectable connectable, final Context context,
                                                   final Set<String> producerBarrierIds, final Set<String> visitedConnectionIds) {
        if (connectable == null) {
            return;
        }

        if (connectable.type() != ConnectableType.FUNNEL) {
            producerBarrierIds.add(connectable.id());
            return;
        }

        for (final String incomingConnectionId : connectable.incomingConnectionIds()) {
            if (!visitedConnectionIds.add(incomingConnectionId)) {
                continue;
            }

            final LiveConnection incomingConnection = context.getConnection(incomingConnectionId);
            if (incomingConnection == null) {
                continue;
            }

            collectUpstreamProducerBarrierIds(context.getConnectable(incomingConnection.sourceId()), context, producerBarrierIds, visitedConnectionIds);
        }
    }

    private Optional<UnsupportedReason> validateDestination(final LiveConnectable destination) {
        if (destination.type() == ConnectableType.PROCESSOR) {
            if (destination.physicalScheduledState() != ScheduledState.RUNNING) {
                return Optional.of(UnsupportedReason.DESTINATION_NOT_RUNNING);
            }

            if (destination.validationStatus() != ValidationStatus.VALID) {
                return Optional.of(UnsupportedReason.DESTINATION_NOT_VALID);
            }
        } else if ((destination.type() == ConnectableType.INPUT_PORT || destination.type() == ConnectableType.OUTPUT_PORT) && !destination.running()) {
            return Optional.of(UnsupportedReason.DESTINATION_NOT_RUNNING);
        }

        return Optional.empty();
    }

    private boolean hasRetainedFeedbackPath(final String destinationId, final Set<String> producerBarrierIds,
                                            final Set<String> removedConnectionIds, final Context context) {
        if (destinationId == null || producerBarrierIds.isEmpty()) {
            return false;
        }

        final Deque<String> pendingConnectables = new ArrayDeque<>();
        final Set<String> visitedConnectables = new HashSet<>();
        pendingConnectables.add(destinationId);

        while (!pendingConnectables.isEmpty()) {
            final String connectableId = pendingConnectables.removeFirst();
            if (!visitedConnectables.add(connectableId)) {
                continue;
            }

            final LiveConnectable connectable = context.getConnectable(connectableId);
            if (connectable == null) {
                continue;
            }

            for (final String outgoingConnectionId : connectable.outgoingConnectionIds()) {
                if (removedConnectionIds.contains(outgoingConnectionId)) {
                    continue;
                }

                final LiveConnection outgoingConnection = context.getConnection(outgoingConnectionId);
                if (outgoingConnection == null) {
                    continue;
                }

                final String downstreamConnectableId = outgoingConnection.destinationId();
                if (producerBarrierIds.contains(downstreamConnectableId)) {
                    return true;
                }

                pendingConnectables.addLast(downstreamConnectableId);
            }
        }

        return false;
    }

    private boolean isRemovedEndpoint(final FlowUpdateImpact flowUpdateImpact, final String componentInstanceId) {
        return componentInstanceId != null && flowUpdateImpact.getRemovedEndpointIds().contains(componentInstanceId);
    }

    private boolean isRetainedGroupHierarchy(final String processGroupId, final Set<String> removedProcessGroupIds, final Context context) {
        String currentGroupId = processGroupId;
        final Set<String> visitedGroupIds = new HashSet<>();
        while (currentGroupId != null) {
            if (!visitedGroupIds.add(currentGroupId)) {
                return false;
            }

            if (removedProcessGroupIds.contains(currentGroupId)) {
                return false;
            }

            final LiveProcessGroup processGroup = context.getProcessGroup(currentGroupId);
            if (processGroup == null) {
                return false;
            }

            currentGroupId = processGroup.parentProcessGroupId();
        }

        return true;
    }

    private Set<String> getCandidateProducerBarrierIds(final List<RemovedConnectionDescriptor> orderedConnections,
                                                       final Map<String, ConnectionResult> classifiedResults) {
        final Set<String> candidateProducerBarrierIds = new LinkedHashSet<>();
        for (final RemovedConnectionDescriptor descriptor : orderedConnections) {
            final ConnectionResult connectionResult = classifiedResults.get(descriptor.getConnectionInstanceId());
            if (connectionResult != null && connectionResult.classification() == Classification.CANDIDATE) {
                candidateProducerBarrierIds.addAll(connectionResult.producerBarrierComponentIds());
            }
        }

        return candidateProducerBarrierIds;
    }

    private static <T> Collector<T, ?, Set<T>> toOrderedSet() {
        return Collectors.toCollection(LinkedHashSet::new);
    }

    public interface Context {
        LiveConnection getConnection(String connectionId);

        LiveConnectable getConnectable(String connectableId);

        LiveProcessGroup getProcessGroup(String processGroupId);
    }

    enum Classification {
        NO_DRAIN,
        CANDIDATE,
        UNSUPPORTED
    }

    enum UnsupportedReason {
        CONNECTION_NOT_FOUND,
        SOURCE_CHANGED_REMOVAL,
        CONNECTION_IN_REMOVED_GROUP,
        SOURCE_COMPONENT_REMOVED,
        DESTINATION_COMPONENT_REMOVED,
        SOURCE_COMPONENT_NOT_FOUND,
        DESTINATION_COMPONENT_NOT_FOUND,
        SELF_LOOP,
        UNSUPPORTED_SOURCE_TYPE,
        UNSUPPORTED_DESTINATION_TYPE,
        DESTINATION_NOT_RUNNING,
        DESTINATION_NOT_VALID,
        PRODUCER_BARRIER_IS_REMOVED_DESTINATION,
        RETAINED_FEEDBACK_PATH,
        FUNNEL_SOURCE_WITHOUT_SUPPORTED_PRODUCER
    }

    record LiveConnection(String id, String sourceId, String destinationId, boolean knownQueueEmpty) {
    }

    record LiveConnectable(String id, ConnectableType type, String processGroupId,
                           ScheduledState physicalScheduledState, ValidationStatus validationStatus, boolean running,
                           Set<String> incomingConnectionIds, Set<String> outgoingConnectionIds) {
        LiveConnectable {
            incomingConnectionIds = copyOrderedSet(incomingConnectionIds);
            outgoingConnectionIds = copyOrderedSet(outgoingConnectionIds);
        }
    }

    record LiveProcessGroup(String id, String parentProcessGroupId) {
    }

    record ConnectionResult(RemovedConnectionDescriptor connection, Classification classification,
                            UnsupportedReason unsupportedReason, Set<String> producerBarrierComponentIds) {
        ConnectionResult {
            producerBarrierComponentIds = copyOrderedSet(producerBarrierComponentIds);
        }

        static ConnectionResult noDrain(final RemovedConnectionDescriptor connection) {
            return new ConnectionResult(connection, Classification.NO_DRAIN, null, Collections.emptySet());
        }

        static ConnectionResult candidate(final RemovedConnectionDescriptor connection, final Set<String> producerBarrierComponentIds) {
            return new ConnectionResult(connection, Classification.CANDIDATE, null, producerBarrierComponentIds);
        }

        static ConnectionResult unsupported(final RemovedConnectionDescriptor connection, final UnsupportedReason unsupportedReason) {
            return new ConnectionResult(connection, Classification.UNSUPPORTED, unsupportedReason, Collections.emptySet());
        }
    }

    record BatchResult(List<ConnectionResult> connectionResults, Set<String> producerBarrierComponentIds) {
        BatchResult {
            connectionResults = Collections.unmodifiableList(new ArrayList<>(connectionResults));
            producerBarrierComponentIds = copyOrderedSet(producerBarrierComponentIds);
        }

        boolean isSupported() {
            return connectionResults.stream().noneMatch(result -> result.classification() == Classification.UNSUPPORTED);
        }

        Optional<ConnectionResult> getFirstUnsupportedConnectionResult() {
            return connectionResults.stream().filter(result -> result.classification() == Classification.UNSUPPORTED).findFirst();
        }
    }

    static final class FlowManagerContext implements Context {
        private final FlowManager flowManager;

        FlowManagerContext(final FlowManager flowManager) {
            this.flowManager = Objects.requireNonNull(flowManager, "Flow Manager required");
        }

        @Override
        public LiveConnection getConnection(final String connectionId) {
            final Connection connection = flowManager.getConnection(connectionId);
            if (connection == null) {
                return null;
            }

            return new LiveConnection(
                    connection.getIdentifier(),
                    getConnectableIdentifier(connection.getSource()),
                    getConnectableIdentifier(connection.getDestination()),
                    connection.getFlowFileQueue().isEmpty());
        }

        @Override
        public LiveConnectable getConnectable(final String connectableId) {
            final Connectable connectable = flowManager.findConnectable(connectableId);
            if (connectable == null) {
                return null;
            }

            ScheduledState physicalScheduledState = null;
            ValidationStatus validationStatus = null;
            boolean running = false;
            if (connectable instanceof final ProcessorNode processorNode) {
                physicalScheduledState = processorNode.getPhysicalScheduledState();
                validationStatus = processorNode.getValidationStatus();
            } else if (connectable instanceof final Port port) {
                running = port.isRunning();
            }

            return new LiveConnectable(
                    connectable.getIdentifier(),
                    connectable.getConnectableType(),
                    connectable.getProcessGroupIdentifier(),
                    physicalScheduledState,
                    validationStatus,
                    running,
                    getConnectionIdentifiers(connectable.getIncomingConnections()),
                    getConnectionIdentifiers(connectable.getConnections()));
        }

        @Override
        public LiveProcessGroup getProcessGroup(final String processGroupId) {
            final ProcessGroup processGroup = flowManager.getGroup(processGroupId);
            if (processGroup == null) {
                return null;
            }

            final ProcessGroup parent = processGroup.getParent();
            return new LiveProcessGroup(processGroup.getIdentifier(), parent == null ? null : parent.getIdentifier());
        }

        private String getConnectableIdentifier(final Connectable connectable) {
            return connectable == null ? null : connectable.getIdentifier();
        }

        private Set<String> getConnectionIdentifiers(final Collection<Connection> connections) {
            if (connections == null || connections.isEmpty()) {
                return Collections.emptySet();
            }

            final Set<String> connectionIdentifiers = new LinkedHashSet<>(connections.size());
            for (final Connection connection : connections) {
                connectionIdentifiers.add(connection.getIdentifier());
            }

            return connectionIdentifiers;
        }
    }

    private static <T> Set<T> copyOrderedSet(final Collection<T> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }

        return Collections.unmodifiableSet(new LinkedHashSet<>(values));
    }
}
