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
package org.apache.nifi.cluster.coordination.node;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.ClusterTopologyEventListener;
import org.apache.nifi.cluster.coordination.flow.FlowElection;
import org.apache.nifi.cluster.coordination.heartbeat.NodeHeartbeat;
import org.apache.nifi.cluster.coordination.http.HttpResponseMapper;
import org.apache.nifi.cluster.coordination.http.StandardHttpResponseMapper;
import org.apache.nifi.cluster.coordination.http.replication.RequestCompletionCallback;
import org.apache.nifi.cluster.coordination.node.state.NodeIdentifierDescriptor;
import org.apache.nifi.cluster.event.Event;
import org.apache.nifi.cluster.event.NodeEvent;
import org.apache.nifi.cluster.exception.NoClusterCoordinatorException;
import org.apache.nifi.cluster.firewall.ClusterNodeFirewall;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.IllegalNodeDisconnectionException;
import org.apache.nifi.cluster.manager.exception.IllegalNodeOffloadException;
import org.apache.nifi.cluster.protocol.ComponentRevisionSnapshot;
import org.apache.nifi.cluster.protocol.ConnectionRequest;
import org.apache.nifi.cluster.protocol.ConnectionResponse;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.NodeProtocolSender;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.cluster.protocol.impl.ClusterCoordinationProtocolSenderListener;
import org.apache.nifi.cluster.protocol.message.ClusterWorkloadRequestMessage;
import org.apache.nifi.cluster.protocol.message.ClusterWorkloadResponseMessage;
import org.apache.nifi.cluster.protocol.message.ConnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ConnectionResponseMessage;
import org.apache.nifi.cluster.protocol.message.DisconnectMessage;
import org.apache.nifi.cluster.protocol.message.NodeConnectionStatusResponseMessage;
import org.apache.nifi.cluster.protocol.message.NodeStatusChangeMessage;
import org.apache.nifi.cluster.protocol.message.OffloadMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage.MessageType;
import org.apache.nifi.cluster.protocol.message.ReconnectionRequestMessage;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.revision.RevisionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class NodeClusterCoordinator implements ClusterCoordinator, ProtocolHandler, RequestCompletionCallback {

    private static final Logger logger = LoggerFactory.getLogger(NodeClusterCoordinator.class);
    private static final String EVENT_CATEGORY = "Clustering";
    private static final Duration NODE_API_TIMEOUT = Duration.ofSeconds(10);

    private static final Pattern COUNTER_URI_PATTERN = Pattern.compile("/nifi-api/counters/[a-f0-9\\-]{36}");

    private final String instanceId = UUID.randomUUID().toString();
    private volatile NodeIdentifier nodeId;

    private final ClusterCoordinationProtocolSenderListener senderListener;
    private final EventReporter eventReporter;
    private final ClusterNodeFirewall firewall;
    private final RevisionManager revisionManager;
    private final NiFiProperties nifiProperties;
    private final LeaderElectionManager leaderElectionManager;
    private final AtomicLong latestUpdateId = new AtomicLong(-1);
    private final FlowElection flowElection;
    private final NodeProtocolSender nodeProtocolSender;
    private final StateManager stateManager;

    private volatile FlowService flowService;
    private volatile boolean connected;
    private volatile boolean closed = false;
    private volatile boolean requireElection = true;

    private final ConcurrentMap<String, NodeConnectionStatus> nodeStatuses = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CircularFifoQueue<NodeEvent>> nodeEvents = new ConcurrentHashMap<>();

    private final List<ClusterTopologyEventListener> eventListeners = new CopyOnWriteArrayList<>();

    public NodeClusterCoordinator(
            final ClusterCoordinationProtocolSenderListener senderListener,
            final EventReporter eventReporter,
            final LeaderElectionManager leaderElectionManager,
            final FlowElection flowElection,
            final ClusterNodeFirewall firewall,
            final RevisionManager revisionManager,
            final NiFiProperties nifiProperties,
            final NodeProtocolSender nodeProtocolSender,
            final StateManagerProvider stateManagerProvider
    ) throws IOException {
        this.senderListener = senderListener;
        this.flowService = null;
        this.eventReporter = eventReporter;
        this.firewall = firewall;
        this.revisionManager = revisionManager;
        this.nifiProperties = nifiProperties;
        this.leaderElectionManager = leaderElectionManager;
        this.flowElection = flowElection;
        this.nodeProtocolSender = nodeProtocolSender;
        this.stateManager = stateManagerProvider.getStateManager(ClusterRoles.CLUSTER_COORDINATOR);

        recoverState();

        senderListener.addHandler(this);
    }

    private void recoverState() throws IOException {
        final StateMap stateMap = stateManager.getState(Scope.LOCAL);
        if (stateMap == null) {
            logger.debug("No state to restore");
            return;
        }

        final ObjectMapper mapper = new ObjectMapper();
        final JsonFactory jsonFactory = new JsonFactory();
        jsonFactory.setCodec(mapper);

        final Map<NodeIdentifier, NodeConnectionStatus> connectionStatusMap = new HashMap<>();
        NodeIdentifier localNodeId = null;

        final Map<String, String> state = stateMap.toMap();
        for (final Map.Entry<String, String> entry : state.entrySet()) {
            final String nodeUuid = entry.getKey();
            final String nodeIdentifierJson = entry.getValue();
            logger.debug("Recovering state for {} = {}", nodeUuid, nodeIdentifierJson);

            try (final JsonParser jsonParser = jsonFactory.createParser(nodeIdentifierJson)) {
                final NodeIdentifierDescriptor nodeIdDesc = jsonParser.readValueAs(NodeIdentifierDescriptor.class);
                final NodeIdentifier nodeId = nodeIdDesc.toNodeIdentifier();

                connectionStatusMap.put(nodeId, new NodeConnectionStatus(nodeId, DisconnectionCode.NOT_YET_CONNECTED));
                if (nodeIdDesc.isLocalNodeIdentifier()) {
                    if (localNodeId == null) {
                        localNodeId = nodeId;
                    } else {
                        logger.warn("When recovering state, determined that two Node Identifiers claim to be the local Node Identifier: {} and {}. Will ignore both of these and wait until " +
                            "connecting to cluster to determine which Node Identiifer is the local Node Identifier", localNodeId.getFullDescription(), nodeId.getFullDescription());
                        localNodeId = null;
                    }
                }
            }
        }

        if (!connectionStatusMap.isEmpty()) {
            resetNodeStatuses(connectionStatusMap);
        }

        if (localNodeId != null) {
            logger.debug("Recovered state indicating that Local Node Identifier is {}", localNodeId);
            setLocalNodeIdentifier(localNodeId);
        }
    }

    private void storeState() {
        final ObjectMapper mapper = new ObjectMapper();
        final JsonFactory jsonFactory = new JsonFactory();
        jsonFactory.setCodec(mapper);

        try {
            final Map<String, String> stateMap = new HashMap<>();

            final NodeIdentifier localNodeId = getLocalNodeIdentifier();
            for (final NodeIdentifier nodeId : getNodeIdentifiers()) {
                final boolean isLocalId = nodeId.equals(localNodeId);
                final NodeIdentifierDescriptor descriptor = NodeIdentifierDescriptor.fromNodeIdentifier(nodeId, isLocalId);

                try (final StringWriter writer = new StringWriter()) {
                    final JsonGenerator jsonGenerator = jsonFactory.createGenerator(writer);
                    jsonGenerator.writeObject(descriptor);

                    final String serializedDescriptor = writer.toString();
                    stateMap.put(nodeId.getId(), serializedDescriptor);
                }
            }

            stateManager.setState(stateMap, Scope.LOCAL);
            logger.debug("Stored the following state as the Cluster Topology: {}", stateMap);
        } catch (final Exception e) {
            logger.warn("Failed to store cluster topology to local State Manager. Upon restart of NiFi, the cluster topology may not be accurate until joining the cluster.", e);
        }
    }


    public void registerEventListener(final ClusterTopologyEventListener eventListener) {
        this.eventListeners.add(eventListener);
    }

    public void unregisterEventListener(final ClusterTopologyEventListener eventListener) {
        this.eventListeners.remove(eventListener);
    }

    @Override
    public void shutdown() {
        if (closed) {
            return;
        }

        closed = true;

        final NodeIdentifier localId = getLocalNodeIdentifier();
        if (localId != null) {
            final NodeConnectionStatus shutdownStatus = new NodeConnectionStatus(localId, DisconnectionCode.NODE_SHUTDOWN);
            updateNodeStatus(shutdownStatus, false);
            logger.info("Node ID [{}] Disconnection Code [{}] send completed", localId, DisconnectionCode.NODE_SHUTDOWN);
        }
    }

    @Override
    public void setLocalNodeIdentifier(final NodeIdentifier nodeId) {
        if (nodeId == null || nodeId.equals(this.nodeId)) {
            return;
        }

        this.nodeId = nodeId;
        nodeStatuses.computeIfAbsent(nodeId.getId(), id -> new NodeConnectionStatus(nodeId, DisconnectionCode.NOT_YET_CONNECTED));
        eventListeners.forEach(listener -> listener.onLocalNodeIdentifierSet(nodeId));

        storeState();
    }

    @Override
    public NodeIdentifier getLocalNodeIdentifier() {
        return nodeId;
    }

    public NodeIdentifier waitForElectedClusterCoordinator() {
        return waitForNodeIdentifier(() -> getElectedActiveCoordinatorNode(false));
    }

    private NodeIdentifier waitForNodeIdentifier(final Supplier<NodeIdentifier> fetchNodeId) {
        NodeIdentifier localNodeId = null;
        while (localNodeId == null) {
            localNodeId = fetchNodeId.get();
            if (localNodeId == null) {
                if (closed) {
                    return null;
                }

                try {
                    Thread.sleep(100L);
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }

        return localNodeId;
    }

    private Optional<String> getElectedActiveCoordinatorAddress() {
        return leaderElectionManager.getLeader(ClusterRoles.CLUSTER_COORDINATOR);
    }

    @Override
    public void resetNodeStatuses(final Map<NodeIdentifier, NodeConnectionStatus> statusMap) {
        logger.info("Resetting cluster node statuses from {} to {}", nodeStatuses, statusMap);

        // For each proposed replacement, update the nodeStatuses map if and only if the replacement
        // has a larger update id than the current value.
        for (final Map.Entry<NodeIdentifier, NodeConnectionStatus> entry : statusMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final NodeConnectionStatus proposedStatus = entry.getValue();

            if (proposedStatus.getState() == NodeConnectionState.REMOVED) {
                removeNode(nodeId);
            } else {
                forcefullyUpdateNodeStatus(nodeId, proposedStatus, false);
            }
        }

        storeState();
    }

    private NodeConnectionStatus removeNode(final NodeIdentifier nodeId) {
        final NodeConnectionStatus status = nodeStatuses.remove(nodeId.getId());
        nodeEvents.remove(nodeId.getId());
        if (status != null) {
            onNodeRemoved(nodeId);
        }

        return status;
    }

    private boolean removeNodeConditionally(final NodeIdentifier nodeId, final NodeConnectionStatus expectedStatus) {
        final boolean removed = nodeStatuses.remove(nodeId.getId(), expectedStatus);
        if (removed) {
            nodeEvents.remove(nodeId.getId());
            onNodeRemoved(nodeId);
        }

        return removed;
    }

    /**
     * Updates the status of the node with the given ID to the given status if and only if the updated status's Update ID is >= the Update ID of the current
     * Node status or if there is currently no node with the given identifier
     * @param nodeId the NodeIdentifier for the node whose ID is to be updated
     * @param updatedStatus the new status for the node
     * @return the previous status for the node
     */
    private NodeConnectionStatus updateNodeStatus(final NodeIdentifier nodeId, final NodeConnectionStatus updatedStatus) {
        return updateNodeStatus(nodeId, updatedStatus, true);
    }

    private NodeConnectionStatus updateNodeStatus(final NodeIdentifier nodeId, final NodeConnectionStatus updatedStatus, final boolean storeState) {
        final String nodeUuid = nodeId.getId();

        while (true) {
            final NodeConnectionStatus currentStatus = nodeStatuses.get(nodeUuid);
            if (currentStatus == null) {
                onNodeAdded(nodeId, storeState);

                // Return null because that was the previous state
                return null;
            }

            if (currentStatus.getUpdateIdentifier() > updatedStatus.getUpdateIdentifier()) {
                logger.debug("Received status update {} but ignoring it because it has an Update ID of {} and the current status has an Update ID of {}",
                    updatedStatus, updatedStatus.getUpdateIdentifier(), currentStatus.getUpdateIdentifier());
                return currentStatus;
            }

            final boolean updated = nodeStatuses.replace(nodeUuid, currentStatus, updatedStatus);
            if (updated) {
                onNodeStateChange(nodeId, updatedStatus.getState());
                logger.info("Status of {} changed from {} to {}", nodeId, currentStatus, updatedStatus);
                return currentStatus;
            }
        }
    }

    private NodeConnectionStatus forcefullyUpdateNodeStatus(final NodeIdentifier nodeId, final NodeConnectionStatus updatedStatus, final boolean storeState) {
        final NodeConnectionStatus evictedStatus = nodeStatuses.put(nodeId.getId(), updatedStatus);
        if (evictedStatus == null) {
            onNodeAdded(nodeId, storeState);
        } else {
            onNodeStateChange(nodeId, updatedStatus.getState());
        }

        return evictedStatus;
    }

    private boolean updateNodeStatusConditionally(final NodeIdentifier nodeId, final NodeConnectionStatus expectedStatus, final NodeConnectionStatus updatedStatus) {
        final boolean updated;
        if (expectedStatus == null) {
            final NodeConnectionStatus existingValue = nodeStatuses.putIfAbsent(nodeId.getId(), updatedStatus);
            updated = existingValue == null;

            if (updated) {
                onNodeAdded(nodeId, true);
            }
        } else {
            updated = nodeStatuses.replace(nodeId.getId(), expectedStatus, updatedStatus);
        }

        if (updated) {
            onNodeStateChange(nodeId, updatedStatus.getState());
        }

        return updated;
    }

    @Override
    public boolean resetNodeStatus(final NodeConnectionStatus connectionStatus, final long qualifyingUpdateId) {
        final NodeIdentifier nodeId = connectionStatus.getNodeIdentifier();
        final NodeConnectionStatus currentStatus = getConnectionStatus(nodeId);

        if (currentStatus == null) {
            return replaceNodeStatus(nodeId, null, connectionStatus);
        } else if (currentStatus.getUpdateIdentifier() == qualifyingUpdateId) {
            return replaceNodeStatus(nodeId, currentStatus, connectionStatus);
        }

        // The update identifier is not the same. We will not replace the value
        return false;
    }

    /**
     * Attempts to update the nodeStatuses map by changing the value for the
     * given node id from the current status to the new status, as in
     * ConcurrentMap.replace(nodeId, currentStatus, newStatus) but with the
     * difference that this method can handle a <code>null</code> value for
     * currentStatus
     *
     * @param nodeId the node id
     * @param currentStatus the current status, or <code>null</code> if there is
     * no value currently
     * @param newStatus the new status to set
     * @return <code>true</code> if the map was updated, false otherwise
     */
    private boolean replaceNodeStatus(final NodeIdentifier nodeId, final NodeConnectionStatus currentStatus, final NodeConnectionStatus newStatus) {
        if (newStatus == null) {
            logger.error("Cannot change node status for {} from {} to {} because new status is null", nodeId, currentStatus, newStatus);
            logger.error("", new NullPointerException());
        }

        if (currentStatus == null) {
            if (newStatus.getState() == NodeConnectionState.REMOVED) {
                return removeNodeConditionally(nodeId, currentStatus);
            } else {
                return updateNodeStatusConditionally(nodeId, null, newStatus);
            }
        }

        if (newStatus.getState() == NodeConnectionState.REMOVED) {
            if (removeNodeConditionally(nodeId, currentStatus)) {
                storeState();
                return true;
            } else {
                return false;
            }
        } else {
            return updateNodeStatusConditionally(nodeId, currentStatus, newStatus);
        }
    }

    @Override
    public void requestNodeConnect(final NodeIdentifier nodeId, final String userDn) {
        if (requireElection && !flowElection.isElectionComplete() && flowElection.isVoteCounted(nodeId)) {
            // If we receive a heartbeat from a node that we already know, we don't want to request that it reconnect
            // to the cluster because no flow has yet been elected. However, if the node has not yet voted, we want to send
            // a reconnect request because we want this node to cast its vote for the flow, and this happens on connection
            logger.debug("Received heartbeat for {} and node is not connected. Will not request node connect to cluster, "
                + "though, because the Flow Election is still in progress", nodeId);
            return;
        }

        if (userDn == null) {
            reportEvent(nodeId, Severity.INFO, "Requesting that node connect to cluster");
        } else {
            reportEvent(nodeId, Severity.INFO, "Requesting that node connect to cluster on behalf of " + userDn);
        }

        updateNodeStatus(new NodeConnectionStatus(nodeId, NodeConnectionState.CONNECTING, null, null, null, System.currentTimeMillis()));

        // create the request
        final ReconnectionRequestMessage request = new ReconnectionRequestMessage();
        request.setNodeId(nodeId);
        request.setInstanceId(instanceId);

        // If we still are requiring that an election take place, we do not want to include our local dataflow, because we don't
        // yet know what the cluster's dataflow looks like. However, if we don't require election, then we've connected to the
        // cluster, which means that our flow is correct.
        final boolean includeDataFlow = !requireElection;
        requestReconnectionAsynchronously(request, 10, 5, includeDataFlow);
    }

    @Override
    public void finishNodeConnection(final NodeIdentifier nodeId) {
        final NodeConnectionState state = getConnectionState(nodeId);
        if (state == null) {
            logger.debug("Attempted to finish node connection for {} but node is not known. Requesting that node connect", nodeId);
            requestNodeConnect(nodeId, null);
            return;
        }

        if (state == NodeConnectionState.CONNECTED) {
            // already connected. Nothing to do.
            return;
        }

        if (state == NodeConnectionState.DISCONNECTED || state == NodeConnectionState.DISCONNECTING) {
            logger.debug("Attempted to finish node connection for {} but node state was {}. Requesting that node connect", nodeId, state);
            requestNodeConnect(nodeId, null);
            return;
        }

        logger.info("{} is now connected", nodeId);
        updateNodeStatus(new NodeConnectionStatus(nodeId, NodeConnectionState.CONNECTED));
    }

    @Override
    public void finishNodeOffload(final NodeIdentifier nodeId) {
        final NodeConnectionState state = getConnectionState(nodeId);
        if (state == null) {
            logger.warn("Attempted to finish node offload for {} but node is not known.", nodeId);
            return;
        }

        if (state != NodeConnectionState.OFFLOADING) {
            logger.warn("Attempted to finish node offload for {} but node is not in the offloading state, it is currently {}.", nodeId, state);
            return;
        }

        logger.info("{} is now offloaded", nodeId);

        updateNodeStatus(new NodeConnectionStatus(nodeId, NodeConnectionState.OFFLOADED));
    }

    @Override
    public Future<Void> requestNodeOffload(final NodeIdentifier nodeId, final OffloadCode offloadCode, final String explanation) {
        final Set<NodeIdentifier> offloadNodeIds = getNodeIdentifiers(NodeConnectionState.OFFLOADING, NodeConnectionState.OFFLOADED);
        if (offloadNodeIds.contains(nodeId)) {
            logger.debug("Attempted to offload node but the node is already offloading or offloaded");
            // no need to do anything here, the node is currently offloading or already offloaded
            return CompletableFuture.completedFuture(null);
        }

        final Set<NodeIdentifier> disconnectedNodeIds = getNodeIdentifiers(NodeConnectionState.DISCONNECTED);
        if (!disconnectedNodeIds.contains(nodeId)) {
            throw new IllegalNodeOffloadException("Cannot offload node " + nodeId + " because it is not currently disconnected");
        }

        logger.info("Requesting that {} is offloaded due to {}", nodeId, explanation == null ? offloadCode : explanation);

        updateNodeStatus(new NodeConnectionStatus(nodeId, NodeConnectionState.OFFLOADING, offloadCode, explanation));

        final OffloadMessage request = new OffloadMessage();
        request.setNodeId(nodeId);
        request.setExplanation(explanation);

        addNodeEvent(nodeId, "Offload requested due to " + explanation);
        return offloadAsynchronously(request, 10, 5);
    }

    @Override
    public Future<Void> requestNodeDisconnect(final NodeIdentifier nodeId, final DisconnectionCode disconnectionCode, final String explanation) {
        final Set<NodeIdentifier> connectedNodeIds = getNodeIdentifiers(NodeConnectionState.CONNECTED);
        if (connectedNodeIds.size() == 1 && connectedNodeIds.contains(nodeId)) {
            throw new IllegalNodeDisconnectionException("Cannot disconnect node " + nodeId + " because it is the only node currently connected");
        }

        logger.info("Requesting that {} disconnect due to {}", nodeId, explanation == null ? disconnectionCode : explanation);

        updateNodeStatus(new NodeConnectionStatus(nodeId, disconnectionCode, explanation));

        // There is no need to tell the node that it's disconnected if it is due to being
        // shutdown, as we will not be able to connect to the node anyway.
        if (disconnectionCode == DisconnectionCode.NODE_SHUTDOWN) {
            return CompletableFuture.completedFuture(null);
        }

        final DisconnectMessage request = new DisconnectMessage();
        request.setNodeId(nodeId);
        request.setExplanation(explanation);

        addNodeEvent(nodeId, "Disconnection requested due to " + explanation);
        return disconnectAsynchronously(request, 10, 5);
    }

    @Override
    public void disconnectionRequestedByNode(final NodeIdentifier nodeId, final DisconnectionCode disconnectionCode, final String explanation) {
        logger.info("{} requested disconnection from cluster due to {}", nodeId, explanation == null ? disconnectionCode : explanation);
        updateNodeStatus(new NodeConnectionStatus(nodeId, disconnectionCode, explanation), false);

        final Severity severity = switch (disconnectionCode) {
            case STARTUP_FAILURE, MISMATCHED_FLOWS, UNKNOWN -> Severity.ERROR;
            case LACK_OF_HEARTBEAT -> Severity.WARNING;
            default -> Severity.INFO;
        };

        reportEvent(nodeId, severity, "Node disconnected from cluster due to " + explanation);
    }

    @Override
    public void removeNode(final NodeIdentifier nodeId, final String userDn) {
        // Remove the node from the cluster state before any notifications are sent to the cluster participants.  This
        // ensures that potential communication failures do not cause the operation to fail.
        removeNode(nodeId);
        storeState();
        reportEvent(nodeId, Severity.INFO, "User " + userDn + " requested that node be removed from cluster");
        notifyOthersOfNodeStatusChange(new NodeConnectionStatus(nodeId, NodeConnectionState.REMOVED));
    }

    private void onNodeRemoved(final NodeIdentifier nodeId) {
        eventListeners.forEach(listener -> listener.onNodeRemoved(nodeId));
    }

    private void onNodeAdded(final NodeIdentifier nodeId, final boolean storeState) {
        if (storeState) {
            storeState();
        }
        eventListeners.forEach(listener -> listener.onNodeAdded(nodeId));
    }

    private void onNodeStateChange(final NodeIdentifier nodeId, final NodeConnectionState nodeConnectionState) {
        eventListeners.forEach(listener -> listener.onNodeStateChange(nodeId, nodeConnectionState));
    }

    @Override
    public NodeConnectionStatus getConnectionStatus(final NodeIdentifier nodeId) {
        return nodeStatuses.get(nodeId.getId());
    }

    private NodeConnectionState getConnectionState(final NodeIdentifier nodeId) {
        final NodeConnectionStatus status = getConnectionStatus(nodeId);
        return status == null ? null : status.getState();
    }

    @Override
    public List<NodeConnectionStatus> getConnectionStatuses() {
        return new ArrayList<>(nodeStatuses.values());
    }

    @Override
    public Map<NodeConnectionState, List<NodeIdentifier>> getConnectionStates() {
        final Map<NodeConnectionState, List<NodeIdentifier>> connectionStates = new HashMap<>();
        for (final Map.Entry<String, NodeConnectionStatus> entry : nodeStatuses.entrySet()) {
            final NodeConnectionState state = entry.getValue().getState();
            final List<NodeIdentifier> nodeIds = connectionStates.computeIfAbsent(state, s -> new ArrayList<>());
            nodeIds.add(entry.getValue().getNodeIdentifier());
        }

        return connectionStates;
    }

    @Override
    public boolean isBlockedByFirewall(final Set<String> nodeIdentities) {
        if (firewall == null) {
            return false;
        }

        for (final String nodeId : nodeIdentities) {
            if (firewall.isPermissible(nodeId)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean isApiReachable(final NodeIdentifier nodeId) {
        final String apiAddress = nodeId.getApiAddress();
        final int apiPort = nodeId.getApiPort();
        try {
            try (final Socket soc = new Socket()) {
                soc.connect(new InetSocketAddress(apiAddress, apiPort), (int) NODE_API_TIMEOUT.toMillis());
            }
            return true;
        } catch (final Exception e) {
            logger.debug("Node is not reachable at API address {} and port {}", apiAddress, apiPort, e);
            return false;
        }
    }

    @Override
    public void reportEvent(final NodeIdentifier nodeId, final Severity severity, final String event) {
        eventReporter.reportEvent(severity, EVENT_CATEGORY, nodeId == null ? event : "Event Reported for " + nodeId + " -- " + event);
        if (nodeId != null) {
            addNodeEvent(nodeId, severity, event);
        }

        final String message = nodeId == null ? event : "Event Reported for " + nodeId + " -- " + event;
        switch (severity) {
            case ERROR:
                logger.error(message);
                break;
            case WARNING:
                logger.warn(message);
                break;
            case INFO:
                logger.info(message);
                break;
        }
    }

    @Override
    public NodeIdentifier getNodeIdentifier(final String uuid) {
        final NodeConnectionStatus status = nodeStatuses.get(uuid);
        return status == null ? null : status.getNodeIdentifier();
    }

    @Override
    public Set<NodeIdentifier> getNodeIdentifiers(final NodeConnectionState... states) {
        final Set<NodeConnectionState> statesOfInterest = new HashSet<>();
        if (states.length == 0) {
            for (final NodeConnectionState state : NodeConnectionState.values()) {
                statesOfInterest.add(state);
            }
        } else {
            for (final NodeConnectionState state : states) {
                statesOfInterest.add(state);
            }
        }

        return nodeStatuses.entrySet().stream()
                .filter(entry -> statesOfInterest.contains(entry.getValue().getState()))
                .map(entry -> entry.getValue().getNodeIdentifier())
                .collect(Collectors.toSet());
    }

    @Override
    public NodeIdentifier getPrimaryNode() {
        final Optional<String> primaryNodeLeader = leaderElectionManager.getLeader(ClusterRoles.PRIMARY_NODE);
        if (!primaryNodeLeader.isPresent()) {
            return null;
        }

        return nodeStatuses.values().stream()
                .map(NodeConnectionStatus::getNodeIdentifier)
                .filter(nodeId -> primaryNodeLeader.get().equals(nodeId.getSocketAddress() + ":" + nodeId.getSocketPort()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public NodeIdentifier getElectedActiveCoordinatorNode() {
        return getElectedActiveCoordinatorNode(true);
    }

    private NodeIdentifier getElectedActiveCoordinatorNode(final boolean warnOnError) {
        final Optional<String> electedActiveCoordinatorAddress;
        try {
            electedActiveCoordinatorAddress = getElectedActiveCoordinatorAddress();
        } catch (final NoClusterCoordinatorException ncce) {
            logger.debug("There is currently no elected active Cluster Coordinator");
            return null;
        }

        if (!electedActiveCoordinatorAddress.isPresent()) {
            logger.debug("There is currently no elected active Cluster Coordinator");
            return null;
        }

        final String electedNodeAddress = electedActiveCoordinatorAddress.get().trim();

        final int colonLoc = electedNodeAddress.indexOf(':');
        if (colonLoc < 1) {
            if (warnOnError) {
                logger.warn("Failed to determine which node is elected active Cluster Coordinator: Manager reports the address as {}, but this is not a valid address", electedNodeAddress);
            }

            return null;
        }

        final String electedNodeHostname = electedNodeAddress.substring(0, colonLoc);
        final String portString = electedNodeAddress.substring(colonLoc + 1);
        final int electedNodePort;
        try {
            electedNodePort = Integer.parseInt(portString);
        } catch (final NumberFormatException nfe) {
            if (warnOnError) {
                logger.warn("Failed to determine which node is elected active Cluster Coordinator: Manager reports the address as {}, but this is not a valid address", electedNodeAddress);
            }

            return null;
        }

        final Set<NodeIdentifier> connectedNodeIds = getNodeIdentifiers();
        final NodeIdentifier electedNodeId = connectedNodeIds.stream()
                .filter(nodeId -> nodeId.getSocketAddress().equals(electedNodeHostname) && nodeId.getSocketPort() == electedNodePort)
                .findFirst()
                .orElse(null);

        if (electedNodeId == null && warnOnError) {
            logger.debug("Failed to determine which node is elected active Cluster Coordinator: Manager reports the address as {},"
                    + "but there is no node with this address. Will attempt to communicate with node to determine its information", electedNodeAddress);

            try {
                final NodeConnectionStatus connectionStatus = senderListener.requestNodeConnectionStatus(electedNodeHostname, electedNodePort);
                logger.debug("Received NodeConnectionStatus {}", connectionStatus);

                if (connectionStatus == null) {
                    return null;
                }

                final NodeConnectionStatus existingStatus = this.nodeStatuses.putIfAbsent(connectionStatus.getNodeIdentifier().getId(), connectionStatus);
                if (existingStatus == null) {
                    onNodeAdded(connectionStatus.getNodeIdentifier(), true);
                    return connectionStatus.getNodeIdentifier();
                } else {
                    return existingStatus.getNodeIdentifier();
                }
            } catch (final Exception e) {
                logger.warn("Failed to determine which node is elected active Cluster Coordinator: Manager reports the address as {}, but there is no node with this address. "
                        + "Attempted to determine the node's information but failed to retrieve its information due to {}", electedNodeAddress, e.toString());

                if (logger.isDebugEnabled()) {
                    logger.warn("", e);
                }
            }
        }

        return electedNodeId;
    }

    @Override
    public boolean isActiveClusterCoordinator() {
        return leaderElectionManager != null && leaderElectionManager.isLeader(ClusterRoles.CLUSTER_COORDINATOR);
    }

    @Override
    public List<NodeEvent> getNodeEvents(final NodeIdentifier nodeId) {
        final CircularFifoQueue<NodeEvent> eventQueue = nodeEvents.get(nodeId.getId());
        if (eventQueue == null) {
            return Collections.emptyList();
        }

        synchronized (eventQueue) {
            return new ArrayList<>(eventQueue);
        }
    }

    @Override
    public void setFlowService(final FlowService flowService) {
        if (this.flowService != null) {
            throw new IllegalStateException("Flow Service has already been set");
        }
        this.flowService = flowService;
    }

    private void addNodeEvent(final NodeIdentifier nodeId, final String event) {
        addNodeEvent(nodeId, Severity.INFO, event);
    }

    private void addNodeEvent(final NodeIdentifier nodeId, final Severity severity, final String message) {
        final NodeEvent event = new Event(nodeId.toString(), message, severity);
        final CircularFifoQueue<NodeEvent> eventQueue = nodeEvents.computeIfAbsent(nodeId.getId(), id -> new CircularFifoQueue<>());
        synchronized (eventQueue) {
            eventQueue.add(event);
        }
    }

    /**
     * Updates the status of the node with the given ID to the given status and
     * returns <code>true</code> if successful, <code>false</code> if no node
     * exists with the given ID
     *
     * @param status the new status of the node
     */
    // visible for testing.
    void updateNodeStatus(final NodeConnectionStatus status) {
        updateNodeStatus(status, true);
    }

    void updateNodeStatus(final NodeConnectionStatus status, final boolean waitForCoordinator) {
        final NodeIdentifier nodeId = status.getNodeIdentifier();

        // In this case, we are using nodeStatuses.put() (i.e., forcefully updating node status) instead of getting the current value and
        // comparing that to the new value and using the one with the largest update id. This is because
        // this method is called when something occurs that causes this node to change the status of the
        // node in question. We only use comparisons against the current value when we receive an update
        // about a node status from a different node, since those may be received out-of-order.
        final NodeConnectionStatus currentStatus = forcefullyUpdateNodeStatus(nodeId, status, true);
        final NodeConnectionState currentState = currentStatus == null ? null : currentStatus.getState();
        if (Objects.equals(status, currentStatus)) {
            logger.debug("Received notification of Node Status Change for {} but the status remained the same: {}", nodeId, status);
        } else {
            logger.info("Status of {} changed from {} to {}", nodeId, currentStatus, status);
        }

        logger.debug("State of cluster nodes is now {}", nodeStatuses);

        latestUpdateId.updateAndGet(curVal -> Math.max(curVal, status.getUpdateIdentifier()));

        if (currentState == null || currentState != status.getState()) {
            final boolean notifyAllNodes = isActiveClusterCoordinator();
            if (notifyAllNodes) {
                logger.debug("Notifying all nodes that status changed from {} to {}", currentStatus, status);
            } else {
                logger.debug("Notifying cluster coordinator that node status changed from {} to {}", currentStatus, status);
            }

            notifyOthersOfNodeStatusChange(status, notifyAllNodes, waitForCoordinator);
        } else {
            logger.debug("Not notifying other nodes that status changed because previous state of {} is same as new state of {}", currentState, status.getState());
        }
    }

    void notifyOthersOfNodeStatusChange(final NodeConnectionStatus updatedStatus) {
        notifyOthersOfNodeStatusChange(updatedStatus, isActiveClusterCoordinator(), true);
    }

    /**
     * Notifies other nodes that the status of a node changed
     *
     * @param updatedStatus the updated status for a node in the cluster
     * @param notifyAllNodes if <code>true</code> will notify all nodes. If
     * <code>false</code>, will notify only the cluster coordinator
     */
    void notifyOthersOfNodeStatusChange(final NodeConnectionStatus updatedStatus, final boolean notifyAllNodes, final boolean waitForCoordinator) {
        // If this node is the active cluster coordinator, then we are going to replicate to all nodes.
        // Otherwise, get the active coordinator (or wait for one to become active) and then notify the coordinator.
        final Set<NodeIdentifier> nodesToNotify;
        if (notifyAllNodes) {
            nodesToNotify = getNodeIdentifiers();

            // Do not notify ourselves because we already know about the status update.
            nodesToNotify.remove(getLocalNodeIdentifier());
        } else if (waitForCoordinator) {
            nodesToNotify = Collections.singleton(waitForElectedClusterCoordinator());
        } else {
            final NodeIdentifier nodeId = getElectedActiveCoordinatorNode();
            if (nodeId == null) {
                return;
            }
            nodesToNotify = Collections.singleton(nodeId);
        }

        final NodeStatusChangeMessage message = new NodeStatusChangeMessage();
        message.setNodeId(updatedStatus.getNodeIdentifier());
        message.setNodeConnectionStatus(updatedStatus);
        senderListener.notifyNodeStatusChange(nodesToNotify, message);
    }

    private Future<Void> offloadAsynchronously(final OffloadMessage request, final int attempts, final int retrySeconds) {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        final Thread offloadThread = new Thread(() -> {
            final NodeIdentifier nodeId = request.getNodeId();

            Exception lastException = null;
            for (int i = 0; i < attempts; i++) {
                try {
                    senderListener.offload(request);
                    reportEvent(nodeId, Severity.INFO, "Node was offloaded due to " + request.getExplanation());

                    future.complete(null);
                    return;
                } catch (final Exception e) {
                    logger.error("Failed to notify {} that it has been offloaded due to {}", request.getNodeId(), request.getExplanation(), e);
                    lastException = e;

                    try {
                        Thread.sleep(retrySeconds * 1000L);
                    } catch (final InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            updateNodeStatus(new NodeConnectionStatus(nodeId, NodeConnectionState.DISCONNECTED, null,
                "Attempted to offload node but failed to notify node that it was to offload its data. State reset to disconnected."));
            addNodeEvent(nodeId, "Failed to initiate node offload: " + lastException);

            future.completeExceptionally(lastException);
        }, "Offload " + request.getNodeId());

        offloadThread.start();
        return future;
    }

    private Future<Void> disconnectAsynchronously(final DisconnectMessage request, final int attempts, final int retrySeconds) {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        final Thread disconnectThread = new Thread(() -> {
            final NodeIdentifier nodeId = request.getNodeId();

            Exception lastException = null;
            for (int i = 0; i < attempts; i++) {
                // If the node is restarted, it will attempt to reconnect. In that case, we don't want to disconnect the node
                // again. So we instead log the fact that the state has now transitioned to this point and consider the task completed.
                final NodeConnectionState currentConnectionState = getConnectionState(nodeId);
                if (currentConnectionState == NodeConnectionState.CONNECTING || currentConnectionState == NodeConnectionState.CONNECTED) {
                    reportEvent(nodeId, Severity.INFO, String.format(
                        "State of Node %s has now transitioned from DISCONNECTED to %s so will no longer attempt to notify node that it is disconnected.", nodeId, currentConnectionState));
                    future.completeExceptionally(new IllegalStateException("Node was marked as disconnected but its state transitioned from DISCONNECTED back to " + currentConnectionState +
                        " before the node could be notified. This typically indicates that the node was restarted."));

                    return;
                }

                // Try to send disconnect notice to the node
                try {
                    senderListener.disconnect(request);
                    reportEvent(nodeId, Severity.INFO, "Node disconnected due to " + request.getExplanation());
                    future.complete(null);
                    return;
                } catch (final Exception e) {
                    logger.error("Failed to notify {} that it has been disconnected from the cluster due to {}", request.getNodeId(), request.getExplanation());
                    lastException = e;

                    try {
                        Thread.sleep(retrySeconds * 1000L);
                    } catch (final InterruptedException ie) {
                        future.completeExceptionally(ie);
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            future.completeExceptionally(lastException);
        }, "Disconnect " + request.getNodeId());

        disconnectThread.start();
        return future;
    }

    public void validateHeartbeat(final NodeHeartbeat heartbeat) {
        final long localUpdateCount = revisionManager.getRevisionUpdateCount();
        final long nodeUpdateCount = heartbeat.getRevisionUpdateCount();

        if (nodeUpdateCount > localUpdateCount) {
            // If the node's Revision Update Count is larger than ours, it indicates that the node has the incorrect set of Revisions.
            // This can happen, for instance, if the node connects to the cluster at the same time that a new node is joining and becoming the Cluster Coordinator.
            // This case is very rare but can occur on occasion. As a result, we check for that here and if it occurs, request that the node disconnect so that
            // it can reconnect.
            final String message = String.format("Node has a Revision Update Count of %s but local value is only %s. Node appears not to have the appropriate set of Component Revisions",
                heartbeat.getRevisionUpdateCount(), localUpdateCount);
            logger.warn("Requesting that {} reconnect to the cluster due to: {}", heartbeat.getNodeIdentifier(), message);
            requestNodeConnect(heartbeat.getNodeIdentifier(), null);
        }
    }

    private void requestReconnectionAsynchronously(final ReconnectionRequestMessage request, final int reconnectionAttempts, final int retrySeconds, final boolean includeDataFlow) {
        final Thread reconnectionThread = new Thread(() -> {
            // create the request
            while (flowService == null) {
                try {
                    Thread.sleep(100L);
                } catch (final InterruptedException ie) {
                    logger.info("Could not send Reconnection request to {} because thread was "
                            + "interrupted before FlowService was made available", request.getNodeId());
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            for (int i = 0; i < reconnectionAttempts; i++) {
                try {
                    if (NodeConnectionState.CONNECTING != getConnectionState(request.getNodeId())) {
                        // the node status has changed. It's no longer appropriate to attempt reconnection.
                        return;
                    }

                    if (includeDataFlow) {
                        request.setDataFlow(new StandardDataFlow(flowService.createDataFlowFromController()));
                    }

                    request.setNodeConnectionStatuses(getConnectionStatuses());
                    final ComponentRevisionSnapshot componentRevisionSnapshot = ComponentRevisionSnapshot.fromRevisionSnapshot(revisionManager.getAllRevisions());
                    request.setComponentRevisions(componentRevisionSnapshot);

                    // Issue a reconnection request to the node.
                    senderListener.requestReconnection(request);

                    // successfully told node to reconnect -- we're done!
                    logger.info("Successfully requested that {} join the cluster", request.getNodeId());

                    return;
                } catch (final Exception e) {
                    logger.warn("Problem encountered issuing reconnection request to node {}", request.getNodeId(), e);
                    eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, "Problem encountered issuing reconnection request to node "
                            + request.getNodeId() + " due to: " + e);
                }

                try {
                    Thread.sleep(1000L * retrySeconds);
                } catch (final InterruptedException ie) {
                    break;
                }
            }

            // We failed to reconnect too many times. We must now mark node as disconnected.
            if (NodeConnectionState.CONNECTING == getConnectionState(request.getNodeId())) {
                requestNodeDisconnect(request.getNodeId(), DisconnectionCode.UNABLE_TO_COMMUNICATE,
                        "Attempted to request that node reconnect to cluster but could not communicate with node");
            }
        }, "Reconnect " + request.getNodeId());

        reconnectionThread.start();
    }

    @Override
    public ProtocolMessage handle(final ProtocolMessage protocolMessage, final Set<String> nodeIdentities) throws ProtocolException {
        return switch (protocolMessage.getType()) {
            case CONNECTION_REQUEST ->
                    handleConnectionRequest((ConnectionRequestMessage) protocolMessage, nodeIdentities);
            case NODE_STATUS_CHANGE -> {
                handleNodeStatusChange((NodeStatusChangeMessage) protocolMessage);
                yield null;
            }
            case NODE_CONNECTION_STATUS_REQUEST -> handleNodeConnectionStatusRequest();
            default ->
                    throw new ProtocolException("Cannot handle Protocol Message " + protocolMessage + " because it is not of the correct type");
        };
    }

    private NodeConnectionStatusResponseMessage handleNodeConnectionStatusRequest() {
        final NodeConnectionStatusResponseMessage msg = new NodeConnectionStatusResponseMessage();
        final NodeIdentifier self = getLocalNodeIdentifier();
        if (self != null) {
            final NodeConnectionStatus connectionStatus = nodeStatuses.get(self.getId());
            msg.setNodeConnectionStatus(connectionStatus);
        }

        return msg;
    }

    private String summarizeStatusChange(final NodeConnectionStatus oldStatus, final NodeConnectionStatus status) {
        final StringBuilder sb = new StringBuilder();

        if (oldStatus == null || status.getState() != oldStatus.getState()) {
            sb.append("Node Status changed from ").append(oldStatus == null ? "[Unknown Node]" : oldStatus.getState().toString()).append(" to ").append(status.getState().toString());
            if (status.getReason() != null) {
                sb.append(" due to ").append(status.getReason());
            } else if (status.getDisconnectCode() != null) {
                sb.append(" due to ").append(status.getDisconnectCode().toString());
            }
        }

        return sb.toString();
    }

    private void handleNodeStatusChange(final NodeStatusChangeMessage statusChangeMessage) {
        final NodeConnectionStatus updatedStatus = statusChangeMessage.getNodeConnectionStatus();
        final NodeIdentifier nodeId = statusChangeMessage.getNodeId();
        logger.debug("Handling request {}", statusChangeMessage);

        final NodeConnectionStatus oldStatus = nodeStatuses.get(statusChangeMessage.getNodeId().getId());

        if (oldStatus == null && updatedStatus.getState() == NodeConnectionState.DISCONNECTED ) {
            // There is no need to tell that node is getting disconnected if there was no status earlier.
            return;
        }

        // Either remove the value from the map or update the map depending on the connection state
        if (statusChangeMessage.getNodeConnectionStatus().getState() == NodeConnectionState.REMOVED) {
            if (removeNodeConditionally(nodeId, oldStatus)) {
                storeState();
                logger.info("Status of {} changed from {} to {}", statusChangeMessage.getNodeId(), oldStatus, updatedStatus);
            }
        } else {
            updateNodeStatus(nodeId, updatedStatus);
        }

        logger.debug("State of cluster nodes is now {}", nodeStatuses);

        final NodeConnectionStatus status = statusChangeMessage.getNodeConnectionStatus();
        final String summary = summarizeStatusChange(oldStatus, status);
        if (!StringUtils.isEmpty(summary)) {
            addNodeEvent(nodeId, summary);
        }

        // Update our counter so that we are in-sync with the cluster on the
        // most up-to-date version of the NodeConnectionStatus' Update Identifier.
        // We do this so that we can accurately compare status updates that are generated
        // locally against those generated from other nodes in the cluster.
        NodeConnectionStatus.updateIdGenerator(updatedStatus.getUpdateIdentifier());

        if (isActiveClusterCoordinator()) {
            notifyOthersOfNodeStatusChange(statusChangeMessage.getNodeConnectionStatus());
        }
    }

    @Override
    public String getFlowElectionStatus() {
        if (!requireElection) {
            return null;
        }

        return flowElection.getStatusDescription();
    }

    @Override
    public boolean isFlowElectionComplete() {
        return !requireElection || flowElection.isElectionComplete();
    }

    private void registerNodeId(final NodeIdentifier nodeIdentifier) {
        final NodeConnectionStatus proposedConnectionStatus = new NodeConnectionStatus(nodeIdentifier, DisconnectionCode.NOT_YET_CONNECTED);
        final NodeConnectionStatus existingStatus = nodeStatuses.putIfAbsent(nodeIdentifier.getId(), proposedConnectionStatus);

        removeConflictingNodeIds(nodeIdentifier);

        if (existingStatus == null) {
            // there is no node with that ID
            logger.info("No existing node with ID {}; will add Node as {}", nodeIdentifier.getId(), nodeIdentifier.getFullDescription());
            logger.debug("After adding node {}, node statuses are {}", nodeIdentifier, nodeStatuses);

            onNodeAdded(nodeIdentifier, true);
        } else {
            // there is a node with that ID but it's the same node.
            logger.debug("A node already exists with ID {}; existing Node Identifier is: {}", nodeIdentifier.getId(), existingStatus.getNodeIdentifier().getFullDescription());
        }
    }

    private void removeConflictingNodeIds(final NodeIdentifier nodeIdentifier) {
        final Set<NodeIdentifier> conflictingNodeIds = findConflictingNodeIds(nodeIdentifier);
        if (!conflictingNodeIds.isEmpty()) {
            final Set<String> fullNodeIdDescriptions = conflictingNodeIds.stream()
                .map(NodeIdentifier::getFullDescription)
                .collect(Collectors.toSet());

            logger.warn("New Node {} was registered for this cluster, but this Node Identifier conflicts with {} others: {}; " +
                "each of these conflicting Node Identifiers will be removed from the cluster",
                nodeIdentifier.getFullDescription(), fullNodeIdDescriptions.size(), fullNodeIdDescriptions);

            conflictingNodeIds.forEach(uuid -> removeNode(uuid));
        }
    }

    private Set<NodeIdentifier> findConflictingNodeIds(final NodeIdentifier nodeId) {
        return nodeStatuses.values().stream()
            .map(NodeConnectionStatus::getNodeIdentifier)
            .filter(potential -> !potential.equals(nodeId))
            .filter(nodeId::logicallyEquals)
            .collect(Collectors.toSet());
    }

    private ConnectionResponseMessage handleConnectionRequest(final ConnectionRequestMessage requestMessage, final Set<String> nodeIdentities) {
        final NodeIdentifier nodeIdentifier = requestMessage.getConnectionRequest().getProposedNodeIdentifier();
        final NodeIdentifier withNodeIdentities = addNodeIdentities(nodeIdentifier, nodeIdentities);
        final DataFlow dataFlow = requestMessage.getConnectionRequest().getDataFlow();
        final ConnectionRequest requestWithNodeIdentities = new ConnectionRequest(withNodeIdentities, dataFlow);

        // Resolve Node identifier.
        registerNodeId(nodeIdentifier);

        if (isBlockedByFirewall(nodeIdentities)) {
            // if the socket address is not listed in the firewall, then return a null response
            logger.info("Firewall blocked connection request from node {} with Node Identities {}", nodeIdentifier, nodeIdentities);
            final ConnectionResponse response = ConnectionResponse.createBlockedByFirewallResponse();
            final ConnectionResponseMessage responseMessage = new ConnectionResponseMessage();
            responseMessage.setConnectionResponse(response);
            return responseMessage;
        }

        if (requireElection) {
            final DataFlow electedDataFlow = flowElection.castVote(dataFlow, withNodeIdentities);
            if (electedDataFlow == null) {
                logger.info("Received Connection Request from {}; responding with Flow Election In Progress message", withNodeIdentities);
                return createFlowElectionInProgressResponse();
            } else {
                logger.info("Received Connection Request from {}; responding with DataFlow that was elected", withNodeIdentities);
                return createConnectionResponse(requestWithNodeIdentities, nodeIdentifier, electedDataFlow);
            }
        }

        logger.info("Received Connection Request from {}; responding with my DataFlow", withNodeIdentities);
        return createConnectionResponse(requestWithNodeIdentities, nodeIdentifier);
    }

    private ConnectionResponseMessage createFlowElectionInProgressResponse() {
        final ConnectionResponseMessage responseMessage = new ConnectionResponseMessage();
        final String statusDescription = flowElection.getStatusDescription();
        responseMessage.setConnectionResponse(new ConnectionResponse(5, "Cluster is still voting on which Flow is the correct flow for the cluster. " + statusDescription));
        return responseMessage;
    }

    private ConnectionResponseMessage createConnectionResponse(final ConnectionRequest request, final NodeIdentifier resolvedNodeIdentifier) {
        DataFlow dataFlow = null;
        if (flowService != null) {
            try {
                dataFlow = flowService.createDataFlowFromController();
            } catch (final IOException ioe) {
                logger.error("Unable to obtain current dataflow from FlowService in order to provide the flow to "
                    + resolvedNodeIdentifier + ". Will tell node to try again later", ioe);
            }
        }

        return createConnectionResponse(request, resolvedNodeIdentifier, dataFlow);
    }


    private ConnectionResponseMessage createConnectionResponse(final ConnectionRequest request, final NodeIdentifier resolvedNodeIdentifier, final DataFlow clusterDataFlow) {
        if (clusterDataFlow == null) {
            final ConnectionResponseMessage responseMessage = new ConnectionResponseMessage();
            responseMessage.setConnectionResponse(new ConnectionResponse(5, "The cluster dataflow is not yet available"));
            return responseMessage;
        }

        // Set node's status to 'CONNECTING'
        NodeConnectionStatus status = getConnectionStatus(resolvedNodeIdentifier);
        if (status == null) {
            addNodeEvent(resolvedNodeIdentifier, "Connection requested from new node. Setting status to connecting.");
        } else {
            addNodeEvent(resolvedNodeIdentifier, "Connection requested from existing node. Setting status to connecting.");
        }

        status = new NodeConnectionStatus(resolvedNodeIdentifier, NodeConnectionState.CONNECTING, null, null, null, System.currentTimeMillis());
        updateNodeStatus(status);

        final ComponentRevisionSnapshot componentRevisionSnapshot = ComponentRevisionSnapshot.fromRevisionSnapshot(revisionManager.getAllRevisions());
        final ConnectionResponse response = new ConnectionResponse(resolvedNodeIdentifier, clusterDataFlow, instanceId, getConnectionStatuses(), componentRevisionSnapshot);

        final ConnectionResponseMessage responseMessage = new ConnectionResponseMessage();
        responseMessage.setConnectionResponse(response);
        return responseMessage;
    }


    private NodeIdentifier addNodeIdentities(final NodeIdentifier nodeId, final Set<String> nodeIdentities) {
        return new NodeIdentifier(nodeId.getId(), nodeId.getApiAddress(), nodeId.getApiPort(),
                nodeId.getSocketAddress(), nodeId.getSocketPort(),
                nodeId.getLoadBalanceAddress(), nodeId.getLoadBalancePort(),
                nodeId.getSiteToSiteAddress(), nodeId.getSiteToSitePort(),
                nodeId.getSiteToSiteHttpApiPort(), nodeId.isSiteToSiteSecure(), nodeIdentities);
    }

    @Override
    public boolean canHandle(final ProtocolMessage msg) {
        return MessageType.CONNECTION_REQUEST == msg.getType() || MessageType.NODE_STATUS_CHANGE == msg.getType()
                || MessageType.NODE_CONNECTION_STATUS_REQUEST == msg.getType();
    }

    private boolean isMutableRequest(final String method) {
        return "DELETE".equalsIgnoreCase(method) || "POST".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method);
    }

    /**
     * Callback that is called after an HTTP Request has been replicated to
     * nodes in the cluster. This allows us to disconnect nodes that did not
     * complete the request, if applicable.
     */
    @Override
    public void afterRequest(final String uriPath, final String method, final Set<NodeResponse> nodeResponses) {
        // if we are not the active cluster coordinator, then we are not responsible for monitoring the responses,
        // as the cluster coordinator is responsible for performing the actual request replication.
        if (!isActiveClusterCoordinator()) {
            return;
        }

        final boolean mutableRequest = isMutableRequest(method);

        /*
         * Nodes that encountered issues handling the request are marked as
         * disconnected for mutable requests (e.g., post, put, delete). For
         * other requests (e.g., get, head), the nodes remain in their current
         * state even if they had problems handling the request.
         */
        if (mutableRequest) {
            final HttpResponseMapper responseMerger = new StandardHttpResponseMapper(nifiProperties);
            final Set<NodeResponse> problematicNodeResponses = responseMerger.getProblematicNodeResponses(nodeResponses);

            // all nodes failed
            final boolean allNodesFailed = problematicNodeResponses.size() == nodeResponses.size();

            // some nodes had a problematic response because of a missing counter, ensure the are not disconnected
            final boolean someNodesFailedMissingCounter = !problematicNodeResponses.isEmpty()
                    && problematicNodeResponses.size() < nodeResponses.size() && isMissingCounter(problematicNodeResponses, uriPath);

            // ensure nodes stay connected in certain scenarios
            if (allNodesFailed) {
                logger.warn("All nodes failed to process URI {} {}. As a result, no node will be disconnected from cluster", method, uriPath);
                return;
            }

            if (someNodesFailedMissingCounter) {
                return;
            }

            // disconnect problematic nodes
            if (!problematicNodeResponses.isEmpty() && problematicNodeResponses.size() < nodeResponses.size()) {
                final Set<NodeIdentifier> failedNodeIds = problematicNodeResponses.stream().map(NodeResponse::getNodeId).collect(Collectors.toSet());
                logger.warn("The following nodes failed to process URI {} '{}'.  Requesting each node reconnect to cluster.", uriPath, failedNodeIds);
                for (final NodeIdentifier nodeId : failedNodeIds) {
                    // Update the node to 'CONNECTING' status and request that the node connect
                    final NodeConnectionStatus reconnectionStatus = new NodeConnectionStatus(nodeId, NodeConnectionState.CONNECTING);
                    updateNodeStatus(reconnectionStatus);
                    requestNodeConnect(nodeId, null);
                }
            }
        }
    }

    /**
     * Determines if all problematic responses were due to 404 NOT_FOUND.
     * Assumes that problematicNodeResponses is not empty and is not comprised
     * of responses from all nodes in the cluster (at least one node contained
     * the counter in question).
     *
     * @param problematicNodeResponses The problematic node responses
     * @param uriPath The path of the URI for the request
     * @return Whether all problematic node responses were due to a missing
     * counter
     */
    private boolean isMissingCounter(final Set<NodeResponse> problematicNodeResponses, final String uriPath) {
        if (COUNTER_URI_PATTERN.matcher(uriPath).matches()) {
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

    @Override
    public void setConnected(final boolean connected) {
        this.connected = connected;

        // Once we have connected to the cluster, election is no longer required.
        // It is required only upon startup so that if multiple nodes are started up
        // at the same time, and they have different flows, that we don't choose the
        // wrong flow as the 'golden copy' by electing that node as the elected
        // active Cluster Coordinator.
        if (connected) {
            logger.info("This node is now connected to the cluster. Will no longer require election of DataFlow.");
            requireElection = false;
        }
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public Map<NodeIdentifier, NodeWorkload> getClusterWorkload() {
        final ClusterWorkloadRequestMessage request = new ClusterWorkloadRequestMessage();
        final ClusterWorkloadResponseMessage response = nodeProtocolSender.clusterWorkload(request);
        return response.getNodeWorkloads();
    }
}
