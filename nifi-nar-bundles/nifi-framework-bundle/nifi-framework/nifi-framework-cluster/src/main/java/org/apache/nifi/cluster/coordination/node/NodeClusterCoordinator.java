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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.HttpResponseMerger;
import org.apache.nifi.cluster.coordination.http.StandardHttpResponseMerger;
import org.apache.nifi.cluster.coordination.http.replication.RequestCompletionCallback;
import org.apache.nifi.cluster.event.Event;
import org.apache.nifi.cluster.event.NodeEvent;
import org.apache.nifi.cluster.firewall.ClusterNodeFirewall;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.IllegalNodeDisconnectionException;
import org.apache.nifi.cluster.manager.exception.NoClusterCoordinatorException;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.protocol.ComponentRevision;
import org.apache.nifi.cluster.protocol.ConnectionRequest;
import org.apache.nifi.cluster.protocol.ConnectionResponse;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.cluster.protocol.impl.ClusterCoordinationProtocolSenderListener;
import org.apache.nifi.cluster.protocol.message.ConnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ConnectionResponseMessage;
import org.apache.nifi.cluster.protocol.message.DisconnectMessage;
import org.apache.nifi.cluster.protocol.message.NodeConnectionStatusResponseMessage;
import org.apache.nifi.cluster.protocol.message.NodeStatusChangeMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage.MessageType;
import org.apache.nifi.cluster.protocol.message.ReconnectionRequestMessage;
import org.apache.nifi.controller.cluster.ZooKeeperClientConfig;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.web.revision.RevisionManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeClusterCoordinator implements ClusterCoordinator, ProtocolHandler, RequestCompletionCallback {
    private static final Logger logger = LoggerFactory.getLogger(NodeClusterCoordinator.class);
    private static final String EVENT_CATEGORY = "Clustering";

    private static final Pattern COUNTER_URI_PATTERN = Pattern.compile("/nifi-api/counters/[a-f0-9\\-]{36}");

    private final String instanceId = UUID.randomUUID().toString();
    private volatile NodeIdentifier nodeId;

    private final ClusterCoordinationProtocolSenderListener senderListener;
    private final EventReporter eventReporter;
    private final ClusterNodeFirewall firewall;
    private final RevisionManager revisionManager;

    // Curator used to determine which node is coordinator
    private final CuratorFramework curatorClient;
    private final String nodesPathPrefix;
    private final String coordinatorPath;

    private volatile FlowService flowService;
    private volatile boolean connected;
    private volatile String coordinatorAddress;
    private volatile boolean closed = false;

    private final ConcurrentMap<NodeIdentifier, NodeConnectionStatus> nodeStatuses = new ConcurrentHashMap<>();
    private final ConcurrentMap<NodeIdentifier, CircularFifoQueue<NodeEvent>> nodeEvents = new ConcurrentHashMap<>();

    public NodeClusterCoordinator(final ClusterCoordinationProtocolSenderListener senderListener, final EventReporter eventReporter,
        final ClusterNodeFirewall firewall, final RevisionManager revisionManager, final Properties nifiProperties) {
        this.senderListener = senderListener;
        this.flowService = null;
        this.eventReporter = eventReporter;
        this.firewall = firewall;
        this.revisionManager = revisionManager;

        final RetryPolicy retryPolicy = new RetryNTimes(10, 500);
        final ZooKeeperClientConfig zkConfig = ZooKeeperClientConfig.createConfig(nifiProperties);

        curatorClient = CuratorFrameworkFactory.newClient(zkConfig.getConnectString(),
            zkConfig.getSessionTimeoutMillis(), zkConfig.getConnectionTimeoutMillis(), retryPolicy);

        curatorClient.start();
        nodesPathPrefix = zkConfig.resolvePath("cluster/nodes");
        coordinatorPath = nodesPathPrefix + "/coordinator";

        senderListener.addHandler(this);
    }

    @Override
    public void shutdown() {
        if (closed) {
            return;
        }

        closed = true;

        final NodeConnectionStatus shutdownStatus = new NodeConnectionStatus(getLocalNodeIdentifier(), DisconnectionCode.NODE_SHUTDOWN);
        updateNodeStatus(shutdownStatus, false);
        logger.info("Successfully notified other nodes that I am shutting down");

        curatorClient.close();
    }

    @Override
    public void setLocalNodeIdentifier(final NodeIdentifier nodeId) {
        this.nodeId = nodeId;
        nodeStatuses.computeIfAbsent(nodeId, id -> new NodeConnectionStatus(id, DisconnectionCode.NOT_YET_CONNECTED));
    }

    @Override
    public NodeIdentifier getLocalNodeIdentifier() {
        return nodeId;
    }

    private NodeIdentifier waitForLocalNodeIdentifier() {
        return waitForNodeIdentifier(() -> getLocalNodeIdentifier());
    }

    private NodeIdentifier waitForElectedClusterCoordinator() {
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
                }
            }
        }

        return localNodeId;
    }

    private String getElectedActiveCoordinatorAddress() throws IOException {
        final String curAddress = coordinatorAddress;
        if (curAddress != null) {
            return curAddress;
        }

        try {
            // Get coordinator address and add watcher to change who we are heartbeating to if the value changes.
            final byte[] coordinatorAddressBytes = curatorClient.getData().usingWatcher(new Watcher() {
                @Override
                public void process(final WatchedEvent event) {
                    coordinatorAddress = null;
                }
            }).forPath(coordinatorPath);
            final String address = coordinatorAddress = new String(coordinatorAddressBytes, StandardCharsets.UTF_8);

            logger.info("Determined that Cluster Coordinator is located at {}", address);
            return address;
        } catch (final KeeperException.NoNodeException nne) {
            throw new NoClusterCoordinatorException();
        } catch (Exception e) {
            throw new IOException("Unable to determine Cluster Coordinator from ZooKeeper", e);
        }
    }

    @Override
    public void resetNodeStatuses(final Map<NodeIdentifier, NodeConnectionStatus> statusMap) {
        logger.info("Resetting cluster node statuses from {} to {}", nodeStatuses, statusMap);
        coordinatorAddress = null;

        // For each proposed replacement, update the nodeStatuses map if and only if the replacement
        // has a larger update id than the current value.
        for (final Map.Entry<NodeIdentifier, NodeConnectionStatus> entry : statusMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final NodeConnectionStatus proposedStatus = entry.getValue();

            boolean updated = false;
            while (!updated) {
                final NodeConnectionStatus currentStatus = nodeStatuses.get(nodeId);
                updated = replaceNodeStatus(nodeId, currentStatus, proposedStatus);
            }
        }
    }

    /**
     * Attempts to update the nodeStatuses map by changing the value for the given node id from the current status to the new status, as in
     * ConcurrentMap.replace(nodeId, currentStatus, newStatus) but with the difference that this method can handle a <code>null</code> value
     * for currentStatus
     *
     * @param nodeId the node id
     * @param currentStatus the current status, or <code>null</code> if there is no value currently
     * @param newStatus the new status to set
     * @return <code>true</code> if the map was updated, false otherwise
     */
    private boolean replaceNodeStatus(final NodeIdentifier nodeId, final NodeConnectionStatus currentStatus, final NodeConnectionStatus newStatus) {
        if (newStatus == null) {
            logger.error("Cannot change node status for {} from {} to {} because new status is null", nodeId, currentStatus, newStatus);
            logger.error("", new NullPointerException());
        }

        if (currentStatus == null) {
            final NodeConnectionStatus existingValue = nodeStatuses.putIfAbsent(nodeId, newStatus);
            return existingValue == null;
        }

        return nodeStatuses.replace(nodeId, currentStatus, newStatus);
    }

    @Override
    public void requestNodeConnect(final NodeIdentifier nodeId, final String userDn) {
        if (userDn == null) {
            reportEvent(nodeId, Severity.INFO, "Requesting that node connect to cluster");
        } else {
            reportEvent(nodeId, Severity.INFO, "Requesting that node connect to cluster on behalf of " + userDn);
        }

        updateNodeStatus(new NodeConnectionStatus(nodeId, NodeConnectionState.CONNECTING, null, null, System.currentTimeMillis(), getRoles(nodeId)));

        // create the request
        final ReconnectionRequestMessage request = new ReconnectionRequestMessage();
        request.setNodeId(nodeId);
        request.setInstanceId(instanceId);

        requestReconnectionAsynchronously(request, 10, 5);
    }

    private Set<String> getRoles(final NodeIdentifier nodeId) {
        final NodeConnectionStatus status = getConnectionStatus(nodeId);
        return status == null ? Collections.emptySet() : status.getRoles();
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
        updateNodeStatus(new NodeConnectionStatus(nodeId, NodeConnectionState.CONNECTED, getRoles(nodeId)));
    }


    @Override
    public void requestNodeDisconnect(final NodeIdentifier nodeId, final DisconnectionCode disconnectionCode, final String explanation) {
        final Set<NodeIdentifier> connectedNodeIds = getNodeIdentifiers(NodeConnectionState.CONNECTED);
        if (connectedNodeIds.size() == 1 && connectedNodeIds.contains(nodeId)) {
            throw new IllegalNodeDisconnectionException("Cannot disconnect node " + nodeId + " because it is the only node currently connected");
        }

        logger.info("Requesting that {} disconnect due to {}", nodeId, explanation == null ? disconnectionCode : explanation);

        updateNodeStatus(new NodeConnectionStatus(nodeId, disconnectionCode, explanation));

        // There is no need to tell the node that it's disconnected if it is due to being
        // shutdown, as we will not be able to connect to the node anyway.
        if (disconnectionCode == DisconnectionCode.NODE_SHUTDOWN) {
            return;
        }

        final DisconnectMessage request = new DisconnectMessage();
        request.setNodeId(nodeId);
        request.setExplanation(explanation);

        addNodeEvent(nodeId, "Disconnection requested due to " + explanation);
        disconnectAsynchronously(request, 10, 5);
    }

    @Override
    public void disconnectionRequestedByNode(final NodeIdentifier nodeId, final DisconnectionCode disconnectionCode, final String explanation) {
        logger.info("{} requested disconnection from cluster due to {}", nodeId, explanation == null ? disconnectionCode : explanation);
        updateNodeStatus(new NodeConnectionStatus(nodeId, disconnectionCode, explanation));

        final Severity severity;
        switch (disconnectionCode) {
            case STARTUP_FAILURE:
            case MISMATCHED_FLOWS:
            case UNKNOWN:
                severity = Severity.ERROR;
                break;
            case LACK_OF_HEARTBEAT:
                severity = Severity.WARNING;
                break;
            default:
                severity = Severity.INFO;
                break;
        }

        reportEvent(nodeId, severity, "Node disconnected from cluster due to " + explanation);
    }

    @Override
    public void removeNode(final NodeIdentifier nodeId, final String userDn) {
        reportEvent(nodeId, Severity.INFO, "User " + userDn + " requested that node be removed from cluster");
        nodeStatuses.remove(nodeId);
        nodeEvents.remove(nodeId);
        notifyOthersOfNodeStatusChange(new NodeConnectionStatus(nodeId, NodeConnectionState.REMOVED, Collections.emptySet()));
    }

    @Override
    public NodeConnectionStatus getConnectionStatus(final NodeIdentifier nodeId) {
        return nodeStatuses.get(nodeId);
    }

    private NodeConnectionState getConnectionState(final NodeIdentifier nodeId) {
        final NodeConnectionStatus status = getConnectionStatus(nodeId);
        return status == null ? null : status.getState();
    }


    @Override
    public Map<NodeConnectionState, List<NodeIdentifier>> getConnectionStates() {
        final Map<NodeConnectionState, List<NodeIdentifier>> connectionStates = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, NodeConnectionStatus> entry : nodeStatuses.entrySet()) {
            final NodeConnectionState state = entry.getValue().getState();
            final List<NodeIdentifier> nodeIds = connectionStates.computeIfAbsent(state, s -> new ArrayList<NodeIdentifier>());
            nodeIds.add(entry.getKey());
        }

        return connectionStates;
    }

    @Override
    public boolean isBlockedByFirewall(final String hostname) {
        return firewall != null && !firewall.isPermissible(hostname);
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
    public synchronized void updateNodeRoles(final NodeIdentifier nodeId, final Set<String> roles) {
        boolean updated = false;
        while (!updated) {
            final NodeConnectionStatus currentStatus = nodeStatuses.get(nodeId);
            if (currentStatus == null) {
                throw new UnknownNodeException("Cannot update roles for " + nodeId + " to " + roles + " because the node is not part of this cluster");
            }

            if (currentStatus.getRoles().equals(roles)) {
                logger.debug("Roles for {} already up-to-date as {}", nodeId, roles);
                return;
            }

            final NodeConnectionStatus updatedStatus = new NodeConnectionStatus(currentStatus, roles);
            updated = replaceNodeStatus(nodeId, currentStatus, updatedStatus);

            if (updated) {
                logger.info("Updated Roles of {} from {} to {}", nodeId, currentStatus, updatedStatus);
                notifyOthersOfNodeStatusChange(updatedStatus);
            }
        }

        // If any other node contains any of the given roles, revoke the role from the other node.
        for (final String role : roles) {
            for (final Map.Entry<NodeIdentifier, NodeConnectionStatus> entry : nodeStatuses.entrySet()) {
                if (entry.getKey().equals(nodeId)) {
                    continue;
                }

                updated = false;
                while (!updated) {
                    final NodeConnectionStatus status = entry.getValue();
                    if (status.getRoles().contains(role)) {
                        final Set<String> newRoles = new HashSet<>(status.getRoles());
                        newRoles.remove(role);

                        final NodeConnectionStatus updatedStatus = new NodeConnectionStatus(status, newRoles);
                        updated = replaceNodeStatus(entry.getKey(), status, updatedStatus);

                        if (updated) {
                            logger.info("Updated Roles of {} from {} to {}", nodeId, status, updatedStatus);
                            notifyOthersOfNodeStatusChange(updatedStatus);
                        }
                    } else {
                        updated = true;
                    }
                }
            }
        }
    }

    @Override
    public NodeIdentifier getNodeIdentifier(final String uuid) {
        for (final NodeIdentifier nodeId : nodeStatuses.keySet()) {
            if (nodeId.getId().equals(uuid)) {
                return nodeId;
            }
        }

        return null;
    }

    // method is synchronized because it modifies local node state and then broadcasts the change. We synchronize any time that this
    // is done so that we don't have an issue where we create a NodeConnectionStatus, then another thread creates one and sends it
    // before the first one is sent (as this results in the first status having a larger id, which means that the first status is never
    // seen by other nodes).
    @Override
    public synchronized void addRole(final String clusterRole) {
        final NodeIdentifier localNodeId = waitForLocalNodeIdentifier();
        final NodeConnectionStatus status = getConnectionStatus(localNodeId);
        final Set<String> roles = new HashSet<>();
        if (status != null) {
            roles.addAll(status.getRoles());
        }

        final boolean roleAdded = roles.add(clusterRole);

        if (roleAdded) {
            updateNodeRoles(localNodeId, roles);
            logger.info("Cluster role {} added. This node is now responsible for the following roles: {}", clusterRole, roles);
        }
    }

    // method is synchronized because it modifies local node state and then broadcasts the change. We synchronize any time that this
    // is done so that we don't have an issue where we create a NodeConnectionStatus, then another thread creates one and sends it
    // before the first one is sent (as this results in the first status having a larger id, which means that the first status is never
    // seen by other nodes).
    @Override
    public synchronized void removeRole(final String clusterRole) {
        final NodeIdentifier localNodeId = waitForLocalNodeIdentifier();
        final NodeConnectionStatus status = getConnectionStatus(localNodeId);
        final Set<String> roles = new HashSet<>();
        if (status != null) {
            roles.addAll(status.getRoles());
        }

        final boolean roleRemoved = roles.remove(clusterRole);

        if (roleRemoved) {
            updateNodeRoles(localNodeId, roles);
            logger.info("Cluster role {} removed. This node is now responsible for the following roles: {}", clusterRole, roles);
        }
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
            .map(entry -> entry.getKey())
            .collect(Collectors.toSet());
    }

    @Override
    public NodeIdentifier getPrimaryNode() {
        return nodeStatuses.values().stream()
            .filter(status -> status.getRoles().contains(ClusterRoles.PRIMARY_NODE))
            .findFirst()
            .map(status -> status.getNodeIdentifier())
            .orElse(null);
    }

    @Override
    public NodeIdentifier getElectedActiveCoordinatorNode() {
        return getElectedActiveCoordinatorNode(true);
    }

    private NodeIdentifier getElectedActiveCoordinatorNode(final boolean warnOnError) {
        final String electedNodeAddress;
        try {
            electedNodeAddress = getElectedActiveCoordinatorAddress();
        } catch (final NoClusterCoordinatorException ncce) {
            logger.debug("There is currently no elected active Cluster Coordinator");
            return null;
        } catch (final IOException ioe) {
            if (warnOnError) {
                logger.warn("Failed to determine which node is elected active Cluster Coordinator. There may be no coordinator currently: " + ioe);
                if (logger.isDebugEnabled()) {
                    logger.warn("", ioe);
                }
            }

            return null;
        }

        final int colonLoc = electedNodeAddress.indexOf(':');
        if (colonLoc < 1) {
            if (warnOnError) {
                logger.warn("Failed to determine which node is elected active Cluster Coordinator: ZooKeeper reports the address as {}, but this is not a valid address", electedNodeAddress);
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
                logger.warn("Failed to determine which node is elected active Cluster Coordinator: ZooKeeper reports the address as {}, but this is not a valid address", electedNodeAddress);
            }

            return null;
        }

        final Set<NodeIdentifier> connectedNodeIds = getNodeIdentifiers();
        final NodeIdentifier electedNodeId = connectedNodeIds.stream()
            .filter(nodeId -> nodeId.getSocketAddress().equals(electedNodeHostname) && nodeId.getSocketPort() == electedNodePort)
            .findFirst()
            .orElse(null);

        if (electedNodeId == null && warnOnError) {
            logger.debug("Failed to determine which node is elected active Cluster Coordinator: ZooKeeper reports the address as {},"
                + "but there is no node with this address. Will attempt to communicate with node to determine its information", electedNodeAddress);

            try {
                final NodeConnectionStatus connectionStatus = senderListener.requestNodeConnectionStatus(electedNodeHostname, electedNodePort);
                logger.debug("Received NodeConnectionStatus {}", connectionStatus);

                if (connectionStatus == null) {
                    return null;
                }

                final NodeConnectionStatus existingStatus = this.nodeStatuses.putIfAbsent(connectionStatus.getNodeIdentifier(), connectionStatus);
                if (existingStatus == null) {
                    return connectionStatus.getNodeIdentifier();
                } else {
                    return existingStatus.getNodeIdentifier();
                }
            } catch (final Exception e) {
                logger.warn("Failed to determine which node is elected active Cluster Coordinator: ZooKeeper reports the address as {}, but there is no node with this address. "
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
        final NodeIdentifier self = getLocalNodeIdentifier();
        return self != null && self.equals(getElectedActiveCoordinatorNode());
    }

    @Override
    public List<NodeEvent> getNodeEvents(final NodeIdentifier nodeId) {
        final CircularFifoQueue<NodeEvent> eventQueue = nodeEvents.get(nodeId);
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
        final CircularFifoQueue<NodeEvent> eventQueue = nodeEvents.computeIfAbsent(nodeId, id -> new CircularFifoQueue<>());
        synchronized (eventQueue) {
            eventQueue.add(event);
        }
    }

    /**
     * Updates the status of the node with the given ID to the given status and returns <code>true</code>
     * if successful, <code>false</code> if no node exists with the given ID
     *
     * @param status the new status of the node
     */
    // visible for testing.
    void updateNodeStatus(final NodeConnectionStatus status) {
        updateNodeStatus(status, true);
    }

    void updateNodeStatus(final NodeConnectionStatus status, final boolean waitForCoordinator) {
        final NodeIdentifier nodeId = status.getNodeIdentifier();

        // In this case, we are using nodeStatuses.put() instead of getting the current value and
        // comparing that to the new value and using the one with the largest update id. This is because
        // this method is called when something occurs that causes this node to change the status of the
        // node in question. We only use comparisons against the current value when we receive an update
        // about a node status from a different node, since those may be received out-of-order.
        final NodeConnectionStatus currentStatus = nodeStatuses.put(nodeId, status);
        final NodeConnectionState currentState = currentStatus == null ? null : currentStatus.getState();
        logger.info("Status of {} changed from {} to {}", nodeId, currentStatus, status);
        logger.debug("State of cluster nodes is now {}", nodeStatuses);

        if (currentState == null || currentState != status.getState()) {
            // We notify all nodes of the status change if either this node is the current cluster coordinator, OR if the node was
            // the cluster coordinator and no longer is. This is done because if a user disconnects the cluster coordinator, we need
            // to broadcast to the cluster that this node is no longer the coordinator. Otherwise, all nodes but this one will still
            // believe that this node is connected to the cluster.
            final boolean notifyAllNodes = isActiveClusterCoordinator() || (currentStatus != null && currentStatus.getRoles().contains(ClusterRoles.CLUSTER_COORDINATOR));
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
     * @param notifyAllNodes if <code>true</code> will notify all nodes. If <code>false</code>, will notify only the cluster coordinator
     */
    void notifyOthersOfNodeStatusChange(final NodeConnectionStatus updatedStatus, final boolean notifyAllNodes, final boolean waitForCoordinator) {
        // If this node is the active cluster coordinator, then we are going to replicate to all nodes.
        // Otherwise, get the active coordinator (or wait for one to become active) and then notify the coordinator.
        final Set<NodeIdentifier> nodesToNotify;
        if (notifyAllNodes) {
            nodesToNotify = getNodeIdentifiers(NodeConnectionState.CONNECTED, NodeConnectionState.CONNECTING);

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

    private void disconnectAsynchronously(final DisconnectMessage request, final int attempts, final int retrySeconds) {
        final Thread disconnectThread = new Thread(new Runnable() {
            @Override
            public void run() {
                final NodeIdentifier nodeId = request.getNodeId();

                for (int i = 0; i < attempts; i++) {
                    try {
                        senderListener.disconnect(request);
                        reportEvent(nodeId, Severity.INFO, "Node disconnected due to " + request.getExplanation());
                        return;
                    } catch (final Exception e) {
                        logger.error("Failed to notify {} that it has been disconnected from the cluster due to {}", request.getNodeId(), request.getExplanation());

                        try {
                            Thread.sleep(retrySeconds * 1000L);
                        } catch (final InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
            }
        }, "Disconnect " + request.getNodeId());

        disconnectThread.start();
    }

    private void requestReconnectionAsynchronously(final ReconnectionRequestMessage request, final int reconnectionAttempts, final int retrySeconds) {
        final Thread reconnectionThread = new Thread(new Runnable() {
            @Override
            public void run() {
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

                        request.setDataFlow(new StandardDataFlow(flowService.createDataFlow()));
                        request.setNodeConnectionStatuses(new ArrayList<>(nodeStatuses.values()));
                        request.setComponentRevisions(revisionManager.getAllRevisions().stream().map(rev -> ComponentRevision.fromRevision(rev)).collect(Collectors.toList()));

                        // Issue a reconnection request to the node.
                        senderListener.requestReconnection(request);

                        // successfully told node to reconnect -- we're done!
                        logger.info("Successfully requested that {} join the cluster", request.getNodeId());

                        return;
                    } catch (final Exception e) {
                        logger.warn("Problem encountered issuing reconnection request to node " + request.getNodeId(), e);
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
            }
        }, "Reconnect " + request.getNodeId());

        reconnectionThread.start();
    }

    @Override
    public ProtocolMessage handle(final ProtocolMessage protocolMessage) throws ProtocolException {
        switch (protocolMessage.getType()) {
            case CONNECTION_REQUEST:
                return handleConnectionRequest((ConnectionRequestMessage) protocolMessage);
            case NODE_STATUS_CHANGE:
                handleNodeStatusChange((NodeStatusChangeMessage) protocolMessage);
                return null;
            case NODE_CONNECTION_STATUS_REQUEST:
                return handleNodeConnectionStatusRequest();
            default:
                throw new ProtocolException("Cannot handle Protocol Message " + protocolMessage + " because it is not of the correct type");
        }
    }

    private NodeConnectionStatusResponseMessage handleNodeConnectionStatusRequest() {
        final NodeConnectionStatus connectionStatus = nodeStatuses.get(getLocalNodeIdentifier());
        final NodeConnectionStatusResponseMessage msg = new NodeConnectionStatusResponseMessage();
        msg.setNodeConnectionStatus(connectionStatus);
        return msg;
    }

    private String summarizeStatusChange(final NodeConnectionStatus oldStatus, final NodeConnectionStatus status) {
        final StringBuilder sb = new StringBuilder();

        if (oldStatus != null && status.getState() == oldStatus.getState()) {
            // Check if roles changed
            final Set<String> oldRoles = oldStatus.getRoles();
            final Set<String> newRoles = status.getRoles();

            final Set<String> rolesRemoved = new HashSet<>(oldRoles);
            rolesRemoved.removeAll(newRoles);

            final Set<String> rolesAdded = new HashSet<>(newRoles);
            rolesAdded.removeAll(oldRoles);

            if (!rolesRemoved.isEmpty()) {
                sb.append("Relinquished role");
                if (rolesRemoved.size() != 1) {
                    sb.append("s");
                }

                sb.append(" ").append(rolesRemoved);
            }

            if (!rolesAdded.isEmpty()) {
                if (sb.length() > 0) {
                    sb.append("; ");
                }

                sb.append("Acquired role");
                if (rolesAdded.size() != 1) {
                    sb.append("s");
                }

                sb.append(" ").append(rolesAdded);
            }
        } else {
            sb.append("Node Status changed from ").append(oldStatus == null ? "[Unknown Node]" : oldStatus.getState().toString()).append(" to ").append(status.getState().toString());
            if (status.getState() == NodeConnectionState.CONNECTED) {
                sb.append(" (Roles=").append(status.getRoles().toString()).append(")");
            } else if (status.getDisconnectReason() != null) {
                sb.append(" due to ").append(status.getDisconnectReason());
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

        boolean updated = false;
        while (!updated) {
            final NodeConnectionStatus oldStatus = nodeStatuses.get(statusChangeMessage.getNodeId());

            // Either remove the value from the map or update the map depending on the connection state
            if (statusChangeMessage.getNodeConnectionStatus().getState() == NodeConnectionState.REMOVED) {
                updated = nodeStatuses.remove(nodeId, oldStatus);
            } else {
                updated = replaceNodeStatus(nodeId, oldStatus, updatedStatus);
            }

            if (updated) {
                logger.info("Status of {} changed from {} to {}", statusChangeMessage.getNodeId(), oldStatus, updatedStatus);
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
            }
        }

        if (isActiveClusterCoordinator()) {
            notifyOthersOfNodeStatusChange(statusChangeMessage.getNodeConnectionStatus());
        }
    }

    private NodeIdentifier resolveNodeId(final NodeIdentifier proposedIdentifier) {
        final NodeConnectionStatus proposedConnectionStatus = new NodeConnectionStatus(proposedIdentifier, DisconnectionCode.NOT_YET_CONNECTED);
        final NodeConnectionStatus existingStatus = nodeStatuses.putIfAbsent(proposedIdentifier, proposedConnectionStatus);

        NodeIdentifier resolvedNodeId = proposedIdentifier;
        if (existingStatus == null) {
            // there is no node with that ID
            resolvedNodeId = proposedIdentifier;
            logger.debug("No existing node with ID {}; resolved node ID is as-proposed", proposedIdentifier.getId());
        } else if (existingStatus.getNodeIdentifier().logicallyEquals(proposedIdentifier)) {
            // there is a node with that ID but it's the same node.
            resolvedNodeId = proposedIdentifier;
            logger.debug("No existing node with ID {}; resolved node ID is as-proposed", proposedIdentifier.getId());
        } else {
            // there is a node with that ID and it's a different node
            resolvedNodeId = new NodeIdentifier(UUID.randomUUID().toString(), proposedIdentifier.getApiAddress(), proposedIdentifier.getApiPort(),
                proposedIdentifier.getSocketAddress(), proposedIdentifier.getSocketPort(), proposedIdentifier.getSiteToSiteAddress(),
                proposedIdentifier.getSiteToSitePort(), proposedIdentifier.getSiteToSiteHttpApiPort(), proposedIdentifier.isSiteToSiteSecure());
            logger.debug("A node already exists with ID {}. Proposed Node Identifier was {}; existing Node Identifier is {}; Resolved Node Identifier is {}",
                proposedIdentifier.getId(), proposedIdentifier, getNodeIdentifier(proposedIdentifier.getId()), resolvedNodeId);
        }

        return resolvedNodeId;
    }

    private ConnectionResponseMessage handleConnectionRequest(final ConnectionRequestMessage requestMessage) {
        final NodeIdentifier proposedIdentifier = requestMessage.getConnectionRequest().getProposedNodeIdentifier();
        final ConnectionRequest requestWithDn = new ConnectionRequest(addRequestorDn(proposedIdentifier, requestMessage.getRequestorDN()));

        // Resolve Node identifier.
        final NodeIdentifier resolvedNodeId = resolveNodeId(proposedIdentifier);
        final ConnectionResponse response = createConnectionResponse(requestWithDn, resolvedNodeId);
        final ConnectionResponseMessage responseMessage = new ConnectionResponseMessage();
        responseMessage.setConnectionResponse(response);
        return responseMessage;
    }

    private ConnectionResponse createConnectionResponse(final ConnectionRequest request, final NodeIdentifier resolvedNodeIdentifier) {
        if (isBlockedByFirewall(resolvedNodeIdentifier.getSocketAddress())) {
            // if the socket address is not listed in the firewall, then return a null response
            logger.info("Firewall blocked connection request from node " + resolvedNodeIdentifier);
            return ConnectionResponse.createBlockedByFirewallResponse();
        }

        // Set node's status to 'CONNECTING'
        NodeConnectionStatus status = getConnectionStatus(resolvedNodeIdentifier);
        if (status == null) {
            addNodeEvent(resolvedNodeIdentifier, "Connection requested from new node.  Setting status to connecting.");
        } else {
            addNodeEvent(resolvedNodeIdentifier, "Connection requested from existing node.  Setting status to connecting");
        }

        status = new NodeConnectionStatus(resolvedNodeIdentifier, NodeConnectionState.CONNECTING, null, null, System.currentTimeMillis(), getRoles(resolvedNodeIdentifier));
        updateNodeStatus(status);

        DataFlow dataFlow = null;
        if (flowService != null) {
            try {
                dataFlow = flowService.createDataFlow();
            } catch (final IOException ioe) {
                logger.error("Unable to obtain current dataflow from FlowService in order to provide the flow to "
                    + resolvedNodeIdentifier + ". Will tell node to try again later", ioe);
            }
        }

        if (dataFlow == null) {
            // Create try-later response based on flow retrieval delay to give
            // the flow management service a chance to retrieve a current flow
            final int tryAgainSeconds = 5;
            addNodeEvent(resolvedNodeIdentifier, Severity.WARNING, "Connection requested from node, but manager was unable to obtain current flow. "
                + "Instructing node to try again in " + tryAgainSeconds + " seconds.");

            // return try later response
            return new ConnectionResponse(tryAgainSeconds);
        }

        return new ConnectionResponse(resolvedNodeIdentifier, dataFlow, instanceId, new ArrayList<>(nodeStatuses.values()),
            revisionManager.getAllRevisions().stream().map(rev -> ComponentRevision.fromRevision(rev)).collect(Collectors.toList()));
    }

    private NodeIdentifier addRequestorDn(final NodeIdentifier nodeId, final String dn) {
        return new NodeIdentifier(nodeId.getId(), nodeId.getApiAddress(), nodeId.getApiPort(),
            nodeId.getSocketAddress(), nodeId.getSocketPort(),
            nodeId.getSiteToSiteAddress(), nodeId.getSiteToSitePort(),
            nodeId.getSiteToSiteHttpApiPort(), nodeId.isSiteToSiteSecure(), dn);
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
     * Callback that is called after an HTTP Request has been replicated to nodes in the cluster.
     * This allows us to disconnect nodes that did not complete the request, if applicable.
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
            final HttpResponseMerger responseMerger = new StandardHttpResponseMerger();
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
                final Set<NodeIdentifier> failedNodeIds = problematicNodeResponses.stream().map(response -> response.getNodeId()).collect(Collectors.toSet());
                logger.warn(String.format("The following nodes failed to process URI %s '%s'.  Requesting each node disconnect from cluster.", uriPath, failedNodeIds));
                for (final NodeIdentifier nodeId : failedNodeIds) {
                    requestNodeDisconnect(nodeId, DisconnectionCode.FAILED_TO_SERVICE_REQUEST, "Failed to process request " + method + " " + uriPath);
                }
            }
        }
    }

    /**
     * Determines if all problematic responses were due to 404 NOT_FOUND. Assumes that problematicNodeResponses is not empty and is not comprised of responses from all nodes in the cluster (at least
     * one node contained the counter in question).
     *
     * @param problematicNodeResponses The problematic node responses
     * @param uriPath The path of the URI for the request
     * @return Whether all problematic node responses were due to a missing counter
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
        this.coordinatorAddress = null; // if connection state changed, we are not sure about the coordinator. Check for address again.
    }

    @Override
    public boolean isConnected() {
        return connected;
    }
}
