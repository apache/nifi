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

package org.apache.nifi.cluster.manager.impl;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.node.Node.Status;
import org.apache.nifi.cluster.protocol.ClusterManagerProtocolSender;
import org.apache.nifi.cluster.protocol.ConnectionRequest;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.message.NodeStatusChangeMessage;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebClusterManagerCoordinator implements ClusterCoordinator {
    private static final Logger logger = LoggerFactory.getLogger(WebClusterManagerCoordinator.class);
    private static final AtomicLong nodeStatusIdGenerator = new AtomicLong(0L);

    private final WebClusterManager clusterManager;
    private final ClusterManagerProtocolSender protocolSender;

    public WebClusterManagerCoordinator(final WebClusterManager clusterManager, final ClusterManagerProtocolSender protocolSender) {
        this.clusterManager = clusterManager;
        this.protocolSender = protocolSender;
    }

    @Override
    public void requestNodeConnect(final NodeIdentifier nodeId) {
        final Node node = clusterManager.getRawNode(nodeId.getId());

        if (node == null) {
            final ConnectionRequest connectionRequest = new ConnectionRequest(nodeId);
            clusterManager.requestConnection(connectionRequest);
        } else {
            updateNodeStatus(nodeId, new NodeConnectionStatus(DisconnectionCode.NOT_YET_CONNECTED, "Requesting that Node Connect to the Cluster"));
            clusterManager.requestReconnection(nodeId.getId(), "Anonymous");
        }
    }

    @Override
    public void finishNodeConnection(final NodeIdentifier nodeId) {
        final boolean updated = updateNodeStatus(nodeId, new NodeConnectionStatus(NodeConnectionState.CONNECTED));
        if (!updated) {
            logger.error("Attempting to Finish Node Connection but could not find Node with Identifier {}", nodeId);
        }
    }

    @Override
    public void requestNodeDisconnect(final NodeIdentifier nodeId, final DisconnectionCode disconnectionCode, final String explanation) {
        try {
            clusterManager.requestDisconnection(nodeId, false, explanation);

            if (disconnectionCode == DisconnectionCode.LACK_OF_HEARTBEAT) {
                final Node node = clusterManager.getRawNode(nodeId.getId());
                if (node != null) {
                    updateNodeStatus(node, Status.DISCONNECTED, true);
                }
            }
        } catch (final Exception e) {
            logger.error("Failed to request node {} disconnect from cluster due to {}", nodeId, e);
            logger.error("", e);
        }
    }

    @Override
    public void disconnectionRequestedByNode(final NodeIdentifier nodeId, final DisconnectionCode disconnectionCode, final String explanation) {
        updateNodeStatus(nodeId, new NodeConnectionStatus(disconnectionCode, explanation));

        final Severity severity;
        switch (disconnectionCode) {
            case STARTUP_FAILURE:
            case MISMATCHED_FLOWS:
            case UNKNOWN:
                severity = Severity.ERROR;
                break;
            default:
                severity = Severity.INFO;
                break;
        }

        reportEvent(nodeId, severity, "Node disconnected from cluster due to " + explanation);
    }

    @Override
    public NodeConnectionStatus getConnectionStatus(final NodeIdentifier nodeId) {
        final Node node = clusterManager.getNode(nodeId.getId());
        if (node == null) {
            return null;
        }

        final Status status = node.getStatus();
        final NodeConnectionState connectionState = NodeConnectionState.valueOf(status.name());
        return new NodeConnectionStatus(connectionState, node.getConnectionRequestedTimestamp());
    }

    @Override
    public Set<NodeIdentifier> getNodeIdentifiers(final NodeConnectionState state) {
        final Status status = Status.valueOf(state.name());
        final Set<Node> nodes = clusterManager.getNodes(status);
        return nodes.stream()
            .map(node -> node.getNodeId())
            .collect(Collectors.toSet());
    }

    @Override
    public boolean isBlockedByFirewall(final String hostname) {
        return clusterManager.isBlockedByFirewall(hostname);
    }

    @Override
    public void reportEvent(final NodeIdentifier nodeId, final Severity severity, final String event) {
        final String messagePrefix = nodeId == null ? "" : nodeId.getApiAddress() + ":" + nodeId.getApiPort() + " -- ";
        switch (severity) {
            case INFO:
                logger.info(messagePrefix + event);
                break;
            case WARNING:
                logger.warn(messagePrefix + event);
                break;
            case ERROR:
                logger.error(messagePrefix + event);
                break;
        }

        clusterManager.reportEvent(nodeId, severity, messagePrefix + event);
    }

    @Override
    public void setPrimaryNode(final NodeIdentifier nodeId) {
        clusterManager.setPrimaryNodeId(nodeId);
    }

    @Override
    public NodeIdentifier getNodeIdentifier(final String uuid) {
        final Node node = clusterManager.getNode(uuid);
        return node == null ? null : node.getNodeId();
    }


    /**
     * Updates the status of the node with the given ID to the given status and returns <code>true</code>
     * if successful, <code>false</code> if no node exists with the given ID
     *
     * @param nodeId the ID of the node whose status is changed
     * @param status the new status of the node
     * @return <code>true</code> if the node exists and is updated, <code>false</code> if the node does not exist
     */
    private boolean updateNodeStatus(final NodeIdentifier nodeId, final NodeConnectionStatus status) {
        final long statusUpdateId = nodeStatusIdGenerator.incrementAndGet();

        final Node node = clusterManager.getRawNode(nodeId.getId());
        if (node == null) {
            return false;
        }

        final Status nodeStatus = Status.valueOf(status.getState().name());
        final Status oldStatus = node.setStatus(nodeStatus);

        if (nodeStatus != oldStatus) {
            final Set<NodeIdentifier> nodesToNotify = clusterManager.getNodes(Status.CONNECTED, Status.CONNECTING).stream()
                .map(toNotify -> toNotify.getNodeId())
                .collect(Collectors.toSet());

            final NodeStatusChangeMessage message = new NodeStatusChangeMessage();
            message.setNodeId(nodeId);
            message.setNodeConnectionStatus(status);
            // TODO: When this is sent from one node to another, we need to ensure that we check the current
            // 'revision number' on the node and include that as the Update ID because we need a way to indicate
            // which status change event occurred first. I.e., when the status of a node is updated on any node
            // that is not the elected leader, we need to ensure that our nodeStatusIdGenerator also is updated.
            message.setStatusUpdateIdentifier(statusUpdateId);

            protocolSender.notifyNodeStatusChange(nodesToNotify, message);
        }

        return true;
    }

    /**
     * Updates the status of the given node to the given new status. This method exists only because the NCM currently handles
     * some of the status changing and we want it to call into this coordinator instead to change the status.
     *
     * @param rawNode the node whose status should be updated
     * @param nodeStatus the new status of the node
     */
    void updateNodeStatus(final Node rawNode, final Status nodeStatus) {
        // TODO: Remove this method when NCM is removed
        updateNodeStatus(rawNode, nodeStatus, false);
    }


    /**
     * Updates the status of the given node to the given new status. This method exists only because the NCM currently handles
     * some of the status changing and we want it to call into this coordinator instead to change the status.
     *
     * @param rawNode the node whose status should be updated
     * @param nodeStatus the new status of the node
     * @param heartbeatDisconnect indicates whether or not the node is being disconnected due to lack of heartbeat
     */
    void updateNodeStatus(final Node rawNode, final Status nodeStatus, final boolean heartbeatDisconnect) {
        // TODO: Remove this method when NCM is removed.
        final long statusUpdateId = nodeStatusIdGenerator.incrementAndGet();
        final Status oldStatus;
        if (heartbeatDisconnect) {
            oldStatus = rawNode.setHeartbeatDisconnection();
        } else {
            oldStatus = rawNode.setStatus(nodeStatus);
        }

        if (nodeStatus != oldStatus) {
            final Set<NodeIdentifier> nodesToNotify = clusterManager.getNodes(Status.CONNECTED, Status.CONNECTING).stream()
                .map(toNotify -> toNotify.getNodeId())
                .collect(Collectors.toSet());

            final NodeStatusChangeMessage message = new NodeStatusChangeMessage();
            message.setNodeId(rawNode.getNodeId());
            message.setNodeConnectionStatus(new NodeConnectionStatus(NodeConnectionState.valueOf(nodeStatus.name())));
            message.setStatusUpdateIdentifier(statusUpdateId);

            protocolSender.notifyNodeStatusChange(nodesToNotify, message);
        }
    }
}
