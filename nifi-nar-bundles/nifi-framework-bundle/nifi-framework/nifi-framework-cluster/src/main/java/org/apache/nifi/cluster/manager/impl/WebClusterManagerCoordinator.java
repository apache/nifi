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

import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.node.Node.Status;
import org.apache.nifi.cluster.protocol.ConnectionRequest;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebClusterManagerCoordinator implements ClusterCoordinator {
    private static final Logger logger = LoggerFactory.getLogger(WebClusterManagerCoordinator.class);

    private final WebClusterManager clusterManager;

    public WebClusterManagerCoordinator(final WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    @Override
    public void requestNodeConnect(final NodeIdentifier nodeId) {
        final Node node = clusterManager.getRawNode(nodeId.getId());

        if (node == null) {
            final ConnectionRequest connectionRequest = new ConnectionRequest(nodeId);
            clusterManager.requestConnection(connectionRequest);
        } else {
            node.setStatus(Status.DISCONNECTED);
            clusterManager.requestReconnection(nodeId.getId(), "Anonymous");
        }
    }

    @Override
    public void finishNodeConnection(final NodeIdentifier nodeId) {
        final Node node = clusterManager.getRawNode(nodeId.getId());
        if (node == null) {
            logger.error("Attempting to Finish Node Connection but could not find Node with Identifier {}", nodeId);
            return;
        }

        node.setStatus(Status.CONNECTED);
    }

    @Override
    public void requestNodeDisconnect(final NodeIdentifier nodeId, final DisconnectionCode disconnectionCode, final String explanation) {
        try {
            clusterManager.requestDisconnection(nodeId, false, explanation);

            if (disconnectionCode == DisconnectionCode.LACK_OF_HEARTBEAT) {
                final Node node = clusterManager.getRawNode(nodeId.getId());
                if (node != null) {
                    node.setHeartbeatDisconnection();
                }
            }
        } catch (final Exception e) {
            logger.error("Failed to request node {} disconnect from cluster due to {}", nodeId, e);
            logger.error("", e);
        }
    }

    @Override
    public void disconnectionRequestedByNode(final NodeIdentifier nodeId, final DisconnectionCode disconnectionCode, final String explanation) {
        final Node node = clusterManager.getRawNode(nodeId.getId());
        if (node != null) {
            node.setStatus(Status.DISCONNECTED);

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
        final Set<NodeIdentifier> nodeIds = new HashSet<>(nodes.size());
        for (final Node node : nodes) {
            nodeIds.add(node.getNodeId());
        }

        return nodeIds;
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
}
