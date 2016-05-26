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

package org.apache.nifi.cluster.coordination;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.reporting.Severity;

/**
 * <p>
 * Responsible for coordinating nodes in the cluster
 * <p>
 */
public interface ClusterCoordinator {

    /**
     * Sends a request to the node to connect to the cluster. This will immediately
     * set the NodeConnectionStatus to DISCONNECTED.
     *
     * @param nodeId the identifier of the node
     */
    void requestNodeConnect(NodeIdentifier nodeId);

    /**
     * Indicates that the node has sent a valid heartbeat and should now
     * be considered part of the cluster
     *
     * @param nodeId the identifier of the node
     */
    void finishNodeConnection(NodeIdentifier nodeId);

    /**
     * Sends a request to the node to disconnect from the cluster.
     * The node will be marked as disconnected immediately.
     *
     * @param nodeId the identifier of the node
     * @param disconnectionCode the code that represents why this node is being asked to disconnect
     * @param explanation an explanation as to why the node is being asked to disconnect
     *            from the cluster
     */
    void requestNodeDisconnect(NodeIdentifier nodeId, DisconnectionCode disconnectionCode, String explanation);

    /**
     * Notifies the Cluster Coordinator that the node with the given ID has requested to disconnect
     * from the cluster. If no node exists in the cluster with the given ID, this method has no effect.
     *
     * @param nodeId the identifier of the node
     * @param disconnectionCode the code that represents why this node is requesting to disconnect
     * @param explanation an explanation as to why the node is requesting to disconnect from the cluster
     */
    void disconnectionRequestedByNode(NodeIdentifier nodeId, DisconnectionCode disconnectionCode, String explanation);

    /**
     * Returns the current status of the node with the given identifier
     *
     * @param nodeId the identifier of the node
     *
     * @return the current status of the node with the given identifier,
     *         or <code>null</code> if no node is known with the given identifier
     */
    NodeConnectionStatus getConnectionStatus(NodeIdentifier nodeId);

    /**
     * Returns the identifiers of all nodes that have the given connection state
     *
     * @param state the state
     * @return the identifiers of all nodes that have the given connection state
     */
    Set<NodeIdentifier> getNodeIdentifiers(NodeConnectionState state);

    /**
     * Returns a Map of NodeConnectionStatus to all Node Identifiers that have that status.
     *
     * @return the NodeConnectionStatus for each Node in the cluster, grouped by the Connection Status
     */
    Map<NodeConnectionState, List<NodeIdentifier>> getConnectionStates();

    /**
     * Checks if the given hostname is blocked by the configured firewall, returning
     * <code>true</code> if the node is blocked, <code>false</code> if the node is
     * allowed through the firewall or if there is no firewall configured
     *
     * @param hostname the hostname of the node that is attempting to connect to the cluster
     *
     * @return <code>true</code> if the node is blocked, <code>false</code> if the node is
     *         allowed through the firewall or if there is no firewall configured
     */
    boolean isBlockedByFirewall(String hostname);

    /**
     * Reports that some event occurred that is relevant to the cluster
     *
     * @param nodeId the identifier of the node that the event pertains to, or <code>null</code> if not applicable
     * @param severity the severity of the event
     * @param event an explanation of the event
     */
    void reportEvent(NodeIdentifier nodeId, Severity severity, String event);

    /**
     * Updates the node that is considered the Primary Node
     *
     * @param nodeId the id of the Primary Node
     */
    void setPrimaryNode(NodeIdentifier nodeId);

    /**
     * Returns the NodeIdentifier that exists that has the given UUID, or <code>null</code> if no NodeIdentifier
     * exists for the given UUID
     *
     * @param uuid the UUID of the NodeIdentifier to obtain
     * @return the NodeIdentifier that exists that has the given UUID, or <code>null</code> if no NodeIdentifier
     *         exists for the given UUID
     */
    NodeIdentifier getNodeIdentifier(String uuid);
}
