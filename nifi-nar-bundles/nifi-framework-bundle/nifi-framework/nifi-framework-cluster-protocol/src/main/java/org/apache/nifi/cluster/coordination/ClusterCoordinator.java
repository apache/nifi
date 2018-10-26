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

import org.apache.nifi.cluster.coordination.node.OffloadCode;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.coordination.node.NodeWorkload;
import org.apache.nifi.cluster.event.NodeEvent;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.services.FlowService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * Responsible for coordinating nodes in the cluster
 * <p>
 */
public interface ClusterCoordinator {

    /**
     * Sends a request to the node to connect to the cluster. This will immediately
     * set the NodeConnectionStatus to CONNECTING.
     *
     * @param nodeId the identifier of the node
     * @param userDn the DN of the user that requested that the node connect, or <code>null</code> if the action is not user-driven
     */
    void requestNodeConnect(NodeIdentifier nodeId, String userDn);

    /**
     * Notifies the Cluster Coordinator that the NiFi instance is being shutdown so that
     * the coordinator is able to perform cleanup of resources
     */
    void shutdown();

    /**
     * Indicates that the node has sent a valid heartbeat and should now
     * be considered part of the cluster
     *
     * @param nodeId the identifier of the node
     */
    void finishNodeConnection(NodeIdentifier nodeId);

    /**
     * Indicates that the node has finished being offloaded
     *
     * @param nodeId the identifier of the node
     */
    void finishNodeOffload(NodeIdentifier nodeId);

    /**
     * Sends a request to the node to be offloaded.
     * The node will be marked as offloading immediately.
     * <p>
     * When a node is offloaded:
     * <ul>
     *     <li>all processors on the node are stopped</li>
     *     <li>all processors on the node are terminated</li>
     *     <li>all remote process groups on the node stop transmitting</li>
     *     <li>all flowfiles on the node are sent to other nodes in the cluster</li>
     * </ul>
     * @param nodeId the identifier of the node
     * @param offloadCode the code that represents why this node is being asked to be offloaded
     * @param explanation an explanation as to why the node is being asked to be offloaded
     */
    void requestNodeOffload(NodeIdentifier nodeId, OffloadCode offloadCode, String explanation);

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
     * Removes the given disconnected node from the cluster
     *
     * @param nodeId the node to remove
     * @param userDn the DN of the user requesting that the node be removed
     */
    void removeNode(NodeIdentifier nodeId, String userDn);

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
     * @param states the states of interest
     * @return the identifiers of all nodes that have the given connection state
     */
    Set<NodeIdentifier> getNodeIdentifiers(NodeConnectionState... states);

    /**
     * Returns a Map of NodeConnectionStates to all Node Identifiers that have that state.
     *
     * @return the NodeConnectionState for each Node in the cluster, grouped by the Connection State
     */
    Map<NodeConnectionState, List<NodeIdentifier>> getConnectionStates();

    /**
     * Returns a List of the NodeConnectionStatus for each node in the cluster
     *
     * @return a List of the NodeConnectionStatus for each node in the cluster
     */
    List<NodeConnectionStatus> getConnectionStatuses();

    /**
     * Checks if the given hostname is blocked by the configured firewall, returning
     * <code>true</code> if the node is blocked, <code>false</code> if the node is
     * allowed through the firewall or if there is no firewall configured
     *
     * @param nodeIdentities the identities of the node that is attempting to connect to the cluster
     *
     * @return <code>true</code> if the node is blocked, <code>false</code> if the node is
     *         allowed through the firewall or if there is no firewall configured
     */
    boolean isBlockedByFirewall(Set<String> nodeIdentities);

    /**
     * Reports that some event occurred that is relevant to the cluster
     *
     * @param nodeId the identifier of the node that the event pertains to, or <code>null</code> if not applicable
     * @param severity the severity of the event
     * @param event an explanation of the event
     */
    void reportEvent(NodeIdentifier nodeId, Severity severity, String event);

    /**
     * Returns the NodeIdentifier that exists that has the given UUID, or <code>null</code> if no NodeIdentifier
     * exists for the given UUID
     *
     * @param uuid the UUID of the NodeIdentifier to obtain
     * @return the NodeIdentifier that exists that has the given UUID, or <code>null</code> if no NodeIdentifier
     *         exists for the given UUID
     */
    NodeIdentifier getNodeIdentifier(String uuid);

    /**
     * Returns all of the events that have occurred for the given node
     *
     * @param nodeId the identifier of the node
     * @return all of the events that have occurred for the given node
     */
    List<NodeEvent> getNodeEvents(NodeIdentifier nodeId);

    /**
     * @return the identifier of the node that is elected primary, or <code>null</code> if either there is no
     *         primary or the primary is not known by this node.
     */
    NodeIdentifier getPrimaryNode();

    /**
     * @return the identifier of the node that is elected the active cluster coordinator, or <code>null</code> if
     *         there is no active cluster coordinator elected.
     */
    NodeIdentifier getElectedActiveCoordinatorNode();

    /**
     * @return the identifier of this node, if it is known, <code>null</code> if the Node Identifier has not yet been established.
     */
    NodeIdentifier getLocalNodeIdentifier();

    /**
     * @return <code>true</code> if this node has been elected the active cluster coordinator, <code>false</code> otherwise.
     */
    boolean isActiveClusterCoordinator();

    /**
     * Updates the Flow Service to use for obtaining the current flow
     *
     * @param flowService the flow service to use for obtaining the current flow
     */
    void setFlowService(FlowService flowService);

    /**
     * Clears the current state of all nodes and replaces them with the values provided in the given map
     *
     * @param statusMap the new states of all nodes in the cluster
     */
    void resetNodeStatuses(Map<NodeIdentifier, NodeConnectionStatus> statusMap);

    /**
     * Resets the status of the node to be in accordance with the given NodeConnectionStatus if and only if the
     * currently held status for this node has an Update ID equal to the given <code>qualifyingUpdateId</code>
     *
     * @param connectionStatus the new status of the node
     * @param qualifyingUpdateId the Update ID to compare the current ID with. If the current ID for the node described by the provided
     *            NodeConnectionStatus is not equal to this value, the value will not be updated
     * @return <code>true</code> if the node status was updated, <code>false</code> if the <code>qualifyingUpdateId</code> is out of date.
     */
    boolean resetNodeStatus(NodeConnectionStatus connectionStatus, long qualifyingUpdateId);

    /**
     * Notifies the Cluster Coordinator of the Node Identifier that the coordinator is currently running on
     *
     * @param nodeId the ID of the current node
     */
    void setLocalNodeIdentifier(NodeIdentifier nodeId);

    /**
     * Notifies the Cluster Coordinator whether or not the node is connected to the cluster
     *
     * @param connected <code>true</code> if the node is connected to a cluster, <code>false</code> otherwise.
     */
    void setConnected(boolean connected);

    /**
     * Indicates whether or not the node is currently connected to the cluster
     *
     * @return <code>true</code> if connected, <code>false</code> otherwise
     */
    boolean isConnected();

    /**
     * @return <code>true</code> if Flow Election is complete, <code>false</code> otherwise
     */
    boolean isFlowElectionComplete();

    /**
     * @return the current status of Flow Election.
     */
    String getFlowElectionStatus();

    /**
     * @return the current cluster workload retrieved from the cluster coordinator.
     * @throws IOException thrown when it failed to communicate with the cluster coordinator.
     */
    Map<NodeIdentifier, NodeWorkload> getClusterWorkload() throws IOException;

    /**
     * Registers the given event listener so that it is notified whenever a cluster topology event occurs
     * @param eventListener the event listener to notify
     */
    void registerEventListener(ClusterTopologyEventListener eventListener);

    /**
     * Stops notifying the given listener when cluster topology events occurs
     * @param eventListener the event listener to stop notifying
     */
    void unregisterEventListener(ClusterTopologyEventListener eventListener);
}
