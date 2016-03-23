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
package org.apache.nifi.cluster.manager;

import java.util.List;
import java.util.Set;

import org.apache.nifi.cluster.event.Event;
import org.apache.nifi.cluster.manager.exception.IllegalNodeDeletionException;
import org.apache.nifi.cluster.manager.exception.IllegalNodeDisconnectionException;
import org.apache.nifi.cluster.manager.exception.IllegalNodeReconnectionException;
import org.apache.nifi.cluster.manager.exception.NodeDisconnectionException;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.node.Node.Status;
import org.apache.nifi.cluster.protocol.ConnectionRequest;
import org.apache.nifi.cluster.protocol.ConnectionResponse;
import org.apache.nifi.cluster.protocol.Heartbeat;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.remote.cluster.NodeInformant;
import org.apache.nifi.reporting.BulletinRepository;

/**
 * Defines the interface for a ClusterManager. The cluster manager is a threadsafe centralized manager for a cluster. Members of a cluster are nodes. A member becomes a node by issuing a connection
 * request to the manager. The manager maintains the set of nodes. Nodes may be disconnected, reconnected, and deleted.
 *
 * Nodes are responsible for sending heartbeats to the manager to indicate their liveliness. A manager may disconnect a node if it does not receive a heartbeat within a configurable time period. A
 * cluster manager instance may be configured with how often to monitor received heartbeats (getHeartbeatMonitoringIntervalSeconds()) and the maximum time that may elapse between node heartbeats
 * before disconnecting the node (getMaxHeartbeatGapSeconds()).
 *
 * Since only a single node may execute isolated processors, the cluster manager maintains the notion of a primary node. The primary node is chosen at cluster startup and retains the role until a user
 * requests a different node to be the primary node.
 *
 */
public interface ClusterManager extends NodeInformant {

    /**
     * Handles a node's heartbeat.
     *
     * @param heartbeat a heartbeat
     *
     */
    void handleHeartbeat(Heartbeat heartbeat);

    /**
     * @param statuses the statuses of the nodes
     * @return the set of nodes
     */
    Set<Node> getNodes(Status... statuses);

    /**
     * @param nodeId node identifier
     * @return returns the node with the given identifier or null if node does not exist
     */
    Node getNode(String nodeId);

    /**
     * @param statuses statuses
     * @return the set of node identifiers with the given node status
     */
    Set<NodeIdentifier> getNodeIds(Status... statuses);

    /**
     * Deletes the node with the given node identifier. If the given node is the primary node, then a subsequent request may be made to the manager to set a new primary node.
     *
     * @param nodeId the node identifier
     * @param userDn the Distinguished Name of the user requesting the node be deleted from the cluster
     *
     * @throws UnknownNodeException if the node does not exist
     * @throws IllegalNodeDeletionException if the node is not in a disconnected state
     */
    void deleteNode(String nodeId, String userDn) throws UnknownNodeException, IllegalNodeDeletionException;

    /**
     * Requests a connection to the cluster.
     *
     * @param request the request
     *
     * @return the response
     */
    ConnectionResponse requestConnection(ConnectionRequest request);

    /**
     * Services reconnection requests for a given node. If the node indicates reconnection failure, then the node will be set to disconnected. Otherwise, a reconnection request will be sent to the
     * node, initiating the connection handshake.
     *
     * @param nodeId a node identifier
     * @param userDn the Distinguished Name of the user requesting the reconnection
     *
     * @throws UnknownNodeException if the node does not exist
     * @throws IllegalNodeReconnectionException if the node is not disconnected
     */
    void requestReconnection(String nodeId, String userDn) throws UnknownNodeException, IllegalNodeReconnectionException;

    /**
     * Requests the node with the given identifier be disconnected.
     *
     * @param nodeId the node identifier
     * @param userDn the Distinguished Name of the user requesting the disconnection
     *
     * @throws UnknownNodeException if the node does not exist
     * @throws IllegalNodeDisconnectionException if the node cannot be disconnected due to the cluster's state (e.g., node is last connected node or node is primary)
     * @throws UnknownNodeException if the node does not exist
     * @throws IllegalNodeDisconnectionException if the node is not disconnected
     * @throws NodeDisconnectionException if the disconnection failed
     */
    void requestDisconnection(String nodeId, String userDn) throws UnknownNodeException, IllegalNodeDisconnectionException, NodeDisconnectionException;

    /**
     * @return the time in seconds to wait between successive executions of heartbeat monitoring
     */
    int getHeartbeatMonitoringIntervalSeconds();

    /**
     * @return the maximum time in seconds that is allowed between successive heartbeats of a node before disconnecting the node
     */
    int getMaxHeartbeatGapSeconds();

    /**
     * Returns a list of node events for the node with the given identifier. The events will be returned in order of most recent to least recent according to the creation date of the event.
     *
     * @param nodeId the node identifier
     *
     * @return the list of events or an empty list if no node exists with the given identifier
     */
    List<Event> getNodeEvents(final String nodeId);

    /**
     * @return the primary node of the cluster or null if no primary node exists
     */
    Node getPrimaryNode();

    /**
     * @return the bulletin repository
     */
    BulletinRepository getBulletinRepository();
}
