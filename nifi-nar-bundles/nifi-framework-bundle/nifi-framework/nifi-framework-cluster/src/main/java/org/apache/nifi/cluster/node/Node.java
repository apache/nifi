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
package org.apache.nifi.cluster.node;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.cluster.protocol.NodeIdentifier;

/**
 * Represents a connected flow controller. Nodes always have an immutable identifier and a status. The status may be changed, but never null.
 *
 * A Node may be cloned, but the cloning is a shallow copy of the instance.
 *
 * This class overrides hashCode and equals and considers two instances to be equal if they have the equal NodeIdentifiers.
 *
 */
public class Node implements Cloneable, Comparable<Node> {

    /**
     * The semantics of a Node status are as follows:
     * <ul>
     * <li>CONNECTED -- a flow controller that is connected to the cluster. A connecting node transitions to connected after the cluster receives the flow controller's first heartbeat. A connected
     * node can transition to disconnecting.</li>
     * <li>CONNECTING -- a flow controller has issued a connection request to the cluster, but has not yet sent a heartbeat. A connecting node can transition to disconnecting or connected. The cluster
     * will not accept any external requests to change the flow while any node is connecting.</li>
     * <li>DISCONNECTED -- a flow controller that is not connected to the cluster. A disconnected node can transition to connecting.</li>
     * <li>DISCONNECTING -- a flow controller that is in the process of disconnecting from the cluster. A disconnecting node will always transition to disconnected.</li>
     * </ul>
     */
    public static enum Status {

        CONNECTED,
        CONNECTING,
        DISCONNECTED,
        DISCONNECTING
    }

    /**
     * the node's unique identifier
     */
    private final NodeIdentifier nodeId;

    /**
     * the node statue
     */
    private Status status;


    /**
     * the last time the connection for this node was requested
     */
    private AtomicLong connectionRequestedTimestamp = new AtomicLong(0L);

    /**
     * a flag to indicate this node was disconnected because of a lack of heartbeat
     */
    private boolean heartbeatDisconnection;

    public Node(final NodeIdentifier id, final Status status) {
        if (id == null) {
            throw new IllegalArgumentException("ID may not be null.");
        } else if (status == null) {
            throw new IllegalArgumentException("Status may not be null.");
        }
        this.nodeId = id;
        this.status = status;
    }

    public NodeIdentifier getNodeId() {
        return nodeId;
    }

    /**
     * Returns the time of the last received connection request for this node.
     *
     * @return the time when the connection request for this node was received.
     */
    public long getConnectionRequestedTimestamp() {
        return connectionRequestedTimestamp.get();
    }

    /**
     * Sets the time when the connection request for this node was last received.
     *
     * This method is thread-safe and may be called without obtaining any lock.
     *
     * @param connectionRequestedTimestamp timestamp
     */
    public void setConnectionRequestedTimestamp(long connectionRequestedTimestamp) {
        this.connectionRequestedTimestamp.set(connectionRequestedTimestamp);
    }

    /**
     * Returns true if the node was disconnected due to lack of heartbeat; false otherwise.
     *
     * @return true if the node was disconnected due to lack of heartbeat; false otherwise.
     */
    public boolean isHeartbeatDisconnection() {
        return heartbeatDisconnection;
    }

    /**
     * Sets the status to disconnected and flags the node as being disconnected by lack of heartbeat.
     */
    public Status setHeartbeatDisconnection() {
        final Status oldStatus = setStatus(Status.DISCONNECTED);
        heartbeatDisconnection = true;
        return oldStatus;
    }

    /**
     * @return the status
     */
    public synchronized Status getStatus() {
        return status;
    }

    /**
     * Updates the status to the given value, returning the previous status
     *
     * @param status a status
     * @return the previous status for the node
     */
    public synchronized Status setStatus(final Status status) {
        if (status == null) {
            throw new IllegalArgumentException("Status may not be null.");
        }
        final Status oldStatus = this.status;
        this.status = status;
        heartbeatDisconnection = false;
        return oldStatus;
    }

    @Override
    public Node clone() {
        final Node clone = new Node(nodeId, status);
        clone.heartbeatDisconnection = heartbeatDisconnection;
        clone.connectionRequestedTimestamp = connectionRequestedTimestamp;
        return clone;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Node other = (Node) obj;
        if (this.nodeId != other.nodeId && (this.nodeId == null || !this.nodeId.equals(other.nodeId))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + (this.nodeId != null ? this.nodeId.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        return nodeId.toString();
    }

    @Override
    public int compareTo(final Node o) {
        if (o == null) {
            return -1;
        }
        return getNodeId().getId().compareTo(o.getNodeId().getId());
    }
}
