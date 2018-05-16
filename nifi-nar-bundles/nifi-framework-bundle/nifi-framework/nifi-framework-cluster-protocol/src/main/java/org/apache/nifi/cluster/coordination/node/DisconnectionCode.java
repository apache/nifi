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

/**
 * An enumeration of the reasons that a node may be disconnected
 * from the cluster
 */
public enum DisconnectionCode {
    /**
     * The node was disconnected for an unreported reason
     */
    UNKNOWN("Unknown Reason"),

    /**
     * The node has not yet connected to the cluster
     */
    NOT_YET_CONNECTED("Has Not Yet Connected to Cluster"),

    /**
     * A user explicitly disconnected the node from the cluster
     */
    USER_DISCONNECTED("User Disconnected Node"),

    /**
     * The node was disconnected because it stopped heartbeating
     */
    LACK_OF_HEARTBEAT("Lack of Heartbeat"),

    /**
     * The firewall prevented the node from joining the cluster
     */
    BLOCKED_BY_FIREWALL("Blocked by Firewall"),

    /**
     * The node failed to startup properly
     */
    STARTUP_FAILURE("Node Failed to Startup Properly"),

    /**
     * The node's flow did not match the cluster's flow
     */
    MISMATCHED_FLOWS("Node's Flow did not Match Cluster Flow"),

    /**
     * The node was missing a bundle used the cluster flow.
     */
    MISSING_BUNDLE("Node was missing bundle used by Cluster Flow"),

    /**
     * Cannot communicate with the node
     */
    UNABLE_TO_COMMUNICATE("Unable to Communicate with Node"),

    /**
     * Node did not service a request that was replicated to it
     */
    FAILED_TO_SERVICE_REQUEST("Failed to Service Request"),

    /**
     * Coordinator received a heartbeat from node, but the node is disconnected from the cluster
     */
    HEARTBEAT_RECEIVED_FROM_DISCONNECTED_NODE("Heartbeat Received from Disconnected Node"),

    /**
     * Node is being shut down
     */
    NODE_SHUTDOWN("Node was Shutdown");

    private final String description;

    private DisconnectionCode(final String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return description;
    }
}
