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

package org.apache.nifi.cluster.coordination.heartbeat;

import org.apache.nifi.cluster.protocol.NodeIdentifier;

/**
 * A HeartbeatMonitor is responsible for monitoring some remote resource for heartbeats from each
 * node in a cluster and reacting to those heartbeats (or lack thereof).
 */
public interface HeartbeatMonitor {

    /**
     * Begin monitoring for heartbeats
     */
    void start();

    /**
     * Stop monitoring heartbeats
     */
    void stop();

    /**
     * Returns the latest heartbeat that has been obtained for the node with
     * the given id
     *
     * @param nodeId the id of the node whose heartbeat should be retrieved
     * @return the latest heartbeat that has been obtained for the node with
     *         the given id, or <code>null</code> if no heartbeat has been obtained
     */
    NodeHeartbeat getLatestHeartbeat(NodeIdentifier nodeId);

    /**
     * Removes the heartbeat for the given node from the monitor and the
     * remote location where heartbeats are sent
     *
     * @param nodeId the id of the node whose heartbeat should be removed
     */
    void removeHeartbeat(NodeIdentifier nodeId);

    /**
     * Clears all heartbeats that have been received
     */
    void purgeHeartbeats();

    /**
     * @return the address that heartbeats should be sent to when this node is elected coordinator.
     */
    String getHeartbeatAddress();
}
