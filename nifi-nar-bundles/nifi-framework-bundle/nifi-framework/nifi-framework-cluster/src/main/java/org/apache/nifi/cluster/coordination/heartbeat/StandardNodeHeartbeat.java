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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.cluster.HeartbeatPayload;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.Heartbeat;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;

public class StandardNodeHeartbeat implements NodeHeartbeat {

    private final NodeIdentifier nodeId;
    private final long timestamp;
    private final NodeConnectionStatus connectionStatus;
    private final Set<String> roles;
    private final int flowFileCount;
    private final long flowFileBytes;
    private final int activeThreadCount;
    private final long systemStartTime;

    public StandardNodeHeartbeat(final NodeIdentifier nodeId, final long timestamp, final NodeConnectionStatus connectionStatus,
        final Set<String> roles, final int flowFileCount, final long flowFileBytes, final int activeThreadCount, final long systemStartTime) {
        this.timestamp = timestamp;
        this.nodeId = nodeId;
        this.connectionStatus = connectionStatus;
        this.roles = roles == null ? Collections.emptySet() : Collections.unmodifiableSet(new HashSet<>(roles));
        this.flowFileCount = flowFileCount;
        this.flowFileBytes = flowFileBytes;
        this.activeThreadCount = activeThreadCount;
        this.systemStartTime = systemStartTime;
    }

    @Override
    public NodeIdentifier getNodeIdentifier() {
        return nodeId;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public NodeConnectionStatus getConnectionStatus() {
        return connectionStatus;
    }

    @Override
    public Set<String> getRoles() {
        return roles;
    }

    @Override
    public int getFlowFileCount() {
        return flowFileCount;
    }

    @Override
    public long getFlowFileBytes() {
        return flowFileBytes;
    }

    @Override
    public int getActiveThreadCount() {
        return activeThreadCount;
    }


    @Override
    public long getSystemStartTime() {
        return systemStartTime;
    }

    public static StandardNodeHeartbeat fromHeartbeatMessage(final HeartbeatMessage message, final long timestamp) {
        final Heartbeat heartbeat = message.getHeartbeat();
        final HeartbeatPayload payload = HeartbeatPayload.unmarshal(heartbeat.getPayload());

        return new StandardNodeHeartbeat(heartbeat.getNodeIdentifier(), timestamp, heartbeat.getConnectionStatus(),
            heartbeat.getRoles(), (int) payload.getTotalFlowFileCount(), payload.getTotalFlowFileBytes(),
            payload.getActiveThreadCount(), payload.getSystemStartTime());
    }
}