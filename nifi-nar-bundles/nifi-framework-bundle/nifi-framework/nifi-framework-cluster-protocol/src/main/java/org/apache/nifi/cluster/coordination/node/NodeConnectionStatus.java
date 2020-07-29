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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.jaxb.message.NodeConnectionStatusAdapter;

/**
 * Describes the current status of a node
 */
@XmlJavaTypeAdapter(NodeConnectionStatusAdapter.class)
public class NodeConnectionStatus {
    private static final AtomicLong idGenerator = new AtomicLong(0L);

    private final long updateId;
    private final NodeIdentifier nodeId;
    private final NodeConnectionState state;
    private final OffloadCode offloadCode;
    private final DisconnectionCode disconnectCode;
    private final String reason;
    private final Long connectionRequestTime;


    public NodeConnectionStatus(final NodeIdentifier nodeId, final NodeConnectionState state) {
        this(nodeId, state, null, null, null, null);
    }

    public NodeConnectionStatus(final NodeIdentifier nodeId, final DisconnectionCode disconnectionCode) {
        this(nodeId, NodeConnectionState.DISCONNECTED, null, disconnectionCode, disconnectionCode.toString(), null);
    }

    public NodeConnectionStatus(final NodeIdentifier nodeId, final NodeConnectionState state, final OffloadCode offloadCode, final String offloadExplanation) {
        this(nodeId, state, offloadCode, null, offloadExplanation, null);
    }

    public NodeConnectionStatus(final NodeIdentifier nodeId, final DisconnectionCode disconnectionCode, final String disconnectionExplanation) {
        this(nodeId, NodeConnectionState.DISCONNECTED, null, disconnectionCode, disconnectionExplanation, null);
    }

    public NodeConnectionStatus(final NodeIdentifier nodeId, final NodeConnectionState state, final DisconnectionCode disconnectionCode) {
        this(nodeId, state, null, disconnectionCode, disconnectionCode == null ? null : disconnectionCode.toString(), null);
    }

    public NodeConnectionStatus(final NodeConnectionStatus status) {
        this(status.getNodeIdentifier(), status.getState(), status.getOffloadCode(), status.getDisconnectCode(), status.getReason(), status.getConnectionRequestTime());
    }

    public NodeConnectionStatus(final NodeIdentifier nodeId, final NodeConnectionState state, final OffloadCode offloadCode,
                                final DisconnectionCode disconnectCode, final String reason, final Long connectionRequestTime) {
        this(idGenerator.getAndIncrement(), nodeId, state, offloadCode, disconnectCode, reason, connectionRequestTime);
    }

    public NodeConnectionStatus(final long updateId, final NodeIdentifier nodeId, final NodeConnectionState state, final OffloadCode offloadCode,
                                final DisconnectionCode disconnectCode, final String reason, final Long connectionRequestTime) {
        this.updateId = updateId;
        this.nodeId = nodeId;
        this.state = state;
        this.offloadCode = offloadCode;
        if (state == NodeConnectionState.DISCONNECTED && disconnectCode == null) {
            this.disconnectCode = DisconnectionCode.UNKNOWN;
            this.reason = this.disconnectCode.toString();
        } else {
            this.disconnectCode = disconnectCode;
            this.reason = reason;
        }

        this.connectionRequestTime = (connectionRequestTime == null && state == NodeConnectionState.CONNECTING) ? Long.valueOf(System.currentTimeMillis()) : connectionRequestTime;
    }

    public long getUpdateIdentifier() {
        return updateId;
    }

    public NodeIdentifier getNodeIdentifier() {
        return nodeId;
    }

    public NodeConnectionState getState() {
        return state;
    }

    public OffloadCode getOffloadCode() {
        return offloadCode;
    }

    public DisconnectionCode getDisconnectCode() {
        return disconnectCode;
    }

    public String getReason() {
        return reason;
    }

    public Long getConnectionRequestTime() {
        return connectionRequestTime;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        final NodeConnectionState state = getState();
        sb.append("NodeConnectionStatus[nodeId=").append(nodeId).append(", state=").append(state);
        if (state == NodeConnectionState.OFFLOADED || state == NodeConnectionState.OFFLOADING) {
            sb.append(", Offload Code=").append(getOffloadCode()).append(", Offload Reason=").append(getReason());
        }
        if (state == NodeConnectionState.DISCONNECTED || state == NodeConnectionState.DISCONNECTING) {
            sb.append(", Disconnect Code=").append(getDisconnectCode()).append(", Disconnect Reason=").append(getReason());
        }
        sb.append(", updateId=").append(getUpdateIdentifier());
        sb.append("]");
        return sb.toString();
    }

    /**
     * Updates the ID Generator so that it is at least equal to the given minimum value
     *
     * @param minimumValue the minimum value that the ID Generator should be set to
     */
    static void updateIdGenerator(long minimumValue) {
        idGenerator.updateAndGet(curValue -> Math.max(minimumValue, curValue));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
        result = prime * result + ((state == null) ? 0 : state.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (!(obj instanceof NodeConnectionStatus)) {
            return false;
        }

        NodeConnectionStatus other = (NodeConnectionStatus) obj;
        return Objects.deepEquals(getNodeIdentifier(), other.getNodeIdentifier())
            && Objects.deepEquals(getState(), other.getState());
    }
}
