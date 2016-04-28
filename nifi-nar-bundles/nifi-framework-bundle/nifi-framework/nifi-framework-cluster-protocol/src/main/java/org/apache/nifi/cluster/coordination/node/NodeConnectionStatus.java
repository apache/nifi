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

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.nifi.cluster.protocol.jaxb.message.NodeConnectionStatusAdapter;

/**
 * Describes the current status of a node
 */
@XmlJavaTypeAdapter(NodeConnectionStatusAdapter.class)
public class NodeConnectionStatus {
    private final NodeConnectionState state;
    private final DisconnectionCode disconnectCode;
    private final String disconnectReason;
    private final Long connectionRequestTime;

    public NodeConnectionStatus(final NodeConnectionState state) {
        this(state, null, null, null);
    }

    public NodeConnectionStatus(final NodeConnectionState state, final long connectionRequestTime) {
        this(state, null, null, connectionRequestTime);
    }

    public NodeConnectionStatus(final DisconnectionCode disconnectionCode) {
        this(NodeConnectionState.DISCONNECTED, disconnectionCode, disconnectionCode.name(), null);
    }

    public NodeConnectionStatus(final DisconnectionCode disconnectionCode, final String disconnectionExplanation) {
        this(NodeConnectionState.DISCONNECTED, disconnectionCode, disconnectionExplanation, null);
    }

    public NodeConnectionStatus(final NodeConnectionState state, final DisconnectionCode disconnectionCode) {
        this(state, disconnectionCode, disconnectionCode.name(), null);
    }

    public NodeConnectionStatus(final NodeConnectionState state, final DisconnectionCode disconnectCode, final String disconnectReason, final Long connectionRequestTime) {
        this.state = state;
        if (state == NodeConnectionState.DISCONNECTED && disconnectCode == null) {
            this.disconnectCode = DisconnectionCode.UNKNOWN;
            this.disconnectReason = this.disconnectCode.toString();
        } else {
            this.disconnectCode = disconnectCode;
            this.disconnectReason = disconnectReason;
        }

        this.connectionRequestTime = connectionRequestTime;
    }

    public NodeConnectionState getState() {
        return state;
    }

    public DisconnectionCode getDisconnectCode() {
        return disconnectCode;
    }

    public String getDisconnectReason() {
        return disconnectReason;
    }

    public Long getConnectionRequestTime() {
        return connectionRequestTime;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        final NodeConnectionState state = getState();
        sb.append("NodeConnectionStatus[state=").append(state);
        if (state == NodeConnectionState.DISCONNECTED || state == NodeConnectionState.DISCONNECTING) {
            sb.append(", Disconnect Code=").append(getDisconnectCode()).append(", Disconnect Reason=").append(getDisconnectReason());
        }
        sb.append("]");
        return sb.toString();
    }
}
