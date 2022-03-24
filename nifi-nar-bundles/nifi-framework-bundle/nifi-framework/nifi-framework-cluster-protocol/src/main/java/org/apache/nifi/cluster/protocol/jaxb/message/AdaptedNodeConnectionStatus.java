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

package org.apache.nifi.cluster.protocol.jaxb.message;

import org.apache.nifi.cluster.coordination.node.OffloadCode;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.protocol.NodeIdentifier;

public class AdaptedNodeConnectionStatus {
    private Long updateId;
    private NodeIdentifier nodeId;
    private NodeConnectionState state;
    private OffloadCode offloadCode;
    private DisconnectionCode disconnectCode;
    private String reason;
    private Long connectionRequestTime;

    public Long getUpdateId() {
        return updateId;
    }

    public void setUpdateId(Long updateId) {
        this.updateId = updateId;
    }

    public NodeIdentifier getNodeId() {
        return nodeId;
    }

    public void setNodeId(NodeIdentifier nodeId) {
        this.nodeId = nodeId;
    }

    public NodeConnectionState getState() {
        return state;
    }

    public void setState(NodeConnectionState state) {
        this.state = state;
    }

    public OffloadCode getOffloadCode() {
        return offloadCode;
    }

    public DisconnectionCode getDisconnectCode() {
        return disconnectCode;
    }

    public void setOffloadCode(OffloadCode offloadCode) {
        this.offloadCode = offloadCode;
    }

    public void setDisconnectCode(DisconnectionCode disconnectCode) {
        this.disconnectCode = disconnectCode;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public Long getConnectionRequestTime() {
        return connectionRequestTime;
    }

    public void setConnectionRequestTime(Long connectionRequestTime) {
        this.connectionRequestTime = connectionRequestTime;
    }
}
