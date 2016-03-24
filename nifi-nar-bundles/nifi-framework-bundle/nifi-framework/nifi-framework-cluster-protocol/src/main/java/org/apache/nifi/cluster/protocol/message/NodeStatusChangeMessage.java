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

package org.apache.nifi.cluster.protocol.message;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.NodeIdentifier;

/**
 * Message to indicate that the status of a node in the cluster has changed
 */
@XmlRootElement(name = "nodeStatusChange")
public class NodeStatusChangeMessage extends ProtocolMessage {
    private NodeConnectionStatus connectionStatus;
    private NodeIdentifier nodeId;
    private Long statusUpdateId = -1L;

    @Override
    public MessageType getType() {
        return MessageType.NODE_STATUS_CHANGE;
    }

    public void setNodeConnectionStatus(final NodeConnectionStatus status) {
        this.connectionStatus = status;
    }

    public NodeConnectionStatus getNodeConnectionStatus() {
        return connectionStatus;
    }

    public NodeIdentifier getNodeId() {
        return nodeId;
    }

    public void setNodeId(NodeIdentifier nodeId) {
        this.nodeId = nodeId;
    }

    public Long getStatusUpdateIdentifier() {
        return statusUpdateId;
    }

    public void setStatusUpdateIdentifier(Long statusUpdateId) {
        this.statusUpdateId = statusUpdateId;
    }
}
