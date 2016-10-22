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

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;

@XmlRootElement(name = "heartbeatResponse")
public class HeartbeatResponseMessage extends ProtocolMessage {

    private List<NodeConnectionStatus> updatedNodeStatuses = new ArrayList<>();
    private String flowElectionMessage = null;

    @Override
    public MessageType getType() {
        return MessageType.HEARTBEAT_RESPONSE;
    }

    public List<NodeConnectionStatus> getUpdatedNodeStatuses() {
        return updatedNodeStatuses;
    }

    public void setUpdatedNodeStatuses(final List<NodeConnectionStatus> nodeStatuses) {
        this.updatedNodeStatuses = new ArrayList<>(nodeStatuses);
    }

    public String getFlowElectionMessage() {
        return flowElectionMessage;
    }

    public void setFlowElectionMessage(String flowElectionMessage) {
        this.flowElectionMessage = flowElectionMessage;
    }
}
