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
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.cluster.protocol.jaxb.message.NodeIdentifierAdapter;

/**
 * @author unattributed
 */
@XmlRootElement(name = "reconnectionRequestMessage")
public class ReconnectionRequestMessage extends ProtocolMessage {

    private NodeIdentifier nodeId;
    private StandardDataFlow dataFlow;
    private boolean primary;
    private Integer managerRemoteSiteListeningPort;
    private Boolean managerRemoteSiteCommsSecure;
    private String instanceId;

    public ReconnectionRequestMessage() {
    }

    @XmlJavaTypeAdapter(NodeIdentifierAdapter.class)
    public NodeIdentifier getNodeId() {
        return nodeId;
    }

    public void setNodeId(NodeIdentifier nodeId) {
        this.nodeId = nodeId;
    }

    public StandardDataFlow getDataFlow() {
        return dataFlow;
    }

    public void setDataFlow(StandardDataFlow dataFlow) {
        this.dataFlow = dataFlow;
    }

    public boolean isPrimary() {
        return primary;
    }

    public void setPrimary(boolean primary) {
        this.primary = primary;
    }

    @Override
    public MessageType getType() {
        return MessageType.RECONNECTION_REQUEST;
    }

    public void setManagerRemoteSiteListeningPort(final Integer listeningPort) {
        this.managerRemoteSiteListeningPort = listeningPort;
    }

    public Integer getManagerRemoteSiteListeningPort() {
        return managerRemoteSiteListeningPort;
    }

    public void setManagerRemoteSiteCommsSecure(final Boolean remoteSiteCommsSecure) {
        this.managerRemoteSiteCommsSecure = remoteSiteCommsSecure;
    }

    public Boolean isManagerRemoteSiteCommsSecure() {
        return managerRemoteSiteCommsSecure;
    }

    public void setInstanceId(final String instanceId) {
        this.instanceId = instanceId;
    }

    public String getInstanceId() {
        return instanceId;
    }
}
