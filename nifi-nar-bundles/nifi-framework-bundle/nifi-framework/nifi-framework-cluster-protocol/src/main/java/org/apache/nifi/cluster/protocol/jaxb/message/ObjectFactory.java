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

import javax.xml.bind.annotation.XmlRegistry;

import org.apache.nifi.cluster.protocol.message.ConnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ConnectionResponseMessage;
import org.apache.nifi.cluster.protocol.message.DisconnectMessage;
import org.apache.nifi.cluster.protocol.message.FlowRequestMessage;
import org.apache.nifi.cluster.protocol.message.FlowResponseMessage;
import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;
import org.apache.nifi.cluster.protocol.message.HeartbeatResponseMessage;
import org.apache.nifi.cluster.protocol.message.ClusterWorkloadRequestMessage;
import org.apache.nifi.cluster.protocol.message.ClusterWorkloadResponseMessage;
import org.apache.nifi.cluster.protocol.message.MulticastProtocolMessage;
import org.apache.nifi.cluster.protocol.message.NodeConnectionStatusRequestMessage;
import org.apache.nifi.cluster.protocol.message.NodeConnectionStatusResponseMessage;
import org.apache.nifi.cluster.protocol.message.NodeStatusChangeMessage;
import org.apache.nifi.cluster.protocol.message.PingMessage;
import org.apache.nifi.cluster.protocol.message.ReconnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ReconnectionResponseMessage;
import org.apache.nifi.cluster.protocol.message.ServiceBroadcastMessage;

/**
 */
@XmlRegistry
public class ObjectFactory {

    public ObjectFactory() {
    }

    public ReconnectionRequestMessage createReconnectionRequestMessage() {
        return new ReconnectionRequestMessage();
    }

    public ReconnectionResponseMessage createReconnectionResponseMessage() {
        return new ReconnectionResponseMessage();
    }

    public DisconnectMessage createDisconnectionMessage() {
        return new DisconnectMessage();
    }

    public ConnectionRequestMessage createConnectionRequestMessage() {
        return new ConnectionRequestMessage();
    }

    public ConnectionResponseMessage createConnectionResponseMessage() {
        return new ConnectionResponseMessage();
    }

    public ServiceBroadcastMessage createServiceBroadcastMessage() {
        return new ServiceBroadcastMessage();
    }

    public HeartbeatMessage createHeartbeatMessage() {
        return new HeartbeatMessage();
    }

    public FlowRequestMessage createFlowRequestMessage() {
        return new FlowRequestMessage();
    }

    public FlowResponseMessage createFlowResponseMessage() {
        return new FlowResponseMessage();
    }

    public PingMessage createPingMessage() {
        return new PingMessage();
    }

    public MulticastProtocolMessage createMulticastProtocolMessage() {
        return new MulticastProtocolMessage();
    }

    public NodeStatusChangeMessage createNodeStatusChangeMessage() {
        return new NodeStatusChangeMessage();
    }

    public NodeConnectionStatusRequestMessage createNodeConnectionStatusRequestMessage() {
        return new NodeConnectionStatusRequestMessage();
    }

    public NodeConnectionStatusResponseMessage createNodeConnectionStatusResponsetMessage() {
        return new NodeConnectionStatusResponseMessage();
    }

    public HeartbeatResponseMessage createHeartbeatResponse() {
        return new HeartbeatResponseMessage();
    }

    public ClusterWorkloadRequestMessage createClusterWorkloadRequest() {
        return new ClusterWorkloadRequestMessage();
    }

    public ClusterWorkloadResponseMessage createClusterWorkloadResponse() {
        return new ClusterWorkloadResponseMessage();
    }

}
