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
package org.apache.nifi.cluster.protocol;

import java.util.Set;

import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.message.OffloadMessage;
import org.apache.nifi.cluster.protocol.message.DisconnectMessage;
import org.apache.nifi.cluster.protocol.message.NodeStatusChangeMessage;
import org.apache.nifi.cluster.protocol.message.ReconnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ReconnectionResponseMessage;
import org.apache.nifi.reporting.BulletinRepository;

/**
 * An interface for sending protocol messages from the cluster coordinator to nodes.
 *
 */
public interface ClusterCoordinationProtocolSender {

    /**
     * Sends a "reconnection request" message to a node.
     *
     * @param msg a message
     * @return the response
     * @throws ProtocolException if communication failed
     */
    ReconnectionResponseMessage requestReconnection(ReconnectionRequestMessage msg) throws ProtocolException;

    /**
     * Sends an "offload request" message to a node.
     *
     * @param msg a message
     * @throws ProtocolException if communication failed
     */
    void offload(OffloadMessage msg) throws ProtocolException;

    /**
     * Sends a "disconnection request" message to a node.
     *
     * @param msg a message
     * @throws ProtocolException if communication failed
     */
    void disconnect(DisconnectMessage msg) throws ProtocolException;

    /**
     * Sets the {@link BulletinRepository} that can be used to report bulletins
     *
     * @param bulletinRepository repo
     */
    void setBulletinRepository(final BulletinRepository bulletinRepository);

    /**
     * Notifies all nodes in the given set that a node in the cluster has a new status
     *
     * @param nodesToNotify the nodes that should be notified of the change
     * @param msg the message that indicates which node's status changed and what it changed to
     */
    void notifyNodeStatusChange(Set<NodeIdentifier> nodesToNotify, NodeStatusChangeMessage msg);

    /**
     * Sends a request to the given hostname and port to request its connection status
     *
     * @return the connection status returned from the node at the given hostname & port
     */
    NodeConnectionStatus requestNodeConnectionStatus(String hostname, int port);
}
