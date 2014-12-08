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

import org.apache.nifi.cluster.protocol.message.DisconnectMessage;
import org.apache.nifi.cluster.protocol.message.FlowRequestMessage;
import org.apache.nifi.cluster.protocol.message.FlowResponseMessage;
import org.apache.nifi.cluster.protocol.message.PrimaryRoleAssignmentMessage;
import org.apache.nifi.cluster.protocol.message.ReconnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ReconnectionResponseMessage;
import org.apache.nifi.reporting.BulletinRepository;

/**
 * An interface for sending protocol messages from the cluster manager to nodes.
 * 
 * @author unattributed
 */
public interface ClusterManagerProtocolSender {
    
    /**
     * Sends a "flow request" message to a node.
     * @param msg a message
     * @return the response
     * @throws ProtocolException if communication failed 
     */
    FlowResponseMessage requestFlow(FlowRequestMessage msg) throws ProtocolException;

    /**
     * Sends a "reconnection request" message to a node.
     * @param msg a message
     * @return 
     * @throws ProtocolException if communication failed
     */
    ReconnectionResponseMessage requestReconnection(ReconnectionRequestMessage msg) throws ProtocolException;
    
    /**
     * Sends a "disconnection request" message to a node.
     * @param msg a message
     * @throws ProtocolException if communication failed
     */
    void disconnect(DisconnectMessage msg) throws ProtocolException;
    
    /**
     * Sends an "assign primary role" message to a node.
     * @param msg a message
     * @throws ProtocolException if communication failed 
     */
    void assignPrimaryRole(PrimaryRoleAssignmentMessage msg) throws ProtocolException;

    /**
     * Sets the {@link BulletinRepository} that can be used to report bulletins
     * @param bulletinRepository
     */
    void setBulletinRepository(final BulletinRepository bulletinRepository);
}
