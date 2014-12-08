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

import org.apache.nifi.cluster.protocol.message.ConnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ConnectionResponseMessage;
import org.apache.nifi.cluster.protocol.message.ControllerStartupFailureMessage;
import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;
import org.apache.nifi.cluster.protocol.message.NodeBulletinsMessage;
import org.apache.nifi.cluster.protocol.message.ReconnectionFailureMessage;

/**
 * An interface for sending protocol messages from a node to the cluster manager.
 * @author unattributed
 */
public interface NodeProtocolSender {
    
    /**
     * Sends a "connection request" message to the cluster manager.
     * @param msg a message
     * @return the response
     * @throws UnknownServiceAddressException if the cluster manager's address is not known
     * @throws ProtocolException if communication failed
     */
    ConnectionResponseMessage requestConnection(ConnectionRequestMessage msg) throws ProtocolException, UnknownServiceAddressException;
    
    /**
     * Sends a "heartbeat" message to the cluster manager.
     * @param msg a message
     * @throws UnknownServiceAddressException if the cluster manager's address is not known
     * @throws ProtocolException if communication failed
     */
    void heartbeat(HeartbeatMessage msg) throws ProtocolException, UnknownServiceAddressException;
    
    /**
     * Sends a bulletins message to the cluster manager.
     * @param msg
     * @throws ProtocolException
     * @throws UnknownServiceAddressException 
     */
    void sendBulletins(NodeBulletinsMessage msg) throws ProtocolException, UnknownServiceAddressException;
    
    /**
     * Sends a failure notification if the controller was unable start.
     * @param msg a message
     * @throws UnknownServiceAddressException if the cluster manager's address is not known
     * @throws ProtocolException if communication failed
     */
    void notifyControllerStartupFailure(ControllerStartupFailureMessage msg) throws ProtocolException, UnknownServiceAddressException;
    
    /**
     * Sends a failure notification if the node was unable to reconnect to the cluster
     * @param msg a message
     * @throws UnknownServiceAddressException if the cluster manager's address is not known
     * @throws ProtocolException if communication failed
     */
    void notifyReconnectionFailure(ReconnectionFailureMessage msg) throws ProtocolException, UnknownServiceAddressException;
    
}
