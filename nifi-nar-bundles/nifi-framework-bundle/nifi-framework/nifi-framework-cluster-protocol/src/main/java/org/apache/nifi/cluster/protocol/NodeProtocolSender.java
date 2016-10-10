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

import org.apache.nifi.cluster.protocol.message.ClusterWorkloadRequestMessage;
import org.apache.nifi.cluster.protocol.message.ClusterWorkloadResponseMessage;
import org.apache.nifi.cluster.protocol.message.ConnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ConnectionResponseMessage;
import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;
import org.apache.nifi.cluster.protocol.message.HeartbeatResponseMessage;

/**
 * An interface for sending protocol messages from a node to the cluster
 * manager.
 *
 */
public interface NodeProtocolSender {

    /**
     * Sends a "connection request" message to the cluster manager.
     *
     * @param msg a message
     * @return the response
     * @throws UnknownServiceAddressException if the cluster manager's address
     * is not known
     * @throws ProtocolException if communication failed
     */
    ConnectionResponseMessage requestConnection(ConnectionRequestMessage msg) throws ProtocolException, UnknownServiceAddressException;

    /**
     * Sends a heartbeat to the address given
     *
     * @param msg the heartbeat message to send
     * @param address the address of the Cluster Coordinator in &lt;hostname&gt;:&lt;port&gt; format
     * @throws ProtocolException if unable to send the heartbeat
     *
     * @return the response from the Cluster Coordinator
     */
    HeartbeatResponseMessage heartbeat(HeartbeatMessage msg, String address) throws ProtocolException;

    /**
     * Sends a "cluster workflow request" message to the Cluster Coordinator.
     * @param msg a request message
     * @return the response from the Cluster Coordinator
     * @throws ProtocolException if communication failed
     */
    ClusterWorkloadResponseMessage clusterWorkload(ClusterWorkloadRequestMessage msg) throws ProtocolException;
}
