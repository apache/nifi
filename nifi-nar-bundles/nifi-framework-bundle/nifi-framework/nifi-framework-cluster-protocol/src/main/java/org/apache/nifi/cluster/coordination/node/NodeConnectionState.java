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

import javax.xml.bind.annotation.XmlEnum;

@XmlEnum(String.class)
public enum NodeConnectionState {
    /**
     * A node has issued a connection request to the cluster, but has not yet
     * sent a heartbeat. A connecting node can transition to DISCONNECTED or CONNECTED. The cluster
     * will not accept any external requests to change the flow while any node is in
     * this state.
     */
    CONNECTING,

    /**
     * A node that is connected to the cluster. A connecting node transitions
     * to connected after the cluster receives the node's first heartbeat. A
     * connected node can transition to disconnecting.
     */
    CONNECTED,

    /**
     * A node that is in the process of offloading its flow files from the node.
     */
    OFFLOADING,

    /**
     * A node that is in the process of disconnecting from the cluster.
     * A DISCONNECTING node will always transition to DISCONNECTED.
     */
    DISCONNECTING,

    /**
     * A node that has offloaded its flow files from the node.
     */
    OFFLOADED,

    /**
     * A node that is not connected to the cluster.
     * A DISCONNECTED node can transition to CONNECTING.
     */
    DISCONNECTED,

    /**
     * A NodeConnectionState of REMOVED indicates that the node was removed from the cluster
     * and is used in order to notify other nodes in the cluster.
     */
    REMOVED;
}
