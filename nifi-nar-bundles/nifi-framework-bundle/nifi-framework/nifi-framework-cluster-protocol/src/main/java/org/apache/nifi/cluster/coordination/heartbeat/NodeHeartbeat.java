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

package org.apache.nifi.cluster.coordination.heartbeat;

import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.NodeIdentifier;

public interface NodeHeartbeat {
    /**
     * @return the time at which the node reported the heartbeat, according
     *         to the system that received the heartbeat
     */
    long getTimestamp();

    /**
     * @return the identifier of the node that sent the heartbeat
     */
    NodeIdentifier getNodeIdentifier();

    /**
     * @return the Connection Status reported by the node
     */
    NodeConnectionStatus getConnectionStatus();

    /**
     * @return the number of FlowFiles that are queued up on the node
     */
    int getFlowFileCount();

    /**
     * @return the total size of all FlowFiles that are queued up on the node
     */
    long getFlowFileBytes();

    /**
     * @return the number of threads that are actively running in Processors and Reporting Tasks on the node
     */
    int getActiveThreadCount();

    /**
     * @return the time that the node reports having started NiFi
     */
    long getSystemStartTime();
}
