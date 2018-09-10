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

package org.apache.nifi.controller.queue;

import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.Collection;

public interface LoadBalancedFlowFileQueue extends FlowFileQueue {
    /**
     * Adds the given FlowFiles to this queue, as they have been received from another node in the cluster
     * @param flowFiles the FlowFiles received from the peer
     */
    void receiveFromPeer(Collection<FlowFileRecord> flowFiles);

    /**
     * Distributes the given FlowFiles to the appropriate partitions. Unlike the {@link #putAll(Collection)} method,
     * this does not alter the size of the FlowFile Queue itself, as it is intended only to place the FlowFiles into
     * their appropriate partitions
     *
     * @param flowFiles the FlowFiles to distribute
     */
    void distributeToPartitions(Collection<FlowFileRecord> flowFiles);

    /**
     * Notifies the queue that the given FlowFiles have been successfully transferred to another node
     * @param flowFiles the FlowFiles that were transferred
     */
    void onTransfer(Collection<FlowFileRecord> flowFiles);

    /**
     * Notifies the queue the a transaction containing the given FlowFiles was aborted
     * @param flowFiles the FlowFiles in the transaction
     */
    void onAbort(Collection<FlowFileRecord> flowFiles);

    /**
     * Handles updating the repositories for the given FlowFiles, which have been expired
     * @param flowFiles the expired FlowFiles
     */
    void handleExpiredRecords(Collection<FlowFileRecord> flowFiles);
}
