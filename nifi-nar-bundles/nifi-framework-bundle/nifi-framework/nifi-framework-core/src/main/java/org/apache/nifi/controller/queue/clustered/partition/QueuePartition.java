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

package org.apache.nifi.controller.queue.clustered.partition;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.queue.DropFlowFileRequest;
import org.apache.nifi.controller.queue.FlowFileQueueContents;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.flowfile.FlowFilePrioritizer;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Represents a portion of a FlowFile Queue such that a FlowFile Queue can be broken into
 * a local queue partition and 0 or more Remote Queue Partitions.
 */
public interface QueuePartition {
    /**
     * Discovers any FlowFiles that have been swapped out, returning a summary of the swap files' contents
     * @return a summary of the swap files' contents
     */
    SwapSummary recoverSwappedFlowFiles();

    /**
     * @return the Node Identifier that this Queue Partition corresponds to, or and empty Optional if the Node Identifier is not yet known.
     */
    Optional<NodeIdentifier> getNodeIdentifier();

    /**
     * @return the name of the Partition that is used when serializing swap flowfiles in order to denote that a swap file belongs to this partition
     */
    String getSwapPartitionName();

    /**
     * Adds the given FlowFile to this partition
     * @param flowFile the FlowFile to add
     */
    void put(FlowFileRecord flowFile);

    /**
     * Adds the given FlowFiles to this partition
     * @param flowFiles the FlowFiles to add
     */
    void putAll(Collection<FlowFileRecord> flowFiles);

    /**
     * Drops the FlowFiles in this partition
     * @param dropRequest the FlowFile Drop Request
     * @param requestor the user making the request
     */
    void dropFlowFiles(DropFlowFileRequest dropRequest, String requestor);

    /**
     * Updates the prioritizers to use when queueing data
     * @param newPriorities the new priorities
     */
    void setPriorities(List<FlowFilePrioritizer> newPriorities);

    /**
     * Starts distributing FlowFiles to their desired destinations
     *
     * @param flowFilePartitioner the Partitioner that is being used to determine which FlowFiles should belong to this Partition
     */
    void start(FlowFilePartitioner flowFilePartitioner);

    /**
     * Stop distributing FlowFiles to other nodes in the cluster. This does not interrupt any active transactions but will cause the
     * partition to not create any more transactions until it is started again.
     */
    void stop();

    /**
     * Provides a {@link FlowFileQueueContents} that can be transferred to another partition
     * @param newPartitionName the name of the partition to which the data is being transferred (see {@link #getSwapPartitionName()}.
     * @return the contents of the queue
     */
    FlowFileQueueContents packageForRebalance(String newPartitionName);

    /**
     * @return the current size of the partition's queue
     */
    QueueSize size();
}
