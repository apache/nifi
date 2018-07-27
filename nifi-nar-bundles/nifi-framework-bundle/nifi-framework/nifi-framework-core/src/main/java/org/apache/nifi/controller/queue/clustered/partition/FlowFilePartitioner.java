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

import org.apache.nifi.controller.repository.FlowFileRecord;

public interface FlowFilePartitioner {

    /**
     * Determines which partition the given FlowFile should go to
     *
     * @param flowFile the FlowFile to partition
     * @param partitions the partitions to choose from
     * @param localPartition the local partition, which is also included in the given array of partitions
     * @return the partition for the FlowFile
     */
    QueuePartition getPartition(FlowFileRecord flowFile, QueuePartition[] partitions,  QueuePartition localPartition);

    /**
     * @return <code>true</code> if a change in the size of a cluster should result in re-balancing all FlowFiles in queue,
     *         <code>false</code> if a change in the size of a cluster does not require re-balancing.
     */
    boolean isRebalanceOnClusterResize();

    /**
     * @return <code>true</code> if FlowFiles should be rebalanced to another partition if they cannot be sent to the designated peer,
     * <code>false</code> if a failure should result in the FlowFiles remaining in same partition.
     */
    boolean isRebalanceOnFailure();

    /**
     * @return <code>true</code> if the return value of {@link #getPartition(FlowFileRecord, QueuePartition[], QueuePartition)} will be the same
     * regardless of how many times it is called or which FlowFiles are passed.
     */
    default boolean isPartitionStatic() {
        return false;
    }
}
