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

package org.apache.nifi.controller.queue.clustered;

import org.apache.nifi.controller.queue.FlowFileQueueContents;
import org.apache.nifi.controller.queue.clustered.partition.FlowFilePartitioner;
import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.Collection;
import java.util.function.Function;

public interface TransferFailureDestination {
    /**
     * Puts all of the given FlowFiles to the appropriate destination queue
     *
     * @param flowFiles the FlowFiles to transfer
     * @param partitionerUsed the partitioner that was used to determine that the given FlowFiles should be grouped together in the first place
     */
    void putAll(Collection<FlowFileRecord> flowFiles, FlowFilePartitioner partitionerUsed);

    /**
     * Puts all of the given FlowFile Queue Contents to the appropriate destination queue
     *
     * @param queueContents a function that returns the FlowFileQueueContents, given a Partition Name
     * @param partitionerUsed the partitioner that was used to determine that the given FlowFiles should be grouped together in the first place
     */
    void putAll(Function<String, FlowFileQueueContents> queueContents, FlowFilePartitioner partitionerUsed);

    /**
     * Indicates whether or not FlowFiles will need to be rebalanced when transferred to the destination.
     *
     * @param  partitionerUsed the partitioner that was used to determine that FlowFiles should be grouped together in the first place
     * @return <code>true</code> if FlowFiles will be rebalanced when transferred, <code>false</code> otherwise
     */
    boolean isRebalanceOnFailure(FlowFilePartitioner partitionerUsed);
}
