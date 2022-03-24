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

import org.apache.nifi.controller.queue.FlowFileQueueContents;
import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.Collection;

/**
 * A partition whose sole job it is to redistribute FlowFiles to the appropriate partitions.
 */
public interface RebalancingPartition extends QueuePartition {

    /**
     * Inherits all of the FlowFiles, including FlowFiles that have been swaped out, in order to
     * redistribute them across the cluster
     *
     * @param queueContents the contents of a FlowFileQueue (or partition)
     */
    void rebalance(FlowFileQueueContents queueContents);

    /**
     * Inherits all of the givne FlowFiles in order to redistribute them across the cluster
     *
     * @param flowFiles the FlowFiles to redistribute
     */
    void rebalance(Collection<FlowFileRecord> flowFiles);

}
