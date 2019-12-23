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

import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.function.Function;

public class AvailableSeekingPartitioner implements FlowFilePartitioner {
    private final FlowFilePartitioner partitionerDelegate;
    private final Function<QueueSize, Boolean> fullCheck;

    public AvailableSeekingPartitioner(FlowFilePartitioner partitionerDelegate, Function<QueueSize, Boolean> fullCheck) {
        this.partitionerDelegate = partitionerDelegate;
        this.fullCheck = fullCheck;
    }

    @Override
    public QueuePartition getPartition(FlowFileRecord flowFile, QueuePartition[] partitions, QueuePartition localPartition) {
        for (int attemptCounter = 0; attemptCounter < partitions.length; attemptCounter++) {
            QueuePartition selectedPartition = partitionerDelegate.getPartition(flowFile, partitions, localPartition);

            if (!fullCheck.apply(selectedPartition.size())) {
                return selectedPartition;
            }
        }

        // As we don't want to return null here, fall back to original logic if all partitions are full.
        return partitionerDelegate.getPartition(flowFile, partitions, localPartition);
    }

    @Override
    public boolean isRebalanceOnClusterResize() {
        return partitionerDelegate.isRebalanceOnClusterResize();
    }

    @Override
    public boolean isRebalanceOnFailure() {
        return partitionerDelegate.isRebalanceOnFailure();
    }

    @Override
    public boolean isPartitionStatic() {
        return partitionerDelegate.isPartitionStatic();
    }
}
