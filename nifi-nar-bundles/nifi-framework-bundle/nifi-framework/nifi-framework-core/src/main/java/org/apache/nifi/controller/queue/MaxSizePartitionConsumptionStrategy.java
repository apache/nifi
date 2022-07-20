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

import org.apache.nifi.controller.queue.clustered.dto.PartitionStatus;

import java.util.Map;
import java.util.function.Supplier;

public class MaxSizePartitionConsumptionStrategy implements PartitionConsumptionStrategy {
    private final String partitionId;
    private final Supplier<MaxQueueSize> maxSizeSupplier;

    public MaxSizePartitionConsumptionStrategy(final String partitionId, final Supplier<MaxQueueSize> maxSizeSupplier) {
        this.partitionId = partitionId;
        this.maxSizeSupplier = maxSizeSupplier;
    }

    @Override
    public boolean shouldSendFlowFile(final Map<String, PartitionStatus> statuses) {
        return !isFull(statuses.get(partitionId));
    }

    private boolean isFull(final PartitionStatus status) {
        final MaxQueueSize maxSize = maxSizeSupplier.get();

        // Check if max size is set
        if (maxSize.getMaxBytes() <= 0 && maxSize.getMaxCount() <= 0) {
            return false;
        }

        if (maxSize.getMaxCount() > 0 && status.getObjectCount() >= maxSize.getMaxCount()) {
            return true;
        }

        if (maxSize.getMaxBytes() > 0 && status.getTotalSizeBytes() >= maxSize.getMaxBytes()) {
            return true;
        }

        return false;
    }
}
