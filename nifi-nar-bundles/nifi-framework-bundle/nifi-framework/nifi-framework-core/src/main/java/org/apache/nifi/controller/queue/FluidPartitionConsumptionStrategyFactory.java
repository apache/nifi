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

public class FluidPartitionConsumptionStrategyFactory implements PartitionConsumptionStrategyFactory {
    final String partitionId;
    final Supplier<MaxQueueSize> maxQueueSizeSupplier;
    final Supplier<QueueSize> queueSizeSupplier;
    final Map<String, PartitionStatus> partitionStatuses;

    public FluidPartitionConsumptionStrategyFactory(
            final String partitionId, final Supplier<MaxQueueSize> maxQueueSizeSupplier,
            final Supplier<QueueSize> queueSizeSupplier, final Map<String, PartitionStatus> partitionStatuses) {
        this.partitionId = partitionId;
        this.maxQueueSizeSupplier = maxQueueSizeSupplier;
        this.queueSizeSupplier = queueSizeSupplier;
        this.partitionStatuses = partitionStatuses;
    }

    @Override
    public PartitionConsumptionStrategy create() {
        return new CompositePartitionConsumptionStrategy(
                new MaxSizePartitionConsumptionStrategy(partitionId, maxQueueSizeSupplier),
                createCountingPartitionConsumptionStrategy()
        );
    }

    private PartitionConsumptionStrategy createCountingPartitionConsumptionStrategy() {
        final PartitionStatus status = partitionStatuses.get(partitionId);
        final float ratioBase = (status == null) ? 0 : (float) status.getFlowFilesOut();

        final Integer sum = partitionStatuses.values().stream().map(s -> s.getFlowFilesOut()).reduce(0, Integer::sum);
        final float ration = (sum == 0) ? 0 : ratioBase / (float) sum;

        final int queueSize = queueSizeSupplier.get().getObjectCount();
        final int flowFilesToTransfer = Double.valueOf(Math.ceil(ration * queueSize)).intValue();

        return new CountingPartitionConsumptionStrategy(flowFilesToTransfer);
    }
}
