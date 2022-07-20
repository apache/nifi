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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CompositePartitionConsumptionStrategy implements PartitionConsumptionStrategy {
    private final Set<PartitionConsumptionStrategy> payload = new HashSet<>();

    public CompositePartitionConsumptionStrategy(final PartitionConsumptionStrategy... payload) {
        if (payload.length == 0) {
            throw new IllegalArgumentException("At least one PartitionConsumptionStrategy must be provided");
        }

        for (final PartitionConsumptionStrategy strategy: payload) {
            this.payload.add(strategy);
        }
    }

    @Override
    public boolean shouldSendFlowFile(final Map<String, PartitionStatus> statuses) {
        for (final PartitionConsumptionStrategy strategy : payload) {
            if (!strategy.shouldSendFlowFile(statuses)) {
                return false;
            }
        }

        return true;
    }
}
