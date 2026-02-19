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
package org.apache.nifi.kafka.service.api.consumer;

import org.apache.nifi.kafka.service.api.common.PartitionState;

import java.util.Collection;

/**
 * Callback interface for handling Kafka consumer group rebalance events.
 * <p>
 * When a rebalance occurs and partitions are being revoked from a consumer,
 * Kafka calls {@code onPartitionsRevoked()} on the ConsumerRebalanceListener.
 * This is the ONLY time when the consumer is still in a valid state to commit offsets.
 * After this callback returns, the consumer is no longer part of an active group
 * and any commit attempts will fail with RebalanceInProgressException.
 * </p>
 * <p>
 * This callback allows processors to be notified during the rebalance so they can
 * commit their session (FlowFiles) before Kafka offsets are committed. This ensures:
 * <ul>
 *   <li>No data loss: NiFi session is committed before Kafka offsets</li>
 *   <li>No duplicates: Kafka offsets are committed while consumer is still valid</li>
 * </ul>
 * </p>
 */
@FunctionalInterface
public interface RebalanceCallback {

    /**
     * Called during {@code onPartitionsRevoked()} when partitions with uncommitted offsets
     * are being revoked from this consumer.
     * <p>
     * The implementation should commit any pending work (e.g., NiFi session with FlowFiles)
     * for the specified partitions. After this method returns, Kafka offsets will be committed
     * for these partitions.
     * </p>
     *
     * @param revokedPartitions the partitions being revoked that have uncommitted offsets
     */
    void onPartitionsRevoked(Collection<PartitionState> revokedPartitions);
}
