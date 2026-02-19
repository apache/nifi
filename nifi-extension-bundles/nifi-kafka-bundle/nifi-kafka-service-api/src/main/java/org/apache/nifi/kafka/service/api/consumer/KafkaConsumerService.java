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
import org.apache.nifi.kafka.service.api.common.TopicPartitionSummary;
import org.apache.nifi.kafka.service.api.record.ByteRecord;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

/**
 * Kafka Consumer Service must be closed to avoid leaking connection resources
 */
public interface KafkaConsumerService extends Closeable {
    /**
     * Commit record information to Kafka Brokers
     *
     * @param pollingSummary Polling Summary information to be committed
     */
    void commit(PollingSummary pollingSummary);

    /**
     * Rolls back the offsets of the records so that any records that have been polled since the last commit are re-polled
     */
    void rollback();

    /**
     * @return <code>true</code> if the service is closed; <code>false</code> otherwise
     */
    boolean isClosed();

    /**
     * Poll Subscriptions for Records
     *
     * @return Stream of Records or empty when none returned
     */
    Iterable<ByteRecord> poll(Duration maxWaitDuration);

    /**
     * Get Partition State information for subscription
     *
     * @return List of Partition State information
     */
    List<PartitionState> getPartitionStates();

    /**
     * Get current lag (in records) for the specified topic partition
     *
     * @param topicPartitionSummary Topic Partition to query for consumer lag
     * @return OptionalLong containing the current lag or empty when not available
     */
    OptionalLong currentLag(TopicPartitionSummary topicPartitionSummary);

    /**
     * Check if a Kafka consumer group rebalance has occurred and partitions were revoked.
     * When partitions are revoked, the processor should commit its session before calling
     * {@link #commitOffsetsForRevokedPartitions()} to avoid data loss.
     *
     * @return <code>true</code> if partitions have been revoked and are pending commit; <code>false</code> otherwise
     */
    default boolean hasRevokedPartitions() {
        return false;
    }

    /**
     * Get the collection of partitions that were revoked during a rebalance and are pending commit.
     * This does not clear the revoked partitions; call {@link #commitOffsetsForRevokedPartitions()}
     * or {@link #clearRevokedPartitions()} to clear them.
     *
     * @return Collection of revoked partition states, or empty collection if none
     */
    default Collection<PartitionState> getRevokedPartitions() {
        return Collections.emptyList();
    }

    /**
     * Commit offsets for partitions that were revoked during a rebalance.
     * This method should be called by the processor AFTER successfully committing its session
     * to ensure no data loss occurs. After calling this method, the revoked partitions are cleared.
     */
    default void commitOffsetsForRevokedPartitions() {
    }

    /**
     * Clear the revoked partitions without committing their offsets.
     * This should be called when the processor decides not to commit (e.g., on rollback).
     */
    default void clearRevokedPartitions() {
    }

    /**
     * Set a callback to be invoked during consumer group rebalance when partitions are revoked.
     * The callback is invoked inside {@code onPartitionsRevoked()} before Kafka offsets are committed,
     * allowing the processor to commit its session (FlowFiles) first.
     * <p>
     * This is critical for preventing both data loss and duplicates during rebalance:
     * <ul>
     *   <li>The callback commits the NiFi session (FlowFiles) first</li>
     *   <li>Then Kafka offsets are committed while consumer is still in valid state</li>
     * </ul>
     * </p>
     *
     * @param callback the callback to invoke during rebalance, or null to clear
     */
    default void setRebalanceCallback(RebalanceCallback callback) {
    }
}
