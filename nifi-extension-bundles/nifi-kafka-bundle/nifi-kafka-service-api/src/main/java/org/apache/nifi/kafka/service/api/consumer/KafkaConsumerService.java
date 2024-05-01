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
import org.apache.nifi.kafka.service.api.record.ByteRecord;

import java.util.List;

/**
 * Kafka Consumer Service must be closed to avoid leaking connection resources
 */
public interface KafkaConsumerService extends AutoCloseable {
    /**
     * Commit record information to Kafka Brokers
     *
     * @param pollingSummary Polling Summary information to be committed
     */
    void commit(PollingSummary pollingSummary);

    /**
     * Poll Subscriptions for Records
     *
     * @param pollingContext Polling Context containing subscription information
     * @return Stream of Records or empty when none returned
     */
    Iterable<ByteRecord> poll(PollingContext pollingContext);

    /**
     * Get Partition State information for subscription
     *
     * @param pollingContext Polling Context containing subscription information
     * @return List of Partition State information
     */
    List<PartitionState> getPartitionStates(PollingContext pollingContext);
}
