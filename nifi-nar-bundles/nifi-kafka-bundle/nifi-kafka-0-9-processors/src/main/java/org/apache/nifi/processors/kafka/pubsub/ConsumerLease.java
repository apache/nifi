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
package org.apache.nifi.processors.kafka.pubsub;

import java.io.Closeable;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

/**
 * This class represents a lease to access a Kafka Consumer object. The lease is
 * intended to be obtained from a ConsumerPool. The lease is closeable to allow
 * for the clean model of a try w/resources whereby non-exceptional cases mean
 * the lease will be returned to the pool for future use by others. A given
 * lease may only belong to a single thread a time.
 */
public interface ConsumerLease extends Closeable {

    /**
     * Executes a poll on the underlying Kafka Consumer.
     *
     * @return ConsumerRecords retrieved in the poll.
     * @throws KafkaException if issue occurs talking to underlying resource.
     */
    ConsumerRecords<byte[], byte[]> poll() throws KafkaException;

    /**
     * Notifies Kafka to commit the offsets for the specified topic/partition
     * pairs to the specified offsets w/the given metadata. This can offer
     * higher performance than the other commitOffsets call as it allows the
     * kafka client to collect more data from Kafka before committing the
     * offsets.
     *
     * @param offsets offsets
     * @throws KafkaException if issue occurs talking to underlying resource.
     */
    void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) throws KafkaException;

    /**
     * Notifies that this lease is poisoned and should not be reused.
     */
    void poison();

    /**
     * Notifies that this lease is to be returned. The pool may optionally reuse
     * this lease with another client. No further references by the caller
     * should occur after calling close.
     */
    @Override
    void close();

}
