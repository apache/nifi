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
package org.apache.nifi.kafka.service.api;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.kafka.service.api.consumer.KafkaConsumerService;
import org.apache.nifi.kafka.service.api.consumer.PollingContext;
import org.apache.nifi.kafka.service.api.producer.KafkaProducerService;
import org.apache.nifi.kafka.service.api.producer.ProducerConfiguration;

import java.util.concurrent.ExecutionException;

public interface KafkaConnectionService extends ControllerService {

    KafkaConsumerService getConsumerService(PollingContext pollingContext);

    KafkaProducerService getProducerService(ProducerConfiguration producerConfiguration);

    String getBrokerUri();

    /**
     * Compute the total committed-offset lag (records remaining) for the consumer group and topics described
     * by the supplied {@link PollingContext}. Uses a Kafka Admin client internally and does NOT create a
     * consumer or join the consumer group, so calling this method does not trigger a group rebalance and is
     * safe to invoke while other live consumers are reading from the same group.
     *
     * <p>For every partition of every topic that matches the supplied context, the value contributed to the
     * total is {@code endOffset - committedOffset}. When the consumer group has no committed offset for a
     * particular partition, the partition contributes its full {@code endOffset - beginningOffset} (i.e.
     * every record in the partition is treated as backlog for that group). Partitions are discovered via
     * {@code describeTopics} when the polling context lists explicit topic names, or via {@code listTopics}
     * filtered by the polling context's topic pattern when a pattern is configured.</p>
     *
     * @param pollingContext describes the topics or topic pattern and the consumer group id to query.
     * @return total records remaining for the configured consumer group across every partition of every
     *         matched topic. A return value of {@code 0} means the group is currently caught up.
     * @throws ExecutionException if any Kafka Admin operation fails
     * @throws InterruptedException if the calling thread is interrupted while waiting for an Admin response
     */
    long getCommittedOffsetLag(PollingContext pollingContext) throws ExecutionException, InterruptedException;
}
