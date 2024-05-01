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
package org.apache.nifi.kafka.processors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ConsumeKafkaOffsetsIT extends AbstractConsumeKafkaIT {

    private static final String RECORD_VALUE = "recordA,recordB,recordC";

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        addKafkaConnectionService(runner);

        runner.setProperty(ConsumeKafka.CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());
    }

    @Test
    void testVerifyOffsetsNotCommitted() throws ExecutionException, InterruptedException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));
        final String[] values = RECORD_VALUE.split(",");
        produceAndConsume(topic, groupId, values, false);

        final Set<TopicPartition> topicPartitions = Collections.singleton(new TopicPartition(topic, 0));
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                getKafkaConsumerProperties(kafkaContainer.getBootstrapServers(), groupId, false))) {
            final Map<TopicPartition, OffsetAndMetadata> committedOffsets = consumer.committed(topicPartitions);
            assertEquals(1, committedOffsets.entrySet().size());
            Map.Entry<TopicPartition, OffsetAndMetadata> entry = committedOffsets.entrySet().iterator().next();
            assertEquals(topic, entry.getKey().topic());
            assertNull(entry.getValue());
        }
    }

    @Test
    void testVerifyOffsetsCommitted() throws ExecutionException, InterruptedException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));
        final String[] values = RECORD_VALUE.split(",");
        produceAndConsume(topic, groupId, values, true);

        final Set<TopicPartition> topicPartitions = Collections.singleton(new TopicPartition(topic, 0));
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                getKafkaConsumerProperties(kafkaContainer.getBootstrapServers(), groupId, false))) {
            final Map<TopicPartition, OffsetAndMetadata> committedOffsets = consumer.committed(topicPartitions);
            assertEquals(1, committedOffsets.entrySet().size());
            Map.Entry<TopicPartition, OffsetAndMetadata> entry = committedOffsets.entrySet().iterator().next();
            assertEquals(topic, entry.getKey().topic());
            assertEquals(values.length - 1, entry.getValue().offset());
        }
    }

    private void produceAndConsume(final String topic, final String groupId, final String[] values, final boolean commitOffsets)
            throws ExecutionException, InterruptedException {
        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.COMMIT_OFFSETS, Boolean.toString(commitOffsets));

        runner.run(1, false, true);
        final Collection<ProducerRecord<String, String>> records = new ArrayList<>();
        for (String value : values) {
            records.add(new ProducerRecord<>(topic, null, (String) null, value, Collections.emptyList()));
        }
        produce(topic, records);
        final long pollUntil = System.currentTimeMillis() + DURATION_POLL.toMillis();
        while ((System.currentTimeMillis() < pollUntil) && (runner.getFlowFilesForRelationship("success").isEmpty())) {
            runner.run(1, false, false);
        }
        runner.assertTransferCount("success", values.length);
    }
}
