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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.service.consumer.Kafka3ConsumerService;
import org.apache.nifi.kafka.service.consumer.Subscription;
import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Integration tests for verifying that ConsumeKafka correctly handles consumer group rebalances
 * without causing duplicate message processing.
 */
class ConsumeKafkaRebalanceIT extends AbstractConsumeKafkaIT {

    private static final int NUM_PARTITIONS = 3;
    private static final int MESSAGES_PER_PARTITION = 20;

    /**
     * Tests that when onPartitionsRevoked is called (simulating rebalance), the consumer
     * correctly commits offsets, and a subsequent consumer in the same group doesn't
     * re-consume the same messages (no duplicates).
     *
     * This test:
     * 1. Produces messages to a multi-partition topic
     * 2. Consumer 1 polls and processes messages
     * 3. Simulates rebalance by calling onPartitionsRevoked on Consumer 1
     * 4. Consumer 2 joins and continues consuming from committed offsets
     * 5. Verifies no duplicate messages were consumed
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testRebalanceDoesNotCauseDuplicates() throws Exception {
        final String topic = "rebalance-test-" + UUID.randomUUID();
        final String groupId = "rebalance-group-" + UUID.randomUUID();
        final int totalMessages = NUM_PARTITIONS * MESSAGES_PER_PARTITION;

        createTopic(topic, NUM_PARTITIONS);
        produceMessagesToTopic(topic, NUM_PARTITIONS, MESSAGES_PER_PARTITION);

        final Set<String> consumedMessages = new HashSet<>();
        final AtomicInteger duplicateCount = new AtomicInteger(0);
        final ComponentLog mockLog = mock(ComponentLog.class);

        final Properties props1 = getConsumerProperties(groupId);
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer1 = new KafkaConsumer<>(props1)) {
            final Subscription subscription = new Subscription(groupId, Collections.singletonList(topic), AutoOffsetReset.EARLIEST);
            final Kafka3ConsumerService service1 = new Kafka3ConsumerService(mockLog, kafkaConsumer1, subscription);

            int consumer1Count = 0;
            int maxAttempts = 20;
            while (consumer1Count < totalMessages / 2 && maxAttempts-- > 0) {
                for (ByteRecord record : service1.poll(Duration.ofSeconds(2))) {
                    final String messageId = record.getTopic() + "-" + record.getPartition() + "-" + record.getOffset();
                    if (!consumedMessages.add(messageId)) {
                        duplicateCount.incrementAndGet();
                    }
                    consumer1Count++;
                }
            }

            final Set<TopicPartition> assignment = kafkaConsumer1.assignment();
            service1.onPartitionsRevoked(assignment);
            // Simulate processor committing offsets after successful session commit
            service1.commitOffsetsForRevokedPartitions();
            service1.close();
        }

        final Properties props2 = getConsumerProperties(groupId);
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer2 = new KafkaConsumer<>(props2)) {
            final Subscription subscription = new Subscription(groupId, Collections.singletonList(topic), AutoOffsetReset.EARLIEST);
            final Kafka3ConsumerService service2 = new Kafka3ConsumerService(mockLog, kafkaConsumer2, subscription);

            int emptyPolls = 0;
            while (emptyPolls < 5 && consumedMessages.size() < totalMessages) {
                boolean hasRecords = false;
                for (ByteRecord record : service2.poll(Duration.ofSeconds(2))) {
                    hasRecords = true;
                    final String messageId = record.getTopic() + "-" + record.getPartition() + "-" + record.getOffset();
                    if (!consumedMessages.add(messageId)) {
                        duplicateCount.incrementAndGet();
                    }
                }
                if (!hasRecords) {
                    emptyPolls++;
                } else {
                    emptyPolls = 0;
                }
            }

            service2.close();
        }

        assertEquals(0, duplicateCount.get(),
                "Expected no duplicate messages but found " + duplicateCount.get());
        assertEquals(totalMessages, consumedMessages.size(),
                "Expected to consume " + totalMessages + " unique messages but got " + consumedMessages.size());
    }

    /**
     * Tests that offsets can be committed after rebalance when processor calls commitOffsetsForRevokedPartitions.
     *
     * This test:
     * 1. Creates a consumer and polls messages
     * 2. Manually invokes onPartitionsRevoked (simulating what Kafka does during rebalance)
     * 3. Calls commitOffsetsForRevokedPartitions (simulating processor committing after session commit)
     * 4. Verifies that offsets were committed to Kafka
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testOffsetsCommittedDuringRebalance() throws Exception {
        final String topic = "rebalance-offset-test-" + UUID.randomUUID();
        final String groupId = "rebalance-offset-group-" + UUID.randomUUID();
        final int messagesPerPartition = 10;

        createTopic(topic, NUM_PARTITIONS);
        produceMessagesToTopic(topic, NUM_PARTITIONS, messagesPerPartition);

        final ComponentLog mockLog = mock(ComponentLog.class);
        final Properties props = getConsumerProperties(groupId);

        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(props)) {
            final Subscription subscription = new Subscription(groupId, Collections.singletonList(topic), AutoOffsetReset.EARLIEST);
            final Kafka3ConsumerService service = new Kafka3ConsumerService(mockLog, kafkaConsumer, subscription);

            int polledCount = 0;
            int maxAttempts = 20;
            while (polledCount < 15 && maxAttempts-- > 0) {
                final Iterator<ByteRecord> iterator = service.poll(Duration.ofSeconds(2)).iterator();
                while (iterator.hasNext()) {
                    iterator.next();
                    polledCount++;
                }
            }

            assertTrue(polledCount > 0, "Should have polled at least some messages");

            final Set<TopicPartition> assignment = kafkaConsumer.assignment();
            assertFalse(assignment.isEmpty(), "Consumer should have partition assignments");

            service.onPartitionsRevoked(assignment);
            // Simulate processor committing offsets after successful session commit
            service.commitOffsetsForRevokedPartitions();
            service.close();
        }

        try (KafkaConsumer<byte[], byte[]> verifyConsumer = new KafkaConsumer<>(getConsumerProperties(groupId))) {
            final Set<TopicPartition> partitions = new HashSet<>();
            for (int i = 0; i < NUM_PARTITIONS; i++) {
                partitions.add(new TopicPartition(topic, i));
            }

            final Map<TopicPartition, OffsetAndMetadata> committedOffsets = verifyConsumer.committed(partitions);

            long totalCommitted = committedOffsets.values().stream()
                    .filter(o -> o != null)
                    .mapToLong(OffsetAndMetadata::offset)
                    .sum();

            assertTrue(totalCommitted > 0,
                    "Expected offsets to be committed after commitOffsetsForRevokedPartitions, but total committed offset was " + totalCommitted);
        }
    }

    /**
     * Tests that records are NOT lost when a rebalance occurs before processing is complete.
     *
     * This test simulates the scenario where:
     * 1. Consumer polls and iterates through records (tracking offsets internally)
     * 2. Rebalance occurs (onPartitionsRevoked called) BEFORE the processor commits its session
     * 3. Consumer "fails" (simulating crash or processing failure) without committing offsets
     * 4. New consumer joins with the same group
     * 5. The new consumer receives the same records since they were never successfully processed
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testNoDataLossWhenRebalanceOccursBeforeProcessingComplete() throws Exception {
        final String topic = "dataloss-test-" + UUID.randomUUID();
        final String groupId = "dataloss-group-" + UUID.randomUUID();
        final int messagesPerPartition = 10;
        final int totalMessages = NUM_PARTITIONS * messagesPerPartition;

        createTopic(topic, NUM_PARTITIONS);
        produceMessagesToTopic(topic, NUM_PARTITIONS, messagesPerPartition);

        final ComponentLog mockLog = mock(ComponentLog.class);
        int recordsPolledByFirstConsumer = 0;

        // Consumer 1: Poll and iterate records, then rebalance occurs, but processing "fails"
        final Properties props1 = getConsumerProperties(groupId);
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer1 = new KafkaConsumer<>(props1)) {
            final Subscription subscription = new Subscription(groupId, Collections.singletonList(topic), AutoOffsetReset.EARLIEST);
            final Kafka3ConsumerService service1 = new Kafka3ConsumerService(mockLog, kafkaConsumer1, subscription);

            // Poll and iterate through records - this tracks offsets internally
            int maxAttempts = 20;
            while (recordsPolledByFirstConsumer < totalMessages && maxAttempts-- > 0) {
                final Iterator<ByteRecord> iterator = service1.poll(Duration.ofSeconds(2)).iterator();
                while (iterator.hasNext()) {
                    iterator.next();
                    recordsPolledByFirstConsumer++;
                }
            }

            assertTrue(recordsPolledByFirstConsumer > 0, "First consumer should have polled some records");

            // Simulate rebalance occurring before processor commits its session
            final Set<TopicPartition> assignment = kafkaConsumer1.assignment();
            assertFalse(assignment.isEmpty(), "Consumer should have partition assignments");
            service1.onPartitionsRevoked(assignment);

            // DO NOT call any "commit" or "process" method - simulating that the processor
            // never completed processing (e.g., session commit failed, process crashed, etc.)

            service1.close();
        }

        // Consumer 2: Should receive the SAME records because processing was never completed
        int recordsPolledBySecondConsumer = 0;
        final Properties props2 = getConsumerProperties(groupId);
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer2 = new KafkaConsumer<>(props2)) {
            final Subscription subscription = new Subscription(groupId, Collections.singletonList(topic), AutoOffsetReset.EARLIEST);
            final Kafka3ConsumerService service2 = new Kafka3ConsumerService(mockLog, kafkaConsumer2, subscription);

            // Poll for records - if no data loss, we should get the same records again
            int emptyPolls = 0;
            while (emptyPolls < 5) {
                boolean hasRecords = false;
                final Iterator<ByteRecord> iterator = service2.poll(Duration.ofSeconds(2)).iterator();
                while (iterator.hasNext()) {
                    iterator.next();
                    hasRecords = true;
                    recordsPolledBySecondConsumer++;
                }
                if (!hasRecords) {
                    emptyPolls++;
                } else {
                    emptyPolls = 0;
                }
            }

            service2.close();
        }

        // Records should NOT be lost - the second consumer should receive
        // at least the records that were polled by the first consumer but never processed
        assertTrue(recordsPolledBySecondConsumer >= recordsPolledByFirstConsumer,
                "Data loss detected! First consumer polled " + recordsPolledByFirstConsumer +
                " records but second consumer only received " + recordsPolledBySecondConsumer +
                " records. Expected second consumer to receive at least " + recordsPolledByFirstConsumer +
                " records since processing was never completed.");
    }

    /**
     * Produces messages to a specific topic with a given number of partitions.
     */
    private void produceMessagesToTopic(final String topic, final int numPartitions, final int messagesPerPartition) throws Exception {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int partition = 0; partition < numPartitions; partition++) {
                for (int i = 0; i < messagesPerPartition; i++) {
                    final String key = "key-" + partition + "-" + i;
                    final String value = "value-" + partition + "-" + i;
                    producer.send(new ProducerRecord<>(topic, partition, key, value)).get();
                }
            }
        }
    }

    private void createTopic(final String topic, final int numPartitions) throws Exception {
        final Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());

        try (Admin admin = Admin.create(adminProps)) {
            final NewTopic newTopic = new NewTopic(topic, numPartitions, (short) 1);
            admin.createTopics(Collections.singletonList(newTopic)).all().get(30, TimeUnit.SECONDS);
            waitForTopicReady(admin, topic, numPartitions);
        }
    }

    private void waitForTopicReady(final Admin admin, final String topic, final int expectedPartitions) throws Exception {
        final long startTime = System.currentTimeMillis();
        final long timeoutMillis = 30000;

        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            try {
                final Map<String, TopicDescription> descriptions = admin.describeTopics(Collections.singletonList(topic))
                        .allTopicNames()
                        .get(10, TimeUnit.SECONDS);
                final TopicDescription description = descriptions.get(topic);
                if (description != null && description.partitions().size() == expectedPartitions) {
                    return;
                }
            } catch (ExecutionException ignored) {
                // Topic not ready yet, continue polling
            }
            Thread.sleep(100);
        }
        throw new RuntimeException("Topic " + topic + " not ready after " + timeoutMillis + "ms");
    }

    private Properties getConsumerProperties(final String groupId) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        // Use shorter session timeout to speed up rebalance detection
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        return props;
    }
}
