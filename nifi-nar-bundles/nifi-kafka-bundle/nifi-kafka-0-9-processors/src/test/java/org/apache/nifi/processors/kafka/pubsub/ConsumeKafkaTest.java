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

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConsumeKafkaTest {

    static class MockConsumerPool extends ConsumerPool {

        final int actualMaxLeases;
        final List<String> actualTopics;
        final Map<String, String> actualKafkaProperties;
        boolean throwKafkaExceptionOnPoll = false;
        boolean throwKafkaExceptionOnCommit = false;
        Queue<ConsumerRecords<byte[], byte[]>> nextPlannedRecordsQueue = new ArrayDeque<>();
        Map<TopicPartition, OffsetAndMetadata> nextExpectedCommitOffsets = null;
        Map<TopicPartition, OffsetAndMetadata> actualCommitOffsets = null;
        boolean wasConsumerLeasePoisoned = false;
        boolean wasConsumerLeaseClosed = false;
        boolean wasPoolClosed = false;

        public MockConsumerPool(int maxLeases, List<String> topics, Map<String, String> kafkaProperties, ComponentLog logger) {
            super(maxLeases, topics, kafkaProperties, null);
            actualMaxLeases = maxLeases;
            actualTopics = topics;
            actualKafkaProperties = kafkaProperties;
        }

        @Override
        public ConsumerLease obtainConsumer() {
            return new ConsumerLease() {
                @Override
                public ConsumerRecords<byte[], byte[]> poll() {
                    if (throwKafkaExceptionOnPoll) {
                        throw new KafkaException("i planned to fail");
                    }
                    final ConsumerRecords<byte[], byte[]> records = nextPlannedRecordsQueue.poll();
                    return (records == null) ? ConsumerRecords.empty() : records;
                }

                @Override
                public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
                    if (throwKafkaExceptionOnCommit) {
                        throw new KafkaException("i planned to fail");
                    }
                    actualCommitOffsets = offsets;
                }

                @Override
                public void poison() {
                    wasConsumerLeasePoisoned = true;
                }

                @Override
                public void close() {
                    wasConsumerLeaseClosed = true;
                }
            };
        }

        @Override
        public void close() {
            wasPoolClosed = true;
        }

        void resetState() {
            throwKafkaExceptionOnPoll = false;
            throwKafkaExceptionOnCommit = false;
            nextPlannedRecordsQueue = null;
            nextExpectedCommitOffsets = null;
            wasConsumerLeasePoisoned = false;
            wasConsumerLeaseClosed = false;
            wasPoolClosed = false;
        }

    }

    @Test
    public void validateCustomValidatorSettings() throws Exception {
        ConsumeKafka consumeKafka = new ConsumeKafka();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka.TOPICS, "foo");
        runner.setProperty(ConsumeKafka.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, ConsumeKafka.OFFSET_EARLIEST);
        runner.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        runner.assertValid();
        runner.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "Foo");
        runner.assertNotValid();
        runner.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        runner.assertValid();
        runner.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        runner.assertValid();
        runner.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        runner.assertNotValid();
    }

    @Test
    public void validatePropertiesValidation() throws Exception {
        ConsumeKafka consumeKafka = new ConsumeKafka();
        TestRunner runner = TestRunners.newTestRunner(consumeKafka);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "okeydokey:1234");
        runner.setProperty(ConsumeKafka.TOPICS, "foo");
        runner.setProperty(ConsumeKafka.GROUP_ID, "foo");
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, ConsumeKafka.OFFSET_EARLIEST);

        runner.removeProperty(ConsumeKafka.GROUP_ID);
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("invalid because group.id is required"));
        }

        runner.setProperty(ConsumeKafka.GROUP_ID, "");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
        }

        runner.setProperty(ConsumeKafka.GROUP_ID, "  ");
        try {
            runner.assertValid();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getMessage().contains("must contain at least one character that is not white space"));
        }
    }

    @Test
    public void validateGetAllMessages() throws Exception {
        String groupName = "validateGetAllMessages";

        final byte[][] firstPassValues = new byte[][]{
            "Hello-1".getBytes(StandardCharsets.UTF_8),
            "Hello-2".getBytes(StandardCharsets.UTF_8),
            "Hello-3".getBytes(StandardCharsets.UTF_8)
        };
        final ConsumerRecords<byte[], byte[]> firstRecs = createConsumerRecords("foo", 1, 1L, firstPassValues);

        final byte[][] secondPassValues = new byte[][]{
            "Hello-4".getBytes(StandardCharsets.UTF_8),
            "Hello-5".getBytes(StandardCharsets.UTF_8),
            "Hello-6".getBytes(StandardCharsets.UTF_8)
        };
        final ConsumerRecords<byte[], byte[]> secondRecs = createConsumerRecords("bar", 1, 1L, secondPassValues);

        final List<String> expectedTopics = new ArrayList<>();
        expectedTopics.add("foo");
        expectedTopics.add("bar");
        final MockConsumerPool mockPool = new MockConsumerPool(1, expectedTopics, Collections.EMPTY_MAP, null);
        mockPool.nextPlannedRecordsQueue.add(firstRecs);
        mockPool.nextPlannedRecordsQueue.add(secondRecs);

        ConsumeKafka proc = new ConsumeKafka() {
            @Override
            protected ConsumerPool createConsumerPool(final int maxLeases, final List<String> topics, final Map<String, String> props, final ComponentLog log) {
                return mockPool;
            }
        };
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "0.0.0.0:1234");
        runner.setProperty(ConsumeKafka.TOPICS, "foo,bar");
        runner.setProperty(ConsumeKafka.GROUP_ID, groupName);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, ConsumeKafka.OFFSET_EARLIEST);

        runner.run(1, false);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.REL_SUCCESS);

        assertEquals(expectedTopics, mockPool.actualTopics);

        assertEquals(1, flowFiles.stream().map(ff -> new String(ff.toByteArray())).filter(content -> content.equals("Hello-1")).count());
        assertEquals(1, flowFiles.stream().map(ff -> new String(ff.toByteArray())).filter(content -> content.equals("Hello-2")).count());
        assertEquals(1, flowFiles.stream().map(ff -> new String(ff.toByteArray())).filter(content -> content.equals("Hello-3")).count());

        if (mockPool.nextPlannedRecordsQueue.isEmpty()) {
            assertEquals(1, flowFiles.stream().map(ff -> new String(ff.toByteArray())).filter(content -> content.equals("Hello-4")).count());
            assertEquals(1, flowFiles.stream().map(ff -> new String(ff.toByteArray())).filter(content -> content.equals("Hello-5")).count());
            assertEquals(1, flowFiles.stream().map(ff -> new String(ff.toByteArray())).filter(content -> content.equals("Hello-6")).count());
            assertEquals(2, mockPool.actualCommitOffsets.size());
            assertEquals(4L, mockPool.actualCommitOffsets.get(new TopicPartition("foo", 1)).offset());
            assertEquals(4L, mockPool.actualCommitOffsets.get(new TopicPartition("bar", 1)).offset());
        } else {
            assertEquals(2, mockPool.actualCommitOffsets.size());
            assertEquals(4L, mockPool.actualCommitOffsets.get(new TopicPartition("foo", 1)).offset());
        }

        //asert that all consumers were closed as expected
        //assert that the consumer pool was properly closed
        assertFalse(mockPool.wasConsumerLeasePoisoned);
        assertTrue(mockPool.wasConsumerLeaseClosed);
        assertFalse(mockPool.wasPoolClosed);
        runner.run(1, true);
        assertFalse(mockPool.wasConsumerLeasePoisoned);
        assertTrue(mockPool.wasConsumerLeaseClosed);
        assertTrue(mockPool.wasPoolClosed);

    }

    @Test
    public void validateGetLotsOfMessages() throws Exception {
        String groupName = "validateGetLotsOfMessages";

        final byte[][] firstPassValues = new byte[10010][1];
        for (final byte[] value : firstPassValues) {
            value[0] = 0x12;
        }
        final ConsumerRecords<byte[], byte[]> firstRecs = createConsumerRecords("foo", 1, 1L, firstPassValues);

        final byte[][] secondPassValues = new byte[][]{
            "Hello-4".getBytes(StandardCharsets.UTF_8),
            "Hello-5".getBytes(StandardCharsets.UTF_8),
            "Hello-6".getBytes(StandardCharsets.UTF_8)
        };
        final ConsumerRecords<byte[], byte[]> secondRecs = createConsumerRecords("bar", 1, 1L, secondPassValues);

        final List<String> expectedTopics = new ArrayList<>();
        expectedTopics.add("foo");
        expectedTopics.add("bar");
        final MockConsumerPool mockPool = new MockConsumerPool(1, expectedTopics, Collections.EMPTY_MAP, null);
        mockPool.nextPlannedRecordsQueue.add(firstRecs);
        mockPool.nextPlannedRecordsQueue.add(secondRecs);

        ConsumeKafka proc = new ConsumeKafka() {
            @Override
            protected ConsumerPool createConsumerPool(final int maxLeases, final List<String> topics, final Map<String, String> props, final ComponentLog log) {
                return mockPool;
            }
        };
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "0.0.0.0:1234");
        runner.setProperty(ConsumeKafka.TOPICS, "foo,bar");
        runner.setProperty(ConsumeKafka.GROUP_ID, groupName);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, ConsumeKafka.OFFSET_EARLIEST);

        runner.run(1, false);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.REL_SUCCESS);

        assertEquals(10010, flowFiles.stream().map(ff -> ff.toByteArray()).filter(content -> content.length == 1 && content[0] == 0x12).count());
        assertEquals(1, mockPool.nextPlannedRecordsQueue.size());

        assertEquals(1, mockPool.actualCommitOffsets.size());
        assertEquals(10011L, mockPool.actualCommitOffsets.get(new TopicPartition("foo", 1)).offset());

        //asert that all consumers were closed as expected
        //assert that the consumer pool was properly closed
        assertFalse(mockPool.wasConsumerLeasePoisoned);
        assertTrue(mockPool.wasConsumerLeaseClosed);
        assertFalse(mockPool.wasPoolClosed);
        runner.run(1, true);
        assertFalse(mockPool.wasConsumerLeasePoisoned);
        assertTrue(mockPool.wasConsumerLeaseClosed);
        assertTrue(mockPool.wasPoolClosed);

    }

    private ConsumerRecords<byte[], byte[]> createConsumerRecords(final String topic, final int partition, final long startingOffset, final byte[][] rawRecords) {
        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> map = new HashMap<>();
        final TopicPartition tPart = new TopicPartition(topic, partition);
        final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        long offset = startingOffset;
        for (final byte[] rawRecord : rawRecords) {
            final ConsumerRecord<byte[], byte[]> rec = new ConsumerRecord(topic, partition, offset++, UUID.randomUUID().toString().getBytes(), rawRecord);
            records.add(rec);
        }
        map.put(tPart, records);
        return new ConsumerRecords(map);
    }

    private ConsumerRecords<byte[], byte[]> mergeRecords(final ConsumerRecords<byte[], byte[]>... records) {
        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> map = new HashMap<>();
        for (final ConsumerRecords<byte[], byte[]> rec : records) {
            rec.partitions().stream().forEach((part) -> {
                final List<ConsumerRecord<byte[], byte[]>> conRecs = rec.records(part);
                if (map.get(part) != null) {
                    throw new IllegalStateException("already have that topic/partition in the record map");
                }
                map.put(part, conRecs);
            });
        }
        return new ConsumerRecords<>(map);
    }

    @Test
    public void validateGetAllMessagesWithProvidedDemarcator() throws Exception {
        String groupName = "validateGetAllMessagesWithProvidedDemarcator";

        final byte[][] firstPassValues = new byte[][]{
            "Hello-1".getBytes(StandardCharsets.UTF_8),
            "Hello-2".getBytes(StandardCharsets.UTF_8),
            "Hello-3".getBytes(StandardCharsets.UTF_8)
        };

        final byte[][] secondPassValues = new byte[][]{
            "Hello-4".getBytes(StandardCharsets.UTF_8),
            "Hello-5".getBytes(StandardCharsets.UTF_8),
            "Hello-6".getBytes(StandardCharsets.UTF_8)
        };
        final ConsumerRecords<byte[], byte[]> consumerRecs = mergeRecords(
                createConsumerRecords("foo", 1, 1L, firstPassValues),
                createConsumerRecords("bar", 1, 1L, secondPassValues)
        );

        final List<String> expectedTopics = new ArrayList<>();
        expectedTopics.add("foo");
        expectedTopics.add("bar");
        final MockConsumerPool mockPool = new MockConsumerPool(1, expectedTopics, Collections.EMPTY_MAP, null);
        mockPool.nextPlannedRecordsQueue.add(consumerRecs);

        ConsumeKafka proc = new ConsumeKafka() {
            @Override
            protected ConsumerPool createConsumerPool(final int maxLeases, final List<String> topics, final Map<String, String> props, final ComponentLog log) {
                return mockPool;
            }
        };

        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "0.0.0.0:1234");
        runner.setProperty(ConsumeKafka.TOPICS, "foo,bar");
        runner.setProperty(ConsumeKafka.GROUP_ID, groupName);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, ConsumeKafka.OFFSET_EARLIEST);
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, "blah");

        runner.run(1, false);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.REL_SUCCESS);

        assertEquals(2, flowFiles.size());

        assertEquals(1, flowFiles.stream().map(ff -> new String(ff.toByteArray())).filter(content -> content.equals("Hello-1blahHello-2blahHello-3")).count());
        assertEquals(1, flowFiles.stream().map(ff -> new String(ff.toByteArray())).filter(content -> content.equals("Hello-4blahHello-5blahHello-6")).count());

        //asert that all consumers were closed as expected
        //assert that the consumer pool was properly closed
        assertFalse(mockPool.wasConsumerLeasePoisoned);
        assertTrue(mockPool.wasConsumerLeaseClosed);
        assertFalse(mockPool.wasPoolClosed);
        runner.run(1, true);
        assertFalse(mockPool.wasConsumerLeasePoisoned);
        assertTrue(mockPool.wasConsumerLeaseClosed);
        assertTrue(mockPool.wasPoolClosed);

        assertEquals(2, mockPool.actualCommitOffsets.size());
        assertEquals(4L, mockPool.actualCommitOffsets.get(new TopicPartition("foo", 1)).offset());
        assertEquals(4L, mockPool.actualCommitOffsets.get(new TopicPartition("bar", 1)).offset());
    }

    @Test
    public void validatePollException() throws Exception {
        String groupName = "validatePollException";

        final byte[][] firstPassValues = new byte[][]{
            "Hello-1".getBytes(StandardCharsets.UTF_8),
            "Hello-2".getBytes(StandardCharsets.UTF_8),
            "Hello-3".getBytes(StandardCharsets.UTF_8)
        };

        final ConsumerRecords<byte[], byte[]> consumerRecs = mergeRecords(
                createConsumerRecords("foo", 1, 1L, firstPassValues)
        );

        final List<String> expectedTopics = new ArrayList<>();
        expectedTopics.add("foo");
        final MockConsumerPool mockPool = new MockConsumerPool(1, expectedTopics, Collections.EMPTY_MAP, null);
        mockPool.nextPlannedRecordsQueue.add(consumerRecs);
        mockPool.throwKafkaExceptionOnPoll = true;

        ConsumeKafka proc = new ConsumeKafka() {
            @Override
            protected ConsumerPool createConsumerPool(final int maxLeases, final List<String> topics, final Map<String, String> props, final ComponentLog log) {
                return mockPool;
            }
        };

        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "0.0.0.0:1234");
        runner.setProperty(ConsumeKafka.TOPICS, "foo");
        runner.setProperty(ConsumeKafka.GROUP_ID, groupName);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, ConsumeKafka.OFFSET_EARLIEST);
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, "blah");

        runner.run(1, true);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.REL_SUCCESS);

        assertEquals(0, flowFiles.size());
        assertNull(null, mockPool.actualCommitOffsets);

        //asert that all consumers were closed as expected
        //assert that the consumer pool was properly closed
        assertTrue(mockPool.wasConsumerLeasePoisoned);
        assertTrue(mockPool.wasConsumerLeaseClosed);
        assertTrue(mockPool.wasPoolClosed);
    }

    @Test
    public void validateCommitOffsetException() throws Exception {
        String groupName = "validateCommitOffsetException";

        final byte[][] firstPassValues = new byte[][]{
            "Hello-1".getBytes(StandardCharsets.UTF_8),
            "Hello-2".getBytes(StandardCharsets.UTF_8),
            "Hello-3".getBytes(StandardCharsets.UTF_8)
        };

        final ConsumerRecords<byte[], byte[]> consumerRecs = mergeRecords(
                createConsumerRecords("foo", 1, 1L, firstPassValues)
        );

        final List<String> expectedTopics = new ArrayList<>();
        expectedTopics.add("foo");
        final MockConsumerPool mockPool = new MockConsumerPool(1, expectedTopics, Collections.EMPTY_MAP, null);
        mockPool.nextPlannedRecordsQueue.add(consumerRecs);
        mockPool.throwKafkaExceptionOnCommit = true;

        ConsumeKafka proc = new ConsumeKafka() {
            @Override
            protected ConsumerPool createConsumerPool(final int maxLeases, final List<String> topics, final Map<String, String> props, final ComponentLog log) {
                return mockPool;
            }
        };

        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "0.0.0.0:1234");
        runner.setProperty(ConsumeKafka.TOPICS, "foo");
        runner.setProperty(ConsumeKafka.GROUP_ID, groupName);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, ConsumeKafka.OFFSET_EARLIEST);
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, "blah");

        runner.run(1, true);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        assertEquals(1, flowFiles.stream().map(ff -> new String(ff.toByteArray())).filter(content -> content.equals("Hello-1blahHello-2blahHello-3")).count());

        //asert that all consumers were closed as expected
        //assert that the consumer pool was properly closed
        assertTrue(mockPool.wasConsumerLeasePoisoned);
        assertTrue(mockPool.wasConsumerLeaseClosed);
        assertTrue(mockPool.wasPoolClosed);

        assertNull(null, mockPool.actualCommitOffsets);
    }
}
