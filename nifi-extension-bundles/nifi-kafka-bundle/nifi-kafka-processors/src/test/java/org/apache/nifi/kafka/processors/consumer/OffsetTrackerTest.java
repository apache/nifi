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
package org.apache.nifi.kafka.processors.consumer;

import org.apache.nifi.kafka.service.api.common.TopicPartitionSummary;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OffsetTrackerTest {

    private static final String FIRST_TOPIC = "orders";
    private static final String SECOND_TOPIC = "shipments";
    private static final int FIRST_PARTITION = 0;
    private static final int SECOND_PARTITION = 1;
    private static final byte[] RECORD_KEY = "key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIRST_VALUE = "alpha".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SECOND_VALUE = "beta".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FIRST_KEY = "k1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SECOND_KEY = "k2".getBytes(StandardCharsets.UTF_8);
    private static final long SECOND_TOPIC_BUNDLED_COUNT = 3L;
    private static final byte[] NEWLINE_DEMARCATOR = new byte[]{10};

    @Test
    void testUpdateAggregatesRecordsAndBytesByPartition() {
        final OffsetTracker offsetTracker = new OffsetTracker();
        recordConsumed(offsetTracker, newByteRecord(FIRST_TOPIC, FIRST_PARTITION, RECORD_KEY, FIRST_VALUE, 1));
        recordConsumed(offsetTracker, newByteRecord(FIRST_TOPIC, FIRST_PARTITION, null, SECOND_VALUE, 1));
        recordConsumed(offsetTracker, newByteRecord(FIRST_TOPIC, SECOND_PARTITION, RECORD_KEY, FIRST_VALUE, 1));
        recordConsumed(offsetTracker, newByteRecord(SECOND_TOPIC, FIRST_PARTITION, null, SECOND_VALUE, SECOND_TOPIC_BUNDLED_COUNT));
        offsetTracker.update(newByteRecord(FIRST_TOPIC, FIRST_PARTITION, RECORD_KEY, FIRST_VALUE, 1));
        offsetTracker.update(newByteRecord(FIRST_TOPIC, FIRST_PARTITION, null, SECOND_VALUE, 1));
        offsetTracker.update(newByteRecord(FIRST_TOPIC, SECOND_PARTITION, RECORD_KEY, FIRST_VALUE, 1));
        offsetTracker.update(newByteRecord(SECOND_TOPIC, FIRST_PARTITION, null, SECOND_VALUE, SECOND_TOPIC_BUNDLED_COUNT));

        final TopicPartitionSummary firstTopicFirstPartition = new TopicPartitionSummary(FIRST_TOPIC, FIRST_PARTITION);
        final TopicPartitionSummary firstTopicSecondPartition = new TopicPartitionSummary(FIRST_TOPIC, SECOND_PARTITION);
        final TopicPartitionSummary secondTopicFirstPartition = new TopicPartitionSummary(SECOND_TOPIC, FIRST_PARTITION);

        final long firstTopicFirstPartitionBytes = RECORD_KEY.length + FIRST_VALUE.length + SECOND_VALUE.length;
        final long firstTopicSecondPartitionBytes = RECORD_KEY.length + FIRST_VALUE.length;
        final long secondTopicFirstPartitionBytes = SECOND_VALUE.length;
        final long expectedTotalRecordSize = firstTopicFirstPartitionBytes + firstTopicSecondPartitionBytes + secondTopicFirstPartitionBytes;

        assertEquals(2L, offsetTracker.getPartitionRecords().get(firstTopicFirstPartition));
        assertEquals(1L, offsetTracker.getPartitionRecords().get(firstTopicSecondPartition));
        assertEquals(SECOND_TOPIC_BUNDLED_COUNT, offsetTracker.getPartitionRecords().get(secondTopicFirstPartition));

        assertEquals(firstTopicFirstPartitionBytes, offsetTracker.getPartitionBytes().get(firstTopicFirstPartition));
        assertEquals(firstTopicSecondPartitionBytes, offsetTracker.getPartitionBytes().get(firstTopicSecondPartition));
        assertEquals(secondTopicFirstPartitionBytes, offsetTracker.getPartitionBytes().get(secondTopicFirstPartition));

        assertEquals(3L, offsetTracker.getRecordCounts().get(FIRST_TOPIC));
        assertEquals(SECOND_TOPIC_BUNDLED_COUNT, offsetTracker.getRecordCounts().get(SECOND_TOPIC));
        assertEquals(expectedTotalRecordSize, offsetTracker.getTotalRecordSize());
    }

    @Test
    void testRecordConsumedKeepsOriginalBytesWhenUpdateSeesBundledRecord() {
        final OffsetTracker offsetTracker = new OffsetTracker();
        final ByteRecord firstRecord = newByteRecord(FIRST_TOPIC, FIRST_PARTITION, FIRST_KEY, FIRST_VALUE, 1);
        final ByteRecord secondRecord = newByteRecord(FIRST_TOPIC, FIRST_PARTITION, SECOND_KEY, SECOND_VALUE, 1);
        final TopicPartitionSummary topicPartition = new TopicPartitionSummary(FIRST_TOPIC, FIRST_PARTITION);
        final long originalBytes = FIRST_KEY.length + FIRST_VALUE.length + SECOND_KEY.length + SECOND_VALUE.length;

        recordConsumed(offsetTracker, firstRecord);
        recordConsumed(offsetTracker, secondRecord);

        final byte[] bundledValue = new byte[FIRST_VALUE.length + NEWLINE_DEMARCATOR.length + SECOND_VALUE.length];
        System.arraycopy(FIRST_VALUE, 0, bundledValue, 0, FIRST_VALUE.length);
        System.arraycopy(NEWLINE_DEMARCATOR, 0, bundledValue, FIRST_VALUE.length, NEWLINE_DEMARCATOR.length);
        System.arraycopy(SECOND_VALUE, 0, bundledValue, FIRST_VALUE.length + NEWLINE_DEMARCATOR.length, SECOND_VALUE.length);
        offsetTracker.update(newByteRecord(FIRST_TOPIC, FIRST_PARTITION, null, bundledValue, 2));

        assertEquals(2L, offsetTracker.getPartitionRecords().get(topicPartition));
        assertEquals(originalBytes, offsetTracker.getPartitionBytes().get(topicPartition));
        assertEquals(bundledValue.length, offsetTracker.getTotalRecordSize());
    }

    @Test
    void testClearRemovesPartitionAggregates() {
        final OffsetTracker offsetTracker = new OffsetTracker();
        recordConsumed(offsetTracker, newByteRecord(FIRST_TOPIC, FIRST_PARTITION, RECORD_KEY, FIRST_VALUE, 2));
        offsetTracker.update(newByteRecord(FIRST_TOPIC, FIRST_PARTITION, RECORD_KEY, FIRST_VALUE, 2));

        offsetTracker.clear();

        assertTrue(offsetTracker.getPartitionRecords().isEmpty());
        assertTrue(offsetTracker.getPartitionBytes().isEmpty());
        assertTrue(offsetTracker.getRecordCounts().isEmpty());
        assertEquals(0L, offsetTracker.getTotalRecordSize());
    }

    private static void recordConsumed(final OffsetTracker offsetTracker, final ByteRecord consumerRecord) {
        offsetTracker.recordConsumed(
                new TopicPartitionSummary(consumerRecord.getTopic(), consumerRecord.getPartition()),
                consumerRecord.getBundledCount(),
                consumedBytes(consumerRecord));
    }

    private static long consumedBytes(final ByteRecord consumerRecord) {
        long recordSize = consumerRecord.getValue().length;
        if (consumerRecord.getKey().isPresent()) {
            recordSize += consumerRecord.getKey().get().length;
        }
        return recordSize;
    }

    private static ByteRecord newByteRecord(final String topic, final int partition, final byte[] key, final byte[] value, final long bundledCount) {
        return new ByteRecord(topic, partition, 0, 0L, List.of(), key, value, bundledCount);
    }
}
