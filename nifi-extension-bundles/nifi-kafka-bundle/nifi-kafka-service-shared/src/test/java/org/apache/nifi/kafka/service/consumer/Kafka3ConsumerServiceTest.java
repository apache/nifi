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
package org.apache.nifi.kafka.service.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class Kafka3ConsumerServiceTest {

    private static final String TOPIC = "test-topic";
    private static final String GROUP_ID = "test-group";
    private static final int PARTITION_0 = 0;
    private static final int PARTITION_1 = 1;

    @Mock
    private Consumer<byte[], byte[]> consumer;

    @Mock
    private ComponentLog componentLog;

    @Captor
    private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsetsCaptor;

    private Kafka3ConsumerService consumerService;

    @BeforeEach
    void setUp() {
        final Subscription subscription = new Subscription(GROUP_ID, Collections.singletonList(TOPIC), AutoOffsetReset.EARLIEST);
        consumerService = new Kafka3ConsumerService(componentLog, consumer, subscription);
    }

    @Test
    void testOnPartitionsRevokedCommitsUncommittedOffsets() {
        // Arrange: Simulate polling records from two partitions
        final TopicPartition partition0 = new TopicPartition(TOPIC, PARTITION_0);
        final TopicPartition partition1 = new TopicPartition(TOPIC, PARTITION_1);

        final ConsumerRecord<byte[], byte[]> record0 = createRecord(TOPIC, PARTITION_0, 5L);
        final ConsumerRecord<byte[], byte[]> record1 = createRecord(TOPIC, PARTITION_1, 10L);

        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
        recordsMap.put(partition0, List.of(record0));
        recordsMap.put(partition1, List.of(record1));
        final ConsumerRecords<byte[], byte[]> consumerRecords = createConsumerRecords(recordsMap);

        when(consumer.poll(any(Duration.class))).thenReturn(consumerRecords);

        // Act: Poll records (this should track the offsets internally)
        final Iterable<ByteRecord> polledRecords = consumerService.poll(Duration.ofMillis(100));
        // Consume the iterator to ensure records are processed
        for (ByteRecord ignored : polledRecords) {
            // Just iterate through
        }

        // Act: Simulate rebalance - partitions being revoked
        final Collection<TopicPartition> revokedPartitions = List.of(partition0, partition1);
        consumerService.onPartitionsRevoked(revokedPartitions);

        // Assert: Verify that offsets were committed for the revoked partitions
        verify(consumer).commitSync(offsetsCaptor.capture());
        final Map<TopicPartition, OffsetAndMetadata> committedOffsets = offsetsCaptor.getValue();

        assertEquals(2, committedOffsets.size());
        // Offset should be record.offset + 1 (next offset to consume)
        assertEquals(6L, committedOffsets.get(partition0).offset());
        assertEquals(11L, committedOffsets.get(partition1).offset());
    }

    @Test
    void testOnPartitionsRevokedWithNoUncommittedOffsets() {
        // Arrange: No records polled
        final TopicPartition partition0 = new TopicPartition(TOPIC, PARTITION_0);

        // Act: Simulate rebalance without any prior polling
        consumerService.onPartitionsRevoked(List.of(partition0));

        // Assert: No commit should be called since there are no uncommitted offsets
        verify(consumer, never()).commitSync(anyMap());
    }

    @Test
    void testOnPartitionsRevokedOnlyCommitsRevokedPartitions() {
        // Arrange: Poll records from two partitions
        final TopicPartition partition0 = new TopicPartition(TOPIC, PARTITION_0);
        final TopicPartition partition1 = new TopicPartition(TOPIC, PARTITION_1);

        final ConsumerRecord<byte[], byte[]> record0 = createRecord(TOPIC, PARTITION_0, 5L);
        final ConsumerRecord<byte[], byte[]> record1 = createRecord(TOPIC, PARTITION_1, 10L);

        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
        recordsMap.put(partition0, List.of(record0));
        recordsMap.put(partition1, List.of(record1));
        final ConsumerRecords<byte[], byte[]> consumerRecords = createConsumerRecords(recordsMap);

        when(consumer.poll(any(Duration.class))).thenReturn(consumerRecords);

        // Act: Poll records
        final Iterable<ByteRecord> polledRecords = consumerService.poll(Duration.ofMillis(100));
        for (ByteRecord ignored : polledRecords) {
            // Just iterate through
        }

        // Act: Only revoke partition 0, keep partition 1
        consumerService.onPartitionsRevoked(List.of(partition0));

        // Assert: Only partition 0 should be committed
        verify(consumer).commitSync(offsetsCaptor.capture());
        final Map<TopicPartition, OffsetAndMetadata> committedOffsets = offsetsCaptor.getValue();

        assertEquals(1, committedOffsets.size());
        assertEquals(6L, committedOffsets.get(partition0).offset());
        assertFalse(committedOffsets.containsKey(partition1));
    }

    @Test
    void testOnPartitionsRevokedTracksMaxOffset() {
        // Arrange: Poll multiple records from same partition
        final TopicPartition partition0 = new TopicPartition(TOPIC, PARTITION_0);

        final ConsumerRecord<byte[], byte[]> record1 = createRecord(TOPIC, PARTITION_0, 5L);
        final ConsumerRecord<byte[], byte[]> record2 = createRecord(TOPIC, PARTITION_0, 7L);
        final ConsumerRecord<byte[], byte[]> record3 = createRecord(TOPIC, PARTITION_0, 6L); // Out of order

        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
        recordsMap.put(partition0, List.of(record1, record2, record3));
        final ConsumerRecords<byte[], byte[]> consumerRecords = createConsumerRecords(recordsMap);

        when(consumer.poll(any(Duration.class))).thenReturn(consumerRecords);

        // Act: Poll records
        final Iterable<ByteRecord> polledRecords = consumerService.poll(Duration.ofMillis(100));
        for (ByteRecord ignored : polledRecords) {
            // Just iterate through
        }

        // Act: Revoke partition
        consumerService.onPartitionsRevoked(List.of(partition0));

        // Assert: Should commit max offset + 1 (7 + 1 = 8)
        verify(consumer).commitSync(offsetsCaptor.capture());
        final Map<TopicPartition, OffsetAndMetadata> committedOffsets = offsetsCaptor.getValue();

        assertEquals(1, committedOffsets.size());
        assertEquals(8L, committedOffsets.get(partition0).offset());
    }

    @Test
    void testRollbackClearsUncommittedOffsets() {
        // Arrange: Poll records
        final TopicPartition partition0 = new TopicPartition(TOPIC, PARTITION_0);

        final ConsumerRecord<byte[], byte[]> record0 = createRecord(TOPIC, PARTITION_0, 5L);

        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
        recordsMap.put(partition0, List.of(record0));
        final ConsumerRecords<byte[], byte[]> consumerRecords = createConsumerRecords(recordsMap);

        when(consumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        when(consumer.assignment()).thenReturn(Collections.singleton(partition0));
        when(consumer.committed(any())).thenReturn(Collections.singletonMap(partition0, new OffsetAndMetadata(0L)));

        // Act: Poll records
        final Iterable<ByteRecord> polledRecords = consumerService.poll(Duration.ofMillis(100));
        for (ByteRecord ignored : polledRecords) {
            // Just iterate through
        }

        // Act: Rollback
        consumerService.rollback();

        // Act: Now trigger rebalance
        consumerService.onPartitionsRevoked(List.of(partition0));

        // Assert: No commit should happen because rollback cleared the uncommitted offsets
        verify(consumer, never()).commitSync(anyMap());
    }

    private ConsumerRecord<byte[], byte[]> createRecord(final String topic, final int partition, final long offset) {
        return new ConsumerRecord<>(
                topic,
                partition,
                offset,
                System.currentTimeMillis(),
                TimestampType.CREATE_TIME,
                0,
                0,
                null,
                "test-value".getBytes(),
                new RecordHeaders(),
                Optional.empty()
        );
    }

    private ConsumerRecords<byte[], byte[]> createConsumerRecords(
            final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap) {
        // Calculate next offsets from the records (max offset + 1 for each partition)
        final Map<TopicPartition, OffsetAndMetadata> nextOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> entry : recordsMap.entrySet()) {
            long maxOffset = entry.getValue().stream()
                    .mapToLong(ConsumerRecord::offset)
                    .max()
                    .orElse(-1L);
            nextOffsets.put(entry.getKey(), new OffsetAndMetadata(maxOffset + 1));
        }
        return new ConsumerRecords<>(recordsMap, nextOffsets);
    }
}
