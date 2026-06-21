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
package org.apache.nifi.kafka.service.consumer.share;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.nifi.kafka.service.api.consumer.share.Acknowledgement;
import org.apache.nifi.kafka.service.api.consumer.share.ShareAcknowledgementMode;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class Kafka4ShareConsumerServiceTest {

    private static final String TOPIC = "share-topic";
    private static final List<String> TOPICS = Collections.singletonList(TOPIC);
    private static final int PARTITION_0 = 0;

    @Mock
    private ShareConsumer<byte[], byte[]> shareConsumer;

    @Mock
    private ComponentLog componentLog;

    @Test
    void testConstructorSubscribesToConfiguredTopics() {
        new Kafka4ShareConsumerService(componentLog, shareConsumer, ShareAcknowledgementMode.EXPLICIT, TOPICS);
        verify(shareConsumer).subscribe(TOPICS);
    }

    @Test
    void testPollReturnsByteRecordsAndTracksPending() {
        final Kafka4ShareConsumerService service = newService(ShareAcknowledgementMode.EXPLICIT);
        final ConsumerRecord<byte[], byte[]> record0 = createRecord(TOPIC, PARTITION_0, 5L);
        final ConsumerRecord<byte[], byte[]> record1 = createRecord(TOPIC, PARTITION_0, 6L);
        when(shareConsumer.poll(any(Duration.class))).thenReturn(consumerRecords(record0, record1));

        final List<ByteRecord> byteRecords = collect(service.poll(Duration.ofMillis(100)));

        assertEquals(2, byteRecords.size());
        assertEquals(2, service.getPendingRecordCount());
        assertEquals(5L, byteRecords.get(0).getOffset());
        assertEquals(6L, byteRecords.get(1).getOffset());
    }

    @Test
    void testPollConvertsTombstoneToEmptyValue() {
        final Kafka4ShareConsumerService service = newService(ShareAcknowledgementMode.EXPLICIT);
        final ConsumerRecord<byte[], byte[]> tombstone = createTombstoneRecord(TOPIC, PARTITION_0, 7L);
        when(shareConsumer.poll(any(Duration.class))).thenReturn(consumerRecords(tombstone));

        final List<ByteRecord> byteRecords = collect(service.poll(Duration.ofMillis(100)));

        assertEquals(1, byteRecords.size());
        final ByteRecord byteRecord = byteRecords.get(0);
        assertNotNull(byteRecord.getValue());
        assertEquals(0, byteRecord.getValue().length);
    }

    @Test
    void testExplicitAcknowledgementsForwardedToConsumer() {
        final Kafka4ShareConsumerService service = newService(ShareAcknowledgementMode.EXPLICIT);
        final ConsumerRecord<byte[], byte[]> recordAccept = createRecord(TOPIC, PARTITION_0, 1L);
        final ConsumerRecord<byte[], byte[]> recordRelease = createRecord(TOPIC, PARTITION_0, 2L);
        final ConsumerRecord<byte[], byte[]> recordReject = createRecord(TOPIC, PARTITION_0, 3L);
        when(shareConsumer.poll(any(Duration.class))).thenReturn(consumerRecords(recordAccept, recordRelease, recordReject));

        final List<ByteRecord> byteRecords = collect(service.poll(Duration.ofMillis(100)));

        service.acknowledge(byteRecords.get(0), Acknowledgement.ACCEPT);
        service.acknowledge(byteRecords.get(1), Acknowledgement.RELEASE);
        service.acknowledge(byteRecords.get(2), Acknowledgement.REJECT);

        verify(shareConsumer).acknowledge(recordAccept, AcknowledgeType.ACCEPT);
        verify(shareConsumer).acknowledge(recordRelease, AcknowledgeType.RELEASE);
        verify(shareConsumer).acknowledge(recordReject, AcknowledgeType.REJECT);
        assertEquals(0, service.getPendingRecordCount());
    }

    @Test
    void testCommitInExplicitModeAcceptsRemainingPendingRecords() {
        final Kafka4ShareConsumerService service = newService(ShareAcknowledgementMode.EXPLICIT);
        final ConsumerRecord<byte[], byte[]> first = createRecord(TOPIC, PARTITION_0, 1L);
        final ConsumerRecord<byte[], byte[]> second = createRecord(TOPIC, PARTITION_0, 2L);
        when(shareConsumer.poll(any(Duration.class))).thenReturn(consumerRecords(first, second));
        when(shareConsumer.commitSync()).thenReturn(Collections.emptyMap());

        final List<ByteRecord> byteRecords = collect(service.poll(Duration.ofMillis(100)));
        service.acknowledge(byteRecords.get(0), Acknowledgement.ACCEPT);
        service.commit();

        verify(shareConsumer).acknowledge(first, AcknowledgeType.ACCEPT);
        verify(shareConsumer).acknowledge(second, AcknowledgeType.ACCEPT);
        verify(shareConsumer).commitSync();
        assertEquals(0, service.getPendingRecordCount());
    }

    @Test
    void testCommitInImplicitModeDoesNotCallAcknowledge() {
        final Kafka4ShareConsumerService service = newService(ShareAcknowledgementMode.IMPLICIT);
        final ConsumerRecord<byte[], byte[]> first = createRecord(TOPIC, PARTITION_0, 1L);
        when(shareConsumer.poll(any(Duration.class))).thenReturn(consumerRecords(first));
        when(shareConsumer.commitSync()).thenReturn(Collections.emptyMap());

        collect(service.poll(Duration.ofMillis(100)));
        service.commit();

        verify(shareConsumer, never()).acknowledge(any(ConsumerRecord.class), any(AcknowledgeType.class));
        verify(shareConsumer).commitSync();
    }

    @Test
    void testRollbackInExplicitModeReleasesPendingRecordsAndCommits() {
        final Kafka4ShareConsumerService service = newService(ShareAcknowledgementMode.EXPLICIT);
        final ConsumerRecord<byte[], byte[]> first = createRecord(TOPIC, PARTITION_0, 1L);
        final ConsumerRecord<byte[], byte[]> second = createRecord(TOPIC, PARTITION_0, 2L);
        when(shareConsumer.poll(any(Duration.class))).thenReturn(consumerRecords(first, second));
        when(shareConsumer.commitSync()).thenReturn(Collections.emptyMap());

        collect(service.poll(Duration.ofMillis(100)));
        service.rollback();

        verify(shareConsumer).acknowledge(first, AcknowledgeType.RELEASE);
        verify(shareConsumer).acknowledge(second, AcknowledgeType.RELEASE);
        verify(shareConsumer).commitSync();
        assertEquals(0, service.getPendingRecordCount());
    }

    @Test
    void testRollbackInImplicitModeClearsPendingWithoutAcknowledging() {
        final Kafka4ShareConsumerService service = newService(ShareAcknowledgementMode.IMPLICIT);
        final ConsumerRecord<byte[], byte[]> first = createRecord(TOPIC, PARTITION_0, 1L);
        when(shareConsumer.poll(any(Duration.class))).thenReturn(consumerRecords(first));

        collect(service.poll(Duration.ofMillis(100)));
        service.rollback();

        verify(shareConsumer, never()).acknowledge(any(ConsumerRecord.class), any(AcknowledgeType.class));
        verify(shareConsumer, never()).commitSync();
        assertEquals(0, service.getPendingRecordCount());
    }

    @Test
    void testRollbackWithNoPendingRecordsIsNoOp() {
        final Kafka4ShareConsumerService service = newService(ShareAcknowledgementMode.EXPLICIT);
        service.rollback();

        verify(shareConsumer, never()).acknowledge(any(ConsumerRecord.class), any(AcknowledgeType.class));
        verify(shareConsumer, never()).commitSync();
    }

    @Test
    void testAcknowledgeIgnoredInImplicitMode() {
        final Kafka4ShareConsumerService service = newService(ShareAcknowledgementMode.IMPLICIT);
        final ConsumerRecord<byte[], byte[]> first = createRecord(TOPIC, PARTITION_0, 1L);
        when(shareConsumer.poll(any(Duration.class))).thenReturn(consumerRecords(first));

        final List<ByteRecord> byteRecords = collect(service.poll(Duration.ofMillis(100)));
        service.acknowledge(byteRecords.get(0), Acknowledgement.RELEASE);

        verify(shareConsumer, never()).acknowledge(any(ConsumerRecord.class), any(AcknowledgeType.class));
    }

    @Test
    void testAcknowledgeForUnknownRecordIsIgnored() {
        final Kafka4ShareConsumerService service = newService(ShareAcknowledgementMode.EXPLICIT);
        final ByteRecord stranger = new ByteRecord("other-topic", 0, 99L, 0L, Collections.emptyList(), null, new byte[0], 1);

        service.acknowledge(stranger, Acknowledgement.ACCEPT);

        verify(shareConsumer, never()).acknowledge(any(ConsumerRecord.class), any(AcknowledgeType.class));
    }

    @Test
    void testCloseFlagsServiceClosed() {
        final Kafka4ShareConsumerService service = newService(ShareAcknowledgementMode.EXPLICIT);
        assertFalse(service.isClosed());

        service.close();

        assertTrue(service.isClosed());
        verify(shareConsumer).close();
    }

    @Test
    void testCommitClearsPendingEvenIfMultiplePollsBetweenCommits() {
        final Kafka4ShareConsumerService service = newService(ShareAcknowledgementMode.EXPLICIT);
        final ConsumerRecord<byte[], byte[]> first = createRecord(TOPIC, PARTITION_0, 1L);
        final ConsumerRecord<byte[], byte[]> second = createRecord(TOPIC, PARTITION_0, 2L);
        when(shareConsumer.poll(any(Duration.class)))
                .thenReturn(consumerRecords(first))
                .thenReturn(consumerRecords(second));
        when(shareConsumer.commitSync()).thenReturn(Collections.emptyMap());

        collect(service.poll(Duration.ofMillis(100)));
        collect(service.poll(Duration.ofMillis(100)));
        assertEquals(2, service.getPendingRecordCount());

        service.commit();

        verify(shareConsumer).acknowledge(eq(first), eq(AcknowledgeType.ACCEPT));
        verify(shareConsumer).acknowledge(eq(second), eq(AcknowledgeType.ACCEPT));
        verify(shareConsumer, times(1)).commitSync();
        assertEquals(0, service.getPendingRecordCount());
    }

    private Kafka4ShareConsumerService newService(final ShareAcknowledgementMode mode) {
        return new Kafka4ShareConsumerService(componentLog, shareConsumer, mode, TOPICS);
    }

    @SafeVarargs
    private ConsumerRecords<byte[], byte[]> consumerRecords(final ConsumerRecord<byte[], byte[]>... records) {
        if (records.length == 0) {
            return ConsumerRecords.empty();
        }
        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            recordsMap.computeIfAbsent(new TopicPartition(record.topic(), record.partition()), k -> new ArrayList<>()).add(record);
        }
        return new ConsumerRecords<>(recordsMap, Collections.emptyMap());
    }

    private List<ByteRecord> collect(final Iterable<ByteRecord> iterable) {
        final List<ByteRecord> list = new ArrayList<>();
        final Iterator<ByteRecord> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }

    private ConsumerRecord<byte[], byte[]> createRecord(final String topic, final int partition, final long offset) {
        return new ConsumerRecord<>(
                topic,
                partition,
                offset,
                System.currentTimeMillis(),
                TimestampType.CREATE_TIME,
                0,
                "value-%d".formatted(offset).length(),
                null,
                "value-%d".formatted(offset).getBytes(),
                new RecordHeaders(),
                Optional.empty()
        );
    }

    private ConsumerRecord<byte[], byte[]> createTombstoneRecord(final String topic, final int partition, final long offset) {
        return new ConsumerRecord<>(
                topic,
                partition,
                offset,
                System.currentTimeMillis(),
                TimestampType.CREATE_TIME,
                0,
                0,
                null,
                null,
                new RecordHeaders(),
                Optional.empty()
        );
    }
}
