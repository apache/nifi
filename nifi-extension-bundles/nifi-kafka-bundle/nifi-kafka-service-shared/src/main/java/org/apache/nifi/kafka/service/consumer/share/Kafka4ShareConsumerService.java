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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.nifi.kafka.service.api.consumer.share.Acknowledgement;
import org.apache.nifi.kafka.service.api.consumer.share.KafkaShareConsumerService;
import org.apache.nifi.kafka.service.api.consumer.share.ShareAcknowledgementMode;
import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.logging.ComponentLog;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Kafka share-group consumer implementation backed by {@link ShareConsumer} (KIP-932).
 * Requires Kafka 4.2+ brokers with share-group features enabled.
 *
 * <p>The {@link ShareConsumer} API is annotated {@code @Evolving} in the Kafka client and may
 * change in incompatible ways between minor Kafka releases.</p>
 */
public class Kafka4ShareConsumerService implements KafkaShareConsumerService, Closeable {

    private final ComponentLog componentLog;
    private final ShareConsumer<byte[], byte[]> consumer;
    private final ShareAcknowledgementMode acknowledgementMode;

    /**
     * Records delivered by the most recent {@link #poll(Duration)} that have not yet been
     * explicitly acknowledged. Tracked by topic-partition-offset coordinates so callers can
     * reference records via the {@link ByteRecord} they received from the service without the
     * service needing to expose the underlying {@link ConsumerRecord}. Insertion-order tracking
     * makes any commit-time auto-ACCEPT iteration deterministic.
     */
    private final Map<RecordCoordinates, ConsumerRecord<byte[], byte[]>> pendingRecords = new LinkedHashMap<>();

    private volatile boolean closed = false;

    public Kafka4ShareConsumerService(final ComponentLog componentLog,
                                      final ShareConsumer<byte[], byte[]> consumer,
                                      final ShareAcknowledgementMode acknowledgementMode,
                                      final Collection<String> topics) {
        this.componentLog = Objects.requireNonNull(componentLog, "Component Log required");
        this.consumer = Objects.requireNonNull(consumer, "Consumer required");
        this.acknowledgementMode = Objects.requireNonNull(acknowledgementMode, "Acknowledgement Mode required");
        Objects.requireNonNull(topics, "Topics required");

        consumer.subscribe(topics);
    }

    @Override
    public Iterable<ByteRecord> poll(final Duration maxWaitDuration) {
        final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(maxWaitDuration);
        if (consumerRecords.isEmpty()) {
            return List.of();
        }

        final List<ByteRecord> byteRecords = new ArrayList<>(consumerRecords.count());
        for (final ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
            final RecordCoordinates coordinates = RecordCoordinates.of(consumerRecord);
            pendingRecords.put(coordinates, consumerRecord);
            byteRecords.add(toByteRecord(consumerRecord));
        }
        return byteRecords;
    }

    @Override
    public void acknowledge(final ByteRecord record, final Acknowledgement acknowledgement) {
        Objects.requireNonNull(record, "Record required");
        Objects.requireNonNull(acknowledgement, "Acknowledgement required");

        if (acknowledgementMode != ShareAcknowledgementMode.EXPLICIT) {
            componentLog.debug("Per-record acknowledgement called but consumer is in [{}] mode; ignoring", acknowledgementMode);
            return;
        }

        final RecordCoordinates coordinates = RecordCoordinates.of(record);
        final ConsumerRecord<byte[], byte[]> consumerRecord = pendingRecords.remove(coordinates);
        if (consumerRecord == null) {
            componentLog.debug("Record {} not found among pending records; cannot acknowledge", coordinates);
            return;
        }

        consumer.acknowledge(consumerRecord, toAcknowledgeType(acknowledgement));
    }

    @Override
    public void commit() {
        if (acknowledgementMode == ShareAcknowledgementMode.EXPLICIT) {
            // Explicit mode requires every delivered record to be acknowledged before the next poll/commit.
            // Treat any record left unacknowledged at this point as ACCEPT so the commit can succeed.
            for (final ConsumerRecord<byte[], byte[]> remaining : pendingRecords.values()) {
                consumer.acknowledge(remaining, AcknowledgeType.ACCEPT);
            }
        }

        final Map<TopicIdPartition, Optional<KafkaException>> result = consumer.commitSync();
        pendingRecords.clear();
        logPartitionFailures("commit", result);
    }

    /**
     * {@inheritDoc}
     *
     * <p>In {@link ShareAcknowledgementMode#EXPLICIT} this acknowledges all pending records as
     * {@link AcknowledgeType#RELEASE} and commits, so the records are immediately eligible for
     * redelivery to another consumer in the share group.</p>
     *
     * <p>In {@link ShareAcknowledgementMode#IMPLICIT} per-record acknowledgement is not permitted,
     * and the next {@link #commit()} or {@link #poll(Duration)} would otherwise treat the records as
     * implicitly accepted. This implementation drops the local pending tracking, but the only way
     * to actually release the records is for the caller to {@link #close()} the consumer and let
     * the broker's record-acquisition lock expire (broker-level
     * {@code group.share.record.lock.duration.ms}). After that timeout the records become eligible
     * for redelivery to another consumer.</p>
     */
    @Override
    public void rollback() {
        if (pendingRecords.isEmpty()) {
            return;
        }

        if (acknowledgementMode == ShareAcknowledgementMode.EXPLICIT) {
            for (final ConsumerRecord<byte[], byte[]> consumerRecord : pendingRecords.values()) {
                consumer.acknowledge(consumerRecord, AcknowledgeType.RELEASE);
            }
            try {
                final Map<TopicIdPartition, Optional<KafkaException>> result = consumer.commitSync();
                logPartitionFailures("rollback", result);
            } catch (final Exception e) {
                componentLog.warn("Failed to commit RELEASE acknowledgements during rollback for {} records", pendingRecords.size(), e);
            }
            pendingRecords.clear();
        } else {
            componentLog.debug("Dropped {} pending records in implicit acknowledgement mode; redelivery requires consumer close and broker lock expiry",
                    pendingRecords.size());
            pendingRecords.clear();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
        consumer.close();
    }

    private void logPartitionFailures(final String operation, final Map<TopicIdPartition, Optional<KafkaException>> result) {
        for (final Map.Entry<TopicIdPartition, Optional<KafkaException>> entry : result.entrySet()) {
            final Optional<KafkaException> failure = entry.getValue();
            if (failure.isPresent()) {
                componentLog.warn("Share consumer {} reported failure for partition {}", operation, entry.getKey(), failure.get());
            }
        }
    }

    private ByteRecord toByteRecord(final ConsumerRecord<byte[], byte[]> consumerRecord) {
        final List<RecordHeader> recordHeaders = new ArrayList<>();
        consumerRecord.headers().forEach(header -> recordHeaders.add(new RecordHeader(header.key(), header.value())));

        byte[] value = consumerRecord.value();
        if (value == null) {
            value = new byte[0];
        }

        return new ByteRecord(
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.timestamp(),
                recordHeaders,
                consumerRecord.key(),
                value,
                1
        );
    }

    private AcknowledgeType toAcknowledgeType(final Acknowledgement acknowledgement) {
        return switch (acknowledgement) {
            case ACCEPT -> AcknowledgeType.ACCEPT;
            case RELEASE -> AcknowledgeType.RELEASE;
            case REJECT -> AcknowledgeType.REJECT;
        };
    }

    /**
     * Coordinates that uniquely identify a record within a poll batch. Used to map the {@link ByteRecord}
     * the caller receives back to the underlying {@link ConsumerRecord} the share consumer needs in order
     * to call {@code acknowledge}.
     */
    private record RecordCoordinates(String topic, int partition, long offset) {

        static RecordCoordinates of(final ConsumerRecord<byte[], byte[]> consumerRecord) {
            return new RecordCoordinates(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
        }

        static RecordCoordinates of(final ByteRecord byteRecord) {
            return new RecordCoordinates(byteRecord.getTopic(), byteRecord.getPartition(), byteRecord.getOffset());
        }
    }

    // Visible for tests
    int getPendingRecordCount() {
        return pendingRecords.size();
    }
}
