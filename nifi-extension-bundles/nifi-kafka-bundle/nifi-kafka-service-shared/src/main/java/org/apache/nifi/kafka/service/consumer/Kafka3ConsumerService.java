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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.nifi.kafka.service.api.common.OffsetSummary;
import org.apache.nifi.kafka.service.api.common.PartitionState;
import org.apache.nifi.kafka.service.api.common.TopicPartitionSummary;
import org.apache.nifi.kafka.service.api.consumer.KafkaConsumerService;
import org.apache.nifi.kafka.service.api.consumer.PollingSummary;
import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.logging.ComponentLog;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Kafka 3 Consumer Service implementation with Object Pooling for subscribed Kafka Consumers
 */
public class Kafka3ConsumerService implements KafkaConsumerService, Closeable, ConsumerRebalanceListener {

    private final ComponentLog componentLog;
    private final Consumer<byte[], byte[]> consumer;
    private final Subscription subscription;
    private volatile boolean closed = false;

    public Kafka3ConsumerService(final ComponentLog componentLog, final Consumer<byte[], byte[]> consumer, final Subscription subscription) {
        this.componentLog = Objects.requireNonNull(componentLog, "Component Log required");
        this.consumer = consumer;
        this.subscription = subscription;

        final Optional<Pattern> topicPatternFound = subscription.getTopicPattern();
        if (topicPatternFound.isPresent()) {
            final Pattern topicPattern = topicPatternFound.get();
            consumer.subscribe(topicPattern, this);
        } else {
            final Collection<String> topics = subscription.getTopics();
            consumer.subscribe(topics, this);
        }
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        componentLog.info("Kafka assigned the following Partitions to this consumer: {}", partitions);
    }

    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        componentLog.info("Kafka revoked the following Partitions from this consumer: {}", partitions);
        rollback(new HashSet<>(partitions));
    }

    @Override
    public void commit(final PollingSummary pollingSummary) {
        final Map<TopicPartition, OffsetAndMetadata> offsets = getOffsets(pollingSummary);

        final long started = System.currentTimeMillis();
        consumer.commitSync(offsets);
        final long elapsed = started - System.currentTimeMillis();

        componentLog.debug("Committed Records in [{} ms] for {}", elapsed, pollingSummary);
    }

    @Override
    public void rollback() {
        rollback(consumer.assignment());
    }

    private void rollback(final Set<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }

        try {
            final Map<TopicPartition, OffsetAndMetadata> metadataMap = consumer.committed(partitions);
            for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : metadataMap.entrySet()) {
                final TopicPartition topicPartition = entry.getKey();
                final OffsetAndMetadata offsetAndMetadata = entry.getValue();

                if (offsetAndMetadata == null) {
                    consumer.seekToBeginning(Collections.singleton(topicPartition));
                    componentLog.debug("Rolling back offsets so that {}-{} it is at the beginning", topicPartition.topic(), topicPartition.partition());
                } else {
                    consumer.seek(topicPartition, offsetAndMetadata.offset());
                    componentLog.debug("Rolling back offsets so that {}-{} has offset of {}", topicPartition.topic(), topicPartition.partition(), offsetAndMetadata.offset());
                }
            }
        } catch (final Exception rollbackException) {
            componentLog.warn("Attempted to rollback Kafka message offset but was unable to do so", rollbackException);
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public Iterable<ByteRecord> poll(final Duration maxWaitDuration) {
        final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(maxWaitDuration);
        if (consumerRecords.isEmpty()) {
            return List.of();
        }

        return new RecordIterable(consumerRecords);
    }

    @Override
    public List<PartitionState> getPartitionStates() {
        final Iterator<String> topics = subscription.getTopics().iterator();

        final List<PartitionState> partitionStates;

        if (topics.hasNext()) {
            final String topic = topics.next();
            partitionStates = consumer.partitionsFor(topic)
                .stream()
                .map(partitionInfo -> new PartitionState(partitionInfo.topic(), partitionInfo.partition()))
                .collect(Collectors.toList());
        } else {
            partitionStates = Collections.emptyList();
        }

        return partitionStates;
    }

    @Override
    public void close() {
        closed = true;
        consumer.close();
    }

    private Map<TopicPartition, OffsetAndMetadata> getOffsets(final PollingSummary pollingSummary) {
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        final Map<TopicPartitionSummary, OffsetSummary> summaryOffsets = pollingSummary.getOffsets();
        for (final Map.Entry<TopicPartitionSummary, OffsetSummary> offsetEntry : summaryOffsets.entrySet()) {
            final TopicPartitionSummary topicPartitionSummary = offsetEntry.getKey();
            final TopicPartition topicPartition = new TopicPartition(topicPartitionSummary.getTopic(), topicPartitionSummary.getPartition());

            final OffsetSummary offsetSummary = offsetEntry.getValue();
            // Offset should indicate the offset that we want to consume from next. This will be 1 more than the most recently obtained offset.
            final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offsetSummary.getOffset() + 1);
            offsets.put(topicPartition, offsetAndMetadata);
        }

        return offsets;
    }


    private static class RecordIterable implements Iterable<ByteRecord> {
        private final Iterator<ByteRecord> records;

        private RecordIterable(final Iterable<ConsumerRecord<byte[], byte[]>> consumerRecords) {
            this.records = new RecordIterator(consumerRecords);
        }

        @Override
        public Iterator<ByteRecord> iterator() {
            return records;
        }
    }

    private static class RecordIterator implements Iterator<ByteRecord> {
        private final Iterator<ConsumerRecord<byte[], byte[]>> consumerRecords;

        private RecordIterator(final Iterable<ConsumerRecord<byte[], byte[]>> records) {
            this.consumerRecords = records.iterator();
        }

        @Override
        public boolean hasNext() {
            return consumerRecords.hasNext();
        }

        @Override
        public ByteRecord next() {
            final ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecords.next();
            final List<RecordHeader> recordHeaders = new ArrayList<>();
            consumerRecord.headers().forEach(header -> {
                final RecordHeader recordHeader = new RecordHeader(header.key(), header.value());
                recordHeaders.add(recordHeader);
            });

            // Support Kafka tombstones
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
    }
}
