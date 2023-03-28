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
import org.apache.nifi.kafka.service.api.common.OffsetSummary;
import org.apache.nifi.kafka.service.api.common.PartitionState;
import org.apache.nifi.kafka.service.api.common.TopicPartitionSummary;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.service.api.consumer.KafkaConsumerService;
import org.apache.nifi.kafka.service.api.consumer.PollingContext;
import org.apache.nifi.kafka.service.api.consumer.PollingSummary;
import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.service.consumer.pool.ConsumerObjectPool;
import org.apache.nifi.kafka.service.consumer.pool.Subscription;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Kafka 3 Consumer Service implementation with Object Pooling for subscribed Kafka Consumers
 */
public class Kafka3ConsumerService implements KafkaConsumerService {
    private final ComponentLog componentLog;

    private final ConsumerObjectPool consumerObjectPool;

    public Kafka3ConsumerService(final ComponentLog componentLog, final Properties properties) {
        this.componentLog = Objects.requireNonNull(componentLog, "Component Log required");
        this.consumerObjectPool = new ConsumerObjectPool(properties);
    }

    @Override
    public void commit(final PollingSummary pollingSummary) {
        Objects.requireNonNull(pollingSummary, "Polling Summary required");

        final Subscription subscription = getSubscription(pollingSummary);
        final Map<TopicPartition, OffsetAndMetadata> offsets = getOffsets(pollingSummary);

        final long started = System.currentTimeMillis();
        final long elapsed = runConsumerFunction(subscription, (consumer) -> {
            consumer.commitSync(offsets);
            return started - System.currentTimeMillis();
        });

        componentLog.debug("Committed Records in [{} ms] for {}", elapsed, pollingSummary);
    }

    @Override
    public Iterable<ByteRecord> poll(final PollingContext pollingContext) {
        Objects.requireNonNull(pollingContext, "Polling Context required");
        final Subscription subscription = getSubscription(pollingContext);

        return runConsumerFunction(subscription, (consumer) -> {
            final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(pollingContext.getMaxUncommittedTime());
            return new RecordIterable(consumerRecords);
        });
    }

    @Override
    public List<PartitionState> getPartitionStates(final PollingContext pollingContext) {
        final Subscription subscription = getSubscription(pollingContext);
        final Iterator<String> topics = subscription.getTopics().iterator();

        final List<PartitionState> partitionStates;

        if (topics.hasNext()) {
            final String topic = topics.next();
            partitionStates = runConsumerFunction(subscription, (consumer) ->
                    consumer.partitionsFor(topic)
                            .stream()
                            .map(partitionInfo -> new PartitionState(partitionInfo.topic(), partitionInfo.partition()))
                            .collect(Collectors.toList())
            );
        } else {
            partitionStates = Collections.emptyList();
        }

        return partitionStates;
    }

    @Override
    public void close() {
        consumerObjectPool.close();
    }

    private Subscription getSubscription(final PollingContext pollingContext) {
        final String groupId = pollingContext.getGroupId();
        final Optional<Pattern> topicPatternFound = pollingContext.getTopicPattern();
        final AutoOffsetReset autoOffsetReset = pollingContext.getAutoOffsetReset();

        return topicPatternFound
                .map(pattern -> new Subscription(groupId, pattern, autoOffsetReset))
                .orElseGet(() -> new Subscription(groupId, pollingContext.getTopics(), autoOffsetReset));
    }

    private Map<TopicPartition, OffsetAndMetadata> getOffsets(final PollingSummary pollingSummary) {
        final Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();

        final Map<TopicPartitionSummary, OffsetSummary> summaryOffsets = pollingSummary.getOffsets();
        for (final Map.Entry<TopicPartitionSummary, OffsetSummary> offsetEntry : summaryOffsets.entrySet()) {
            final TopicPartitionSummary topicPartitionSummary = offsetEntry.getKey();
            final TopicPartition topicPartition = new TopicPartition(topicPartitionSummary.getTopic(), topicPartitionSummary.getPartition());

            final OffsetSummary offsetSummary = offsetEntry.getValue();
            final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offsetSummary.getOffset());
            offsets.put(topicPartition, offsetAndMetadata);
        }

        return offsets;
    }

    private <T> T runConsumerFunction(final Subscription subscription, final Function<Consumer<byte[], byte[]>, T> consumerFunction) {
        Consumer<byte[], byte[]> consumer = null;
        try {
            consumer = consumerObjectPool.borrowObject(subscription);
            return consumerFunction.apply(consumer);
        } catch (final Exception e) {
            throw new ConsumerException("Borrow Consumer failed", e);
        } finally {
            if (consumer != null) {
                try {
                    consumerObjectPool.returnObject(subscription, consumer);
                } catch (final Exception e) {
                    componentLog.warn("Return Consumer failed", e);
                }
            }
        }
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
            return new ByteRecord(
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    consumerRecord.timestamp(),
                    recordHeaders,
                    consumerRecord.key(),
                    consumerRecord.value()
            );
        }
    }
}
