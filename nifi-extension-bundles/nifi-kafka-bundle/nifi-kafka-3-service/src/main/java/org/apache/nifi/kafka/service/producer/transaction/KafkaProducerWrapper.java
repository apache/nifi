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
package org.apache.nifi.kafka.service.producer.transaction;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kafka.service.api.producer.KafkaRecordPartitioner;
import org.apache.nifi.kafka.service.api.producer.PublishContext;
import org.apache.nifi.kafka.service.api.record.KafkaRecord;
import org.apache.nifi.kafka.service.producer.ProducerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Abstract away the configured transactionality of the PublishKafka producer.
 */
public abstract class KafkaProducerWrapper {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private static final long MAX_PARTITION_CACHE_MILLIS = Duration.ofMinutes(5).toMillis();

    protected final Producer<byte[], byte[]> producer;
    private final ConcurrentMap<String, CachedInteger> partitionCountCache = new ConcurrentHashMap<>();

    public KafkaProducerWrapper(final Producer<byte[], byte[]> producer) {
        this.producer = producer;
    }

    public void send(final Iterator<KafkaRecord> kafkaRecords, final PublishContext publishContext, final ProducerCallback callback) {
        while (kafkaRecords.hasNext()) {
            final KafkaRecord kafkaRecord = kafkaRecords.next();
            final ProducerRecord<byte[], byte[]> producerRecord = toProducerRecord(kafkaRecord, publishContext);
            producer.send(producerRecord, callback);
            callback.send(producerRecord.topic());
        }
    }

    /**
     * Transaction-enabled publish to Kafka involves the use of special Kafka client library APIs.
     */
    public abstract void commit();

    /**
     * Transaction-enabled publish to Kafka involves the use of special Kafka client library APIs.
     */
    public abstract void abort();

    private ProducerRecord<byte[], byte[]> toProducerRecord(final KafkaRecord kafkaRecord, final PublishContext publishContext) {
        final String topic = Optional.ofNullable(kafkaRecord.getTopic()).orElse(publishContext.getTopic());
        final Integer partition = getPartition(kafkaRecord, publishContext.getFlowFile(), topic, publishContext.getPartitioner());
        return new ProducerRecord<>(topic, partition, kafkaRecord.getTimestamp(), kafkaRecord.getKey(), kafkaRecord.getValue(), toKafkaHeadersNative(kafkaRecord));
    }

    private Integer getPartition(final KafkaRecord kafkaRecord, final FlowFile flowFile, final String topic, final KafkaRecordPartitioner partitioner) {
        final Integer explicitPartition = kafkaRecord.getPartition();
        if (explicitPartition != null) {
            return explicitPartition;
        }

        if (partitioner == null) {
            return null;
        }

        final long partitionerValue = partitioner.partition(topic, flowFile);
        final int numPartitions = getPartitionCount(topic);
        return (int) (Math.abs(partitionerValue) % numPartitions);
    }

    private int getPartitionCount(final String topicName) {
        final CachedInteger cachedValue = partitionCountCache.get(topicName);
        if (cachedValue == null || System.currentTimeMillis() - cachedValue.timestamp() > MAX_PARTITION_CACHE_MILLIS) {
            final int partitionCount = fetchPartitionCount(topicName);
            partitionCountCache.put(topicName, new CachedInteger(partitionCount, System.currentTimeMillis()));
            return partitionCount;
        }

        return cachedValue.value();
    }

    private int fetchPartitionCount(final String topicName) {
        return producer.partitionsFor(topicName).size();
    }

    private List<Header> toKafkaHeadersNative(final KafkaRecord kafkaRecord) {
        return kafkaRecord.getHeaders().stream()
                .map(h -> new RecordHeader(h.key(), h.value()))
                .collect(Collectors.toList());
    }

    private record CachedInteger(int value, long timestamp) {
    }
}
