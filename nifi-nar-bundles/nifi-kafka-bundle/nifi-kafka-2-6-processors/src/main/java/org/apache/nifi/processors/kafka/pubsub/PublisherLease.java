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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.PublishStrategy;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.stream.io.exception.TokenTooLargeException;
import org.apache.nifi.stream.io.util.StreamDemarcator;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Pattern;

public class PublisherLease implements Closeable {
    private final ComponentLog logger;
    private final Producer<byte[], byte[]> producer;
    private final int maxMessageSize;
    private final long maxAckWaitMillis;
    private final boolean useTransactions;
    private final Pattern attributeNameRegex;
    private final Charset headerCharacterSet;
    private final PublishStrategy publishStrategy;
    private final RecordSetWriterFactory recordKeyWriterFactory;
    private volatile boolean poisoned = false;
    private final AtomicLong messagesSent = new AtomicLong(0L);

    private volatile boolean transactionsInitialized = false;
    private volatile boolean activeTransaction = false;

    private InFlightMessageTracker tracker;

    public PublisherLease(final Producer<byte[], byte[]> producer, final int maxMessageSize, final long maxAckWaitMillis,
                          final ComponentLog logger, final boolean useTransactions, final Pattern attributeNameRegex,
                          final Charset headerCharacterSet, final PublishStrategy publishStrategy, final RecordSetWriterFactory recordKeyWriterFactory) {
        this.producer = producer;
        this.maxMessageSize = maxMessageSize;
        this.logger = logger;
        this.maxAckWaitMillis = maxAckWaitMillis;
        this.useTransactions = useTransactions;
        this.attributeNameRegex = attributeNameRegex;
        this.headerCharacterSet = headerCharacterSet;
        this.publishStrategy = publishStrategy;
        this.recordKeyWriterFactory = recordKeyWriterFactory;
    }

    protected void poison() {
        this.poisoned = true;
    }

    public boolean isPoisoned() {
        return poisoned;
    }

    void beginTransaction() {
        if (!useTransactions) {
            return;
        }

        try {
            if (!transactionsInitialized) {
                producer.initTransactions();
                transactionsInitialized = true;
            }

            producer.beginTransaction();
            activeTransaction = true;
        } catch (final Exception e) {
            poison();
            throw e;
        }
    }

    void rollback() {
        if (!useTransactions || !activeTransaction) {
            return;
        }

        try {
            producer.abortTransaction();
        } catch (final Exception e) {
            poison();
            throw e;
        }

        activeTransaction = false;
    }

    void fail(final FlowFile flowFile, final Exception cause) {
        getTracker().fail(flowFile, cause);
        rollback();
    }

    void publish(final FlowFile flowFile, final InputStream flowFileContent, final byte[] messageKey, final byte[] demarcatorBytes, final String topic, final Integer partition) throws IOException {
        if (tracker == null) {
            tracker = new InFlightMessageTracker(logger);
        }

        try {
            byte[] messageContent;
            if (demarcatorBytes == null || demarcatorBytes.length == 0) {
                if (flowFile.getSize() > maxMessageSize) {
                    tracker.fail(flowFile, new TokenTooLargeException("A message in the stream exceeds the maximum allowed message size of " + maxMessageSize + " bytes."));
                    return;
                }
                if (Boolean.TRUE.toString().equals(flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_TOMBSTONE)) && flowFile.getSize() == 0) {
                    messageContent = null;
                } else {
                    // Send FlowFile content as it is, to support sending 0 byte message.
                    messageContent = new byte[(int) flowFile.getSize()];
                    StreamUtils.fillBuffer(flowFileContent, messageContent);
                }
                publish(flowFile, messageKey, messageContent, topic, tracker, partition);
                return;
            }

            try (final StreamDemarcator demarcator = new StreamDemarcator(flowFileContent, demarcatorBytes, maxMessageSize)) {
                while ((messageContent = demarcator.nextToken()) != null) {
                    publish(flowFile, messageKey, messageContent, topic, tracker, partition);

                    if (tracker.isFailed(flowFile)) {
                        // If we have a failure, don't try to send anything else.
                        return;
                    }
                }
            } catch (final TokenTooLargeException ttle) {
                tracker.fail(flowFile, ttle);
            }
        } catch (final Exception e) {
            tracker.fail(flowFile, e);
            poison();
            throw e;
        }
    }

    void publish(final FlowFile flowFile, final RecordSet recordSet, final RecordSetWriterFactory writerFactory, final RecordSchema schema,
                 final String messageKeyField, final String explicitTopic, final Function<Record, Integer> partitioner, final PublishMetadataStrategy metadataStrategy) throws IOException {
        if (tracker == null) {
            tracker = new InFlightMessageTracker(logger);
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

        Record record;
        int recordCount = 0;

        try {
            while ((record = recordSet.next()) != null) {
                recordCount++;
                baos.reset();

                final String topic;
                final List<Header> headers;
                final byte[] messageContent;
                final byte[] messageKey;
                Integer partition;
                if (publishStrategy == PublishStrategy.USE_WRAPPER) {
                    headers = toHeadersWrapper(record.getValue("headers"));
                    final Object key = record.getValue("key");
                    final Object value = record.getValue("value");
                    messageContent = toByteArray("value", value, writerFactory, flowFile);
                    messageKey = toByteArray("key", key, recordKeyWriterFactory, flowFile);

                    if (metadataStrategy == PublishMetadataStrategy.USE_RECORD_METADATA) {
                        final Object metadataObject = record.getValue("metadata");
                        if (metadataObject instanceof Record metadataRecord) {
                            final String recordTopic = metadataRecord.getAsString("topic");
                            topic = recordTopic == null ? explicitTopic : recordTopic;

                            try {
                                partition = metadataRecord.getAsInt("partition");
                            } catch (final Exception e) {
                                logger.warn("Encountered invalid partition for record in {}; will use configured partitioner for Record", flowFile);
                                partition = partitioner == null ? null : partitioner.apply(record);
                            }
                        } else {
                            topic = explicitTopic;
                            partition = partitioner == null ? null : partitioner.apply(record);
                        }
                    } else {
                        topic = explicitTopic;
                        partition = partitioner == null ? null : partitioner.apply(record);
                    }
                } else {
                    final Map<String, String> additionalAttributes;
                    try (final RecordSetWriter writer = writerFactory.createWriter(logger, schema, baos, flowFile)) {
                        final WriteResult writeResult = writer.write(record);
                        additionalAttributes = writeResult.getAttributes();
                        writer.flush();
                    }
                    headers = toHeaders(flowFile, additionalAttributes);
                    messageContent = baos.toByteArray();
                    messageKey = getMessageKey(flowFile, writerFactory, record.getValue(messageKeyField));
                    topic = explicitTopic;
                    partition = partitioner == null ? null : partitioner.apply(record);
                }

                publish(flowFile, headers, messageKey, messageContent, topic, tracker, partition);

                if (tracker.isFailed(flowFile)) {
                    // If we have a failure, don't try to send anything else.
                    return;
                }
            }

            if (recordCount == 0) {
                tracker.trackEmpty(flowFile);
            }
        } catch (final TokenTooLargeException ttle) {
            tracker.fail(flowFile, ttle);
        } catch (final SchemaNotFoundException | MalformedRecordException snfe) {
            throw new IOException(snfe);
        } catch (final Exception e) {
            tracker.fail(flowFile, e);
            poison();
            throw e;
        }
    }

    private List<Header> toHeadersWrapper(final Object fieldHeaders) {
        final List<Header> headers = new ArrayList<>();
        if (fieldHeaders instanceof Record recordHeaders) {
            for (String fieldName : recordHeaders.getRawFieldNames()) {
                final String fieldValue = recordHeaders.getAsString(fieldName);
                headers.add(new RecordHeader(fieldName, fieldValue.getBytes(StandardCharsets.UTF_8)));
            }
        }
        return headers;
    }

    private List<Header> toHeaders(final FlowFile flowFile, final Map<String, ?> additionalAttributes) {
        if (attributeNameRegex == null) {
            return Collections.emptyList();
        }

        final List<Header> headers = new ArrayList<>();
        for (final Map.Entry<String, String> entry : flowFile.getAttributes().entrySet()) {
            if (attributeNameRegex.matcher(entry.getKey()).matches()) {
                headers.add(new RecordHeader(entry.getKey(), entry.getValue().getBytes(headerCharacterSet)));
            }
        }

        for (final Map.Entry<String, ?> entry : additionalAttributes.entrySet()) {
            if (attributeNameRegex.matcher(entry.getKey()).matches()) {
                final Object value = entry.getValue();
                if (value != null) {
                    final String valueString = value.toString();
                    headers.add(new RecordHeader(entry.getKey(), valueString.getBytes(headerCharacterSet)));
                }
            }
        }
        return headers;
    }

    private byte[] toByteArray(final String name, final Object object, final RecordSetWriterFactory writerFactory, final FlowFile flowFile)
            throws IOException, SchemaNotFoundException, MalformedRecordException {
        return switch (object) {
            case null -> null;
            case Record record -> {
                if (writerFactory == null) {
                    throw new MalformedRecordException("Record has a key that is itself a record, but the 'Record Key Writer' of the processor was not configured. If Records are expected to have a " +
                            "Record as the key, the 'Record Key Writer' property must be set.");
                }

                final RecordSchema schema = writerFactory.getSchema(flowFile.getAttributes(), record.getSchema());
                try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                     final RecordSetWriter writer = writerFactory.createWriter(logger, schema, baos, flowFile)) {
                    writer.write(record);
                    writer.flush();
                    yield baos.toByteArray();
                }
            }
            case Byte[] bytesUppercase -> {
                final byte[] bytes = new byte[bytesUppercase.length];
                for (int i = 0; (i < bytesUppercase.length); ++i) {
                    bytes[i] = bytesUppercase[i];
                }
                yield bytes;
            }
            case String string -> string.getBytes(StandardCharsets.UTF_8);
            default ->
                    throw new MalformedRecordException(String.format("Couldn't convert %s record data to byte array.", name));
        };
    }

    private byte[] getMessageKey(final FlowFile flowFile, final RecordSetWriterFactory writerFactory,
                                 final Object keyValue) throws IOException, SchemaNotFoundException {
        return switch (keyValue) {
            case null -> null;
            case byte[] bytes1 -> bytes1;
            case Byte[] bytes -> {
                // This case exists because in our Record API we currently don't have a BYTES type, we use an Array of type
                // Byte, which creates a Byte[] instead of a byte[]. We should address this in the future, but we should
                // account for the log here.
                final byte[] bytesPrimitive = new byte[bytes.length];
                for (int i = 0; i < bytes.length; i++) {
                    bytesPrimitive[i] = bytes[i];
                }
                yield bytesPrimitive;
            }
            case Record keyRecord -> {
                try (final ByteArrayOutputStream os = new ByteArrayOutputStream(1024)) {
                    try (final RecordSetWriter writerKey = writerFactory.createWriter(logger, keyRecord.getSchema(), os, flowFile)) {
                        writerKey.write(keyRecord);
                        writerKey.flush();
                    }
                    yield os.toByteArray();
                }
            }
            default -> keyValue.toString().getBytes(StandardCharsets.UTF_8);
        };
    }

    protected void publish(final FlowFile flowFile, final byte[] messageKey, final byte[] messageContent, final String topic, final InFlightMessageTracker tracker, final Integer partition) {
        final List<Header> headers = toHeaders(flowFile, Collections.emptyMap());
        publish(flowFile, headers, messageKey, messageContent, topic, tracker, partition);
    }

    protected void publish(final FlowFile flowFile, final List<Header> headers, final byte[] messageKey, final byte[] messageContent,
                           final String topic, final InFlightMessageTracker tracker, final Integer partition) {

        final Integer moddedPartition = partition == null ? null : Math.abs(partition) % (producer.partitionsFor(topic).size());
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, moddedPartition, messageKey, messageContent, headers);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                tracker.incrementAcknowledgedCount(flowFile);
            } else {
                tracker.fail(flowFile, exception);
                poison();
            }
        });

        messagesSent.incrementAndGet();
        tracker.incrementSentCount(flowFile);
    }

    void ackConsumerOffsets(final String topic, final int partition, final long offset, final Integer leaderEpoch, final String consumerGroupId) {
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset + 1, Optional.ofNullable(leaderEpoch), null);
        final Map<TopicPartition, OffsetAndMetadata> offsetMap = Collections.singletonMap(topicPartition, offsetAndMetadata);

        logger.debug("Acknowledging Consumer Offsets for topic={}, partition={}, offset={}, consumerGroup={}, leaderEpoch={}", topic, partition, offset, consumerGroupId, leaderEpoch);
        producer.sendOffsetsToTransaction(offsetMap, consumerGroupId);
    }

    public PublishResult complete() {
        if (tracker == null) {
            if (messagesSent.get() == 0L) {
                return PublishResult.EMPTY;
            }

            rollback();
            throw new IllegalStateException("Cannot complete publishing to Kafka because Publisher Lease was already closed");
        }

        try {
            producer.flush();

            if (activeTransaction) {
                producer.commitTransaction();
                activeTransaction = false;
            }
        } catch (final ProducerFencedException | FencedInstanceIdException e) {
            throw e;
        } catch (final Exception e) {
            poison();
            throw e;
        }

        try {
            tracker.awaitCompletion(maxAckWaitMillis);
            return tracker.createPublishResult();
        } catch (final InterruptedException e) {
            logger.warn("Interrupted while waiting for an acknowledgement from Kafka; some FlowFiles may be transferred to 'failure' even though they were received by Kafka");
            Thread.currentThread().interrupt();
            return tracker.failOutstanding(e);
        } catch (final TimeoutException e) {
            logger.warn("Timed out while waiting for an acknowledgement from Kafka; some FlowFiles may be transferred to 'failure' even though they were received by Kafka");
            return tracker.failOutstanding(e);
        } finally {
            tracker = null;
        }
    }

    @Override
    public void close() {
        producer.close(Duration.ofMillis(maxAckWaitMillis));
        tracker = null;
    }

    public InFlightMessageTracker getTracker() {
        if (tracker == null) {
            tracker = new InFlightMessageTracker(logger);
        }

        return tracker;
    }

    public List<ConfigVerificationResult> verifyConfiguration(final String topic) {
        final List<ConfigVerificationResult> verificationResults = new ArrayList<>();

        try {
            final List<PartitionInfo> partitionInfos = producer.partitionsFor(topic);

            verificationResults.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Determine Topic Partitions")
                .outcome(Outcome.SUCCESSFUL)
                .explanation("Determined that there are " + partitionInfos.size() + " partitions for topic " + topic)
                .build());
        } catch (final Exception e) {
            logger.error("Failed to determine Partition Information for Topic {} in order to verify configuration", topic, e);

            verificationResults.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Determine Topic Partitions")
                .outcome(Outcome.FAILED)
                .explanation("Could not fetch Partition Information: " + e)
                .build());
        }

        return verificationResults;
    }
}
