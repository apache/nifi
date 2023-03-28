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

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.attribute.StandardTransitUriProvider;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.kafka.shared.property.KeyFormat;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SchemaValidationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_CONSUMER_GROUP_ID;
import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_CONSUMER_OFFSETS_COMMITTED;
import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_COUNT;
import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_LEADER_EPOCH;
import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_MAX_OFFSET;
import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_OFFSET;
import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_PARTITION;
import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_TIMESTAMP;
import static org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute.KAFKA_TOPIC;
import static org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6.REL_PARSE_FAILURE;
import static org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6.REL_SUCCESS;

/**
 * This class represents a lease to access a Kafka Consumer object. The lease is
 * intended to be obtained from a ConsumerPool. The lease is closeable to allow
 * for the clean model of a try w/resources whereby non-exceptional cases mean
 * the lease will be returned to the pool for future use by others. A given
 * lease may only belong to a single thread a time.
 */
public abstract class ConsumerLease implements Closeable, ConsumerRebalanceListener {
    private static final RecordField EMPTY_SCHEMA_KEY_RECORD_FIELD =
            new RecordField("key", RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(Collections.emptyList())));

    private final Long maxWaitMillis;
    private final Consumer<byte[], byte[]> kafkaConsumer;
    private final ComponentLog logger;
    private final byte[] demarcatorBytes;
    private final KeyEncoding keyEncoding;
    private final String securityProtocol;
    private final String bootstrapServers;
    private final RecordSetWriterFactory writerFactory;
    private final RecordReaderFactory readerFactory;
    private final Charset headerCharacterSet;
    private final Pattern headerNamePattern;
    private final boolean separateByKey;
    private final boolean commitOffsets;
    private final OutputStrategy outputStrategy;
    private final KeyFormat keyFormat;
    private final RecordReaderFactory keyReaderFactory;
    private boolean poisoned = false;
    //used for tracking demarcated flowfiles to their TopicPartition so we can append
    //to them on subsequent poll calls
    private final Map<BundleInformation, BundleTracker> bundleMap = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> uncommittedOffsetsMap = new HashMap<>();
    private long leaseStartNanos = -1;
    private boolean lastPollEmpty = false;
    private int totalMessages = 0;

    ConsumerLease(
            final Long maxWaitMillis,
            final Consumer<byte[], byte[]> kafkaConsumer,
            final byte[] demarcatorBytes,
            final KeyEncoding keyEncoding,
            final String securityProtocol,
            final String bootstrapServers,
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final ComponentLog logger,
            final Charset headerCharacterSet,
            final Pattern headerNamePattern,
            final boolean separateByKey,
            final boolean commitMessageOffsets,
            final OutputStrategy outputStrategy,
            final KeyFormat keyFormat,
            final RecordReaderFactory keyReaderFactory) {
        this.maxWaitMillis = maxWaitMillis;
        this.kafkaConsumer = kafkaConsumer;
        this.demarcatorBytes = demarcatorBytes;
        this.keyEncoding = keyEncoding;
        this.securityProtocol = securityProtocol;
        this.bootstrapServers = bootstrapServers;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.logger = logger;
        this.headerCharacterSet = headerCharacterSet;
        this.headerNamePattern = headerNamePattern;
        this.separateByKey = separateByKey;
        this.commitOffsets = commitMessageOffsets;
        this.outputStrategy = outputStrategy;
        this.keyFormat = keyFormat;
        this.keyReaderFactory = keyReaderFactory;
    }

    /**
     * clears out internal state elements excluding session and consumer as
     * those are managed by the pool itself
     */
    private void resetInternalState() {
        bundleMap.clear();
        uncommittedOffsetsMap.clear();
        leaseStartNanos = -1;
        lastPollEmpty = false;
        totalMessages = 0;
    }

    /**
     * Kafka will call this method whenever it is about to rebalance the
     * consumers for the given partitions. We'll simply take this to mean that
     * we need to quickly commit what we've got and will return the consumer to
     * the pool. This method will be called during the poll() method call of
     * this class and will be called by the same thread calling poll according
     * to the Kafka API docs. After this method executes the session and kafka
     * offsets are committed and this lease is closed.
     *
     * @param partitions partitions being reassigned
     */
    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        logger.debug("Rebalance Alert: Partitions '{}' revoked for lease '{}' with consumer '{}'", partitions, this, kafkaConsumer);
        commit();
    }

    /**
     * This will be called by Kafka when the rebalance has completed. We don't
     * need to do anything with this information other than optionally log it as
     * by this point we've committed what we've got and moved on.
     *
     * @param partitions topic partition set being reassigned
     */
    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        logger.debug("Rebalance Alert: Partitions '{}' assigned for lease '{}' with consumer '{}'", partitions, this, kafkaConsumer);
    }

    public List<TopicPartition> getAssignedPartitions() {
        return null;
    }

    /**
     * Executes a poll on the underlying Kafka Consumer and creates any new
     * flowfiles necessary or appends to existing ones if in demarcation mode.
     */
    void poll() {
        /*
         * Implementation note:
         * Even if ConsumeKafka is not scheduled to poll due to downstream connection back-pressure is engaged,
         * for longer than session.timeout.ms (defaults to 10 sec), Kafka consumer sends heartbeat from background thread.
         * If this situation lasts longer than max.poll.interval.ms (defaults to 5 min), Kafka consumer sends
         * Leave Group request to Group Coordinator. When ConsumeKafka processor is scheduled again, Kafka client checks
         * if this client instance is still a part of consumer group. If not, it rejoins before polling messages.
         * This behavior has been fixed via Kafka KIP-62 and available from Kafka client 0.10.1.0.
         */
        try {
            final ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(10L));
            lastPollEmpty = records.count() == 0;
            processRecords(records);
        } catch (final ProcessException pe) {
            throw pe;
        } catch (final Throwable t) {
            this.poison();
            throw t;
        }
    }

    void abort() {
        rollback(kafkaConsumer.assignment());
        final ProcessSession session = getProcessSession();
        if (session != null) {
            session.rollback();
        }
        resetInternalState();
    }

    /**
     * Notifies Kafka to commit the offsets for the specified topic/partition
     * pairs to the specified offsets w/the given metadata. This can offer
     * higher performance than the other commitOffsets call as it allows the
     * kafka client to collect more data from Kafka before committing the
     * offsets.
     * if false then we didn't do anything and should probably yield if true
     * then we committed new data
     */
    boolean commit() {
        if (uncommittedOffsetsMap.isEmpty()) {
            resetInternalState();
            return false;
        }

        if (isPoisoned()) {
            // Failed to commit the session. Rollback the offsets.
            abort();
            return false;
        }

        try {
            /*
             * Committing the nifi session then the offsets means we have an at
             * least once guarantee here. If we reversed the order we'd have at
             * most once.
             */
            final Collection<FlowFile> bundledFlowFiles = getBundles();
            if (!bundledFlowFiles.isEmpty()) {
                getProcessSession().transfer(bundledFlowFiles, REL_SUCCESS);

                if (logger.isDebugEnabled()) {
                    for (final FlowFile flowFile : bundledFlowFiles) {
                        final String recordCountAttribute = flowFile.getAttribute("record.count");
                        final String recordCount = recordCountAttribute == null ? "1" : recordCountAttribute;
                        logger.debug("Transferred {} with {} records, max offset of {}", flowFile, recordCount, flowFile.getAttribute(KAFKA_MAX_OFFSET));
                    }
                }
            }

            final Map<TopicPartition, OffsetAndMetadata> offsetsMap = new HashMap<>(uncommittedOffsetsMap);
            final Set<TopicPartition> assignedPartitions = kafkaConsumer.assignment();

            getProcessSession().commitAsync(() -> {
                if (commitOffsets) {
                    kafkaConsumer.commitSync(offsetsMap);
                }
                resetInternalState();
            }, failureCause -> {
                // Failed to commit the session. Rollback the offsets.
                logger.error("Failed to commit ProcessSession after consuming records from Kafka. Will rollback Kafka Offsets", failureCause);
                resetInternalState();
                rollback(assignedPartitions);
            });

            return true;
        } catch (final IOException ioe) {
            poison();
            logger.error("Failed to finish writing out FlowFile bundle", ioe);
            throw new ProcessException(ioe);
        } catch (final KafkaException kex) {
            poison();
            logger.warn("Duplicates are likely as we were able to commit the process"
                    + " session but received an exception from Kafka while committing"
                    + " offsets.");
            throw kex;
        } catch (final Throwable t) {
            poison();
            throw t;
        }
    }

    /**
     * Indicates whether we should continue polling for data. If we are not
     * writing data with a demarcator then we're writing individual flow files
     * per kafka message therefore we must be very mindful of memory usage for
     * the flow file objects (not their content) being held in memory. The
     * content of kafka messages will be written to the content repository
     * immediately upon each poll call but we must still be mindful of how much
     * memory can be used in each poll call. We will indicate that we should
     * stop polling our last poll call produced no new results or if we've
     * polling and processing data longer than the specified maximum polling
     * time or if we have reached out specified max flow file limit or if a
     * rebalance has been initiated for one of the partitions we're watching;
     * otherwise true.
     *
     * @return true if should keep polling; false otherwise
     */
    boolean continuePolling() {
        //stop if the last poll produced new no data
        if (lastPollEmpty) {
            return false;
        }

        //stop if we've gone past our desired max uncommitted wait time
        if (leaseStartNanos < 0) {
            leaseStartNanos = System.nanoTime();
        }
        final long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - leaseStartNanos);
        if (maxWaitMillis == null || durationMillis > maxWaitMillis) {
            return false;
        }

        //stop if we've generated enough flowfiles that we need to be concerned about memory usage for the objects
        if (bundleMap.size() > 200) { //a magic number - the number of simultaneous bundles to track
            return false;
        } else {
            return totalMessages < 1000;//admittedlly a magic number - good candidate for processor property
        }
    }

    /**
     * Indicates that the underlying session and consumer should be immediately
     * considered invalid. Once closed the session will be rolled back and the
     * pool should destroy the underlying consumer. This is useful if due to
     * external reasons, such as the processor no longer being scheduled, this
     * lease should be terminated immediately.
     */
    private void poison() {
        poisoned = true;
    }

    /**
     * @return true if this lease has been poisoned; false otherwise
     */
    boolean isPoisoned() {
        return poisoned;
    }

    /**
     * Trigger the consumer's {@link KafkaConsumer#wakeup() wakeup()} method.
     */
    public void wakeup() {
        kafkaConsumer.wakeup();
    }

    /**
     * Abstract method that is intended to be extended by the pool that created
     * this ConsumerLease object. It should ensure that the session given to
     * create this session is rolled back and that the underlying kafka consumer
     * is either returned to the pool for continued use or destroyed if this
     * lease has been poisoned. It can only be called once. Calling it more than
     * once can result in undefined and non threadsafe behavior.
     */
    @Override
    public void close() {
        resetInternalState();
    }

    public abstract ProcessSession getProcessSession();

    public abstract void yield();

    private void processRecords(final ConsumerRecords<byte[], byte[]> records) {
        records.partitions().forEach(partition -> {
            List<ConsumerRecord<byte[], byte[]>> messages = records.records(partition);
            if (!messages.isEmpty()) {
                //update maximum offset map for this topic partition
                long maxOffset = messages.stream()
                        .mapToLong(ConsumerRecord::offset)
                        .max()
                        .getAsLong();

                //write records to content repository and session
                if (demarcatorBytes != null) {
                    writeDemarcatedData(getProcessSession(), messages, partition);
                } else if (readerFactory != null && writerFactory != null) {
                    writeRecordData(getProcessSession(), messages, partition);
                } else {
                    messages.forEach(message -> writeData(getProcessSession(), message, partition));
                }

                totalMessages += messages.size();
                uncommittedOffsetsMap.put(partition, new OffsetAndMetadata(maxOffset + 1L));
            }
        });
    }

    private static String encodeKafkaKey(final byte[] key, final KeyEncoding encoding) {
        if (key == null) {
            return null;
        }

        return switch (encoding) {
            case UTF8 -> new String(key, StandardCharsets.UTF_8);
            case HEX -> Hex.encodeHexString(key);
            case DO_NOT_ADD -> null;
        };
    }

    private Collection<FlowFile> getBundles() throws IOException {
        final List<FlowFile> flowFiles = new ArrayList<>();
        for (final BundleTracker tracker : bundleMap.values()) {
            final boolean includeBundle = processBundle(tracker);
            if (includeBundle) {
                flowFiles.add(tracker.flowFile);
            }
        }
        return flowFiles;
    }

    private boolean processBundle(final BundleTracker bundle) throws IOException {
        final RecordSetWriter writer = bundle.recordWriter;
        if (writer != null) {
            final WriteResult writeResult;

            try {
                writeResult = writer.finishRecordSet();
            } finally {
                writer.close();
            }

            if (writeResult.getRecordCount() == 0) {
                getProcessSession().remove(bundle.flowFile);
                return false;
            }

            final Map<String, String> attributes = new HashMap<>(writeResult.getAttributes());
            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());

            bundle.flowFile = getProcessSession().putAllAttributes(bundle.flowFile, attributes);
        }

        populateAttributes(bundle);
        return true;
    }

    private void writeData(final ProcessSession session, ConsumerRecord<byte[], byte[]> record, final TopicPartition topicPartition) {
        FlowFile flowFile = session.create();
        final BundleTracker tracker = new BundleTracker(record, topicPartition, keyEncoding);
        tracker.incrementRecordCount(1, record.offset(), record.leaderEpoch().orElse(null));
        final byte[] value = record.value();
        if (value != null) {
            flowFile = session.write(flowFile, out -> out.write(value));
        } else {
            flowFile = session.putAttribute(flowFile, KafkaFlowFileAttribute.KAFKA_TOMBSTONE, Boolean.TRUE.toString());
        }
        flowFile = session.putAllAttributes(flowFile, getAttributes(record));
        tracker.updateFlowFile(flowFile);
        populateAttributes(tracker);
        session.transfer(tracker.flowFile, REL_SUCCESS);
    }

    private void writeDemarcatedData(final ProcessSession session, final List<ConsumerRecord<byte[], byte[]>> records, final TopicPartition topicPartition) {
        // Group the Records by their BundleInformation
        final Map<BundleInformation, List<ConsumerRecord<byte[], byte[]>>> map = records.stream()
                .collect(Collectors.groupingBy(rec -> new BundleInformation(topicPartition, null, getAttributes(rec), separateByKey ? rec.key() : null)));

        for (final Map.Entry<BundleInformation, List<ConsumerRecord<byte[], byte[]>>> entry : map.entrySet()) {
            final BundleInformation bundleInfo = entry.getKey();
            final List<ConsumerRecord<byte[], byte[]>> recordList = entry.getValue();

            final boolean demarcateFirstRecord;

            BundleTracker tracker = bundleMap.get(bundleInfo);

            FlowFile flowFile;
            if (tracker == null) {
                tracker = new BundleTracker(recordList.getFirst(), topicPartition, keyEncoding);
                flowFile = session.create();
                flowFile = session.putAllAttributes(flowFile, bundleInfo.attributes);
                tracker.updateFlowFile(flowFile);
                demarcateFirstRecord = false; //have not yet written records for this topic/partition in this lease
            } else {
                demarcateFirstRecord = true; //have already been writing records for this topic/partition in this lease
            }
            flowFile = tracker.flowFile;

            // Determine max offset of any record to provide to the MessageTracker.
            long maxOffset = recordList.getFirst().offset();
            int leaderEpoch = -1;
            for (final ConsumerRecord<byte[], byte[]> record : recordList) {
                maxOffset = Math.max(maxOffset, record.offset());
                leaderEpoch = Math.max(record.leaderEpoch().orElse(leaderEpoch), leaderEpoch);
            }

            tracker.incrementRecordCount(recordList.size(), maxOffset, leaderEpoch >= 0 ? leaderEpoch : null);

            flowFile = session.append(flowFile, out -> {
                boolean useDemarcator = demarcateFirstRecord;
                for (final ConsumerRecord<byte[], byte[]> record : recordList) {
                    if (useDemarcator) {
                        out.write(demarcatorBytes);
                    }
                    final byte[] value = record.value();
                    if (value != null) {
                        out.write(record.value());
                    }
                    useDemarcator = true;
                }
            });

            tracker.updateFlowFile(flowFile);
            bundleMap.put(bundleInfo, tracker);
        }
    }

    private void handleParseFailure(final ConsumerRecord<byte[], byte[]> consumerRecord, final ProcessSession session, final Exception cause) {
        handleParseFailure(consumerRecord, session, cause, "Failed to parse message from Kafka using the configured Record Reader. "
                + "Will route message as its own FlowFile to the 'parse.failure' relationship");
    }

    private void handleParseFailure(final ConsumerRecord<byte[], byte[]> consumerRecord, final ProcessSession session, final Exception cause, final String message) {
        // If we are unable to parse the data, we need to transfer it to 'parse failure' relationship
        final Map<String, String> attributes = getAttributes(consumerRecord);
        attributes.put(KAFKA_OFFSET, String.valueOf(consumerRecord.offset()));
        attributes.put(KAFKA_TIMESTAMP, String.valueOf(consumerRecord.timestamp()));

        FlowFile failureFlowFile = session.create();

        final byte[] value = consumerRecord.value();
        if (value != null) {
            failureFlowFile = session.write(failureFlowFile, out -> out.write(value));
        }
        failureFlowFile = session.putAllAttributes(failureFlowFile, attributes);

        final String transitUri = StandardTransitUriProvider.getTransitUri(securityProtocol, bootstrapServers, consumerRecord.topic());
        session.getProvenanceReporter().receive(failureFlowFile, transitUri);

        session.transfer(failureFlowFile, REL_PARSE_FAILURE);

        if (cause == null) {
            logger.error(message);
        } else {
            logger.error(message, cause);
        }

        session.adjustCounter("Parse Failures", 1, false);
    }

    protected Map<String, String> getAttributes(final ConsumerRecord<?, ?> consumerRecord) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(KAFKA_PARTITION, String.valueOf(consumerRecord.partition()));
        attributes.put(KAFKA_TOPIC, consumerRecord.topic());

        if (headerNamePattern == null) {
            return attributes;
        }

        for (final Header header : consumerRecord.headers()) {
            final String attributeName = header.key();
            final byte[] attributeValue = header.value();
            if (headerNamePattern.matcher(attributeName).matches() && attributeValue != null) {
                attributes.put(attributeName, new String(attributeValue, headerCharacterSet));
            }
        }

        return attributes;
    }

    private void writeRecordData(final ProcessSession session, final List<ConsumerRecord<byte[], byte[]>> records, final TopicPartition topicPartition) {
        // In order to obtain a RecordReader from the RecordReaderFactory, we need to give it a FlowFile.
        // We don't want to create a new FlowFile for each record that we receive, so we will just create
        // a "temporary flowfile" that will be removed in the finally block below and use that to pass to
        // the createRecordReader method.
        RecordSetWriter writer = null;
        try {
            for (final ConsumerRecord<byte[], byte[]> consumerRecord : records) {
                final Map<String, String> attributes = getAttributes(consumerRecord);

                final byte[] recordBytes = consumerRecord.value() == null ? new byte[0] : consumerRecord.value();
                try (final InputStream in = new ByteArrayInputStream(recordBytes)) {
                    final RecordReader reader;

                    try {
                        reader = readerFactory.createRecordReader(attributes, in, recordBytes.length, logger);
                    } catch (final IOException e) {
                        this.yield();
                        rollback(topicPartition);
                        handleParseFailure(consumerRecord, session, e, "Failed to parse message from Kafka due to comms failure. Will roll back session and try again momentarily.");
                        closeWriter(writer);
                        return;
                    } catch (final Exception e) {
                        handleParseFailure(consumerRecord, session, e);
                        continue;
                    }

                    try {
                        int recordCount = 0;
                        Record record;
                        while ((record = reader.nextRecord()) != null) {
                            if (outputStrategy == OutputStrategy.USE_WRAPPER) {
                                record = toWrapperRecord(consumerRecord, record);
                            }
                            writer = writeRecord(session, consumerRecord, topicPartition, record, attributes);
                            ++recordCount;
                        }
                        if ((recordCount == 0) && (outputStrategy == OutputStrategy.USE_WRAPPER)) {
                            // special processing of wrapper record with null value
                            writer = writeRecord(session, consumerRecord, topicPartition, toWrapperRecord(consumerRecord, null), attributes);
                        }
                    } catch (final IOException | MalformedRecordException | SchemaValidationException e) {
                        handleParseFailure(consumerRecord, session, e);
                    }
                }
            }
        } catch (final Exception e) {
            logger.error("Failed to properly receive messages from Kafka. Will roll back session and any un-committed offsets from Kafka.", e);

            closeWriter(writer);
            rollback(topicPartition);

            throw new ProcessException(e);
        }
    }

    private RecordSetWriter writeRecord(final ProcessSession session, final ConsumerRecord<byte[], byte[]> consumerRecord, final TopicPartition topicPartition,
                                        final Record record, final Map<String, String> attributes) throws SchemaNotFoundException, IOException {
        // Determine the bundle for this record.
        final RecordSchema recordSchema = record.getSchema();
        final BundleInformation bundleInfo = new BundleInformation(topicPartition, recordSchema, attributes, separateByKey ? consumerRecord.key() : null);

        BundleTracker tracker = bundleMap.get(bundleInfo);
        final RecordSetWriter writer;
        if (tracker == null) {
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, attributes);

            final OutputStream rawOut = session.write(flowFile);

            final RecordSchema writeSchema;
            try {
                writeSchema = writerFactory.getSchema(flowFile.getAttributes(), recordSchema);
            } catch (final Exception e) {
                logger.error("Failed to obtain Schema for FlowFile. Will roll back the Kafka message offsets.", e);

                rollback(topicPartition);
                this.yield();

                throw new ProcessException(e);
            }

            writer = writerFactory.createWriter(logger, writeSchema, rawOut, flowFile);
            writer.beginRecordSet();

            tracker = new BundleTracker(consumerRecord, topicPartition, keyEncoding, writer);
            tracker.updateFlowFile(flowFile);
            bundleMap.put(bundleInfo, tracker);
        } else {
            writer = tracker.recordWriter;
        }

        try {
            writer.write(record);
        } catch (final RuntimeException re) {
            handleParseFailure(consumerRecord, session, re, "Failed to write message from Kafka using the configured Record Writer. "
                    + "Will route message as its own FlowFile to the 'parse.failure' relationship");
            return writer;
        }

        tracker.incrementRecordCount(1L, consumerRecord.offset(), consumerRecord.leaderEpoch().orElse(null));
        session.adjustCounter("Records Received", 1L, false);
        return writer;
    }

    private MapRecord toWrapperRecord(final ConsumerRecord<byte[], byte[]> consumerRecord, final Record record)
            throws IOException, SchemaNotFoundException, MalformedRecordException {
        final Tuple<RecordField, Object> tupleKey = toWrapperRecordKey(consumerRecord);
        final Tuple<RecordField, Object> tupleValue = toWrapperRecordValue(record);
        final Tuple<RecordField, Object> tupleHeaders = toWrapperRecordHeaders(consumerRecord);
        final Tuple<RecordField, Object> tupleMetadata = toWrapperRecordMetadata(consumerRecord);
        final RecordSchema rootRecordSchema = new SimpleRecordSchema(Arrays.asList(
                tupleKey.getKey(), tupleValue.getKey(), tupleHeaders.getKey(), tupleMetadata.getKey()));

        final Map<String, Object> recordValues = new HashMap<>();
        recordValues.put(tupleKey.getKey().getFieldName(), tupleKey.getValue());
        recordValues.put(tupleValue.getKey().getFieldName(), tupleValue.getValue());
        recordValues.put(tupleHeaders.getKey().getFieldName(), tupleHeaders.getValue());
        recordValues.put(tupleMetadata.getKey().getFieldName(), tupleMetadata.getValue());
        return new MapRecord(rootRecordSchema, recordValues);
    }

    private Tuple<RecordField, Object> toWrapperRecordKey(final ConsumerRecord<byte[], byte[]> consumerRecord)
            throws IOException, SchemaNotFoundException, MalformedRecordException {
        final byte[] key = consumerRecord.key() == null ? new byte[0] : consumerRecord.key();

        return switch (keyFormat) {
            case RECORD -> {
                if (key.length == 0) {
                    yield new Tuple<>(EMPTY_SCHEMA_KEY_RECORD_FIELD, null);
                }

                final Map<String, String> attributes = getAttributes(consumerRecord);
                try (final InputStream is = new ByteArrayInputStream(key);
                     final RecordReader reader = keyReaderFactory.createRecordReader(attributes, is, key.length, logger)) {

                    final Record record = reader.nextRecord();
                    final RecordField recordField = new RecordField("key", RecordFieldType.RECORD.getRecordDataType(record.getSchema()));
                    yield new Tuple<>(recordField, record);
                }
            }
            case STRING -> {
                final RecordField recordField = new RecordField("key", RecordFieldType.STRING.getDataType());
                final String keyString = ((key == null) ? null : new String(key, StandardCharsets.UTF_8));
                yield new Tuple<>(recordField, keyString);
            }
            case BYTE_ARRAY -> {
                final RecordField recordField = new RecordField("key", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()));
                yield new Tuple<>(recordField, ArrayUtils.toObject(key));
            }
        };
    }

    private Tuple<RecordField, Object> toWrapperRecordValue(final Record record) {
        final RecordSchema recordSchema = (record == null) ? null : record.getSchema();
        final RecordField recordField = new RecordField("value", RecordFieldType.RECORD.getRecordDataType(recordSchema));
        return new Tuple<>(recordField, record);
    }

    private Tuple<RecordField, Object> toWrapperRecordHeaders(final ConsumerRecord<byte[], byte[]> consumerRecord) {
        final RecordField recordField = new RecordField("headers", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()));
        final Map<String, String> headers = new HashMap<>();
        for (final Header header : consumerRecord.headers()) {
            headers.put(header.key(), new String(header.value(), headerCharacterSet));
        }
        return new Tuple<>(recordField, headers);
    }

    private static final RecordField FIELD_TOPIC = new RecordField("topic", RecordFieldType.STRING.getDataType());
    private static final RecordField FIELD_PARTITION = new RecordField("partition", RecordFieldType.INT.getDataType());
    private static final RecordField FIELD_OFFSET = new RecordField("offset", RecordFieldType.LONG.getDataType());
    private static final RecordField FIELD_TIMESTAMP = new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType());
    private static final RecordSchema SCHEMA_WRAPPER = new SimpleRecordSchema(Arrays.asList(
            FIELD_TOPIC, FIELD_PARTITION, FIELD_OFFSET, FIELD_TIMESTAMP));

    private Tuple<RecordField, Object> toWrapperRecordMetadata(final ConsumerRecord<byte[], byte[]> consumerRecord) {
        final RecordField recordField = new RecordField("metadata", RecordFieldType.RECORD.getRecordDataType(SCHEMA_WRAPPER));
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("topic", consumerRecord.topic());
        metadata.put("partition", consumerRecord.partition());
        metadata.put("offset", consumerRecord.offset());
        metadata.put("timestamp", consumerRecord.timestamp());
        final Record record = new MapRecord(SCHEMA_WRAPPER, metadata);
        return new Tuple<>(recordField, record);
    }

    private void closeWriter(final RecordSetWriter writer) {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (final Exception ioe) {
            logger.warn("Failed to close Record Writer", ioe);
        }
    }

    private void rollback(final TopicPartition topicPartition) {
        rollback(Collections.singleton(topicPartition));
    }

    private void rollback(final Set<TopicPartition> topicPartitions) {
        try {
            final Map<TopicPartition, OffsetAndMetadata> metadataMap = kafkaConsumer.committed(topicPartitions);
            for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : metadataMap.entrySet()) {
                final TopicPartition topicPartition = entry.getKey();
                final OffsetAndMetadata offsetAndMetadata = entry.getValue();

                if (offsetAndMetadata == null) {
                    kafkaConsumer.seekToBeginning(Collections.singleton(topicPartition));
                    logger.info("Rolling back offsets so that {}-{} it is at the beginning", topicPartition.topic(), topicPartition.partition());
                } else {
                    kafkaConsumer.seek(topicPartition, offsetAndMetadata.offset());
                    logger.info("Rolling back offsets so that {}-{} has offset of {}", topicPartition.topic(), topicPartition.partition(), offsetAndMetadata.offset());
                }
            }
        } catch (final Exception rollbackException) {
            logger.warn("Attempted to rollback Kafka message offset but was unable to do so", rollbackException);
            poison();
        }
    }


    private void populateAttributes(final BundleTracker tracker) {
        final Map<String, String> kafkaAttrs = new HashMap<>();
        kafkaAttrs.put(KAFKA_OFFSET, String.valueOf(tracker.initialOffset));
        kafkaAttrs.put(KAFKA_TIMESTAMP, String.valueOf(tracker.initialTimestamp));
        kafkaAttrs.put(KAFKA_MAX_OFFSET, String.valueOf(tracker.maxOffset));
        if (tracker.leaderEpoch != null) {
            kafkaAttrs.put(KAFKA_LEADER_EPOCH, String.valueOf(tracker.leaderEpoch));
        }

        kafkaAttrs.put(KAFKA_CONSUMER_GROUP_ID, kafkaConsumer.groupMetadata().groupId());
        kafkaAttrs.put(KAFKA_CONSUMER_OFFSETS_COMMITTED, String.valueOf(commitOffsets));

        // If we have a kafka key, we will add it as an attribute only if
        // the FlowFile contains a single Record, or if the Records have been separated by Key,
        // because we then know that even though there are multiple Records, they all have the same key.
        if (tracker.key != null && (tracker.totalRecords == 1 || separateByKey)) {
            if (keyEncoding != KeyEncoding.DO_NOT_ADD) {
                kafkaAttrs.put(KafkaFlowFileAttribute.KAFKA_KEY, tracker.key);
            }
        }

        if (tracker.totalRecords > 1) {
            // Add a record.count attribute to remain consistent with other record-oriented processors. If not
            // reading/writing records, then use "kafka.count" attribute.
            if (tracker.recordWriter == null) {
                kafkaAttrs.put(KAFKA_COUNT, String.valueOf(tracker.totalRecords));
            } else {
                kafkaAttrs.put("record.count", String.valueOf(tracker.totalRecords));
            }
        }

        final FlowFile newFlowFile = getProcessSession().putAllAttributes(tracker.flowFile, kafkaAttrs);
        final long executionDurationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - leaseStartNanos);
        final String transitUri = StandardTransitUriProvider.getTransitUri(securityProtocol, bootstrapServers, tracker.topic);
        getProcessSession().getProvenanceReporter().receive(newFlowFile, transitUri, executionDurationMillis);
        tracker.updateFlowFile(newFlowFile);
    }


    private static class BundleTracker {
        final long initialOffset;
        final long initialTimestamp;
        final int partition;
        final String topic;
        final String key;
        final RecordSetWriter recordWriter;
        FlowFile flowFile;
        long totalRecords = 0;
        long maxOffset;
        Integer leaderEpoch;

        private BundleTracker(final ConsumerRecord<byte[], byte[]> initialRecord, final TopicPartition topicPartition, final KeyEncoding keyEncoding) {
            this(initialRecord, topicPartition, keyEncoding, null);
        }

        private BundleTracker(final ConsumerRecord<byte[], byte[]> initialRecord, final TopicPartition topicPartition, final KeyEncoding keyEncoding, final RecordSetWriter recordWriter) {
            this.initialOffset = initialRecord.offset();
            this.maxOffset = initialOffset;
            this.initialTimestamp = initialRecord.timestamp();
            this.partition = topicPartition.partition();
            this.topic = topicPartition.topic();
            this.recordWriter = recordWriter;
            this.key = encodeKafkaKey(initialRecord.key(), keyEncoding);
            this.leaderEpoch = initialRecord.leaderEpoch().orElse(null);
        }

        private void incrementRecordCount(final long count, final long maxOffset, final Integer leaderEpoch) {
            totalRecords += count;
            this.maxOffset = Math.max(this.maxOffset, maxOffset);
            if (leaderEpoch != null) {
                this.leaderEpoch = (this.leaderEpoch == null) ? leaderEpoch : Math.max(this.leaderEpoch, leaderEpoch);
            }
        }

        private void updateFlowFile(final FlowFile flowFile) {
            this.flowFile = flowFile;
        }
    }

    private record BundleInformation(
            TopicPartition topicPartition,
            RecordSchema schema,
            Map<String, String> attributes,
            byte[] messageKey
    ) {

        @Override
        public int hashCode() {
            return 41 + Objects.hash(topicPartition, schema, attributes) + 37 * Arrays.hashCode(messageKey);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            } else if (obj == null) {
                return false;
            } else if (obj instanceof BundleInformation other) {
                return Objects.equals(topicPartition, other.topicPartition) && Objects.equals(schema, other.schema)
                        && Objects.equals(attributes, other.attributes) && Arrays.equals(this.messageKey, other.messageKey);
            } else {
                return false;
            }
        }
    }
}
