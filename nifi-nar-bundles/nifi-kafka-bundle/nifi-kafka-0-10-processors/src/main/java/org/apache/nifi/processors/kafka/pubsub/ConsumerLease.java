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

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.xml.bind.DatatypeConverter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import static org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_0_10.REL_SUCCESS;
import static org.apache.nifi.processors.kafka.pubsub.KafkaProcessorUtils.HEX_ENCODING;
import static org.apache.nifi.processors.kafka.pubsub.KafkaProcessorUtils.UTF8_ENCODING;

/**
 * This class represents a lease to access a Kafka Consumer object. The lease is
 * intended to be obtained from a ConsumerPool. The lease is closeable to allow
 * for the clean model of a try w/resources whereby non-exceptional cases mean
 * the lease will be returned to the pool for future use by others. A given
 * lease may only belong to a single thread a time.
 */
public abstract class ConsumerLease implements Closeable, ConsumerRebalanceListener {

    private final long maxWaitMillis;
    private final Consumer<byte[], byte[]> kafkaConsumer;
    private final ComponentLog logger;
    private final byte[] demarcatorBytes;
    private final String keyEncoding;
    private final String securityProtocol;
    private final String bootstrapServers;
    private boolean poisoned = false;
    //used for tracking demarcated flowfiles to their TopicPartition so we can append
    //to them on subsequent poll calls
    private final Map<TopicPartition, BundleTracker> bundleMap = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> uncommittedOffsetsMap = new HashMap<>();
    private long leaseStartNanos = -1;
    private boolean lastPollEmpty = false;
    private int totalFlowFiles = 0;

    ConsumerLease(
            final long maxWaitMillis,
            final Consumer<byte[], byte[]> kafkaConsumer,
            final byte[] demarcatorBytes,
            final String keyEncoding,
            final String securityProtocol,
            final String bootstrapServers,
            final ComponentLog logger) {
        this.maxWaitMillis = maxWaitMillis;
        this.kafkaConsumer = kafkaConsumer;
        this.demarcatorBytes = demarcatorBytes;
        this.keyEncoding = keyEncoding;
        this.securityProtocol = securityProtocol;
        this.bootstrapServers = bootstrapServers;
        this.logger = logger;
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
        totalFlowFiles = 0;
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
        logger.debug("Rebalance Alert: Paritions '{}' revoked for lease '{}' with consumer '{}'", new Object[]{partitions, this, kafkaConsumer});
        //force a commit here.  Can reuse the session and consumer after this but must commit now to avoid duplicates if kafka reassigns parittion
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
        logger.debug("Rebalance Alert: Paritions '{}' assigned for lease '{}' with consumer '{}'", new Object[]{partitions, this, kafkaConsumer});
    }

    /**
     * Executes a poll on the underlying Kafka Consumer and creates any new
     * flowfiles necessary or appends to existing ones if in demarcation mode.
     */
    void poll() {
        /**
         * Implementation note: If we take too long (30 secs?) between kafka
         * poll calls and our own record processing to any subsequent poll calls
         * or the commit we can run into a situation where the commit will
         * succeed to the session but fail on committing offsets. This is
         * apparently different than the Kafka scenario of electing to rebalance
         * for other reasons but in this case is due a session timeout. It
         * appears Kafka KIP-62 aims to offer more control over the meaning of
         * various timeouts. If we do run into this case it could result in
         * duplicates.
         */
        try {
            final ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(10);
            lastPollEmpty = records.count() == 0;
            processRecords(records);
        } catch (final Throwable t) {
            this.poison();
            throw t;
        }
    }

    /**
     * Notifies Kafka to commit the offsets for the specified topic/partition
     * pairs to the specified offsets w/the given metadata. This can offer
     * higher performance than the other commitOffsets call as it allows the
     * kafka client to collect more data from Kafka before committing the
     * offsets.
     *
     * if false then we didn't do anything and should probably yield if true
     * then we committed new data
     *
     */
    boolean commit() {
        if (uncommittedOffsetsMap.isEmpty()) {
            resetInternalState();
            return false;
        }
        try {
            /**
             * Committing the nifi session then the offsets means we have an at
             * least once guarantee here. If we reversed the order we'd have at
             * most once.
             */
            final Collection<FlowFile> bundledFlowFiles = getBundles();
            if (!bundledFlowFiles.isEmpty()) {
                getProcessSession().transfer(bundledFlowFiles, REL_SUCCESS);
            }
            getProcessSession().commit();
            kafkaConsumer.commitSync(uncommittedOffsetsMap);
            resetInternalState();
            return true;
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
        if (durationMillis > maxWaitMillis) {
            return false;
        }

        //stop if we've generated enough flowfiles that we need to be concerned about memory usage for the objects
        if (bundleMap.size() > 200) { //a magic number - the number of simultaneous bundles to track
            return false;
        } else {
            return totalFlowFiles < 15000;//admittedlly a magic number - good candidate for processor property
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

    private void processRecords(final ConsumerRecords<byte[], byte[]> records) {

        records.partitions().stream().forEach(partition -> {
            List<ConsumerRecord<byte[], byte[]>> messages = records.records(partition);
            if (!messages.isEmpty()) {
                //update maximum offset map for this topic partition
                long maxOffset = messages.stream()
                        .mapToLong(record -> record.offset())
                        .max()
                        .getAsLong();
                uncommittedOffsetsMap.put(partition, new OffsetAndMetadata(maxOffset + 1L));

                //write records to content repository and session
                if (demarcatorBytes == null) {
                    totalFlowFiles += messages.size();
                    messages.stream().forEach(message -> {
                        writeData(getProcessSession(), message, partition);
                    });
                } else {
                    writeData(getProcessSession(), messages, partition);
                }
            }
        });
    }

    private static String encodeKafkaKey(final byte[] key, final String encoding) {
        if (key == null) {
            return null;
        }

        if (HEX_ENCODING.getValue().equals(encoding)) {
            return DatatypeConverter.printHexBinary(key);
        } else if (UTF8_ENCODING.getValue().equals(encoding)) {
            return new String(key, StandardCharsets.UTF_8);
        } else {
            return null;  // won't happen because it is guaranteed by the Allowable Values
        }
    }

    private Collection<FlowFile> getBundles() {
        final List<FlowFile> flowFiles = new ArrayList<>();
        for (final BundleTracker tracker : bundleMap.values()) {
            populateAttributes(tracker);
            flowFiles.add(tracker.flowFile);
        }
        return flowFiles;
    }

    private void writeData(final ProcessSession session, ConsumerRecord<byte[], byte[]> record, final TopicPartition topicPartition) {
        FlowFile flowFile = session.create();
        final BundleTracker tracker = new BundleTracker(record, topicPartition, keyEncoding);
        tracker.incrementRecordCount(1);
        flowFile = session.write(flowFile, out -> {
            out.write(record.value());
        });
        tracker.updateFlowFile(flowFile);
        populateAttributes(tracker);
        session.transfer(tracker.flowFile, REL_SUCCESS);
    }

    private void writeData(final ProcessSession session, final List<ConsumerRecord<byte[], byte[]>> records, final TopicPartition topicPartition) {
        final ConsumerRecord<byte[], byte[]> firstRecord = records.get(0);
        final boolean demarcateFirstRecord;
        BundleTracker tracker = bundleMap.get(topicPartition);
        FlowFile flowFile;
        if (tracker == null) {
            tracker = new BundleTracker(firstRecord, topicPartition, keyEncoding);
            flowFile = session.create();
            tracker.updateFlowFile(flowFile);
            demarcateFirstRecord = false; //have not yet written records for this topic/partition in this lease
        } else {
            demarcateFirstRecord = true; //have already been writing records for this topic/partition in this lease
        }
        flowFile = tracker.flowFile;
        tracker.incrementRecordCount(records.size());
        flowFile = session.append(flowFile, out -> {
            boolean useDemarcator = demarcateFirstRecord;
            for (final ConsumerRecord<byte[], byte[]> record : records) {
                if (useDemarcator) {
                    out.write(demarcatorBytes);
                }
                out.write(record.value());
                useDemarcator = true;
            }
        });
        tracker.updateFlowFile(flowFile);
        bundleMap.put(topicPartition, tracker);
    }

    private void populateAttributes(final BundleTracker tracker) {
        final Map<String, String> kafkaAttrs = new HashMap<>();
        kafkaAttrs.put(KafkaProcessorUtils.KAFKA_OFFSET, String.valueOf(tracker.initialOffset));
        if (tracker.key != null && tracker.totalRecords == 1) {
            kafkaAttrs.put(KafkaProcessorUtils.KAFKA_KEY, tracker.key);
        }
        kafkaAttrs.put(KafkaProcessorUtils.KAFKA_PARTITION, String.valueOf(tracker.partition));
        kafkaAttrs.put(KafkaProcessorUtils.KAFKA_TOPIC, tracker.topic);
        if (tracker.totalRecords > 1) {
            kafkaAttrs.put(KafkaProcessorUtils.KAFKA_COUNT, String.valueOf(tracker.totalRecords));
        }
        final FlowFile newFlowFile = getProcessSession().putAllAttributes(tracker.flowFile, kafkaAttrs);
        final long executionDurationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - leaseStartNanos);
        final String transitUri = KafkaProcessorUtils.buildTransitURI(securityProtocol, bootstrapServers, tracker.topic);
        getProcessSession().getProvenanceReporter().receive(newFlowFile, transitUri, executionDurationMillis);
        tracker.updateFlowFile(newFlowFile);
    }

    private static class BundleTracker {

        final long initialOffset;
        final int partition;
        final String topic;
        final String key;
        FlowFile flowFile;
        long totalRecords = 0;

        private BundleTracker(final ConsumerRecord<byte[], byte[]> initialRecord, final TopicPartition topicPartition, final String keyEncoding) {
            this.initialOffset = initialRecord.offset();
            this.partition = topicPartition.partition();
            this.topic = topicPartition.topic();
            this.key = encodeKafkaKey(initialRecord.key(), keyEncoding);
        }

        private void incrementRecordCount(final long count) {
            totalRecords += count;
        }

        private void updateFlowFile(final FlowFile flowFile) {
            this.flowFile = flowFile;
        }

    }

}
