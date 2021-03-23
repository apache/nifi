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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * A pool of Kafka Consumers for a given topic. Consumers can be obtained by
 * calling 'obtainConsumer'. Once closed the pool is ready to be immediately
 * used again.
 */
public class ConsumerPool implements Closeable {

    private final BlockingQueue<SimpleConsumerLease> pooledLeases;
    private final List<String> topics;
    private final Pattern topicPattern;
    private final Map<String, Object> kafkaProperties;
    private final long maxWaitMillis;
    private final ComponentLog logger;
    private final byte[] demarcatorBytes;
    private final String keyEncoding;
    private final String securityProtocol;
    private final String bootstrapServers;
    private final boolean honorTransactions;
    private final RecordReaderFactory readerFactory;
    private final RecordSetWriterFactory writerFactory;
    private final Charset headerCharacterSet;
    private final Pattern headerNamePattern;
    private final boolean separateByKey;
    private final int[] partitionsToConsume;
    private final AtomicLong consumerCreatedCountRef = new AtomicLong();
    private final AtomicLong consumerClosedCountRef = new AtomicLong();
    private final AtomicLong leasesObtainedCountRef = new AtomicLong();
    private final Queue<List<TopicPartition>> availableTopicPartitions = new LinkedBlockingQueue<>();

    /**
     * Creates a pool of KafkaConsumer objects that will grow up to the maximum
     * indicated threads from the given context. Consumers are lazily
     * initialized. We may elect to not create up to the maximum number of
     * configured consumers if the broker reported lag time for all topics is
     * below a certain threshold.
     *
     * @param maxConcurrentLeases max allowable consumers at once
     * @param demarcator bytes to use as demarcator between messages; null or
     * empty means no demarcator
     * @param kafkaProperties properties to use to initialize kafka consumers
     * @param topics the topics to subscribe to
     * @param maxWaitMillis maximum time to wait for a given lease to acquire
     * data before committing
     * @param keyEncoding the encoding to use for the key of a kafka message if
     * found
     * @param securityProtocol the security protocol used
     * @param bootstrapServers the bootstrap servers
     * @param logger the logger to report any errors/warnings
     */
    public ConsumerPool(
            final int maxConcurrentLeases,
            final byte[] demarcator,
            final boolean separateByKey,
            final Map<String, Object> kafkaProperties,
            final List<String> topics,
            final long maxWaitMillis,
            final String keyEncoding,
            final String securityProtocol,
            final String bootstrapServers,
            final ComponentLog logger,
            final boolean honorTransactions,
            final Charset headerCharacterSet,
            final Pattern headerNamePattern,
            final int[] partitionsToConsume) {
        this.pooledLeases = new LinkedBlockingQueue<>();
        this.maxWaitMillis = maxWaitMillis;
        this.logger = logger;
        this.demarcatorBytes = demarcator;
        this.keyEncoding = keyEncoding;
        this.securityProtocol = securityProtocol;
        this.bootstrapServers = bootstrapServers;
        this.kafkaProperties = Collections.unmodifiableMap(kafkaProperties);
        this.topics = Collections.unmodifiableList(topics);
        this.topicPattern = null;
        this.readerFactory = null;
        this.writerFactory = null;
        this.honorTransactions = honorTransactions;
        this.headerCharacterSet = headerCharacterSet;
        this.headerNamePattern = headerNamePattern;
        this.separateByKey = separateByKey;
        this.partitionsToConsume = partitionsToConsume;
        enqueueAssignedPartitions(partitionsToConsume);
    }

    public ConsumerPool(
            final int maxConcurrentLeases,
            final byte[] demarcator,
            final boolean separateByKey,
            final Map<String, Object> kafkaProperties,
            final Pattern topics,
            final long maxWaitMillis,
            final String keyEncoding,
            final String securityProtocol,
            final String bootstrapServers,
            final ComponentLog logger,
            final boolean honorTransactions,
            final Charset headerCharacterSet,
            final Pattern headerNamePattern,
            final int[] partitionsToConsume) {
        this.pooledLeases = new LinkedBlockingQueue<>();
        this.maxWaitMillis = maxWaitMillis;
        this.logger = logger;
        this.demarcatorBytes = demarcator;
        this.keyEncoding = keyEncoding;
        this.securityProtocol = securityProtocol;
        this.bootstrapServers = bootstrapServers;
        this.kafkaProperties = Collections.unmodifiableMap(kafkaProperties);
        this.topics = null;
        this.topicPattern = topics;
        this.readerFactory = null;
        this.writerFactory = null;
        this.honorTransactions = honorTransactions;
        this.headerCharacterSet = headerCharacterSet;
        this.headerNamePattern = headerNamePattern;
        this.separateByKey = separateByKey;
        this.partitionsToConsume = partitionsToConsume;
        enqueueAssignedPartitions(partitionsToConsume);
    }

    public ConsumerPool(
            final int maxConcurrentLeases,
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final Map<String, Object> kafkaProperties,
            final Pattern topics,
            final long maxWaitMillis,
            final String securityProtocol,
            final String bootstrapServers,
            final ComponentLog logger,
            final boolean honorTransactions,
            final Charset headerCharacterSet,
            final Pattern headerNamePattern,
            final boolean separateByKey,
            final String keyEncoding,
            final int[] partitionsToConsume) {
        this.pooledLeases = new LinkedBlockingQueue<>();
        this.maxWaitMillis = maxWaitMillis;
        this.logger = logger;
        this.demarcatorBytes = null;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.securityProtocol = securityProtocol;
        this.bootstrapServers = bootstrapServers;
        this.kafkaProperties = Collections.unmodifiableMap(kafkaProperties);
        this.topics = null;
        this.topicPattern = topics;
        this.honorTransactions = honorTransactions;
        this.headerCharacterSet = headerCharacterSet;
        this.headerNamePattern = headerNamePattern;
        this.separateByKey = separateByKey;
        this.keyEncoding = keyEncoding;
        this.partitionsToConsume = partitionsToConsume;
        enqueueAssignedPartitions(partitionsToConsume);
    }

    public ConsumerPool(
            final int maxConcurrentLeases,
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final Map<String, Object> kafkaProperties,
            final List<String> topics,
            final long maxWaitMillis,
            final String securityProtocol,
            final String bootstrapServers,
            final ComponentLog logger,
            final boolean honorTransactions,
            final Charset headerCharacterSet,
            final Pattern headerNamePattern,
            final boolean separateByKey,
            final String keyEncoding,
            final int[] partitionsToConsume) {
        this.pooledLeases = new LinkedBlockingQueue<>();
        this.maxWaitMillis = maxWaitMillis;
        this.logger = logger;
        this.demarcatorBytes = null;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.securityProtocol = securityProtocol;
        this.bootstrapServers = bootstrapServers;
        this.kafkaProperties = Collections.unmodifiableMap(kafkaProperties);
        this.topics = topics;
        this.topicPattern = null;
        this.honorTransactions = honorTransactions;
        this.headerCharacterSet = headerCharacterSet;
        this.headerNamePattern = headerNamePattern;
        this.separateByKey = separateByKey;
        this.keyEncoding = keyEncoding;
        this.partitionsToConsume = partitionsToConsume;
        enqueueAssignedPartitions(partitionsToConsume);
    }

    public int getPartitionCount() {
        // If using regex for topic names, just return -1
        if (topics == null || topics.isEmpty()) {
            return -1;
        }

        int partitionsEachTopic = 0;
        try (final Consumer<byte[], byte[]> consumer = createKafkaConsumer()) {
            for (final String topicName : topics) {
                final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicName);
                final int partitionsThisTopic = partitionInfos.size();
                if (partitionsEachTopic != 0 && partitionsThisTopic != partitionsEachTopic) {
                    throw new IllegalStateException("The specific topic names do not have the same number of partitions");
                }

                partitionsEachTopic = partitionsThisTopic;
            }
        }

        return partitionsEachTopic;
    }

    /**
     * Obtains a consumer from the pool if one is available or lazily
     * initializes a new one if deemed necessary.
     *
     * @param session the session for which the consumer lease will be
     *            associated
     * @param processContext the ProcessContext for which the consumer
     *            lease will be associated
     * @return consumer to use or null if not available or necessary
     */
    public ConsumerLease obtainConsumer(final ProcessSession session, final ProcessContext processContext) {
        // If there are any partition assignments that do not have leases in our pool, create the leases and add them to the pool.
        // This is not necessary for us to handle if using automatic subscriptions because the Kafka protocol will ensure that each consumer
        // has the appropriate partitions. However, if we are using explicit assignment, it's important to create these leases and add them
        // to our pool in order to avoid starvation. E.g., if we have only a single concurrent task and 5 partitions assigned, we cannot simply
        // wait until pooledLeases.poll() returns null to create a new ConsumerLease, as doing so may result in constantly pulling from only a
        // single partition (since we'd get a Lease for Partition 1, then use it, and put it back in the pool).
        recreateAssignedConsumers();

        SimpleConsumerLease lease = pooledLeases.poll();
        if (lease == null) {
            lease = createConsumerLease();
            if (lease == null) {
                return null;
            }
        }

        lease.setProcessSession(session, processContext);

        leasesObtainedCountRef.incrementAndGet();
        return lease;
    }

    private void recreateAssignedConsumers() {
        List<TopicPartition> topicPartitions;
        while ((topicPartitions = availableTopicPartitions.poll()) != null) {
            final SimpleConsumerLease simpleConsumerLease = createConsumerLease(topicPartitions);
            pooledLeases.add(simpleConsumerLease);
        }
    }

    private SimpleConsumerLease createConsumerLease() {
        if (partitionsToConsume != null) {
            logger.debug("Cannot obtain lease to communicate with Kafka. Since partitions are explicitly assigned, cannot create a new lease.");
            return null;
        }

        final Consumer<byte[], byte[]> consumer = createKafkaConsumer();
        consumerCreatedCountRef.incrementAndGet();

        /*
         * For now return a new consumer lease. But we could later elect to
         * have this return null if we determine the broker indicates that
         * the lag time on all topics being monitored is sufficiently low.
         * For now we should encourage conservative use of threads because
         * having too many means we'll have at best useless threads sitting
         * around doing frequent network calls and at worst having consumers
         * sitting idle which could prompt excessive rebalances.
         */
        final SimpleConsumerLease lease = new SimpleConsumerLease(consumer, null);

        // This subscription tightly couples the lease to the given
        // consumer. They cannot be separated from then on.
        if (topics == null) {
            consumer.subscribe(topicPattern, lease);
        } else {
            consumer.subscribe(topics, lease);
        }

        return lease;
    }

    private SimpleConsumerLease createConsumerLease(final List<TopicPartition> topicPartitions) {
        final Consumer<byte[], byte[]> consumer = createKafkaConsumer();
        consumerCreatedCountRef.incrementAndGet();
        consumer.assign(topicPartitions);

        final SimpleConsumerLease lease = new SimpleConsumerLease(consumer, topicPartitions);
        return lease;
    }

    private void enqueueAssignedPartitions(final int[] partitionsToConsume) {
        if (partitionsToConsume == null) {
            return;
        }

        for (final int partition : partitionsToConsume) {
            final List<TopicPartition> topicPartitions = createTopicPartitions(partition);
            availableTopicPartitions.offer(topicPartitions);
        }
    }

    private List<TopicPartition> createTopicPartitions(final int partition) {
        final List<TopicPartition> topicPartitions = new ArrayList<>();
        for (final String topic : topics) {
            final TopicPartition topicPartition = new TopicPartition(topic, partition);
            topicPartitions.add(topicPartition);
        }

        return topicPartitions;
    }

    /**
     * Exposed as protected method for easier unit testing
     *
     * @return consumer
     * @throws KafkaException if unable to subscribe to the given topics
     */
    protected Consumer<byte[], byte[]> createKafkaConsumer() {
        final Map<String, Object> properties = new HashMap<>(kafkaProperties);
        if (honorTransactions) {
            properties.put("isolation.level", "read_committed");
        } else {
            properties.put("isolation.level", "read_uncommitted");
        }
        final Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);
        return consumer;
    }

    /**
     * Closes all consumers in the pool. Can be safely called repeatedly.
     */
    @Override
    public void close() {
        final List<SimpleConsumerLease> leases = new ArrayList<>();
        pooledLeases.drainTo(leases);
        leases.forEach((lease) -> {
            lease.close(true);
        });
    }

    private void closeConsumer(final Consumer<?, ?> consumer) {
        consumerClosedCountRef.incrementAndGet();
        try {
            consumer.unsubscribe();
        } catch (Exception e) {
            logger.warn("Failed while unsubscribing " + consumer, e);
        }

        try {
            consumer.close();
        } catch (Exception e) {
            logger.warn("Failed while closing " + consumer, e);
        }
    }

    PoolStats getPoolStats() {
        return new PoolStats(consumerCreatedCountRef.get(), consumerClosedCountRef.get(), leasesObtainedCountRef.get());
    }

    private class SimpleConsumerLease extends ConsumerLease {
        private final Consumer<byte[], byte[]> consumer;
        private final List<TopicPartition> assignedPartitions;
        private volatile ProcessSession session;
        private volatile ProcessContext processContext;
        private volatile boolean closedConsumer;

        private SimpleConsumerLease(final Consumer<byte[], byte[]> consumer, final List<TopicPartition> assignedPartitions) {
            super(maxWaitMillis, consumer, demarcatorBytes, keyEncoding, securityProtocol, bootstrapServers,
                readerFactory, writerFactory, logger, headerCharacterSet, headerNamePattern, separateByKey);
            this.consumer = consumer;
            this.assignedPartitions = assignedPartitions;
        }

        void setProcessSession(final ProcessSession session, final ProcessContext context) {
            this.session = session;
            this.processContext = context;
        }

        @Override
        public List<TopicPartition> getAssignedPartitions() {
            return assignedPartitions;
        }

        @Override
        public void yield() {
            if (processContext != null) {
                processContext.yield();
            }
        }

        @Override
        public ProcessSession getProcessSession() {
            return session;
        }

        @Override
        public void close() {
            super.close();
            close(false);
        }

        public void close(final boolean forceClose) {
            if (closedConsumer) {
                return;
            }

            super.close();
            if (session != null) {
                session.rollback();
                setProcessSession(null, null);
            }

            if (forceClose || isPoisoned() || !pooledLeases.offer(this)) {
                closedConsumer = true;
                closeConsumer(consumer);

                // If explicit topic/partition assignment is used, make the assignments for this Lease available again.
                if (assignedPartitions != null) {
                    logger.debug("Adding partitions {} back to the pool", assignedPartitions);
                    availableTopicPartitions.offer(assignedPartitions);
                }
            }
        }
    }

    static final class PoolStats {

        final long consumerCreatedCount;
        final long consumerClosedCount;
        final long leasesObtainedCount;

        PoolStats(
                final long consumerCreatedCount,
                final long consumerClosedCount,
                final long leasesObtainedCount
        ) {
            this.consumerCreatedCount = consumerCreatedCount;
            this.consumerClosedCount = consumerClosedCount;
            this.leasesObtainedCount = leasesObtainedCount;
        }

        @Override
        public String toString() {
            return "Created Consumers [" + consumerCreatedCount + "]\n"
                    + "Closed Consumers  [" + consumerClosedCount + "]\n"
                    + "Leases Obtained   [" + leasesObtainedCount + "]\n";
        }

    }

}
