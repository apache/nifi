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
import org.apache.nifi.logging.ComponentLog;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

/**
 * A pool of Kafka Consumers for a given topic. Consumers can be obtained by
 * calling 'obtainConsumer'. Once closed the pool is ready to be immediately
 * used again.
 */
public class ConsumerPool implements Closeable {

    private final AtomicInteger activeLeaseCount = new AtomicInteger(0);
    private final int maxLeases;
    private final Queue<ConsumerLease> consumerLeases;
    private final List<String> topics;
    private final Map<String, Object> kafkaProperties;
    private final ComponentLog logger;

    private final AtomicLong consumerCreatedCountRef = new AtomicLong();
    private final AtomicLong consumerClosedCountRef = new AtomicLong();
    private final AtomicLong leasesObtainedCountRef = new AtomicLong();
    private final AtomicLong productivePollCountRef = new AtomicLong();
    private final AtomicLong unproductivePollCountRef = new AtomicLong();

    /**
     * Creates a pool of KafkaConsumer objects that will grow up to the maximum
     * indicated leases. Consumers are lazily initialized.
     *
     * @param maxLeases maximum number of active leases in the pool
     * @param topics the topics to consume from
     * @param kafkaProperties the properties for each consumer
     * @param logger the logger to report any errors/warnings
     */
    public ConsumerPool(final int maxLeases, final List<String> topics, final Map<String, String> kafkaProperties, final ComponentLog logger) {
        this.maxLeases = maxLeases;
        if (maxLeases <= 0) {
            throw new IllegalArgumentException("Max leases value must be greather than zero.");
        }
        this.logger = logger;
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("Must have a list of one or more topics");
        }
        this.topics = topics;
        this.kafkaProperties = new HashMap<>(kafkaProperties);
        this.consumerLeases = new ArrayDeque<>();
    }

    /**
     * Obtains a consumer from the pool if one is available
     *
     * @return consumer from the pool
     * @throws IllegalArgumentException if pool already contains
     */
    public ConsumerLease obtainConsumer() {
        final ConsumerLease lease;
        final int activeLeases;
        synchronized (this) {
            lease = consumerLeases.poll();
            activeLeases = activeLeaseCount.get();
        }
        if (lease == null && activeLeases >= maxLeases) {
            logger.warn("No available consumers and cannot create any as max consumer leases limit reached - verify pool settings");
            return null;
        }
        leasesObtainedCountRef.incrementAndGet();
        return (lease == null) ? createConsumer() : lease;
    }

    protected Consumer<byte[], byte[]> createKafkaConsumer() {
        return new KafkaConsumer<>(kafkaProperties);
    }

    private ConsumerLease createConsumer() {
        final Consumer<byte[], byte[]> kafkaConsumer = createKafkaConsumer();
        consumerCreatedCountRef.incrementAndGet();
        try {
            kafkaConsumer.subscribe(topics);
        } catch (final KafkaException kex) {
            try {
                kafkaConsumer.close();
                consumerClosedCountRef.incrementAndGet();
            } catch (final Exception ex) {
                consumerClosedCountRef.incrementAndGet();
                //ignore
            }
            throw kex;
        }

        final ConsumerLease lease = new ConsumerLease() {

            private volatile boolean poisoned = false;
            private volatile boolean closed = false;

            @Override
            public ConsumerRecords<byte[], byte[]> poll() {

                if (poisoned) {
                    throw new KafkaException("The consumer is poisoned and should no longer be used");
                }

                try {
                    final ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(50);
                    if (records.isEmpty()) {
                        unproductivePollCountRef.incrementAndGet();
                    } else {
                        productivePollCountRef.incrementAndGet();
                    }
                    return records;
                } catch (final KafkaException kex) {
                    logger.warn("Unable to poll from Kafka consumer so will poison and close this " + kafkaConsumer, kex);
                    poison();
                    close();
                    throw kex;
                }
            }

            @Override
            public void commitOffsets(final Map<TopicPartition, OffsetAndMetadata> offsets) {

                if (poisoned) {
                    throw new KafkaException("The consumer is poisoned and should no longer be used");
                }
                try {
                    kafkaConsumer.commitSync(offsets);
                } catch (final KafkaException kex) {
                    logger.warn("Unable to commit kafka consumer offsets so will poison and close this " + kafkaConsumer, kex);
                    poison();
                    close();
                    throw kex;
                }
            }

            @Override
            public void close() {
                if (closed) {
                    return;
                }
                if (poisoned || activeLeaseCount.get() > maxLeases) {
                    closeConsumer(kafkaConsumer);
                    activeLeaseCount.decrementAndGet();
                    closed = true;
                } else {
                    final boolean added;
                    synchronized (ConsumerPool.this) {
                        added = consumerLeases.offer(this);
                    }
                    if (!added) {
                        closeConsumer(kafkaConsumer);
                        activeLeaseCount.decrementAndGet();
                    }
                }
            }

            @Override
            public void poison() {
                poisoned = true;
            }
        };
        activeLeaseCount.incrementAndGet();
        return lease;
    }

    /**
     * Closes all consumers in the pool. Can be safely recalled.
     */
    @Override
    public void close() {
        final List<ConsumerLease> leases = new ArrayList<>();
        synchronized (this) {
            ConsumerLease lease = null;
            while ((lease = consumerLeases.poll()) != null) {
                leases.add(lease);
            }
        }
        for (final ConsumerLease lease : leases) {
            lease.poison();
            lease.close();
        }
    }

    private void closeConsumer(final Consumer consumer) {
        try {
            consumer.unsubscribe();
        } catch (Exception e) {
            logger.warn("Failed while unsubscribing " + consumer, e);
        }

        try {
            consumer.close();
            consumerClosedCountRef.incrementAndGet();
        } catch (Exception e) {
            consumerClosedCountRef.incrementAndGet();
            logger.warn("Failed while closing " + consumer, e);
        }
    }

    PoolStats getPoolStats() {
        return new PoolStats(consumerCreatedCountRef.get(), consumerClosedCountRef.get(), leasesObtainedCountRef.get(), productivePollCountRef.get(), unproductivePollCountRef.get());
    }

    static final class PoolStats {

        final long consumerCreatedCount;
        final long consumerClosedCount;
        final long leasesObtainedCount;
        final long productivePollCount;
        final long unproductivePollCount;

        PoolStats(
                final long consumerCreatedCount,
                final long consumerClosedCount,
                final long leasesObtainedCount,
                final long productivePollCount,
                final long unproductivePollCount
        ) {
            this.consumerCreatedCount = consumerCreatedCount;
            this.consumerClosedCount = consumerClosedCount;
            this.leasesObtainedCount = leasesObtainedCount;
            this.productivePollCount = productivePollCount;
            this.unproductivePollCount = unproductivePollCount;
        }

        @Override
        public String toString() {
            return "Created Consumers [" + consumerCreatedCount + "]\n"
                    + "Closed Consumers  [" + consumerClosedCount + "]\n"
                    + "Leases Obtained   [" + leasesObtainedCount + "]\n"
                    + "Productive Polls  [" + productivePollCount + "]\n"
                    + "Unproductive Polls  [" + unproductivePollCount + "]\n";
        }

    }

}
