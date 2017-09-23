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
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.nifi.logging.ComponentLog;

public class PublisherPool implements Closeable {
    private final ComponentLog logger;
    private final BlockingQueue<PublisherLease> publisherQueue;
    private final Map<String, Object> kafkaProperties;
    private final int maxMessageSize;
    private final long maxAckWaitMillis;
    private final boolean useTransactions;
    private final Pattern attributeNameRegex;
    private final Charset headerCharacterSet;

    private volatile boolean closed = false;

    PublisherPool(final Map<String, Object> kafkaProperties, final ComponentLog logger, final int maxMessageSize, final long maxAckWaitMillis,
        final boolean useTransactions, final Pattern attributeNameRegex, final Charset headerCharacterSet) {
        this.logger = logger;
        this.publisherQueue = new LinkedBlockingQueue<>();
        this.kafkaProperties = kafkaProperties;
        this.maxMessageSize = maxMessageSize;
        this.maxAckWaitMillis = maxAckWaitMillis;
        this.useTransactions = useTransactions;
        this.attributeNameRegex = attributeNameRegex;
        this.headerCharacterSet = headerCharacterSet;
    }

    public PublisherLease obtainPublisher() {
        if (isClosed()) {
            throw new IllegalStateException("Connection Pool is closed");
        }

        PublisherLease lease = publisherQueue.poll();
        if (lease != null) {
            return lease;
        }

        lease = createLease();
        return lease;
    }

    private PublisherLease createLease() {
        final Map<String, Object> properties = new HashMap<>(kafkaProperties);
        if (useTransactions) {
            properties.put("transactional.id", UUID.randomUUID().toString());
        }

        final Producer<byte[], byte[]> producer = new KafkaProducer<>(properties);

        final PublisherLease lease = new PublisherLease(producer, maxMessageSize, maxAckWaitMillis, logger, useTransactions, attributeNameRegex, headerCharacterSet) {
            @Override
            public void close() {
                if (isPoisoned() || isClosed()) {
                    super.close();
                } else {
                    publisherQueue.offer(this);
                }
            }
        };

        return lease;
    }

    public synchronized boolean isClosed() {
        return closed;
    }

    @Override
    public synchronized void close() {
        closed = true;

        PublisherLease lease;
        while ((lease = publisherQueue.poll()) != null) {
            lease.close();
        }
    }

    /**
     * Returns the number of leases that are currently available
     *
     * @return the number of leases currently available
     */
    protected int available() {
        return publisherQueue.size();
    }
}
