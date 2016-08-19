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
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A pool of Kafka Consumers for a given topic. Clients must create the ConsumerPool and call initialize() before
 * acquiring consumers. Consumers should be returned by calling returnConsumerResource.
 */
public class ConsumerPool implements Closeable {

    private final int size;
    private final BlockingQueue<ConsumerResource> consumers;
    private final String topic;
    private final Properties kafkaProperties;
    private final ComponentLog logger;
    private boolean initialized = false;

    /**
     * Initializes the pool with the given size, topic, properties, and logger, but does not create any consumers until initialize() is called.
     *
     * @param size the number of consumers to pool
     * @param topic the topic to consume from
     * @param kafkaProperties the properties for each consumer
     * @param logger the logger to report any errors/warnings
     */
    public ConsumerPool(final int size, final String topic, final Properties kafkaProperties, final ComponentLog logger) {
        this.size = size;
        this.logger = logger;
        this.topic = topic;
        this.kafkaProperties = kafkaProperties;
        this.consumers = new LinkedBlockingQueue<>(size);
    }

    /**
     * Creates the consumers and subscribes them to the given topic. This method must be called before
     * acquiring any consumers.
     */
    public synchronized void initialize() {
        if (initialized) {
            return;
        }

        for (int i=0; i < size; i++) {
            ConsumerResource resource = createConsumerResource();
            consumers.offer(resource);
        }

        initialized = true;
    }

    /**
     * @return a ConsumerResource from the pool, or a newly created ConsumerResource if none were available in the pool
     * @throws IllegalStateException if attempting to get a consumer before calling initialize()
     */
    public synchronized ConsumerResource getConsumerResource() {
        if (!initialized) {
            throw new IllegalStateException("ConsumerPool must be initialized before acquiring consumers");
        }

        ConsumerResource consumerResource = consumers.poll();
        if (consumerResource == null) {
            consumerResource = createConsumerResource();
        }
        return consumerResource;
    }

    /**
     * If the given ConsumerResource has been poisoned then it is closed and not returned to the pool,
     * otherwise it is attempted to be returned to the pool. If the pool is already full then it is closed
     * and not returned.
     *
     * @param consumerResource
     */
    public synchronized void returnConsumerResource(final ConsumerResource consumerResource) {
        if (consumerResource == null) {
            return;
        }

        if (consumerResource.isPoisoned()) {
            closeConsumer(consumerResource.getConsumer());
        } else {
            boolean added = consumers.offer(consumerResource);
            if (!added) {
                closeConsumer(consumerResource.getConsumer());
            }
        }
    }

    /**
     * Closes all ConsumerResources in the pool and resets the initialization state of this pool.
     *
     * @throws IOException should never throw
     */
    @Override
    public synchronized void close() throws IOException {
        ConsumerResource consumerResource;
        while ((consumerResource = consumers.poll()) != null) {
            closeConsumer(consumerResource.getConsumer());
        }
        initialized = false;
    }

    private ConsumerResource createConsumerResource() {
        final Consumer<byte[],byte[]> kafkaConsumer = new KafkaConsumer<>(kafkaProperties);
        kafkaConsumer.subscribe(Collections.singletonList(this.topic));
        return new ConsumerResource(kafkaConsumer, this, logger);
    }

    private void closeConsumer(Consumer consumer) {
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

}
