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
package org.apache.nifi.kafka.service.consumer.pool;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Pooled Object Factory for Kafka Consumers
 */
class ConsumerPooledObjectFactory extends BaseKeyedPooledObjectFactory<Subscription, Consumer<byte[], byte[]>> {
    private final Properties consumerProperties;

    private final ConsumerFactory consumerFactory;

    /**
     * Consumer Pooled Object Factory constructor with Kafka Consumer Properties
     *
     * @param consumerProperties Kafka Consumer Properties
     * @param consumerFactory Kafka Consumer Factory
     */
    ConsumerPooledObjectFactory(final Properties consumerProperties, final ConsumerFactory consumerFactory) {
        this.consumerProperties = Objects.requireNonNull(consumerProperties, "Consumer Properties required");
        this.consumerFactory = Objects.requireNonNull(consumerFactory, "Consumer Factory required");
    }

    @Override
    public Consumer<byte[], byte[]> create(final Subscription subscription) {
        Objects.requireNonNull(subscription, "Topic Subscription required");

        final Properties properties = new Properties();
        properties.putAll(consumerProperties);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, subscription.getGroupId());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, subscription.getAutoOffsetReset().getValue());

        final Consumer<byte[], byte[]> consumer = consumerFactory.newConsumer(properties);

        final Optional<Pattern> topicPatternFound = subscription.getTopicPattern();
        if (topicPatternFound.isPresent()) {
            final Pattern topicPattern = topicPatternFound.get();
            consumer.subscribe(topicPattern);
        } else {
            final Collection<String> topics = subscription.getTopics();
            consumer.subscribe(topics);
        }

        return consumer;
    }

    /**
     * Wrap Kafka Consumer using Default Pooled Object for tracking
     *
     * @param consumer Kafka Consumer
     * @return Pooled Object wrapper
     */
    @Override
    public PooledObject<Consumer<byte[], byte[]>> wrap(final Consumer<byte[], byte[]> consumer) {
        Objects.requireNonNull(consumer, "Consumer required");
        return new DefaultPooledObject<>(consumer);
    }

    /**
     * Destroy Pooled Object closes wrapped Kafka Consumer
     *
     * @param subscription Subscription
     * @param pooledObject Pooled Object with Consumer to be closed
     */
    @Override
    public void destroyObject(final Subscription subscription, final PooledObject<Consumer<byte[], byte[]>> pooledObject) {
        Objects.requireNonNull(pooledObject, "Pooled Object required");
        final Consumer<byte[], byte[]> consumer = pooledObject.getObject();
        consumer.unsubscribe();
        consumer.close();
    }
}
