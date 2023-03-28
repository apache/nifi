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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Objects;
import java.util.Properties;

/**
 * Standard Kafka Consumer Factory with Byte Array Deserializer for Key and Value elements
 */
class StandardConsumerFactory implements ConsumerFactory {
    /**
     * Create new Kafka Consumer with Byte Array Deserializer and configured properties
     *
     * @param properties Consumer configuration properties
     * @return Kafka Consumer
     */
    @Override
    public Consumer<byte[], byte[]> newConsumer(final Properties properties) {
        Objects.requireNonNull(properties, "Properties required");
        final ByteArrayDeserializer deserializer = new ByteArrayDeserializer();
        return new KafkaConsumer<>(properties, deserializer, deserializer);
    }
}
