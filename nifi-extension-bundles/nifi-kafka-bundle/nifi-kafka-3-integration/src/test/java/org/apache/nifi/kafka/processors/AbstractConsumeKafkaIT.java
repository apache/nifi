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
package org.apache.nifi.kafka.processors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractConsumeKafkaIT extends AbstractKafkaBaseIT {

    protected Properties getKafkaProducerProperties() {
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    protected void produceOne(final String topic, final Integer partition,
                              final String key, final String value, final List<Header> headers) throws ExecutionException, InterruptedException {

        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(getKafkaProducerProperties())) {
            final ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, key, value, headers);
            final Future<RecordMetadata> future = producer.send(record);
            final RecordMetadata metadata = future.get();
            assertEquals(topic, metadata.topic());
            assertTrue(metadata.hasOffset());
            assertEquals(0L, metadata.offset());
        }
    }

    protected void produce(final String topic, final Collection<ProducerRecord<String, String>> records)
            throws ExecutionException, InterruptedException {
        final Collection<RecordMetadata> metadatas = new ArrayList<>();
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(getKafkaProducerProperties())) {
            final Collection<Future<RecordMetadata>> futures = records.stream().map(producer::send).collect(Collectors.toList());
            for (final Future<RecordMetadata> future : futures) {
                metadatas.add(future.get());
            }
            assertEquals(records.size(), futures.size());
            assertEquals(futures.size(), metadatas.size());
        }
        for (RecordMetadata metadata : metadatas) {
            assertEquals(topic, metadata.topic());
            assertTrue(metadata.hasOffset());
        }
    }
}
