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
package org.apache.nifi.processors.kafka.consume.it;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumeKafka_2_6_BaseIT {
    // https://docs.confluent.io/platform/current/installation/versions-interoperability.html#cp-and-apache-kafka-compatibility
    protected static final String IMAGE_NAME = "confluentinc/cp-kafka:6.1.14";  // Kafka 2.7

    protected static final Duration DURATION_POLL = Duration.ofMillis(1000L);

    protected static final KafkaContainer kafka;

    protected static ObjectMapper objectMapper;

    // https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/
    static {
        /*
         * NIFI-11259
         * Consider abstracting this configuration to a base class shared with
         * {@link org.apache.nifi.processors.kafka.publish.it.PublishKafka_2_6_BaseIT}, such that only one testcontainer
         * is needed to service all module ITs.
         */
        kafka = new KafkaContainer(DockerImageName.parse(IMAGE_NAME));
        kafka.start();
    }

    @BeforeAll
    protected static void beforeAll() {
        objectMapper = new ObjectMapper();
    }

    protected void addRecordReaderService(final TestRunner runner) throws InitializationException {
        final String readerId = "record-reader";
        final RecordReaderFactory readerService = new JsonTreeReader();
        runner.addControllerService(readerId, readerService);
        runner.enableControllerService(readerService);
        runner.setProperty(readerId, readerId);
    }

    protected void addRecordWriterService(final TestRunner runner) throws InitializationException {
        final String writerId = "record-writer";
        final RecordSetWriterFactory writerService = new JsonRecordSetWriter();
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);
        runner.setProperty(writerId, writerId);
    }

    protected void addKeyRecordReaderService(final TestRunner runner) throws InitializationException {
        final String readerId = "key-record-reader";
        final RecordReaderFactory readerService = new JsonTreeReader();
        runner.addControllerService(readerId, readerService);
        runner.enableControllerService(readerService);
        runner.setProperty(readerId, readerId);
    }

    protected Properties getProducerProperties() {
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // use of these two properties is intended to induce quick send of test events, so they are available to be consumed
        // final long maxDelayBeforeSend = 100L;
        // properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Long.toString(maxDelayBeforeSend));
        // properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Long.toString(maxDelayBeforeSend));
        return properties;
    }

    protected void produceOne(final String topic, final Integer partition,
                              final String key, final String value, final List<Header> headers)
            throws ExecutionException, InterruptedException {
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProperties())) {
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
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(getProducerProperties())) {
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
