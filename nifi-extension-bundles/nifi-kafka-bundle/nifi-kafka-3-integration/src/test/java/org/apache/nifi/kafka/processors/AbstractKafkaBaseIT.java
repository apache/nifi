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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.kafka.service.Kafka3ConnectionService;
import org.apache.nifi.kafka.service.api.KafkaConnectionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Properties;

public abstract class AbstractKafkaBaseIT {

    protected static final String IMAGE_NAME = "confluentinc/cp-kafka:7.6.1";  // April 2024

    protected static final long TIMESTAMP = System.currentTimeMillis();

    protected static final String CONNECTION_SERVICE_ID = Kafka3ConnectionService.class.getSimpleName();

    protected static final Duration DURATION_POLL = Duration.ofSeconds(3);

    protected static final ConfluentKafkaContainer kafkaContainer;

    // NIFI-11259 - single testcontainers Kafka instance needed for all module integration tests
    static {
        kafkaContainer = new ConfluentKafkaContainer(DockerImageName.parse(IMAGE_NAME));
        kafkaContainer.start();
    }

    protected static ObjectMapper objectMapper;

    @BeforeAll
    protected static void beforeAll() {
        objectMapper = new ObjectMapper();
    }

    protected String addKafkaConnectionService(final TestRunner runner) throws InitializationException {
        final KafkaConnectionService connectionService = new Kafka3ConnectionService();
        runner.addControllerService(CONNECTION_SERVICE_ID, connectionService);
        runner.setProperty(connectionService, Kafka3ConnectionService.BOOTSTRAP_SERVERS, kafkaContainer.getBootstrapServers());
        runner.setProperty(connectionService, Kafka3ConnectionService.MAX_POLL_RECORDS, "1000");
        runner.enableControllerService(connectionService);
        return CONNECTION_SERVICE_ID;
    }

    protected String addRecordReaderService(final TestRunner runner) throws InitializationException {
        final String readerId = ConsumeKafka.RECORD_READER.getName();
        final RecordReaderFactory readerService = new JsonTreeReader();
        runner.addControllerService(readerId, readerService);
        runner.enableControllerService(readerService);
        runner.setProperty(readerId, readerId);
        return readerId;
    }

    protected String addRecordWriterService(final TestRunner runner) throws InitializationException {
        final String writerId = ConsumeKafka.RECORD_WRITER.getName();
        final RecordSetWriterFactory writerService = new JsonRecordSetWriter();
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);
        runner.setProperty(writerId, writerId);
        return writerId;
    }

    protected String addRecordKeyReaderService(final TestRunner runner) throws InitializationException {
        final String readerId = ConsumeKafka.KEY_RECORD_READER.getName();
        final RecordReaderFactory readerService = new JsonTreeReader();
        runner.addControllerService(readerId, readerService);
        runner.enableControllerService(readerService);
        runner.setProperty(readerId, readerId);
        return readerId;
    }

    protected String addRecordKeyWriterService(final TestRunner runner) throws InitializationException {
        final String writerId = PublishKafka.RECORD_KEY_WRITER.getName();
        final RecordSetWriterFactory writerService = new JsonRecordSetWriter();
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);
        runner.setProperty(writerId, writerId);
        return writerId;
    }

    protected Properties getKafkaConsumerProperties() {
        return getKafkaConsumerProperties(kafkaContainer.getBootstrapServers(), "my-group", true);
    }

    protected Properties getKafkaConsumerProperties(
            final String bootstrapServers, final String group, final boolean enableAutoCommit) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(enableAutoCommit));
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }
}
