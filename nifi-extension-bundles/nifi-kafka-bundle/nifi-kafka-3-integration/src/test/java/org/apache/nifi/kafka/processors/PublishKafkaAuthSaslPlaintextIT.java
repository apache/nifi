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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.plain.internals.PlainSaslServer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.nifi.kafka.service.Kafka3ConnectionService;
import org.apache.nifi.kafka.service.api.KafkaConnectionService;
import org.apache.nifi.kafka.shared.property.SaslMechanism;
import org.apache.nifi.kafka.shared.property.SecurityProtocol;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Disabled("circle back to this")
@TestMethodOrder(MethodOrderer.MethodName.class)
public class PublishKafkaAuthSaslPlaintextIT {
    private static final String SERVICE_ID = Kafka3ConnectionService.class.getSimpleName();

    private static final String TEST_RECORD_VALUE = "value-" + System.currentTimeMillis();

    private static final String USERNAME = "nifi";
    private static final String PASSWORD = UUID.randomUUID().toString();

    private static ConfluentKafkaContainer kafka;

    @BeforeAll
    static void beforeAll() {
        kafka = new ConfluentKafkaContainer(DockerImageName.parse(AbstractKafkaBaseIT.IMAGE_NAME))
                .withEnv(getEnvironmentSaslPlaintext());
        kafka.start();
    }

    /**
     * Environment to be provided to docker container to enable SASL authentication.
     * <p>
     * Disable this test for now:
     * <ul>
     * <li><a href="https://github.com/testcontainers/testcontainers-java/issues/3899">Kafka SASL mechanism</a></li>
     * <li><a href="https://github.com/testcontainers/testcontainers-java/issues/6423">Kafka SASL mechanism</a></li>
     * </ul>
     */
    private static Map<String, String> getEnvironmentSaslPlaintext() {
        final Map<String, String> environment = new LinkedHashMap<>();
        environment.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT");
        environment.put("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", "PLAIN");
        environment.put("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", String.format(
                "%s required user_%s=\"%s\";", PlainLoginModule.class.getName(), USERNAME, PASSWORD));
        return environment;
    }

    @AfterAll
    static void afterAll() {
        kafka.stop();
    }

    protected void addKafkaConnectionService(final TestRunner runner) throws InitializationException {
        final Map<String, String> connectionServiceProps = new HashMap<>();
        connectionServiceProps.put(Kafka3ConnectionService.BOOTSTRAP_SERVERS.getName(), kafka.getBootstrapServers());
        connectionServiceProps.put(Kafka3ConnectionService.SECURITY_PROTOCOL.getName(), SecurityProtocol.SASL_PLAINTEXT.name());
        connectionServiceProps.put(Kafka3ConnectionService.SASL_MECHANISM.getName(), SaslMechanism.PLAIN.name());
        connectionServiceProps.put(Kafka3ConnectionService.SASL_USERNAME.getName(), USERNAME);
        connectionServiceProps.put(Kafka3ConnectionService.SASL_PASSWORD.getName(), PASSWORD);

        final KafkaConnectionService connectionService = new Kafka3ConnectionService();
        runner.addControllerService(SERVICE_ID, connectionService, connectionServiceProps);
        runner.enableControllerService(connectionService);
    }

    @Test
    public void test_1_KafkaTestContainerProduceOne() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        addKafkaConnectionService(runner);

        runner.setProperty(PublishKafka.CONNECTION_SERVICE, SERVICE_ID);
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());

        runner.enqueue(TEST_RECORD_VALUE);
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);
    }

    @Test
    public void test_2_KafkaTestContainerConsumeOne() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaConsumerProperties())) {
            consumer.subscribe(Collections.singletonList(getClass().getName()));
            final ConsumerRecords<String, String> records = consumer.poll(AbstractPublishKafkaIT.DURATION_POLL);
            assertEquals(1, records.count());
            final ConsumerRecord<String, String> record = records.iterator().next();
            assertNull(record.key());
            assertEquals(TEST_RECORD_VALUE, record.value());
        }
    }

    /**
     * Configure raw Kafka consumer connection to check NiFi publish.
     */
    private Properties getKafkaConsumerProperties() {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.TRUE.toString());
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(SaslConfigs.SASL_MECHANISM, PlainSaslServer.PLAIN_MECHANISM);
        properties.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, String.format(
                "%s required username=\"%s\" password=\"%s\";",
                PlainLoginModule.class.getName(), USERNAME, PASSWORD));

        return properties;
    }
}
