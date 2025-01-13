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
package org.apache.nifi.kafka.processors.publish.additional;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.nifi.kafka.processors.AbstractPublishKafkaIT;
import org.apache.nifi.kafka.processors.PublishKafka;
import org.apache.nifi.kafka.processors.producer.wrapper.RecordMetadataStrategy;
import org.apache.nifi.kafka.shared.property.PublishStrategy;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class PublishKafkaWrapperX5IT extends AbstractPublishKafkaIT {
    private static final String TEST_TOPIC = "nifi-" + PublishKafkaWrapperX5IT.class.getName();
    private static final String OVERRIDE_TOPIC = "topic1-" + PublishKafkaWrapperX5IT.class.getName();
    private static final Integer TEST_PARTITION = new Random().nextInt(3) - 1;
    private static final Integer OVERRIDE_PARTITION = 1;

    private static final String TEST_RESOURCE = "org/apache/nifi/kafka/processors/publish/additional/wrapperX5.json";

    @BeforeAll
    protected static void beforeAll() {
        AbstractPublishKafkaIT.beforeAll();

        final Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        try (final Admin adminClient = Admin.create(properties)) {
            final int numPartitions = 3;
            final short replicationFactor = 1;
            final NewTopic testTopic = new NewTopic(TEST_TOPIC, numPartitions, replicationFactor);
            final NewTopic overrideTopic = new NewTopic(OVERRIDE_TOPIC, numPartitions, replicationFactor);
            final CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(testTopic, overrideTopic));
            final KafkaFuture<Void> testTopicFuture = topics.values().get(TEST_TOPIC);
            final KafkaFuture<Void> overrideTopicFuture = topics.values().get(OVERRIDE_TOPIC);
            testTopicFuture.get(1, TimeUnit.SECONDS);
            overrideTopicFuture.get(1, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test1ProduceOneFlowFile() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        addRecordReaderService(runner);
        addRecordWriterService(runner);

        runner.setProperty(PublishKafka.TOPIC_NAME, TEST_TOPIC);
        runner.setProperty(PublishKafka.PARTITION, Integer.toString(TEST_PARTITION));
        runner.getLogger().info("partition={}", TEST_PARTITION);
        runner.setProperty(PublishKafka.PUBLISH_STRATEGY, PublishStrategy.USE_WRAPPER.name());
        runner.setProperty(PublishKafka.RECORD_METADATA_STRATEGY, RecordMetadataStrategy.FROM_RECORD.getValue());

        final Map<String, String> attributes = new HashMap<>();
        final byte[] bytesFlowFileTemplate = IOUtils.toByteArray(Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_RESOURCE)));
        final byte[] bytesFlowFile = new String(bytesFlowFileTemplate, StandardCharsets.UTF_8)
                .replace("topic1", OVERRIDE_TOPIC).getBytes(StandardCharsets.UTF_8);
        runner.enqueue(bytesFlowFile, attributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred("success", 1);
    }

    @Test
    public void test2ConsumeOneRecord() throws IOException {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaConsumerProperties())) {
            consumer.subscribe(Arrays.asList(TEST_TOPIC, OVERRIDE_TOPIC));
            final ConsumerRecords<String, String> records = consumer.poll(DURATION_POLL);
            assertEquals(1, records.count());
            for (ConsumerRecord<String, String> record : records) {
                // kafka record metadata
                assertEquals(OVERRIDE_TOPIC, record.topic());
                assertEquals(OVERRIDE_PARTITION, record.partition());
                assertTrue(record.timestamp() > TIMESTAMP);
                assertTrue(record.timestamp() < System.currentTimeMillis());
                // kafka record headers
                final List<Header> headers = Arrays.asList(record.headers().toArray());
                assertEquals(1, headers.size());
                assertTrue(headers.contains(new RecordHeader("a", "b".getBytes(StandardCharsets.UTF_8))));
                // kafka record key
                assertNull(record.key());
                // kafka record value
                final ObjectNode kafkaValue = (ObjectNode) objectMapper.readTree(record.value());
                assertNotNull(kafkaValue);
                assertEquals("1234 First Street", kafkaValue.get("address").textValue());
                assertEquals("12345", kafkaValue.get("zip").textValue());
                assertInstanceOf(ObjectNode.class, kafkaValue.get("account"));
            }
        }
    }
}
