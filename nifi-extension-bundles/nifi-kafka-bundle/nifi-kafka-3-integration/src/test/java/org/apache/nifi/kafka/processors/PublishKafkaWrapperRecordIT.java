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

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.nifi.kafka.shared.property.PublishStrategy;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class PublishKafkaWrapperRecordIT extends AbstractPublishKafkaIT {
    private static final String TEST_RESOURCE = "org/apache/nifi/kafka/processors/publish/ffwrapper.json";

    private static final String KEY_ATTRIBUTE_KEY = "keyAttribute";
    private static final String KEY_ATTRIBUTE_VALUE = "keyAttributeValue";

    private static final int TEST_RECORD_COUNT = 3;

    @Test
    public void test_1_KafkaTestContainerProduceOneFlowFile() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        addRecordReaderService(runner);
        addRecordWriterService(runner);
        addRecordKeyWriterService(runner);

        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.KAFKA_KEY, KEY_ATTRIBUTE_KEY);
        runner.setProperty(PublishKafka.MESSAGE_KEY_FIELD, "address");
        runner.setProperty(PublishKafka.ATTRIBUTE_HEADER_PATTERN, "a.*");
        runner.setProperty(PublishKafka.PUBLISH_STRATEGY, PublishStrategy.USE_WRAPPER.name());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(KEY_ATTRIBUTE_KEY, KEY_ATTRIBUTE_VALUE);
        attributes.put("a1", "valueA1");
        attributes.put("b1", "valueB1");
        final byte[] bytesFlowFile = IOUtils.toByteArray(Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_RESOURCE)));
        runner.enqueue(bytesFlowFile, attributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);
    }

    @Test
    public void test_2_KafkaTestContainerConsumeMultipleRecords() throws IOException {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaConsumerProperties())) {
            consumer.subscribe(Collections.singletonList(getClass().getName()));
            final ConsumerRecords<String, String> records = consumer.poll(DURATION_POLL);
            assertEquals(TEST_RECORD_COUNT, records.count());
            for (ConsumerRecord<String, String> record : records) {
                // kafka record headers
                final List<Header> headers = Arrays.asList(record.headers().toArray());
                assertEquals(1, headers.size());
                final Header header = record.headers().iterator().next();
                assertEquals("a1", header.key());
                assertEquals("valueA1", new String(header.value(), StandardCharsets.UTF_8));
                // kafka record key
                final ObjectNode kafkaKey = (ObjectNode) objectMapper.readTree(record.key());
                assertNotNull(kafkaKey);
                assertEquals("Main", kafkaKey.get("street-name").textValue());
                assertEquals(5, (kafkaKey.get("street-number").intValue() % 100));
                // kafka record value (wrapped record)
                final ObjectNode kafkaValue = (ObjectNode) objectMapper.readTree(record.value());
                assertNotNull(kafkaValue);
                assertNotEquals(0, kafkaValue.get("id").asInt());
                assertEquals(1, kafkaValue.get("name").asText().length());
                assertInstanceOf(ObjectNode.class, kafkaValue.get("address"));
            }
        }
    }
}
