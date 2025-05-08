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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class PublishKafkaIT extends AbstractPublishKafkaIT {
    private static final String TEST_KEY_ATTRIBUTE = "my-key";
    private static final String TEST_KEY_ATTRIBUTE_EL = "${my-key}";
    private static final String TEST_KEY_VALUE = "some-key-value";
    private static final String TEST_RECORD_VALUE = "value-" + System.currentTimeMillis();

    private static final int EXPECTED_RECORD_COUNT = 50000; // Expect 50,000 records
    private static final int MAX_MESSAGE_SIZE = 5 * 1024 * 1024; // 5MB
    private static final byte[] LARGE_SAMPLE_INPUT = new byte[MAX_MESSAGE_SIZE];

    @Test
    public void test_1_KafkaTestContainerProduceOne() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.ATTRIBUTE_HEADER_PATTERN, "a.*");
        runner.setProperty(PublishKafka.KAFKA_KEY, TEST_KEY_ATTRIBUTE_EL);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a1", "valueA1");
        attributes.put("b1", "valueB1");
        attributes.put(TEST_KEY_ATTRIBUTE, TEST_KEY_VALUE);

        runner.enqueue(TEST_RECORD_VALUE, attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);
    }

    @Test
    public void test_2_KafkaTestContainerConsumeOne() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaConsumerProperties())) {
            consumer.subscribe(Collections.singletonList(getClass().getName()));
            final ConsumerRecords<String, String> records = consumer.poll(DURATION_POLL);
            assertEquals(1, records.count());
            final ConsumerRecord<String, String> record = records.iterator().next();
            assertEquals(TEST_KEY_VALUE, record.key());
            assertEquals(TEST_RECORD_VALUE, record.value());
            final List<Header> headers = Arrays.asList(record.headers().toArray());
            assertEquals(1, headers.size());
            final Header header = record.headers().iterator().next();
            assertEquals("a1", header.key());
            assertEquals("valueA1", new String(header.value(), StandardCharsets.UTF_8));
        }
    }

    @Test
    public void test_3_KafkaTestContainerProduceOneLargeFlowFile() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);

        final JsonTreeReader jsonTreeReader = new JsonTreeReader();

        runner.addControllerService("jsonTreeReader", jsonTreeReader);
        runner.setProperty(jsonTreeReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.enableControllerService(jsonTreeReader);

        final JsonRecordSetWriter jsonRecordSetWriter = new JsonRecordSetWriter();

        runner.addControllerService("jsonRecordSetWriter", jsonRecordSetWriter);
        runner.setProperty(jsonRecordSetWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(jsonRecordSetWriter, "output-grouping", "output-oneline");
        runner.enableControllerService(jsonRecordSetWriter);

        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.ATTRIBUTE_HEADER_PATTERN, "a.*");
        runner.setProperty(PublishKafka.KAFKA_KEY, TEST_KEY_ATTRIBUTE_EL);
        runner.setProperty(PublishKafka.RECORD_READER, "jsonTreeReader");
        runner.setProperty(PublishKafka.RECORD_WRITER, "jsonRecordSetWriter");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a1", "valueA1");
        attributes.put("b1", "valueB1");
        attributes.put(TEST_KEY_ATTRIBUTE, TEST_KEY_VALUE);

        populateSampleInput();
        runner.enqueue(LARGE_SAMPLE_INPUT, attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);
    }

    // Create 5MB of sample data with multiple records
    private static void populateSampleInput() {
        StringBuilder sb = new StringBuilder();
        int recordCount = EXPECTED_RECORD_COUNT;
        int approximateRecordSize = MAX_MESSAGE_SIZE / recordCount;

        for (int i = 0; i < recordCount; i++) {
            sb.append("{\"key\": \"").append(i).append("\",\"value\":\"");
            int payloadSize = approximateRecordSize - 30;
            for (int j = 0; j < payloadSize; j++) {
                sb.append((char) ('A' + (j % 26)));
            }
            sb.append("\"}");
            if (i < recordCount - 1) {
                sb.append("\n");
            }
            if (sb.length() >= MAX_MESSAGE_SIZE - 100) {
                break;
            }
        }

        byte[] stringBytes = sb.toString().getBytes();

        Arrays.fill(LARGE_SAMPLE_INPUT, (byte) ' '); // Fill with spaces instead of NULL

        int copyLength = Math.min(stringBytes.length, LARGE_SAMPLE_INPUT.length);
        System.arraycopy(stringBytes, 0, LARGE_SAMPLE_INPUT, 0, copyLength);
    }
}
