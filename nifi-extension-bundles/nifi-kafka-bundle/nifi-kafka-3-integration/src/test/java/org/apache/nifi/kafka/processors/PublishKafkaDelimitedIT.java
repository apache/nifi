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

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.nifi.reporting.InitializationException;
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
import static org.junit.jupiter.api.Assertions.assertNull;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class PublishKafkaDelimitedIT extends AbstractPublishKafkaIT {
    private static final String TEST_RECORD_VALUE = "value-" + System.currentTimeMillis();

    private static final int TEST_RECORD_COUNT = 3;

    private static final String DEMARCATOR = "xx";

    @Test
    public void test1ProduceOneFlowFile() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PublishKafka.CONNECTION_SERVICE, addKafkaConnectionService(runner));
        runner.setProperty(PublishKafka.TOPIC_NAME, getClass().getName());
        runner.setProperty(PublishKafka.MESSAGE_DEMARCATOR, DEMARCATOR);
        runner.setProperty(PublishKafka.ATTRIBUTE_HEADER_PATTERN, "a.*");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a1", "valueA1");
        attributes.put("b1", "valueB1");

        final String value = TEST_RECORD_VALUE + DEMARCATOR + TEST_RECORD_VALUE + DEMARCATOR + TEST_RECORD_VALUE;
        runner.enqueue(value, attributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PublishKafka.REL_SUCCESS, 1);
    }

    @Test
    public void test2ConsumeMultipleRecords() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaConsumerProperties())) {
            consumer.subscribe(Collections.singletonList(getClass().getName()));
            final ConsumerRecords<String, String> records = consumer.poll(DURATION_POLL);
            assertEquals(TEST_RECORD_COUNT, records.count());
            records.forEach(record -> {
                assertNull(record.key());
                assertEquals(TEST_RECORD_VALUE, record.value());
                final List<Header> headers = Arrays.asList(record.headers().toArray());
                assertEquals(1, headers.size());
                final Header header = record.headers().iterator().next();
                assertEquals("a1", header.key());
                assertEquals("valueA1", new String(header.value(), StandardCharsets.UTF_8));
            });
        }
    }
}
