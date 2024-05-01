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
package org.apache.nifi.processors.kafka.publish.it;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class PublishKafka_2_6_IT extends PublishKafka_2_6_BaseIT {
    private static final String TEST_RECORD_VALUE = "value-" + System.currentTimeMillis();

    @Test
    public void test_1_KafkaTestContainerProduceOne() {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafka_2_6.class);
        runner.setValidateExpressionUsage(false);

        final URI uri = URI.create(kafka.getBootstrapServers());
        runner.setProperty("bootstrap.servers", String.format("%s:%s", uri.getHost(), uri.getPort()));
        runner.setProperty("topic", getClass().getName());
        runner.setProperty("attribute-name-regex", "a.*");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a1", "valueA1");
        attributes.put("b1", "valueB1");

        runner.enqueue(TEST_RECORD_VALUE, attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred("success", 1);
    }

    @Test
    public void test_2_KafkaTestContainerConsumeOne() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaConsumerProperties())) {
            consumer.subscribe(Collections.singletonList(getClass().getName()));
            final ConsumerRecords<String, String> records = consumer.poll(DURATION_POLL);
            assertEquals(1, records.count());
            final ConsumerRecord<String, String> record = records.iterator().next();
            assertNull(record.key());
            assertEquals(TEST_RECORD_VALUE, record.value());
            final List<Header> headers = Arrays.asList(record.headers().toArray());
            assertEquals(1, headers.size());
            final Header header = record.headers().iterator().next();
            assertEquals("a1", header.key());
            assertEquals("valueA1", new String(header.value(), StandardCharsets.UTF_8));
        }
    }
}
