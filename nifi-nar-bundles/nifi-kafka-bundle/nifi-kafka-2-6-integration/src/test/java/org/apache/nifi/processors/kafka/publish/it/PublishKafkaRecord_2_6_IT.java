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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class PublishKafkaRecord_2_6_IT extends PublishKafka_2_6_BaseIT {
    private static final String TEST_RESOURCE = "org/apache/nifi/processors/kafka/publish/ff.json";

    private static final int TEST_RECORD_COUNT = 3;

    @Test
    public void test_1_KafkaTestContainerProduceOne() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(PublishKafkaRecord_2_6.class);
        runner.setValidateExpressionUsage(false);
        addRecordReaderService(runner);
        addRecordWriterService(runner);

        final URI uri = URI.create(kafka.getBootstrapServers());
        runner.setProperty("bootstrap.servers", String.format("%s:%s", uri.getHost(), uri.getPort()));
        runner.setProperty("topic", getClass().getName());

        runner.setProperty("message-key-field", "name");
        runner.setProperty("attribute-name-regex", "a.*");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("a1", "valueA1");
        attributes.put("b1", "valueB1");

        final byte[] bytesFlowFile = IOUtils.toByteArray(Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_RESOURCE)));
        runner.enqueue(bytesFlowFile, attributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred("success", 1);
    }

    @Test
    public void test_2_ConsumeMultipleRecords() throws JsonProcessingException {
        final Pattern pattern = Pattern.compile("[ABC]");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaConsumerProperties())) {
            consumer.subscribe(Collections.singletonList(getClass().getName()));
            final ConsumerRecords<String, String> records = consumer.poll(DURATION_POLL);
            assertEquals(TEST_RECORD_COUNT, records.count());
            for (ConsumerRecord<String, String> record : records) {
                assertTrue(pattern.matcher(record.key()).matches(),
                        String.format("%s does not match %s", record.key(), pattern.pattern()));
                final JsonNode jsonNode = objectMapper.readTree(record.value());
                assertNotNull(jsonNode);
                assertEquals(2, jsonNode.size());
                final List<Header> headers = Arrays.asList(record.headers().toArray());
                assertEquals(1, headers.size());
                final Header header = record.headers().iterator().next();
                assertEquals("a1", header.key());
                assertEquals("valueA1", new String(header.value(), StandardCharsets.UTF_8));
            }
        }
    }
}
