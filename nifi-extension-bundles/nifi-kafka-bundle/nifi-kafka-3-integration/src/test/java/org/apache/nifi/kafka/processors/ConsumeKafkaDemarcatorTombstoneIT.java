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

import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Demarcator output when a batch contains a null tombstone, a non-null empty value, or both together with a real value.
 * Null and empty values are written into the bundle as zero-length content. An empty bundle sets kafka.tombstone;
 * a bundle that also contains a non-empty value does not.
 */
class ConsumeKafkaDemarcatorTombstoneIT extends AbstractConsumeKafkaIT {

    private static final String NON_EMPTY_VALUE = "{\"id\":1,\"name\":\"A\"}";

    private static final byte[] NON_EMPTY_BYTES = NON_EMPTY_VALUE.getBytes(StandardCharsets.UTF_8);

    private static final byte[] EMPTY_BYTES = new byte[0];

    private static final String MESSAGE_KEY = "key";

    private static final int FIRST_PARTITION = 0;

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(30);

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        addKafkaConnectionService(runner);
        runner.setProperty(ConsumeKafka.CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
    }

    @Test
    void testDemarcator() throws ExecutionException, InterruptedException {
        final String topic = configure();
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, "|");
        runner.run(1, false, true);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, EMPTY_BYTES);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, null);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, NON_EMPTY_BYTES);

        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "0");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "2");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, "3");
        flowFile.assertContentEquals(NON_EMPTY_VALUE);
        flowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);
    }

    @Test
    void testDemarcatorIncludesNullValue() throws ExecutionException, InterruptedException {
        final String topic = configure();
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, "|");
        runner.run(1, false, true);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, null);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, NON_EMPTY_BYTES);

        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "0");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "1");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, "2");
        flowFile.assertContentEquals(NON_EMPTY_VALUE);
        flowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);
    }

    @Test
    void testDemarcatorEmpty() throws ExecutionException, InterruptedException {
        final String topic = configure();
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, "|");
        runner.run(1, false, true);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, EMPTY_BYTES);

        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "0");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "0");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, "1");
        flowFile.assertContentEquals("");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOMBSTONE, Boolean.TRUE.toString());
    }

    private String configure() {
        final String topic = UUID.randomUUID().toString();
        runner.setProperty(ConsumeKafka.GROUP_ID, topic);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.DEMARCATOR);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());
        return topic;
    }
}
