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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.Kafka3ConnectionService;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumeKafkaIT extends AbstractConsumeKafkaIT {

    private static final String RECORD_VALUE = ProducerRecord.class.getSimpleName();

    private static final int FIRST_PARTITION = 0;

    private static final long FIRST_OFFSET = 0;

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        addKafkaConnectionService(runner);

        runner.setProperty(ConsumeKafka.CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());
    }

    @Test
    void testProcessingStrategyFlowFileNoRecords() {
        final String topic = UUID.randomUUID().toString();
        // reuse of group ID causes issues in test containers test environment
        final String groupId = topic.substring(0, topic.indexOf("-"));

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());

        runner.run();

        runner.assertAllFlowFilesTransferred(ConsumeKafka.SUCCESS, 0);
    }

    @Test
    void testProcessingStrategyFlowFileOneRecord() throws InterruptedException, ExecutionException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());
        runner.setProperty(ConsumeKafka.HEADER_NAME_PATTERN, "b*");

        runner.run(1, false, true);

        final List<Header> headers = Arrays.asList(
                new RecordHeader("aaa", "value".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("bbb", "value".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("ccc", "value".getBytes(StandardCharsets.UTF_8)));
        produceOne(topic, 0, null, RECORD_VALUE, headers);
        while (runner.getFlowFilesForRelationship("success").isEmpty()) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);

        final Iterator<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).iterator();
        assertTrue(flowFiles.hasNext());

        final MockFlowFile flowFile = flowFiles.next();
        flowFile.assertContentEquals(RECORD_VALUE);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_PARTITION, Integer.toString(FIRST_PARTITION));
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(FIRST_OFFSET));
        flowFile.assertAttributeExists(KafkaFlowFileAttribute.KAFKA_TIMESTAMP);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_HEADER_COUNT, "3");
        flowFile.assertAttributeEquals("bbb", "value");
        flowFile.assertAttributeNotExists("aaa");
        flowFile.assertAttributeNotExists("ccc");
    }

    /**
     * Test ability to specify a topic regular expression to query for messages.
     */
    @Test
    public void testTopicPattern() throws ExecutionException, InterruptedException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));
        final String topicTestCase = topic + "-B";
        final String topicPattern = topic + ".*";

        // on use of "pattern" subscription, things seem to work better when topic exists prior to the subscribe event
        produceOne(topicTestCase, 0, null, RECORD_VALUE, null);

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topicPattern);
        runner.setProperty(ConsumeKafka.TOPIC_FORMAT, ConsumeKafka.TOPIC_PATTERN);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());
        runner.run(1, false, true);

        while (runner.getFlowFilesForRelationship("success").isEmpty()) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);
        runner.assertTransferCount("success", 1);
    }

    /**
     * Test ability to specify a topic regular expression to query for messages.
     */
    @Test
    public void testTopicNames() throws ExecutionException, InterruptedException {
        final String topic = "testTopicNames";
        final String groupId = "testTopicNames";

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic + "," + topic + "-2");
        runner.setProperty(ConsumeKafka.TOPIC_FORMAT, ConsumeKafka.TOPIC_NAME);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());
        runner.run(1, false, true);

        produceOne(topic, 0, null, RECORD_VALUE, null);
        while (runner.getFlowFilesForRelationship("success").isEmpty()) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);
        runner.assertTransferCount("success", 1);
    }

    @Test
    public void testConsumesAllRecordsWithoutDuplicates() throws ExecutionException, InterruptedException {
        final String topic = "testConsumesAllRecordsWithoutDuplicates";

        runner.setProperty(ConsumeKafka.GROUP_ID, "testConsumesAllRecordsWithoutDuplicates");
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE.getValue());

        produceOne(topic, 0, null, "1", null);

        // Initialize processor
        runner.run(1, false, true);
        while (runner.getFlowFilesForRelationship("success").isEmpty()) {
            runner.run(1, false, false);
        }

        // Ensure that we have exactly 1 FlowFile output
        runner.assertAllFlowFilesTransferred(ConsumeKafka.SUCCESS, 1);
        runner.clearTransferState();

        // Add another record and ensure that we get exactly 1 more
        produceOne(topic, 0, null, "1", null);
        while (runner.getFlowFilesForRelationship("success").isEmpty()) {
            runner.run(1, false, false);
        }

        runner.assertAllFlowFilesTransferred(ConsumeKafka.SUCCESS, 1);
        runner.clearTransferState();

        // Stop processor, add another, and then ensure that we consume exactly 1 more.
        runner.stop();
        produceOne(topic, 0, null, "1", null);
        runner.run(1, false, true);
        while (runner.getFlowFilesForRelationship("success").isEmpty()) {
            runner.run(1, false, false);
        }

        runner.assertAllFlowFilesTransferred(ConsumeKafka.SUCCESS, 1);
    }

    @Timeout(5)
    @Test
    void testMaxUncommittedSize() throws InterruptedException, ExecutionException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));

        final int recordCount = 100;
        final int maxPollRecords = 10;
        final int flowFilesExpected = recordCount / maxPollRecords;

        final int recordSize = RECORD_VALUE.length();
        final int maxUncommittedSize = recordSize * recordCount;

        // Adjust Poll Records for this method
        final ControllerService connectionService = runner.getControllerService(CONNECTION_SERVICE_ID);
        runner.disableControllerService(connectionService);
        runner.setProperty(connectionService, Kafka3ConnectionService.MAX_POLL_RECORDS, Integer.toString(maxPollRecords));
        runner.enableControllerService(connectionService);

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.DEMARCATOR.getValue());
        runner.setProperty(ConsumeKafka.MESSAGE_DEMARCATOR, System.lineSeparator());

        // Set Uncommitted Size so that processing completes before 5 second test timeout expires
        runner.setProperty(ConsumeKafka.MAX_UNCOMMITTED_SIZE, "%d B".formatted(maxUncommittedSize));
        runner.setProperty(ConsumeKafka.MAX_UNCOMMITTED_TIME, "5 s");

        final Collection<ProducerRecord<String, String>> records = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            records.add(new ProducerRecord<>(topic, RECORD_VALUE));
        }
        produce(topic, records);

        runner.run();

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(flowFilesExpected, flowFiles.size());

        long totalFlowFileSize = 0;
        for (final MockFlowFile flowFile : flowFiles) {
            totalFlowFileSize += flowFile.getSize();
        }

        assertTrue(totalFlowFileSize > maxUncommittedSize, "Total FlowFile Size [%d] less than Max Uncommitted Size [%d]".formatted(totalFlowFileSize, maxUncommittedSize));
    }
}
