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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumeKafkaRecordIT extends AbstractConsumeKafkaIT {
    private static final int TEST_RECORD_COUNT = 2;

    private static final int FIRST_PARTITION = 0;

    private static final String FIRST_OFFSET = "0";

    private static final String MAX_OFFSET = "1";

    private static final String VALID_RECORD_1_TEXT = """
            { "id": 1, "name": "A" }
            """;

    private static final String VALID_RECORD_2_TEXT = """
            { "id": 2, "name": "B" }
            """;

    private static final String INVALID_RECORD_TEXT = "non-json";

    private static final String FIRST_HEADER = "First-Header";

    private static final String SECOND_HEADER = "Second-Header";

    private static final String FIRST_HEADER_PATTERN = "First.*";

    private static final String HEADER_VALUE = "value";

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        addKafkaConnectionService(runner);

        runner.setProperty(ConsumeKafka.CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        addRecordReaderService(runner);
        addRecordWriterService(runner);
    }

    @Test
    void testProcessingStrategyRecordNoRecords() {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD.getValue());

        runner.run();

        runner.assertAllFlowFilesTransferred(ConsumeKafka.SUCCESS, 0);
    }

    @ParameterizedTest
    @EnumSource(value = OutputStrategy.class)
    void testInvalidRecordInMiddle(final OutputStrategy outputStrategy) throws ExecutionException, InterruptedException {
        testSingleInvalidRecord("testInvalidRecordInMiddle", outputStrategy, VALID_RECORD_1_TEXT, INVALID_RECORD_TEXT, VALID_RECORD_2_TEXT);
    }

    @ParameterizedTest
    @EnumSource(value = OutputStrategy.class)
    void testInvalidRecordAtEnd(final OutputStrategy outputStrategy) throws ExecutionException, InterruptedException {
        testSingleInvalidRecord("testInvalidRecordAtEnd", outputStrategy, VALID_RECORD_1_TEXT, VALID_RECORD_2_TEXT, INVALID_RECORD_TEXT);
    }

    @ParameterizedTest
    @EnumSource(value = OutputStrategy.class)
    void testInvalidRecordAtStart(final OutputStrategy outputStrategy) throws ExecutionException, InterruptedException {
        testSingleInvalidRecord("testInvalidRecordAtStart", outputStrategy, INVALID_RECORD_TEXT, VALID_RECORD_1_TEXT, VALID_RECORD_2_TEXT);
    }

    private void testSingleInvalidRecord(final String topicPrefix, final OutputStrategy outputStrategy, final String... recordTexts) throws ExecutionException, InterruptedException {
        final String topicName = topicPrefix + outputStrategy.getValue();
        runner.setProperty(ConsumeKafka.TOPICS, topicName);
        runner.setProperty(ConsumeKafka.GROUP_ID, topicName);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD.getValue());
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, outputStrategy.getValue());
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());

        for (final String text : recordTexts) {
            produceOne(topicName, 0, null, text, List.of());
        }

        runner.run(1, false, true);

        while (runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty()) {
            Thread.sleep(10L);
            runner.run(1, false, false);
        }

        runner.assertTransferCount(ConsumeKafka.SUCCESS, 1);
        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 1);

        assertEquals(String.valueOf(recordTexts.length - 1), runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst().getAttribute("record.count"));
        runner.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE).getFirst().assertContentEquals(INVALID_RECORD_TEXT);
    }

    @Test
    void testProcessingStrategyRecord() throws InterruptedException, ExecutionException, IOException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD.getValue());
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());
        runner.setProperty(ConsumeKafka.HEADER_NAME_PATTERN, FIRST_HEADER_PATTERN);

        runner.run(1, false, true);

        final List<Header> headers = List.of(
                new RecordHeader(FIRST_HEADER, HEADER_VALUE.getBytes(StandardCharsets.UTF_8)),
                new RecordHeader(SECOND_HEADER, HEADER_VALUE.getBytes(StandardCharsets.UTF_8))
        );

        produceOne(topic, FIRST_PARTITION, null, VALID_RECORD_1_TEXT, headers);
        produceOne(topic, FIRST_PARTITION, null, VALID_RECORD_2_TEXT, headers);

        while (runner.getFlowFilesForRelationship("success").isEmpty()) {
            runner.run(1, false, false);
        }
        runner.run(1, true, false);

        final List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFilesForRelationship.size());
        final Iterator<MockFlowFile> flowFiles = flowFilesForRelationship.iterator();
        assertTrue(flowFiles.hasNext());

        final MockFlowFile flowFile = flowFiles.next();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_PARTITION, Integer.toString(FIRST_PARTITION));
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, MAX_OFFSET);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, FIRST_OFFSET);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_COUNT, Integer.toString(TEST_RECORD_COUNT));

        flowFile.assertAttributeEquals("record.count", Integer.toString(TEST_RECORD_COUNT));
        flowFile.assertAttributeEquals(FIRST_HEADER, HEADER_VALUE);
        flowFile.assertAttributeNotExists(SECOND_HEADER);

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord provenanceEvent = provenanceEvents.getFirst();
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent.getEventType());

        final JsonNode jsonNodeTree = objectMapper.readTree(flowFile.getContent());
        assertInstanceOf(ArrayNode.class, jsonNodeTree);
        assertEquals(2, jsonNodeTree.size());

        final JsonNode firstRecord = jsonNodeTree.get(0);
        final JsonNode expectedFirstRecord = objectMapper.readTree(VALID_RECORD_1_TEXT);
        assertEquals(expectedFirstRecord, firstRecord);

        final JsonNode secondRecord = jsonNodeTree.get(1);
        final JsonNode expectedSecondRecord = objectMapper.readTree(VALID_RECORD_2_TEXT);
        assertEquals(expectedSecondRecord, secondRecord);
    }
}
