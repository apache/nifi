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
import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumeKafkaRecordIT extends AbstractConsumeKafkaIT {
    private static final String TEST_RESOURCE = "org/apache/nifi/kafka/processors/publish/ff.json";
    private static final int TEST_RECORD_COUNT = 3;

    private static final int FIRST_PARTITION = 0;
    private static final String VALID_RECORD_1_TEXT = """
            { "id": 1, "name": "A" }
            """;
    private static final String VALID_RECORD_2_TEXT = """
            { "id": 2, "name": "B" }
            """;
    private static final String INVALID_RECORD_TEXT = "non-json";

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        // implementation relies on default values of dependant properties; remove this once refactored
        runner.setProhibitUseOfPropertiesWithUnsatisfiedDependencies(false);
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

    static Stream<Arguments> invalidRecordScenarios() {
        return Stream.of(
                Arguments.of("testInvalidRecordAtStart", new String[]{INVALID_RECORD_TEXT, VALID_RECORD_1_TEXT, VALID_RECORD_2_TEXT}),
                Arguments.of("testInvalidRecordInMiddle", new String[]{VALID_RECORD_1_TEXT, INVALID_RECORD_TEXT, VALID_RECORD_2_TEXT}),
                Arguments.of("testInvalidRecordAtEnd", new String[]{VALID_RECORD_1_TEXT, VALID_RECORD_2_TEXT, INVALID_RECORD_TEXT}));
    }

    @ParameterizedTest
    @MethodSource("invalidRecordScenarios")
    void testInvalidRecord(String testName, String[] recordTexts) throws Exception {
        runner.setProperty(ConsumeKafka.TOPICS, testName);
        runner.setProperty(ConsumeKafka.GROUP_ID, testName);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD.getValue());
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());

        for (String text : recordTexts) {
            produceOne(testName, 0, null, text, List.of());
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
    void testProcessingStrategyFlowFileOneRecord() throws InterruptedException, ExecutionException, IOException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD.getValue());
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());
        runner.setProperty(ConsumeKafka.HEADER_NAME_PATTERN, "a*");

        runner.run(1, false, true);

        final byte[] bytesFlowFile = IOUtils.toByteArray(Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_RESOURCE)));
        final String flowFileString = new String(bytesFlowFile, StandardCharsets.UTF_8).trim();

        final List<Header> headers = Arrays.asList(
                new RecordHeader("aaa", "value".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("bbb", "value".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("ccc", "value".getBytes(StandardCharsets.UTF_8)));
        produceOne(topic, 0, null, flowFileString, headers);
        while (runner.getFlowFilesForRelationship("success").isEmpty()) {
            runner.run(1, false, false);
        }
        runner.run(1, true, false);

        final List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFilesForRelationship.size());
        final Iterator<MockFlowFile> flowFiles = flowFilesForRelationship.iterator();
        assertTrue(flowFiles.hasNext());

        final MockFlowFile flowFile = flowFiles.next();
        flowFile.assertContentEquals(flowFileString);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_PARTITION, Integer.toString(FIRST_PARTITION));
        flowFile.assertAttributeEquals("record.count", Long.toString(TEST_RECORD_COUNT));
        flowFile.assertAttributeEquals("aaa", "value");
        flowFile.assertAttributeNotExists("bbb");
        flowFile.assertAttributeNotExists("ccc");

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord provenanceEvent = provenanceEvents.getFirst();
        assertEquals(ProvenanceEventType.RECEIVE, provenanceEvent.getEventType());

        // [{"id":1,"name":"A"},{"id":2,"name":"B"},{"id":3,"name":"C"}]
        final JsonNode jsonNodeTree = objectMapper.readTree(flowFile.getContent());
        assertInstanceOf(ArrayNode.class, jsonNodeTree);
        final ArrayNode arrayNode = (ArrayNode) jsonNodeTree;
        final Iterator<JsonNode> elements = arrayNode.elements();
        assertEquals(TEST_RECORD_COUNT, arrayNode.size());
        while (elements.hasNext()) {
            final JsonNode jsonNode = elements.next();
            assertTrue(Arrays.asList(1, 2, 3).contains(jsonNode.get("id").asInt()));
            assertTrue(Arrays.asList("A", "B", "C").contains(jsonNode.get("name").asText()));
        }
    }
}
