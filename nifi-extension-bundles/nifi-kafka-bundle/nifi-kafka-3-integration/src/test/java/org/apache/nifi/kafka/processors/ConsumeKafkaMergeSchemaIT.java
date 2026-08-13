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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.kafka.shared.property.SchemaConflictResolution;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumeKafkaMergeSchemaIT extends AbstractConsumeKafkaIT {

    private static final int FIRST_PARTITION = 0;

    private static final String RECORD_WITH_ID = """
            { "id": 1 }
            """;

    private static final String RECORD_WITH_NAME = """
            { "name": "Alice" }
            """;

    private static final String INVALID_RECORD = "not-valid-json";

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
    void testMergedSchemaProducesSingleFlowFile() throws ExecutionException, InterruptedException, IOException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD.getValue());
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, OutputStrategy.USE_VALUE.getValue());
        runner.setProperty(ConsumeKafka.SCHEMA_CONFLICT_RESOLUTION, SchemaConflictResolution.CONTINUE_WITH_MERGED_SCHEMA.getValue());
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());

        runner.run(1, false, true);

        produce(topic, List.of(
                new ProducerRecord<>(topic, FIRST_PARTITION, (String) null, RECORD_WITH_ID, List.<Header>of()),
                new ProducerRecord<>(topic, FIRST_PARTITION, (String) null, RECORD_WITH_NAME, List.<Header>of())));

        while (runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty()) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);

        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, successFlowFiles.size());

        final MockFlowFile flowFile = successFlowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_PARTITION, Integer.toString(FIRST_PARTITION));
        flowFile.assertAttributeEquals("record.count", "2");

        final JsonNode jsonTree = objectMapper.readTree(flowFile.getContent());
        final JsonNode expected = objectMapper.readTree("""
                [
                  { "id": 1, "name": null },
                  { "id": null, "name": "Alice" }
                ]
                """);
        assertEquals(expected, jsonTree);
    }

    @Test
    void testCreateNewFlowFileDefaultProducesMultipleFlowFiles() throws ExecutionException, InterruptedException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD.getValue());
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, OutputStrategy.USE_VALUE.getValue());
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());

        runner.run(1, false, true);

        produce(topic, List.of(
                new ProducerRecord<>(topic, FIRST_PARTITION, (String) null, RECORD_WITH_ID, List.<Header>of()),
                new ProducerRecord<>(topic, FIRST_PARTITION, (String) null, RECORD_WITH_NAME, List.<Header>of())));

        while (runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).size() < 2) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);

        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(2, successFlowFiles.size());

        for (final MockFlowFile flowFile : successFlowFiles) {
            flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
            flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_PARTITION, Integer.toString(FIRST_PARTITION));
        }
    }

    @Test
    void testMergedSchemaDifferentPartitionsProduceSeparateFlowFiles() throws Exception {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));

        try (final AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1))).all().get();
        }

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD.getValue());
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, OutputStrategy.USE_VALUE.getValue());
        runner.setProperty(ConsumeKafka.SCHEMA_CONFLICT_RESOLUTION, SchemaConflictResolution.CONTINUE_WITH_MERGED_SCHEMA.getValue());
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());

        runner.run(1, false, true);

        // Publish as one producer batch so the records are available together for a single poll/onTrigger.
        produce(topic, List.of(
                new ProducerRecord<>(topic, 0, (String) null, RECORD_WITH_ID, List.<Header>of()),
                new ProducerRecord<>(topic, 0, (String) null, RECORD_WITH_NAME, List.<Header>of()),
                new ProducerRecord<>(topic, 1, (String) null, RECORD_WITH_NAME, List.<Header>of())));

        while (totalRecordCount(runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS)) < 3) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);

        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(3, totalRecordCount(successFlowFiles));

        final List<MockFlowFile> partitionZero = successFlowFiles.stream()
                .filter(ff -> "0".equals(ff.getAttribute(KafkaFlowFileAttribute.KAFKA_PARTITION)))
                .toList();
        final List<MockFlowFile> partitionOne = successFlowFiles.stream()
                .filter(ff -> "1".equals(ff.getAttribute(KafkaFlowFileAttribute.KAFKA_PARTITION)))
                .toList();

        assertEquals(1, partitionZero.size());
        assertEquals(1, partitionOne.size());
        assertEquals(2, totalRecordCount(partitionZero));
        assertEquals(1, totalRecordCount(partitionOne));
        // Records from different partitions never share a FlowFile.
        assertTrue(partitionZero.stream().noneMatch(ff -> "1".equals(ff.getAttribute(KafkaFlowFileAttribute.KAFKA_PARTITION))));
    }

    private static int totalRecordCount(final List<MockFlowFile> flowFiles) {
        return flowFiles.stream()
                .mapToInt(ff -> Integer.parseInt(ff.getAttribute("record.count")))
                .sum();
    }

    @Test
    void testMergedSchemaWithParseFailure() throws ExecutionException, InterruptedException, IOException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD.getValue());
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, OutputStrategy.USE_VALUE.getValue());
        runner.setProperty(ConsumeKafka.SCHEMA_CONFLICT_RESOLUTION, SchemaConflictResolution.CONTINUE_WITH_MERGED_SCHEMA.getValue());
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());

        runner.run(1, false, true);

        produce(topic, List.of(
                new ProducerRecord<>(topic, FIRST_PARTITION, (String) null, RECORD_WITH_ID, List.<Header>of()),
                new ProducerRecord<>(topic, FIRST_PARTITION, (String) null, INVALID_RECORD, List.<Header>of()),
                new ProducerRecord<>(topic, FIRST_PARTITION, (String) null, RECORD_WITH_NAME, List.<Header>of())));

        while (runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty()) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);

        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, successFlowFiles.size());

        final MockFlowFile successFlowFile = successFlowFiles.getFirst();
        final JsonNode jsonTree = objectMapper.readTree(successFlowFile.getContent());
        assertInstanceOf(ArrayNode.class, jsonTree);
        assertEquals(2, jsonTree.size());

        final List<MockFlowFile> parseFailureFlowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE);
        assertEquals(1, parseFailureFlowFiles.size());
        parseFailureFlowFiles.getFirst().assertContentEquals(INVALID_RECORD);
    }

    @Test
    void testMergedSchemaWithInjectOffset() throws Exception {
        final MockFlowFile flowFile = runMergedSchemaWithOutputStrategy(OutputStrategy.INJECT_OFFSET);
        flowFile.assertAttributeEquals("record.count", "2");

        final JsonNode jsonTree = objectMapper.readTree(flowFile.getContent());
        assertInstanceOf(ArrayNode.class, jsonTree);
        assertEquals(2, jsonTree.size());
        assertEquals(1, jsonTree.get(0).get("id").asInt());
        assertTrue(jsonTree.get(0).has("kafkaOffset"));
        assertEquals("Alice", jsonTree.get(1).get("name").asText());
        assertTrue(jsonTree.get(1).has("kafkaOffset"));
    }

    @Test
    void testMergedSchemaWithUseWrapper() throws Exception {
        final MockFlowFile flowFile = runMergedSchemaWithOutputStrategy(OutputStrategy.USE_WRAPPER);
        flowFile.assertAttributeEquals("record.count", "2");

        final JsonNode jsonTree = objectMapper.readTree(flowFile.getContent());
        assertInstanceOf(ArrayNode.class, jsonTree);
        assertEquals(2, jsonTree.size());
        assertEquals(1, jsonTree.get(0).get("value").get("id").asInt());
        assertTrue(jsonTree.get(0).has("metadata"));
        assertEquals("Alice", jsonTree.get(1).get("value").get("name").asText());
        assertTrue(jsonTree.get(1).has("metadata"));
    }

    @Test
    void testMergedSchemaWithInjectMetadata() throws Exception {
        final MockFlowFile flowFile = runMergedSchemaWithOutputStrategy(OutputStrategy.INJECT_METADATA);
        flowFile.assertAttributeEquals("record.count", "2");

        final JsonNode jsonTree = objectMapper.readTree(flowFile.getContent());
        assertInstanceOf(ArrayNode.class, jsonTree);
        assertEquals(2, jsonTree.size());
        assertEquals(1, jsonTree.get(0).get("id").asInt());
        assertTrue(jsonTree.get(0).has("kafkaMetadata"));
        assertEquals("Alice", jsonTree.get(1).get("name").asText());
        assertTrue(jsonTree.get(1).has("kafkaMetadata"));
    }

    private MockFlowFile runMergedSchemaWithOutputStrategy(final OutputStrategy outputStrategy)
            throws ExecutionException, InterruptedException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD.getValue());
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, outputStrategy.getValue());
        runner.setProperty(ConsumeKafka.SCHEMA_CONFLICT_RESOLUTION, SchemaConflictResolution.CONTINUE_WITH_MERGED_SCHEMA.getValue());
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());

        runner.run(1, false, true);

        produce(topic, List.of(
                new ProducerRecord<>(topic, FIRST_PARTITION, (String) null, RECORD_WITH_ID, List.<Header>of()),
                new ProducerRecord<>(topic, FIRST_PARTITION, (String) null, RECORD_WITH_NAME, List.<Header>of())));

        while (runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty()) {
            runner.run(1, false, false);
        }

        runner.run(1, true, false);

        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, successFlowFiles.size());
        return successFlowFiles.getFirst();
    }
}
