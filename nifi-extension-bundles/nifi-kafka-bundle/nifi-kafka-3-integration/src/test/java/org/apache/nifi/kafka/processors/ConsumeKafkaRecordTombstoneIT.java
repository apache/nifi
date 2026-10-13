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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.avro.AvroReader;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.processors.producer.wrapper.WrapperRecord;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * RECORD output for a batch that contains a non-null empty value, a null tombstone, and a real value.
 * A null tombstone is omitted from success and parse.failure. A JSON reader emits only the real value.
 * An Avro reader routes the non-null empty value to parse.failure and writes the real value to success.
 */
class ConsumeKafkaRecordTombstoneIT extends AbstractConsumeKafkaIT {

    private static final String NON_EMPTY_VALUE = "{\"name\":\"A\"}";

    private static final byte[] NON_EMPTY_BYTES = NON_EMPTY_VALUE.getBytes(StandardCharsets.UTF_8);

    private static final byte[] EMPTY_BYTES = new byte[0];

    private static final String AVRO_SCHEMA = """
            {
              "type": "record",
              "name": "message",
              "fields": [
                {"name": "name", "type": "string"}
              ]
            }
            """;

    private static final String MESSAGE_KEY = "key";

    private static final int FIRST_PARTITION = 0;

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(30);

    private TestRunner runner;

    private String topic;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        addKafkaConnectionService(runner);
        runner.setProperty(ConsumeKafka.CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        addRecordReaderService(runner);
        addRecordWriterService(runner);

        topic = UUID.randomUUID().toString();
        runner.setProperty(ConsumeKafka.GROUP_ID, topic);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD);
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());
    }

    @Test
    void testOutputStrategyUseValue() throws ExecutionException, InterruptedException, IOException {
        setOutputStrategy(OutputStrategy.USE_VALUE);
        produceJsonRecords();

        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 0);
        runner.assertTransferCount(ConsumeKafka.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        assertEquals("A", record.get("name").asText());
    }

    @Test
    void testOutputStrategyUseWrapper() throws ExecutionException, InterruptedException, IOException {
        setOutputStrategy(OutputStrategy.USE_WRAPPER);
        produceJsonRecords();

        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 0);
        runner.assertTransferCount(ConsumeKafka.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        final ObjectNode value = assertInstanceOf(ObjectNode.class, record.get(WrapperRecord.VALUE));
        assertEquals("A", value.get("name").asText());
    }

    @Test
    void testOutputStrategyInjectMetadata() throws ExecutionException, InterruptedException, IOException {
        setOutputStrategy(OutputStrategy.INJECT_METADATA);
        produceJsonRecords();

        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 0);
        runner.assertTransferCount(ConsumeKafka.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        assertEquals("A", record.get("name").asText());
    }

    @Test
    void testOutputStrategyInjectOffset() throws ExecutionException, InterruptedException, IOException {
        setOutputStrategy(OutputStrategy.INJECT_OFFSET);
        produceJsonRecords();

        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 0);
        runner.assertTransferCount(ConsumeKafka.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        assertEquals("A", record.get("name").asText());
    }

    @Test
    void testOutputStrategyUseValueWithAvroReader() throws ExecutionException, InterruptedException, IOException, InitializationException {
        setOutputStrategy(OutputStrategy.USE_VALUE);
        useAvroReader();
        produceAvroRecords();

        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 1);
        runner.assertTransferCount(ConsumeKafka.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        assertEquals("A", record.get("name").asText());
    }

    @Test
    void testOutputStrategyUseWrapperWithAvroReader() throws ExecutionException, InterruptedException, IOException, InitializationException {
        setOutputStrategy(OutputStrategy.USE_WRAPPER);
        useAvroReader();
        produceAvroRecords();

        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 1);
        runner.assertTransferCount(ConsumeKafka.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        final ObjectNode value = assertInstanceOf(ObjectNode.class, record.get(WrapperRecord.VALUE));
        assertEquals("A", value.get("name").asText());
    }

    @Test
    void testOutputStrategyInjectMetadataWithAvroReader() throws ExecutionException, InterruptedException, IOException, InitializationException {
        setOutputStrategy(OutputStrategy.INJECT_METADATA);
        useAvroReader();
        produceAvroRecords();

        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 1);
        runner.assertTransferCount(ConsumeKafka.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        assertEquals("A", record.get("name").asText());
    }

    @Test
    void testOutputStrategyInjectOffsetWithAvroReader() throws ExecutionException, InterruptedException, IOException, InitializationException {
        setOutputStrategy(OutputStrategy.INJECT_OFFSET);
        useAvroReader();
        produceAvroRecords();

        runner.assertTransferCount(ConsumeKafka.PARSE_FAILURE, 1);
        runner.assertTransferCount(ConsumeKafka.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        final ArrayNode records = assertInstanceOf(ArrayNode.class, objectMapper.readTree(flowFile.getContent()));
        assertEquals(1, records.size());
        final ObjectNode record = assertInstanceOf(ObjectNode.class, records.get(0));
        assertEquals("A", record.get("name").asText());
    }

    private void setOutputStrategy(final OutputStrategy outputStrategy) {
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, outputStrategy);
    }

    private void produceJsonRecords() throws ExecutionException, InterruptedException {
        runner.run(1, false, true);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, EMPTY_BYTES);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, null);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, NON_EMPTY_BYTES);
        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);
    }

    private void produceAvroRecords() throws ExecutionException, InterruptedException, IOException {
        runner.run(1, false, true);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, EMPTY_BYTES);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, null);
        produceBytes(topic, FIRST_PARTITION, MESSAGE_KEY, avroRecord());
        runUntil(runner, testRunner -> !testRunner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty()
                && !testRunner.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE).isEmpty(), POLL_TIMEOUT);
        runner.run(1, true, false);
    }

    private void useAvroReader() throws InitializationException {
        final String readerId = "Avro Reader";
        final AvroReader reader = new AvroReader();
        runner.addControllerService(readerId, reader);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "embedded-avro-schema");
        runner.enableControllerService(reader);
        runner.setProperty(ConsumeKafka.RECORD_READER, readerId);
    }

    private byte[] avroRecord() throws IOException {
        final Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
        final GenericRecord record = new GenericData.Record(schema);
        record.put("name", "A");

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.create(schema, outputStream);
            writer.append(record);
        }

        return outputStream.toByteArray();
    }
}
