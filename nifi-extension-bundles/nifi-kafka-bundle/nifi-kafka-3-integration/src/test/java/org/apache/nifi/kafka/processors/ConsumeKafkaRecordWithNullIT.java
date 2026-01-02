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

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.nifi.avro.AvroReader;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.avro.WriteAvroResultWithExternalSchema;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.WriteAvroSchemaAttributeStrategy;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Demonstrate handling of incoming Avro payloads with null fields where the associated Avro schema specifies that
 * the field is required.  Invalid payloads should be routed to PARSE_FAILURE, as stated in the documentation.
 */
class ConsumeKafkaRecordWithNullIT extends AbstractConsumeKafkaIT {
    private static final int FIRST_PARTITION = 0;

    private static final String AVRO_SCHEMA_NULLABLE = """
            {
              "name": "test",
              "type": "record",
              "fields": [
                {
                  "name": "text",
                  "type": "string"
                },
                {
                  "name": "ordinal",
                  "type": ["null", "long"],
                  "default": null
                }
              ]
            }
            """;
    private static final String AVRO_SCHEMA_REQUIRED = """
            {
              "name": "test",
              "type": "record",
              "fields": [
                {
                  "name": "text",
                  "type": "string"
                },
                {
                  "name": "ordinal",
                  "type": "long"
                }
              ]
            }
            """;

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException, IOException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        addKafkaConnectionService(runner);
        runner.setProperty(ConsumeKafka.CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        addRecordReaderServiceAvro(runner);
        addRecordWriterServiceAvro(runner);
    }

    private void addRecordReaderServiceAvro(final TestRunner runner) throws InitializationException {
        final String readerId = ConsumeKafka.RECORD_READER.getName();
        final RecordReaderFactory readerService = new AvroReader();
        runner.addControllerService(readerId, readerService);
        runner.setProperty(readerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        runner.setProperty(readerService, SchemaAccessUtils.SCHEMA_TEXT, AVRO_SCHEMA_NULLABLE);
        runner.enableControllerService(readerService);
        runner.setProperty(readerId, readerId);
    }

    private void addRecordWriterServiceAvro(final TestRunner runner) throws InitializationException {
        final String writerId = ConsumeKafka.RECORD_WRITER.getName();
        final RecordSetWriterFactory writerService = new AvroRecordSetWriter();
        runner.addControllerService(writerId, writerService);
        runner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        runner.setProperty(writerService, SchemaAccessUtils.SCHEMA_TEXT, AVRO_SCHEMA_REQUIRED);
        runner.enableControllerService(writerService);
        runner.setProperty(writerId, writerId);
    }

    private byte[] generateAvroPayloadWithNullField() throws IOException {
        final Schema avroSchema = new Schema.Parser().parse(AVRO_SCHEMA_NULLABLE);
        final RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema);

        final Map<String, Object> fields1 = Map.of("text", "string1", "ordinal", 1);
        final MapRecord record1 = new MapRecord(recordSchema, fields1);
        final Map<String, Object> fields2 = Map.of("text", "string2");  // this record omits a required field
        final MapRecord record2 = new MapRecord(recordSchema, fields2);

        final BlockingQueue<BinaryEncoder> encoderPool = new LinkedBlockingQueue<>(1);
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final WriteAvroSchemaAttributeStrategy schemaWriter = new WriteAvroSchemaAttributeStrategy();
        try (RecordSetWriter writer = new WriteAvroResultWithExternalSchema(avroSchema, recordSchema, schemaWriter, os, encoderPool, runner.getLogger())) {
            writer.write(new ListRecordSet(recordSchema, List.of(record1, record2)));
            writer.flush();
        }
        return os.toByteArray();
    }

    @Test
    void testProcessingStrategyRecord() throws InterruptedException, ExecutionException, IOException {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));
        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD.getValue());
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());
        runner.run(1, false, true);

        final byte[] payload = generateAvroPayloadWithNullField();
        produceOne(topic, FIRST_PARTITION, null, new String(payload, StandardCharsets.UTF_8), null);
        int pollAttempts = 5;  // before fix, this loop never terminated; bounding the number of loops avoids any issue
        while ((--pollAttempts >= 0) && runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty()
                && runner.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE).isEmpty()) {
            runner.run(1, false, false);
        }
        runner.run(1, true, false);

        assertTrue(pollAttempts >= 0);
        final List<MockFlowFile> flowFilesForRelationshipFail = runner.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE);
        assertEquals(1, flowFilesForRelationshipFail.size());  // the invalid record goes here
        final List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, flowFilesForRelationship.size());  // the valid record goes here
    }
}
