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
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.nifi.avro.AvroReader;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.kafka.shared.property.SchemaConflictResolution;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies Continue with Merged Schema using typed Avro schemas (no inference):
 * disjoint non-nullable fields become nullable so both records can be written together.
 */
class ConsumeKafkaMergeSchemaTypedIT extends AbstractConsumeKafkaIT {

    private static final int FIRST_PARTITION = 0;

    private static final String SCHEMA_WITH_ID = """
            {
              "type": "record",
              "name": "IdRecord",
              "fields": [
                { "name": "id", "type": "long" }
              ]
            }
            """;

    private static final String SCHEMA_WITH_NAME = """
            {
              "type": "record",
              "name": "NameRecord",
              "fields": [
                { "name": "name", "type": "string" }
              ]
            }
            """;

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumeKafka.class);
        addKafkaConnectionService(runner);
        runner.setProperty(ConsumeKafka.CONNECTION_SERVICE, CONNECTION_SERVICE_ID);
        addEmbeddedAvroReader(runner);
        addInheritAvroWriter(runner);
    }

    private void addEmbeddedAvroReader(final TestRunner runner) throws InitializationException {
        final String readerId = ConsumeKafka.RECORD_READER.getName();
        final RecordReaderFactory readerService = new AvroReader();
        runner.addControllerService(readerId, readerService);
        runner.setProperty(readerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "embedded-avro-schema");
        runner.enableControllerService(readerService);
        runner.setProperty(readerId, readerId);
    }

    private void addInheritAvroWriter(final TestRunner runner) throws InitializationException {
        final String writerId = ConsumeKafka.RECORD_WRITER.getName();
        final RecordSetWriterFactory writerService = new AvroRecordSetWriter();
        runner.addControllerService(writerId, writerService);
        runner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA.getValue());
        runner.enableControllerService(writerService);
        runner.setProperty(writerId, writerId);
    }

    @Test
    void testMergedSchemaWithDisjointNonNullableAvroFields() throws Exception {
        final String topic = UUID.randomUUID().toString();
        final String groupId = topic.substring(0, topic.indexOf("-"));

        runner.setProperty(ConsumeKafka.GROUP_ID, groupId);
        runner.setProperty(ConsumeKafka.TOPICS, topic);
        runner.setProperty(ConsumeKafka.PROCESSING_STRATEGY, ProcessingStrategy.RECORD.getValue());
        runner.setProperty(ConsumeKafka.OUTPUT_STRATEGY, OutputStrategy.USE_VALUE.getValue());
        runner.setProperty(ConsumeKafka.SCHEMA_CONFLICT_RESOLUTION, SchemaConflictResolution.CONTINUE_WITH_MERGED_SCHEMA.getValue());
        runner.setProperty(ConsumeKafka.AUTO_OFFSET_RESET, AutoOffsetReset.EARLIEST.getValue());

        runner.run(1, false, true);

        produceBytes(topic, List.of(
                serializeAvro(SCHEMA_WITH_ID, Map.of("id", 1L)),
                serializeAvro(SCHEMA_WITH_NAME, Map.of("name", "Alice"))));

        runUntil(runner, r -> !r.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).isEmpty(), Duration.ofSeconds(30));
        runner.run(1, true, false);

        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, successFlowFiles.size());

        final MockFlowFile flowFile = successFlowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, topic);
        flowFile.assertAttributeEquals("record.count", "2");

        final List<GenericRecord> records = new ArrayList<>();
        final Schema writtenSchema;
        try (DataFileStream<GenericRecord> stream = new DataFileStream<>(
                new ByteArrayInputStream(flowFile.toByteArray()), new GenericDatumReader<>())) {
            writtenSchema = stream.getSchema();
            stream.forEach(records::add);
        }

        assertEquals(2, records.size());

        final Map<String, Object> firstRecord = new LinkedHashMap<>();
        firstRecord.put("id", 1L);
        firstRecord.put("name", null);
        final Map<String, Object> secondRecord = new LinkedHashMap<>();
        secondRecord.put("id", null);
        secondRecord.put("name", "Alice");
        assertEquals(List.of(firstRecord, secondRecord), List.of(toMap(records.get(0)), toMap(records.get(1))));

        assertEquals(List.of("id", "name"), writtenSchema.getFields().stream().map(Schema.Field::name).toList());
        assertTrue(AvroTypeUtil.isNullable(writtenSchema.getField("id").schema()), "id should be nullable in merged schema");
        assertTrue(AvroTypeUtil.isNullable(writtenSchema.getField("name").schema()), "name should be nullable in merged schema");
    }

    private static Map<String, Object> toMap(final GenericRecord record) {
        final Map<String, Object> values = new LinkedHashMap<>();
        for (final Schema.Field field : record.getSchema().getFields()) {
            final Object value = record.get(field.name());
            values.put(field.name(), value instanceof org.apache.avro.util.Utf8 utf8 ? utf8.toString() : value);
        }
        return values;
    }

    private static byte[] serializeAvro(final String schemaText, final Map<String, Object> values) throws IOException {
        final Schema schema = new Schema.Parser().parse(schemaText);
        final GenericRecord record = new GenericData.Record(schema);
        values.forEach(record::put);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.create(schema, outputStream);
            writer.append(record);
        }
        return outputStream.toByteArray();
    }

    private void produceBytes(final String topic, final List<byte[]> values)
            throws ExecutionException, InterruptedException {
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties)) {
            final List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (final byte[] value : values) {
                futures.add(producer.send(new ProducerRecord<>(topic, FIRST_PARTITION, null, value)));
            }
            for (final Future<RecordMetadata> future : futures) {
                final RecordMetadata metadata = future.get();
                assertEquals(topic, metadata.topic());
                assertTrue(metadata.hasOffset());
            }
        }
    }
}
