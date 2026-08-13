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
package org.apache.nifi.kafka.processors.consumer.convert;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MergeSchemaGroupingTest {

    private static final String TOPIC = "topic1";
    private static final String BROKER_URI = "brokerUri";

    private static final RecordSchema SCHEMA_A = new SimpleRecordSchema(List.of(
            new RecordField("fieldA", RecordFieldType.STRING.getDataType())));

    private static final RecordSchema SCHEMA_B = new SimpleRecordSchema(List.of(
            new RecordField("fieldB", RecordFieldType.INT.getDataType())));

    private static final Record RECORD_A = new MapRecord(SCHEMA_A, Map.of("fieldA", "hello"));
    private static final Record RECORD_B = new MapRecord(SCHEMA_B, Map.of("fieldB", 42));

    private final PassThroughSchemaRecordWriter writerFactory = new PassThroughSchemaRecordWriter();

    private MockProcessSession session;
    private ComponentLog logger;
    private MergeSchemaGrouping grouping;

    @BeforeEach
    void setUp() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ConsumeKafka.class);
        runner.addControllerService("writer", writerFactory);
        runner.enableControllerService(writerFactory);

        final Processor processor = runner.getProcessor();
        session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0)), processor);
        logger = runner.getLogger();
        grouping = new MergeSchemaGrouping(writerFactory, logger, BROKER_URI, true);
    }

    @Test
    void testDifferentSchemasInSameTopicPartitionMergeIntoOneFlowFile() throws Exception {
        final ByteRecord first = new ByteRecord(TOPIC, 0, 10, 1000L, List.of(), null, new byte[0], 0L);
        final ByteRecord second = new ByteRecord(TOPIC, 0, 11, 500L, List.of(), null, new byte[0], 0L);

        grouping.addRecord(session, first, RECORD_A, SCHEMA_A, Map.of(), Map.of());
        grouping.addRecord(session, second, RECORD_B, SCHEMA_B, Map.of(), Map.of());
        grouping.finishAllGroups(session);

        final List<MockFlowFile> success = session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, success.size());

        final MockFlowFile flowFile = success.getFirst();
        assertEquals(TOPIC, flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_TOPIC));
        assertEquals("0", flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_PARTITION));
        assertEquals("10", flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_OFFSET));
        assertEquals("11", flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET));
        assertEquals("500", flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_TIMESTAMP));
        assertEquals("2", flowFile.getAttribute("record.count"));
        assertEquals("true", flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_CONSUMER_OFFSETS_COMMITTED));

        assertEquals("hello,\n,42\n", flowFile.getContent());
    }

    @Test
    void testDifferentPartitionsProduceSeparateFlowFiles() throws Exception {
        final ByteRecord first = new ByteRecord(TOPIC, 0, 1, 1000L, List.of(), null, new byte[0], 0L);
        final ByteRecord second = new ByteRecord(TOPIC, 1, 2, 2000L, List.of(), null, new byte[0], 0L);

        grouping.addRecord(session, first, RECORD_A, SCHEMA_A, Map.of(), Map.of());
        grouping.addRecord(session, second, RECORD_B, SCHEMA_B, Map.of(), Map.of());
        grouping.finishAllGroups(session);

        final List<MockFlowFile> success = session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(2, success.size());
        assertTrue(success.stream().anyMatch(ff -> "0".equals(ff.getAttribute(KafkaFlowFileAttribute.KAFKA_PARTITION))));
        assertTrue(success.stream().anyMatch(ff -> "1".equals(ff.getAttribute(KafkaFlowFileAttribute.KAFKA_PARTITION))));
    }

    @Test
    void testDifferentGroupingAttributesProduceSeparateFlowFiles() throws Exception {
        final ByteRecord first = new ByteRecord(TOPIC, 0, 1, 1000L, List.of(), null, new byte[0], 0L);
        final ByteRecord second = new ByteRecord(TOPIC, 0, 2, 2000L, List.of(), null, new byte[0], 0L);

        grouping.addRecord(session, first, RECORD_A, SCHEMA_A, Map.of(), Map.of("hdr", "a"));
        grouping.addRecord(session, second, RECORD_B, SCHEMA_B, Map.of(), Map.of("hdr", "b"));
        grouping.finishAllGroups(session);

        final List<MockFlowFile> success = session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(2, success.size());
        assertTrue(success.stream().anyMatch(ff -> "a".equals(ff.getAttribute("hdr"))));
        assertTrue(success.stream().anyMatch(ff -> "b".equals(ff.getAttribute("hdr"))));
    }

    @Test
    void testDisjointNonNullableFieldsBecomeNullableInMergedSchema() throws Exception {
        final RecordSchema nonNullableSchemaA = new SimpleRecordSchema(List.of(
                new RecordField("fieldA", RecordFieldType.STRING.getDataType(), false)));
        final RecordSchema nonNullableSchemaB = new SimpleRecordSchema(List.of(
                new RecordField("fieldB", RecordFieldType.INT.getDataType(), false)));
        final Record recordA = new MapRecord(nonNullableSchemaA, Map.of("fieldA", "hello"));
        final Record recordB = new MapRecord(nonNullableSchemaB, Map.of("fieldB", 42));

        final AtomicReference<RecordSchema> capturedSchema = new AtomicReference<>();
        final SchemaValidatingRecordWriter validatingWriter = new SchemaValidatingRecordWriter(capturedSchema);

        final TestRunner runner = TestRunners.newTestRunner(ConsumeKafka.class);
        runner.addControllerService("validating-writer", validatingWriter);
        runner.enableControllerService(validatingWriter);

        final Processor processor = runner.getProcessor();
        final MockProcessSession validatingSession = new MockProcessSession(
                new SharedSessionState(processor, new AtomicLong(0)), processor);
        final MergeSchemaGrouping validatingGrouping = new MergeSchemaGrouping(
                validatingWriter, runner.getLogger(), BROKER_URI, true);

        final ByteRecord first = new ByteRecord(TOPIC, 0, 10, 1000L, List.of(), null, new byte[0], 0L);
        final ByteRecord second = new ByteRecord(TOPIC, 0, 11, 2000L, List.of(), null, new byte[0], 0L);

        validatingGrouping.addRecord(validatingSession, first, recordA, nonNullableSchemaA, Map.of(), Map.of());
        validatingGrouping.addRecord(validatingSession, second, recordB, nonNullableSchemaB, Map.of(), Map.of());
        validatingGrouping.finishAllGroups(validatingSession);

        final List<MockFlowFile> success = validatingSession.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, success.size());
        assertEquals("2", success.getFirst().getAttribute("record.count"));
        assertEquals("hello,\n,42\n", success.getFirst().getContent());

        final RecordSchema mergedSchema = capturedSchema.get();
        assertEquals(List.of("fieldA", "fieldB"), mergedSchema.getFieldNames());
        assertTrue(mergedSchema.getField("fieldA").orElseThrow().isNullable());
        assertTrue(mergedSchema.getField("fieldB").orElseThrow().isNullable());
    }

    private static final class PassThroughSchemaRecordWriter extends AbstractControllerService implements RecordSetWriterFactory {
        private final MockRecordWriter writer = new MockRecordWriter(null, false);

        @Override
        public RecordSchema getSchema(final Map<String, String> variables, final RecordSchema readSchema) {
            return readSchema;
        }

        @Override
        public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out,
                final Map<String, String> variables) throws IOException {
            return writer.createWriter(logger, schema, out, variables);
        }
    }

    /**
     * Captures the write schema and rejects records that omit non-nullable fields.
     */
    private static final class SchemaValidatingRecordWriter extends AbstractControllerService implements RecordSetWriterFactory {
        private final MockRecordWriter writer = new MockRecordWriter(null, false);
        private final AtomicReference<RecordSchema> capturedSchema;

        private SchemaValidatingRecordWriter(final AtomicReference<RecordSchema> capturedSchema) {
            this.capturedSchema = capturedSchema;
        }

        @Override
        public RecordSchema getSchema(final Map<String, String> variables, final RecordSchema readSchema) {
            capturedSchema.set(readSchema);
            return readSchema;
        }

        @Override
        public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out,
                final Map<String, String> variables) throws IOException {
            final RecordSetWriter delegate = writer.createWriter(logger, schema, out, variables);
            return new RecordSetWriter() {
                @Override
                public void beginRecordSet() throws IOException {
                    delegate.beginRecordSet();
                }

                @Override
                public WriteResult finishRecordSet() throws IOException {
                    return delegate.finishRecordSet();
                }

                @Override
                public WriteResult write(final Record record) throws IOException {
                    for (final RecordField field : schema.getFields()) {
                        if (!field.isNullable() && record.getValue(field) == null && field.getDefaultValue() == null) {
                            throw new IOException("Missing required field: " + field.getFieldName());
                        }
                    }
                    return delegate.write(record);
                }

                @Override
                public WriteResult write(final RecordSet recordSet) throws IOException {
                    return delegate.write(recordSet);
                }

                @Override
                public String getMimeType() {
                    return delegate.getMimeType();
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }

                @Override
                public void flush() throws IOException {
                    delegate.flush();
                }
            };
        }
    }
}
