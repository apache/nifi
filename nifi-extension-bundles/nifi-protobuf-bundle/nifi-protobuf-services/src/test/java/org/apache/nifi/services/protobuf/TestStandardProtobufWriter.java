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
package org.apache.nifi.services.protobuf;

import com.squareup.wire.schema.Schema;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.MessageIndexWriter;
import org.apache.nifi.schemaregistry.services.MessageName;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.SchemaReferenceWriter;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.services.protobuf.converter.ProtobufDataConverter;
import org.apache.nifi.services.protobuf.schema.ProtoSchemaParser;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.apache.nifi.services.protobuf.ProtoTestUtil.generateInputDataForProto3;
import static org.apache.nifi.services.protobuf.ProtoTestUtil.generateInputDataForRepeatedProto3;
import static org.apache.nifi.services.protobuf.ProtoTestUtil.loadProto3TestSchema;
import static org.apache.nifi.services.protobuf.ProtoTestUtil.loadRepeatedProto3TestSchema;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestStandardProtobufWriter {

    private static final String PROTO_3_MESSAGE = "Proto3Message";
    private static final byte[] FAKE_HEADER = {0x00, 0x00, 0x00, 0x00, 0x2A};
    private static final byte[] FAKE_INDEX = {0x00};

    private TestRunner runner;
    private StandardProtobufWriter writer;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        writer = new StandardProtobufWriter();
        runner.addControllerService("writer", writer);
        runner.setProperty(writer, SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY.getValue());
        runner.setProperty(writer, SCHEMA_TEXT, getTestProto3File());
        runner.setProperty(writer, StandardProtobufWriter.MESSAGE_NAME_RESOLUTION_STRATEGY,
            StandardProtobufWriter.MessageNameResolverStrategy.MESSAGE_NAME_PROPERTY.getValue());
        runner.setProperty(writer, StandardProtobufWriter.MESSAGE_NAME, PROTO_3_MESSAGE);
    }

    @Test
    void testWritePlainProtobufRoundTrip() throws Exception {
        runner.enableControllerService(writer);

        final byte[] output = writeSingleRecord(buildProto3Record());

        assertProto3Message(new ByteArrayInputStream(output));
    }

    @Test
    void testOnlySupportedSchemaAccessStrategiesAreOffered() {
        // The inherited strategy list also contains the Schema Reference Reader strategy, which the writer cannot
        // use to obtain a schema; offering it would let a user configure a service that always fails at runtime.
        final List<PropertyDescriptor> descriptors = writer.getPropertyDescriptors();
        final PropertyDescriptor strategy = descriptors.stream()
            .filter(descriptor -> SCHEMA_ACCESS_STRATEGY.getName().equals(descriptor.getName()))
            .findFirst()
            .orElseThrow();

        final List<String> allowed = strategy.getAllowableValues().stream().map(AllowableValue::getValue).toList();
        assertEquals(List.of(SCHEMA_NAME_PROPERTY.getValue(), SCHEMA_TEXT_PROPERTY.getValue()), allowed);

        // The read-side Schema Reference Reader property is not applicable to a writer.
        assertTrue(descriptors.stream().noneMatch(descriptor -> SCHEMA_REFERENCE_READER.getName().equals(descriptor.getName())));
    }

    @Test
    void testWriteWithoutActiveRecordSetFlushesOnClose() throws Exception {
        runner.enableControllerService(writer);

        final RecordSchema writeSchema = writer.getSchema(emptyMap(), null);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        // No beginRecordSet/finishRecordSet: content is written outside an active record set and must survive close().
        try (RecordSetWriter recordSetWriter = writer.createWriter(runner.getLogger(), writeSchema, out, emptyMap())) {
            recordSetWriter.write(buildProto3Record());
        }

        assertProto3Message(new ByteArrayInputStream(out.toByteArray()));
    }

    @Test
    void testWriteMultipleRecordsFailsFast() throws Exception {
        runner.enableControllerService(writer);

        final RecordSchema writeSchema = writer.getSchema(emptyMap(), null);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (RecordSetWriter recordSetWriter = writer.createWriter(runner.getLogger(), writeSchema, out, emptyMap())) {
            recordSetWriter.beginRecordSet();
            recordSetWriter.write(buildProto3Record());
            assertThrows(IOException.class, () -> recordSetWriter.write(buildProto3Record()));
        }
    }

    @Test
    void testWriteConfluentFramingOrder() throws Exception {
        final FakeSchemaReferenceWriter referenceWriter = new FakeSchemaReferenceWriter();
        final FakeMessageIndexWriter indexWriter = new FakeMessageIndexWriter();
        runner.addControllerService("referenceWriter", referenceWriter);
        runner.addControllerService("indexWriter", indexWriter);
        runner.enableControllerService(referenceWriter);
        runner.enableControllerService(indexWriter);
        runner.setProperty(writer, StandardProtobufWriter.SCHEMA_REFERENCE_WRITER, "referenceWriter");
        runner.setProperty(writer, StandardProtobufWriter.MESSAGE_INDEX_WRITER, "indexWriter");
        runner.enableControllerService(writer);

        final byte[] output = writeSingleRecord(buildProto3Record());

        // Confluent framing: [header][message index][protobuf payload]
        final byte[] header = Arrays.copyOfRange(output, 0, FAKE_HEADER.length);
        final byte[] index = Arrays.copyOfRange(output, FAKE_HEADER.length, FAKE_HEADER.length + FAKE_INDEX.length);
        assertArrayEquals(FAKE_HEADER, header);
        assertArrayEquals(FAKE_INDEX, index);

        final byte[] payload = Arrays.copyOfRange(output, FAKE_HEADER.length + FAKE_INDEX.length, output.length);
        assertProto3Message(new ByteArrayInputStream(payload));
    }

    @Test
    void testRoundTripThroughStandardProtobufReaderProto3() throws Exception {
        runner.enableControllerService(writer);
        final byte[] output = writeSingleRecord(buildProto3Record());

        final StandardProtobufReader reader = createAndEnableReader("proto3reader", getTestProto3File(), PROTO_3_MESSAGE);
        final RecordReader recordReader = reader.createRecordReader(emptyMap(), new ByteArrayInputStream(output), output.length, runner.getLogger());

        final Record record = recordReader.nextRecord();
        assertEquals(true, record.getValue("booleanField"));
        assertEquals("Test text", record.getValue("stringField"));
        assertEquals(Integer.MAX_VALUE, record.getValue("int32Field"));
        assertEquals(Long.MAX_VALUE, record.getValue("int64Field"));

        final Record nested = (Record) record.getValue("nestedMessage");
        assertEquals("ENUM_VALUE_3", nested.getValue("testEnum"));
        final Object[] nested2 = (Object[]) nested.getValue("nestedMessage2");
        final Record oneOfHolder = (Record) nested2[0];
        assertEquals(3, oneOfHolder.getValue("int32Option"));

        assertNull(recordReader.nextRecord());
    }

    @Test
    void testRoundTripThroughStandardProtobufReaderRepeated() throws Exception {
        final String repeatedSchema = getSchemaFile("test_repeated_proto3.proto");
        final StandardProtobufWriter repeatedWriter = createAndEnableWriter("repeatedwriter", repeatedSchema, "RootMessage");
        final byte[] output = writeSingleRecordWith(repeatedWriter, buildRepeatedRecord());

        final StandardProtobufReader reader = createAndEnableReader("repeatedreader", repeatedSchema, "RootMessage");
        final RecordReader recordReader = reader.createRecordReader(emptyMap(), new ByteArrayInputStream(output), output.length, runner.getLogger());

        final Record record = recordReader.nextRecord();
        final Object[] repeatedMessage = (Object[]) record.getValue("repeatedMessage");
        final Record first = (Record) repeatedMessage[0];
        assertArrayEquals(new Object[]{true, false}, (Object[]) first.getValue("booleanField"));
        assertArrayEquals(new Object[]{"Test text1", "Test text2"}, (Object[]) first.getValue("stringField"));
        assertArrayEquals(new Object[]{"ENUM_VALUE_2", "ENUM_VALUE_3"}, (Object[]) first.getValue("testEnum"));

        assertNull(recordReader.nextRecord());
    }

    private MapRecord buildRepeatedRecord() throws Exception {
        final Schema schema = loadRepeatedProto3TestSchema();
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema("RootMessage");
        return new ProtobufDataConverter(schema, "RootMessage", recordSchema, false, false)
            .createRecord(generateInputDataForRepeatedProto3());
    }

    private StandardProtobufReader createAndEnableReader(final String id, final String schemaText, final String messageName) throws Exception {
        final StandardProtobufReader reader = new StandardProtobufReader();
        runner.addControllerService(id, reader);
        runner.setProperty(reader, SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY.getValue());
        runner.setProperty(reader, SCHEMA_TEXT, schemaText);
        runner.setProperty(reader, StandardProtobufReader.MESSAGE_NAME_RESOLUTION_STRATEGY,
            StandardProtobufReader.MessageNameResolverStrategy.MESSAGE_NAME_PROPERTY.getValue());
        runner.setProperty(reader, StandardProtobufReader.MESSAGE_NAME, messageName);
        runner.enableControllerService(reader);
        return reader;
    }

    private StandardProtobufWriter createAndEnableWriter(final String id, final String schemaText, final String messageName) throws Exception {
        final StandardProtobufWriter protobufWriter = new StandardProtobufWriter();
        runner.addControllerService(id, protobufWriter);
        runner.setProperty(protobufWriter, SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY.getValue());
        runner.setProperty(protobufWriter, SCHEMA_TEXT, schemaText);
        runner.setProperty(protobufWriter, StandardProtobufWriter.MESSAGE_NAME_RESOLUTION_STRATEGY,
            StandardProtobufWriter.MessageNameResolverStrategy.MESSAGE_NAME_PROPERTY.getValue());
        runner.setProperty(protobufWriter, StandardProtobufWriter.MESSAGE_NAME, messageName);
        runner.enableControllerService(protobufWriter);
        return protobufWriter;
    }

    private byte[] writeSingleRecordWith(final StandardProtobufWriter protobufWriter, final MapRecord record) throws IOException, SchemaNotFoundException {
        final RecordSchema writeSchema = protobufWriter.getSchema(emptyMap(), null);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (RecordSetWriter recordSetWriter = protobufWriter.createWriter(runner.getLogger(), writeSchema, out, emptyMap())) {
            recordSetWriter.beginRecordSet();
            recordSetWriter.write(record);
            recordSetWriter.finishRecordSet();
            recordSetWriter.flush();
        }
        return out.toByteArray();
    }

    private MapRecord buildProto3Record() throws Exception {
        final Schema schema = loadProto3TestSchema();
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema(PROTO_3_MESSAGE);
        return new ProtobufDataConverter(schema, PROTO_3_MESSAGE, recordSchema, false, false)
            .createRecord(generateInputDataForProto3());
    }

    private byte[] writeSingleRecord(final MapRecord record) throws IOException, SchemaNotFoundException {
        return writeSingleRecordWith(writer, record);
    }

    private void assertProto3Message(final ByteArrayInputStream payload) throws IOException {
        final Schema schema = loadProto3TestSchema();
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema(PROTO_3_MESSAGE);
        final MapRecord record = new ProtobufDataConverter(schema, PROTO_3_MESSAGE, recordSchema, false, false)
            .createRecord(payload);

        assertEquals(true, record.getValue("booleanField"));
        assertEquals("Test text", record.getValue("stringField"));
        assertEquals(Integer.MAX_VALUE, record.getValue("int32Field"));
        assertEquals(Long.MAX_VALUE, record.getValue("int64Field"));
        assertArrayEquals("Test bytes".getBytes(), (byte[]) record.getValue("bytesField"));

        final MapRecord nestedRecord = (MapRecord) record.getValue("nestedMessage");
        assertEquals("ENUM_VALUE_3", nestedRecord.getValue("testEnum"));
    }

    private String getTestProto3File() {
        return getSchemaFile("test_proto3.proto");
    }

    private String getSchemaFile(final String resourceName) {
        try {
            return new String(getClass().getClassLoader().getResourceAsStream(resourceName).readAllBytes());
        } catch (final Exception e) {
            throw new RuntimeException("Failed to read " + resourceName + " from resources", e);
        }
    }

    static class FakeSchemaReferenceWriter extends AbstractControllerService implements SchemaReferenceWriter {
        @Override
        public void writeHeader(final RecordSchema recordSchema, final OutputStream outputStream) throws IOException {
            outputStream.write(FAKE_HEADER);
        }

        @Override
        public Map<String, String> getAttributes(final RecordSchema recordSchema) {
            return Map.of();
        }

        @Override
        public void validateSchema(final RecordSchema recordSchema) {
        }

        @Override
        public Set<SchemaField> getRequiredSchemaFields() {
            return emptySet();
        }
    }

    static class FakeMessageIndexWriter extends AbstractControllerService implements MessageIndexWriter {
        @Override
        public void writeMessageIndex(final Map<String, String> variables, final SchemaDefinition schemaDefinition, final MessageName messageName, final OutputStream outputStream) throws IOException {
            outputStream.write(FAKE_INDEX);
        }
    }
}
