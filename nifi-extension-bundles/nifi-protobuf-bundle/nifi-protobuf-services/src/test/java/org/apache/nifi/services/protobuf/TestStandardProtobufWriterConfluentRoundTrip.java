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

import org.apache.nifi.confluent.schemaregistry.ConfluentEncodedSchemaReferenceReader;
import org.apache.nifi.confluent.schemaregistry.ConfluentEncodedSchemaReferenceWriter;
import org.apache.nifi.confluent.schemaregistry.ConfluentProtobufMessageIndexWriter;
import org.apache.nifi.confluent.schemaregistry.ConfluentProtobufMessageNameResolver;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.schemaregistry.services.StandardSchemaDefinition;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * End-to-end Confluent wire-format tests: {@link StandardProtobufWriter} configured with the real
 * {@link ConfluentEncodedSchemaReferenceWriter} and {@link ConfluentProtobufMessageIndexWriter} produces
 * [header][message index][payload], with exact byte assertions on the header and index (including the
 * single-byte [0] optimization and a nested-message index), and the content round-trips back through
 * {@link StandardProtobufReader} using the matching {@link ConfluentEncodedSchemaReferenceReader} and
 * {@link ConfluentProtobufMessageNameResolver}.
 */
class TestStandardProtobufWriterConfluentRoundTrip {

    private static final long SCHEMA_ID = 42;
    // Confluent header: magic byte 0x00 followed by the big-endian schema id (42).
    private static final byte[] EXPECTED_HEADER = {0x00, 0x00, 0x00, 0x00, 0x2A};
    private static final int HEADER_LENGTH = EXPECTED_HEADER.length;

    private static final String REGISTRY_ID = "registry";
    private static final String REFERENCE_WRITER_ID = "referenceWriter";
    private static final String INDEX_WRITER_ID = "indexWriter";
    private static final String REFERENCE_READER_ID = "referenceReader";
    private static final String MESSAGE_NAME_RESOLVER_ID = "messageNameResolver";

    private static final String SCHEMA_TEXT_VALUE = """
        syntax = "proto3";
        message User {
          int32 id = 1;
          string name = 2;
          message Profile {
            string bio = 1;
          }
        }
        message Company {
          string name = 1;
        }""";

    private static final String CUSTOM_OPTIONS_SCHEMA_TEXT_VALUE = """
        syntax = "proto3";
        message User {
          int32 id = 1 [(confluent.field_meta) = {
            params: [
              {
                key: "connect.type",
                value: "int16"
              }
            ]
          }];
          string name = 2 [(confluent.field_meta) = { doc: "name" }];
          message Profile {
            string bio = 1 [(confluent.field_meta) = { params: [ { key: "k", value: "v" } ] }];
          }
        }
        message Company {
          string name = 1 [(confluent.field_meta) = { doc: "company" }];
        }""";

    private static final String USER_MESSAGE = "User";
    private static final String PROFILE_MESSAGE = "User.Profile";
    private static final String COMPANY_MESSAGE = "Company";

    // Nested message User.Profile -> declaration path [0, 0] -> length 2 then indexes 0, 0 (zigzag varints).
    private static final byte[] NESTED_PROFILE_MESSAGE_INDEX = {0x04, 0x00, 0x00};
    // Root message Company -> declaration path [1] -> length 1 then index 1 (zigzag varints).
    private static final byte[] COMPANY_MESSAGE_INDEX = {0x02, 0x02};

    private static final String ID_FIELD = "id";
    private static final String NAME_FIELD = "name";
    private static final String BIO_FIELD = "bio";

    private static final int ID_VALUE = 7;
    private static final String USER_NAME_VALUE = "Alice";
    private static final String COMPANY_NAME_VALUE = "Acme";
    private static final String BIO_VALUE = "hello";

    private TestRunner runner;
    private MockSchemaRegistry schemaRegistry;
    private ConfluentEncodedSchemaReferenceWriter referenceWriter;
    private ConfluentProtobufMessageIndexWriter indexWriter;
    private ConfluentEncodedSchemaReferenceReader referenceReader;
    private ConfluentProtobufMessageNameResolver messageNameResolver;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);

        schemaRegistry = new MockSchemaRegistry();
        referenceWriter = new ConfluentEncodedSchemaReferenceWriter();
        indexWriter = new ConfluentProtobufMessageIndexWriter();
        referenceReader = new ConfluentEncodedSchemaReferenceReader();
        messageNameResolver = new ConfluentProtobufMessageNameResolver();

        enableService(REGISTRY_ID, schemaRegistry);
        enableService(REFERENCE_WRITER_ID, referenceWriter);
        enableService(INDEX_WRITER_ID, indexWriter);
        enableService(REFERENCE_READER_ID, referenceReader);
        enableService(MESSAGE_NAME_RESOLVER_ID, messageNameResolver);
    }

    @Test
    void testFirstRootMessageSingleByteIndexAndRoundTrip() throws Exception {
        final Map<String, Object> values = new HashMap<>();
        values.put(ID_FIELD, ID_VALUE);
        values.put(NAME_FIELD, USER_NAME_VALUE);
        final MapRecord userRecord = new MapRecord(recordSchema(ID_FIELD, NAME_FIELD), values);

        final byte[] output = writeConfluent(USER_MESSAGE, userRecord);

        // [header][message index][payload]; the first root message collapses to the single index byte 0x00.
        assertArrayEquals(EXPECTED_HEADER, slice(output, 0, HEADER_LENGTH));
        assertEquals(0x00, output[HEADER_LENGTH]);

        final Record readBack = readConfluent(output);
        assertEquals(ID_VALUE, readBack.getValue(ID_FIELD));
        assertEquals(USER_NAME_VALUE, readBack.getValue(NAME_FIELD));
    }

    @Test
    void testNestedMessageIndexAndRoundTrip() throws Exception {
        final Map<String, Object> values = new HashMap<>();
        values.put(BIO_FIELD, BIO_VALUE);
        final MapRecord profileRecord = new MapRecord(recordSchema(BIO_FIELD), values);

        final byte[] output = writeConfluent(PROFILE_MESSAGE, profileRecord);

        assertArrayEquals(EXPECTED_HEADER, slice(output, 0, HEADER_LENGTH));
        assertArrayEquals(NESTED_PROFILE_MESSAGE_INDEX, slice(output, HEADER_LENGTH, NESTED_PROFILE_MESSAGE_INDEX.length));

        final Record readBack = readConfluent(output);
        assertEquals(BIO_VALUE, readBack.getValue(BIO_FIELD));
    }

    @Test
    void testCustomOptionsNestedMessageRoundTrip() throws Exception {
        schemaRegistry.returnSchemaText(CUSTOM_OPTIONS_SCHEMA_TEXT_VALUE);
        final MapRecord profileRecord = new MapRecord(recordSchema(BIO_FIELD), Map.of(BIO_FIELD, BIO_VALUE));

        final byte[] output = writeConfluent(PROFILE_MESSAGE, profileRecord);

        assertArrayEquals(EXPECTED_HEADER, slice(output, 0, HEADER_LENGTH));
        assertArrayEquals(NESTED_PROFILE_MESSAGE_INDEX, slice(output, HEADER_LENGTH, NESTED_PROFILE_MESSAGE_INDEX.length));

        final Record readBack = readConfluent(output);
        assertEquals(BIO_VALUE, readBack.getValue(BIO_FIELD));
    }

    @Test
    void testCustomOptionsSecondRootMessageRoundTrip() throws Exception {
        schemaRegistry.returnSchemaText(CUSTOM_OPTIONS_SCHEMA_TEXT_VALUE);
        final MapRecord companyRecord = new MapRecord(recordSchema(NAME_FIELD), Map.of(NAME_FIELD, COMPANY_NAME_VALUE));

        final byte[] output = writeConfluent(COMPANY_MESSAGE, companyRecord);

        assertArrayEquals(EXPECTED_HEADER, slice(output, 0, HEADER_LENGTH));
        assertArrayEquals(COMPANY_MESSAGE_INDEX, slice(output, HEADER_LENGTH, COMPANY_MESSAGE_INDEX.length));

        final Record readBack = readConfluent(output);
        assertEquals(COMPANY_NAME_VALUE, readBack.getValue(NAME_FIELD));
    }

    @Test
    void testSchemaTextWithConfluentSchemaReferenceWriterIsInvalid() throws Exception {
        final StandardProtobufWriter writer = new StandardProtobufWriter();
        runner.addControllerService("schemaTextWriter", writer);
        runner.setProperty(writer, SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY.getValue());
        runner.setProperty(writer, SCHEMA_TEXT, SCHEMA_TEXT_VALUE);
        runner.setProperty(writer, StandardProtobufWriter.MESSAGE_NAME, USER_MESSAGE);
        runner.assertValid(writer);

        // Inline schema text has no registry identifier or version for the Confluent header
        runner.setProperty(writer, StandardProtobufWriter.SCHEMA_REFERENCE_WRITER, REFERENCE_WRITER_ID);
        runner.assertNotValid(writer);
    }

    private byte[] writeConfluent(final String messageName, final MapRecord record) throws Exception {
        final StandardProtobufWriter writer = new StandardProtobufWriter();
        runner.addControllerService("writer-" + messageName, writer);
        runner.setProperty(writer, SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY.getValue());
        runner.setProperty(writer, SCHEMA_REGISTRY, REGISTRY_ID);
        runner.setProperty(writer, SCHEMA_NAME, "user");
        runner.setProperty(writer, StandardProtobufWriter.MESSAGE_NAME_RESOLUTION_STRATEGY,
            StandardProtobufWriter.MessageNameResolverStrategy.MESSAGE_NAME_PROPERTY.getValue());
        runner.setProperty(writer, StandardProtobufWriter.MESSAGE_NAME, messageName);
        runner.setProperty(writer, StandardProtobufWriter.SCHEMA_REFERENCE_WRITER, REFERENCE_WRITER_ID);
        runner.setProperty(writer, StandardProtobufWriter.MESSAGE_INDEX_WRITER, INDEX_WRITER_ID);
        runner.enableControllerService(writer);

        final RecordSchema writeSchema = writer.getSchema(emptyMap(), null);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (RecordSetWriter recordSetWriter = writer.createWriter(runner.getLogger(), writeSchema, out, emptyMap())) {
            recordSetWriter.beginRecordSet();
            recordSetWriter.write(record);
            recordSetWriter.finishRecordSet();
            recordSetWriter.flush();
        }
        return out.toByteArray();
    }

    private Record readConfluent(final byte[] data) throws Exception {
        final StandardProtobufReader reader = new StandardProtobufReader();
        runner.addControllerService("reader", reader);
        runner.setProperty(reader, SCHEMA_ACCESS_STRATEGY, SCHEMA_REFERENCE_READER_PROPERTY.getValue());
        runner.setProperty(reader, SCHEMA_REFERENCE_READER, REFERENCE_READER_ID);
        runner.setProperty(reader, SCHEMA_REGISTRY, REGISTRY_ID);
        runner.setProperty(reader, StandardProtobufReader.MESSAGE_NAME_RESOLUTION_STRATEGY,
            StandardProtobufReader.MessageNameResolverStrategy.MESSAGE_NAME_RESOLVER.getValue());
        runner.setProperty(reader, StandardProtobufReader.MESSAGE_NAME_RESOLVER, MESSAGE_NAME_RESOLVER_ID);
        runner.enableControllerService(reader);

        final RecordReader recordReader = reader.createRecordReader(emptyMap(), new ByteArrayInputStream(data), data.length, runner.getLogger());
        final Record record = recordReader.nextRecord();
        assertNull(recordReader.nextRecord());
        return record;
    }

    private RecordSchema recordSchema(final String... fieldNames) {
        final RecordField[] fields = new RecordField[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            final RecordFieldType type = ID_FIELD.equals(fieldNames[i]) ? RecordFieldType.INT : RecordFieldType.STRING;
            fields[i] = new RecordField(fieldNames[i], type.getDataType());
        }
        return new SimpleRecordSchema(List.of(fields));
    }

    private void enableService(final String id, final ControllerService service) throws Exception {
        runner.addControllerService(id, service);
        runner.enableControllerService(service);
    }

    private byte[] slice(final byte[] data, final int from, final int length) {
        final byte[] out = new byte[length];
        System.arraycopy(data, from, out, 0, length);
        return out;
    }

    /**
     * Returns a fixed Protobuf SchemaDefinition (with a numeric schema id and version) for any identifier, so that
     * both the writer's name-based lookup and the reader's Confluent-header-based lookup resolve the same schema.
     */
    static class MockSchemaRegistry extends AbstractControllerService implements SchemaRegistry {
        private volatile String schemaText = SCHEMA_TEXT_VALUE;

        void returnSchemaText(final String schemaText) {
            this.schemaText = schemaText;
        }

        @Override
        public RecordSchema retrieveSchema(final SchemaIdentifier schemaIdentifier) {
            throw new UnsupportedOperationException("retrieveSchema is not used in this test");
        }

        @Override
        public SchemaDefinition retrieveSchemaDefinition(final SchemaIdentifier schemaIdentifier) throws SchemaNotFoundException {
            final SchemaIdentifier identifier = SchemaIdentifier.builder().name("user.proto").id(SCHEMA_ID).version(1).build();
            return new StandardSchemaDefinition(identifier, schemaText, SchemaDefinition.SchemaType.PROTOBUF);
        }

        @Override
        public Set<SchemaField> getSuppliedSchemaFields() {
            return EnumSet.of(SchemaField.SCHEMA_NAME, SchemaField.SCHEMA_TEXT, SchemaField.SCHEMA_TEXT_FORMAT,
                SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION);
        }
    }
}
