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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
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

    private static final int SCHEMA_ID = 42;
    // Confluent header: magic byte 0x00 followed by the big-endian schema id (42).
    private static final byte[] EXPECTED_HEADER = {0x00, 0x00, 0x00, 0x00, 0x2A};

    private static final String SCHEMA_TEXT = """
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

        enableService("registry", schemaRegistry);
        enableService("referenceWriter", referenceWriter);
        enableService("indexWriter", indexWriter);
        enableService("referenceReader", referenceReader);
        enableService("messageNameResolver", messageNameResolver);
    }

    @Test
    void testFirstRootMessageSingleByteIndexAndRoundTrip() throws Exception {
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 7);
        values.put("name", "Alice");
        final MapRecord userRecord = new MapRecord(recordSchema("id", "name"), values);

        final byte[] output = writeConfluent("User", userRecord);

        // [header][message index][payload]; the first root message collapses to the single index byte 0x00.
        assertArrayEquals(EXPECTED_HEADER, slice(output, 0, 5));
        assertEquals(0x00, output[5]);

        final Record readBack = readConfluent(output);
        assertEquals(7, readBack.getValue("id"));
        assertEquals("Alice", readBack.getValue("name"));
    }

    @Test
    void testNestedMessageIndexAndRoundTrip() throws Exception {
        final Map<String, Object> values = new HashMap<>();
        values.put("bio", "hello");
        final MapRecord profileRecord = new MapRecord(recordSchema("bio"), values);

        final byte[] output = writeConfluent("User.Profile", profileRecord);

        // Nested message User.Profile -> declaration path [0, 0] -> length 2 then indexes 0, 0 (zigzag varints).
        assertArrayEquals(EXPECTED_HEADER, slice(output, 0, 5));
        assertArrayEquals(new byte[] {0x04, 0x00, 0x00}, slice(output, 5, 8));

        final Record readBack = readConfluent(output);
        assertEquals("hello", readBack.getValue("bio"));
    }

    private byte[] writeConfluent(final String messageName, final MapRecord record) throws Exception {
        final StandardProtobufWriter writer = new StandardProtobufWriter();
        runner.addControllerService("writer-" + messageName, writer);
        runner.setProperty(writer, SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY.getValue());
        runner.setProperty(writer, SCHEMA_REGISTRY, "registry");
        runner.setProperty(writer, SCHEMA_NAME, "user");
        runner.setProperty(writer, StandardProtobufWriter.MESSAGE_NAME_RESOLUTION_STRATEGY,
            StandardProtobufWriter.MessageNameResolverStrategy.MESSAGE_NAME_PROPERTY.getValue());
        runner.setProperty(writer, StandardProtobufWriter.MESSAGE_NAME, messageName);
        runner.setProperty(writer, StandardProtobufWriter.SCHEMA_REFERENCE_WRITER, "referenceWriter");
        runner.setProperty(writer, StandardProtobufWriter.MESSAGE_INDEX_WRITER, "indexWriter");
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
        runner.setProperty(reader, SCHEMA_REFERENCE_READER, "referenceReader");
        runner.setProperty(reader, SCHEMA_REGISTRY, "registry");
        runner.setProperty(reader, StandardProtobufReader.MESSAGE_NAME_RESOLUTION_STRATEGY,
            StandardProtobufReader.MessageNameResolverStrategy.MESSAGE_NAME_RESOLVER.getValue());
        runner.setProperty(reader, StandardProtobufReader.MESSAGE_NAME_RESOLVER, "messageNameResolver");
        runner.enableControllerService(reader);

        final RecordReader recordReader = reader.createRecordReader(emptyMap(), new ByteArrayInputStream(data), data.length, runner.getLogger());
        final Record record = recordReader.nextRecord();
        assertNull(recordReader.nextRecord());
        return record;
    }

    private RecordSchema recordSchema(final String... fieldNames) {
        final RecordField[] fields = new RecordField[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            final RecordFieldType type = "id".equals(fieldNames[i]) ? RecordFieldType.INT : RecordFieldType.STRING;
            fields[i] = new RecordField(fieldNames[i], type.getDataType());
        }
        return new SimpleRecordSchema(List.of(fields));
    }

    private void enableService(final String id, final ControllerService service) throws Exception {
        runner.addControllerService(id, service);
        runner.enableControllerService(service);
    }

    private byte[] slice(final byte[] data, final int from, final int to) {
        final byte[] out = new byte[to - from];
        System.arraycopy(data, from, out, 0, to - from);
        return out;
    }

    /**
     * Returns a fixed Protobuf SchemaDefinition (with a numeric schema id and version) for any identifier, so that
     * both the writer's name-based lookup and the reader's Confluent-header-based lookup resolve the same schema.
     */
    static class MockSchemaRegistry extends AbstractControllerService implements SchemaRegistry {
        private final SchemaDefinition schemaDefinition = new StandardSchemaDefinition(
            SchemaIdentifier.builder().name("user.proto").id((long) SCHEMA_ID).version(1).build(),
            SCHEMA_TEXT,
            SchemaDefinition.SchemaType.PROTOBUF);

        @Override
        public RecordSchema retrieveSchema(final SchemaIdentifier schemaIdentifier) {
            throw new UnsupportedOperationException("retrieveSchema is not used in this test");
        }

        @Override
        public SchemaDefinition retrieveSchemaDefinition(final SchemaIdentifier schemaIdentifier) throws SchemaNotFoundException {
            return schemaDefinition;
        }

        @Override
        public Set<SchemaField> getSuppliedSchemaFields() {
            return emptySet();
        }
    }
}
