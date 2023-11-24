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
package org.apache.nifi.confluent.schemaregistry;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.SimpleRecordSchema;
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
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfluentEncodedSchemaReferenceWriterTest {
    private static final String SERVICE_ID = ConfluentEncodedSchemaReferenceWriter.class.getSimpleName();

    private static final byte MAGIC_BYTE = 0;

    private static final int VERSION = 2;

    private static final long ID = 123456;

    private ConfluentEncodedSchemaReferenceWriter writer;

    @BeforeEach
    void setRunner() throws InitializationException {
        writer = new ConfluentEncodedSchemaReferenceWriter();

        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService(SERVICE_ID, writer);
        runner.enableControllerService(writer);
    }

    @Test
    void testValidateValidSchema() {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().id(ID).version(VERSION).build();
        final RecordSchema recordSchema = createRecordSchema(schemaIdentifier);

        assertDoesNotThrow(() -> writer.validateSchema(recordSchema));
    }

    @Test
    void testValidateInvalidSchema() {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name("test").build();
        final RecordSchema recordSchema = createRecordSchema(schemaIdentifier);

        assertThrows(SchemaNotFoundException.class, () -> writer.validateSchema(recordSchema));
    }

    @Test
    void testWriteHeader() throws IOException {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().id(ID).version(VERSION).build();
        final RecordSchema recordSchema = createRecordSchema(schemaIdentifier);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        writer.writeHeader(recordSchema, out);

        try (final ByteArrayInputStream bytesIn = new ByteArrayInputStream(out.toByteArray());
             final DataInputStream in = new DataInputStream(bytesIn)) {
            assertEquals(MAGIC_BYTE, in.readByte());

            final OptionalLong identifier = schemaIdentifier.getIdentifier();
            assertTrue(identifier.isPresent());

            final long id = identifier.getAsLong();
            assertEquals(id, in.readInt());
        }
    }

    private RecordSchema createRecordSchema(final SchemaIdentifier schemaIdentifier) {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("firstField", RecordFieldType.STRING.getDataType()));
        return new SimpleRecordSchema(fields, schemaIdentifier);
    }
}
