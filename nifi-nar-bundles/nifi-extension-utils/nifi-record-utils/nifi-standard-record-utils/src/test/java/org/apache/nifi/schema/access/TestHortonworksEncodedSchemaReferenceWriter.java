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

package org.apache.nifi.schema.access;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestHortonworksEncodedSchemaReferenceWriter {

    @Test
    public void testEncodeProtocolVersion1() throws IOException {
        final long id = 48;
        final int version = 2;
        final int protocolVersion = 1;

        final HortonworksEncodedSchemaReferenceWriter writer = new HortonworksEncodedSchemaReferenceWriter(protocolVersion);

        final RecordSchema schema = new SimpleRecordSchema(Collections.emptyList(),
                SchemaIdentifier.builder().name("name").id(id).version(version).build());

        final byte[] header;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            writer.writeHeader(schema, baos);
            header = baos.toByteArray();
        }

        try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(header))) {
            assertEquals(protocolVersion, dis.read()); // verify 'protocol version'
            assertEquals(id, dis.readLong()); // verify schema id
            assertEquals(version, dis.readInt()); // verify schema version
            assertEquals(-1, dis.read()); // no more bytes
        }
    }

    @Test
    public void testEncodeProtocolVersion2() throws IOException {
        final long schemaVersionId = 123;
        final int protocolVersion = 2;

        final HortonworksEncodedSchemaReferenceWriter writer = new HortonworksEncodedSchemaReferenceWriter(protocolVersion);

        final RecordSchema schema = new SimpleRecordSchema(Collections.emptyList(),
                SchemaIdentifier.builder().schemaVersionId(schemaVersionId).build());

        final byte[] header;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            writer.writeHeader(schema, baos);
            header = baos.toByteArray();
        }

        try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(header))) {
            assertEquals(protocolVersion, dis.read()); // verify 'protocol version'
            assertEquals(schemaVersionId, dis.readLong()); // verify schema version id
            assertEquals(-1, dis.read()); // no more bytes
        }
    }

    @Test
    public void testEncodeProtocolVersion3() throws IOException {
        final int schemaVersionId = 123;
        final int protocolVersion = 3;

        final HortonworksEncodedSchemaReferenceWriter writer = new HortonworksEncodedSchemaReferenceWriter(protocolVersion);

        final RecordSchema schema = new SimpleRecordSchema(Collections.emptyList(),
                SchemaIdentifier.builder().schemaVersionId((long)schemaVersionId).build());

        final byte[] header;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            writer.writeHeader(schema, baos);
            header = baos.toByteArray();
        }

        try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(header))) {
            assertEquals(protocolVersion, dis.read()); // verify 'protocol version'
            assertEquals(schemaVersionId, dis.readInt()); // verify schema version id
            assertEquals(-1, dis.read()); // no more bytes
        }
    }

    @Test
    public void testValidateWithProtocol1AndValidSchema() throws SchemaNotFoundException {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().id(123456L).version(2).build();
        final RecordSchema recordSchema = createRecordSchema(schemaIdentifier);

        final SchemaAccessWriter schemaAccessWriter = new HortonworksEncodedSchemaReferenceWriter(1);
        schemaAccessWriter.validateSchema(recordSchema);
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testValidateWithProtocol1AndMissingSchemaId() throws SchemaNotFoundException {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name("test").build();
        final RecordSchema recordSchema = createRecordSchema(schemaIdentifier);

        final SchemaAccessWriter schemaAccessWriter = new HortonworksEncodedSchemaReferenceWriter(1);
        schemaAccessWriter.validateSchema(recordSchema);
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testValidateWithProtocol1AndMissingSchemaName() throws SchemaNotFoundException {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().id(123456L).build();
        final RecordSchema recordSchema = createRecordSchema(schemaIdentifier);

        final SchemaAccessWriter schemaAccessWriter = new HortonworksEncodedSchemaReferenceWriter(1);
        schemaAccessWriter.validateSchema(recordSchema);
    }

    @Test
    public void testValidateWithProtocol2AndValidSchema() throws SchemaNotFoundException {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().schemaVersionId(9999L).build();
        final RecordSchema recordSchema = createRecordSchema(schemaIdentifier);

        final SchemaAccessWriter schemaAccessWriter = new HortonworksEncodedSchemaReferenceWriter(2);
        schemaAccessWriter.validateSchema(recordSchema);
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testValidateWithProtocol2AndMissingSchemaVersionId() throws SchemaNotFoundException {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name("test").build();
        final RecordSchema recordSchema = createRecordSchema(schemaIdentifier);

        final SchemaAccessWriter schemaAccessWriter = new HortonworksEncodedSchemaReferenceWriter(2);
        schemaAccessWriter.validateSchema(recordSchema);
    }

    @Test
    public void testValidateWithProtocol3AndValidSchema() throws SchemaNotFoundException {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().schemaVersionId(9999L).build();
        final RecordSchema recordSchema = createRecordSchema(schemaIdentifier);

        final SchemaAccessWriter schemaAccessWriter = new HortonworksEncodedSchemaReferenceWriter(3);
        schemaAccessWriter.validateSchema(recordSchema);
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testValidateWithProtocol3AndMissingSchemaVersionId() throws SchemaNotFoundException {
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().name("test").build();
        final RecordSchema recordSchema = createRecordSchema(schemaIdentifier);

        final SchemaAccessWriter schemaAccessWriter = new HortonworksEncodedSchemaReferenceWriter(3);
        schemaAccessWriter.validateSchema(recordSchema);
    }

    private RecordSchema createRecordSchema(final SchemaIdentifier schemaIdentifier) {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("firstName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("lastName", RecordFieldType.STRING.getDataType()));
        return new SimpleRecordSchema(fields, schemaIdentifier);
    }
}
