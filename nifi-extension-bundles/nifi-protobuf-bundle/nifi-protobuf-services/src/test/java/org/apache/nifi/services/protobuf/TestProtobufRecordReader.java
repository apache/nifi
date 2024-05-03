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

import com.google.protobuf.Descriptors;
import com.squareup.wire.schema.Schema;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.apache.nifi.services.protobuf.ProtoTestUtil.generateInputDataForProto3;
import static org.apache.nifi.services.protobuf.ProtoTestUtil.loadProto3TestSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestProtobufRecordReader {

    private static Schema protoSchema;

    @BeforeAll
    public static void setup(){
        protoSchema = loadProto3TestSchema();
    }

    @Test
    public void testReadRecord() throws Descriptors.DescriptorValidationException, IOException {
        final ProtobufRecordReader reader = createReader(generateInputDataForProto3(), "Proto3Message", protoSchema, generateRecordSchema());
        final Record record = reader.nextRecord(false, false);

        final Object field1 = record.getValue("booleanField");
        assertEquals(true, field1);
        assertInstanceOf(Boolean.class, field1);

        final Object field2 = record.getValue("stringField");
        assertEquals("Test text", field2);
        assertInstanceOf(String.class, field2);

        final Object field3 = record.getValue("int32Field");
        assertEquals(Integer.MAX_VALUE, field3);
        assertInstanceOf(Integer.class, field3);

        final Object field4 = record.getValue("uint32Field");
        assertNotNull(field4);
    }

    @Test
    public void testReadRecordWithCoerceType() throws Descriptors.DescriptorValidationException, IOException {
        final ProtobufRecordReader reader = createReader(generateInputDataForProto3(), "Proto3Message", protoSchema, generateRecordSchema());
        final Record record = reader.nextRecord(true, false);

        final Object field1 = record.getValue("booleanField");
        assertEquals("true", field1);
        assertInstanceOf(String.class, field1);

        final Object field2 = record.getValue("stringField");
        assertEquals("Test text", field2);
        assertInstanceOf(String.class, field2);

        final Object field3 = record.getValue("int32Field");
        assertEquals(String.valueOf(Integer.MAX_VALUE), field3);
        assertInstanceOf(String.class, field3);

        final Object field4 = record.getValue("uint32Field");
        assertNotNull(field4);
    }

    @Test
    public void testReadRecordWithDropUnknownFields() throws Descriptors.DescriptorValidationException, IOException {
        final ProtobufRecordReader reader = createReader(generateInputDataForProto3(), "Proto3Message", protoSchema, generateRecordSchema());
        final Record record = reader.nextRecord(false, true);

        final Object field1 = record.getValue("booleanField");
        assertEquals(true, field1);
        assertInstanceOf(Boolean.class, field1);

        final Object field2 = record.getValue("stringField");
        assertEquals("Test text", field2);
        assertInstanceOf(String.class, field2);

        final Object field3 = record.getValue("int32Field");
        assertEquals(Integer.MAX_VALUE, field3);
        assertInstanceOf(Integer.class, field3);

        final Object field4 = record.getValue("uint32Field");
        assertNull(field4);
    }

    @Test
    public void testReadRecordWithCoerceTypeAndDropUnknownFields() throws Descriptors.DescriptorValidationException, IOException {
        final ProtobufRecordReader reader = createReader(generateInputDataForProto3(), "Proto3Message", protoSchema, generateRecordSchema());
        final Record record = reader.nextRecord(true, true);

        final Object field1 = record.getValue("booleanField");
        assertEquals("true", field1);
        assertInstanceOf(String.class, field1);

        final Object field2 = record.getValue("stringField");
        assertEquals("Test text", field2);
        assertInstanceOf(String.class, field2);

        final Object field3 = record.getValue("int32Field");
        assertEquals(String.valueOf(Integer.MAX_VALUE), field3);
        assertInstanceOf(String.class, field3);

        final Object field4 = record.getValue("uint32Field");
        assertNull(field4);
    }

    private RecordSchema generateRecordSchema() {
        final List<RecordField> fields = new ArrayList<>();
        for (final String fieldName : new String[] {"booleanField", "stringField", "int32Field"}) {
            fields.add(new RecordField(fieldName, RecordFieldType.STRING.getDataType()));
        }
        return new SimpleRecordSchema(fields);
    }

    private ProtobufRecordReader createReader(InputStream in, String message, Schema schema, RecordSchema recordSchema) {
        return new ProtobufRecordReader(schema, message, in, recordSchema);
    }
}
