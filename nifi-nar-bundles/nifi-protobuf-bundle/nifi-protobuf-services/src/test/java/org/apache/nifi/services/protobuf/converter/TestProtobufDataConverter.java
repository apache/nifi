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
package org.apache.nifi.services.protobuf.converter;

import com.google.protobuf.Descriptors;
import com.squareup.wire.schema.Schema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.services.protobuf.ProtoTestUtil;
import org.apache.nifi.services.protobuf.schema.ProtoSchemaParser;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;

import static org.apache.nifi.services.protobuf.ProtoTestUtil.loadProto2TestSchema;
import static org.apache.nifi.services.protobuf.ProtoTestUtil.loadProto3TestSchema;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestProtobufDataConverter {

    @Test
    public void testDataConverterForProto3() throws Descriptors.DescriptorValidationException, IOException {
        final Schema schema = loadProto3TestSchema();
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema("Proto3Message");

        final ProtobufDataConverter dataConverter = new ProtobufDataConverter(schema, "Proto3Message", recordSchema, false, false);
        final MapRecord record = dataConverter.createRecord(ProtoTestUtil.generateInputDataForProto3());

        assertEquals(true, record.getValue("booleanField"));
        assertEquals("Test text", record.getValue("stringField"));
        assertEquals(Integer.MAX_VALUE, record.getValue("int32Field"));
        assertEquals(4294967295L, record.getValue("uint32Field"));
        assertEquals(Integer.MIN_VALUE, record.getValue("sint32Field"));
        assertEquals(4294967294L, record.getValue("fixed32Field"));
        assertEquals(Integer.MAX_VALUE, record.getValue("sfixed32Field"));
        assertEquals(Double.MAX_VALUE, record.getValue("doubleField"));
        assertEquals(Float.MAX_VALUE, record.getValue("floatField"));
        assertArrayEquals("Test bytes".getBytes(), (byte[]) record.getValue("bytesField"));
        assertEquals(Long.MAX_VALUE, record.getValue("int64Field"));
        assertEquals(new BigInteger("18446744073709551615"), DataTypeUtils.toBigInt(record.getValue("uint64Field"), "field12"));
        assertEquals(Long.MIN_VALUE, record.getValue("sint64Field"));
        assertEquals(new BigInteger("18446744073709551614"), DataTypeUtils.toBigInt(record.getValue("fixed64Field"), "field14"));
        assertEquals(Long.MAX_VALUE, record.getValue("sfixed64Field"));

        final MapRecord nestedRecord = (MapRecord) record.getValue("nestedMessage");
        assertEquals("ENUM_VALUE_3", nestedRecord.getValue("testEnum"));

        assertArrayEquals(new Object[]{"Repeated 1", "Repeated 2", "Repeated 3"}, (Object[]) nestedRecord.getValue("repeatedField"));

        // assert only one field is set in the OneOf field
        assertNull(nestedRecord.getValue("stringOption"));
        assertNull(nestedRecord.getValue("booleanOption"));
        assertEquals(3, nestedRecord.getValue("int32Option"));

        assertEquals(Map.of("test_key_entry1", 101, "test_key_entry2", 202), nestedRecord.getValue("testMap"));
    }

    @Test
    public void testDataConverterForProto2() throws Descriptors.DescriptorValidationException, IOException {
        final Schema schema = loadProto2TestSchema();
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema("Proto2Message");

        final ProtobufDataConverter dataConverter = new ProtobufDataConverter(schema, "Proto2Message", recordSchema, false, false);
        final MapRecord record = dataConverter.createRecord(ProtoTestUtil.generateInputDataForProto2());

        assertEquals(true, record.getValue("booleanField"));
        assertEquals("Missing field", record.getValue("stringField"));
        assertEquals(Integer.MAX_VALUE, record.getValue("extensionField"));

        final MapRecord anyValueRecord = (MapRecord) record.getValue("anyField");
        assertEquals("Test field 1", anyValueRecord.getValue("anyStringField1"));
        assertEquals("Test field 2", anyValueRecord.getValue("anyStringField2"));
    }

    @Test
    public void testMissingMessage() {
        final Schema schema = loadProto3TestSchema();
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema("Proto3Message");

        final ProtobufDataConverter dataConverter = new ProtobufDataConverter(schema, "MissingMessage", recordSchema, false, false);

        NullPointerException e = assertThrows(NullPointerException.class, () -> dataConverter.createRecord(ProtoTestUtil.generateInputDataForProto2()));
        assertTrue(e.getMessage().contains("Message with name [MissingMessage] not found in the provided proto files"), e.getMessage());
    }
}
