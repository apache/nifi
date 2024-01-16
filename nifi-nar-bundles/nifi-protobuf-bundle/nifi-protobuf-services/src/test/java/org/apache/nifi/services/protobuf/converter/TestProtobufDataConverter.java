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
import com.squareup.wire.schema.CoreLoaderKt;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.Schema;
import com.squareup.wire.schema.SchemaLoader;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.services.protobuf.ProtoTestUtil;
import org.apache.nifi.services.protobuf.schema.ProtoSchemaParser;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.FileSystems;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.services.protobuf.ProtoTestUtil.BASE_TEST_PATH;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestProtobufDataConverter {

    @Test
    public void testDataConverterForProto3() throws Descriptors.DescriptorValidationException, IOException {
        final SchemaLoader schemaLoader = new SchemaLoader(FileSystems.getDefault());
        schemaLoader.initRoots(Collections.singletonList(Location.get(BASE_TEST_PATH + "test_proto3.proto")), Collections.emptyList());
        final Schema schema = schemaLoader.loadSchema();
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema("Proto3Message");

        final ProtobufDataConverter dataConverter = new ProtobufDataConverter(schema, "Proto3Message", recordSchema, false, false);
        final MapRecord record = dataConverter.createRecord(ProtoTestUtil.generateInputDataForProto3());

        Map<String, Integer> expectedMap = new HashMap<>();
        expectedMap.put("test_key_entry1", 101);
        expectedMap.put("test_key_entry2", 202);

        assertEquals(true, record.getValue("field1"));
        assertEquals("Test text", record.getValue("field2"));
        assertEquals(Integer.MAX_VALUE, record.getValue("field3"));
        assertEquals(4294967295L, record.getValue("field4"));
        assertEquals(Integer.MIN_VALUE, record.getValue("field5"));
        assertEquals(4294967294L, record.getValue("field6"));
        assertEquals(Integer.MAX_VALUE, record.getValue("field7"));
        assertEquals(Double.MAX_VALUE, record.getValue("field8"));
        assertEquals(Float.MAX_VALUE, record.getValue("field9"));
        assertArrayEquals("Test bytes".getBytes(), (byte[]) record.getValue("field10"));
        assertEquals(Long.MAX_VALUE, record.getValue("field11"));
        assertEquals(new BigInteger("18446744073709551615"), DataTypeUtils.toBigInt(record.getValue("field12"), "field12"));
        assertEquals(Long.MIN_VALUE, record.getValue("field13"));
        assertEquals(new BigInteger("18446744073709551614"), DataTypeUtils.toBigInt(record.getValue("field14"), "field14"));
        assertEquals(Long.MAX_VALUE, record.getValue("field15"));

        final MapRecord nestedRecord = (MapRecord) record.getValue("nestedMessage");
        assertEquals("ENUM_VALUE_3", nestedRecord.getValue("testEnum"));

        assertArrayEquals(new Object[] {"Repeated 1", "Repeated 2", "Repeated 3"}, (Object[]) nestedRecord.getValue("repeatedField"));

        assertEquals(3, nestedRecord.getValue("option1"));
        assertEquals(3, nestedRecord.getValue("option2"));
        assertEquals(3, nestedRecord.getValue("option3"));
        assertEquals(expectedMap, nestedRecord.getValue("testMap"));
    }

    @Test
    public void testDataConverterForProto2() throws Descriptors.DescriptorValidationException, IOException {
        final SchemaLoader schemaLoader = new SchemaLoader(FileSystems.getDefault());
        schemaLoader.initRoots(Arrays.asList(
                Location.get(BASE_TEST_PATH, "test_proto2.proto"),
                Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, "google/protobuf/any.proto")), Collections.emptyList());

        final Schema schema = schemaLoader.loadSchema();
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema("Proto2Message");

        final ProtobufDataConverter dataConverter = new ProtobufDataConverter(schema, "Proto2Message", recordSchema, false, false);
        final MapRecord record = dataConverter.createRecord(ProtoTestUtil.generateInputDataForProto2());

        assertEquals(true, record.getValue("field1"));
        assertEquals("Missing field", record.getValue("field2"));
        assertEquals(Integer.MAX_VALUE, record.getValue("extensionField"));

        final MapRecord anyValueRecord = (MapRecord) record.getValue("anyField");
        assertEquals("Test field 1", anyValueRecord.getValue("anyValueField1"));
        assertEquals("Test field 2", anyValueRecord.getValue("anyValueField2"));
    }

    @Test
    public void testMissingMessage() {
        final SchemaLoader schemaLoader = new SchemaLoader(FileSystems.getDefault());
        schemaLoader.initRoots(Collections.singletonList(Location.get(BASE_TEST_PATH, "test_proto3.proto")), Collections.emptyList());

        final Schema schema = schemaLoader.loadSchema();
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema("Proto3Message");

        final ProtobufDataConverter dataConverter = new ProtobufDataConverter(schema, "MissingMessage", recordSchema, false, false);

        NullPointerException e = assertThrows(NullPointerException.class, () ->  dataConverter.createRecord(ProtoTestUtil.generateInputDataForProto2()));
        assertTrue(e.getMessage().contains("Message with name [MissingMessage] not found in the provided proto files"), e.getMessage());
    }
}
