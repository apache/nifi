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

package org.apache.nifi.repository.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class TestSchemaRecordReader {

    @Test
    public void testReadExactlyOnceFields() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new SimpleRecordField("int", FieldType.INT, Repetition.EXACTLY_ONE));
        fields.add(new SimpleRecordField("boolean", FieldType.BOOLEAN, Repetition.EXACTLY_ONE));
        fields.add(new SimpleRecordField("byte array", FieldType.BYTE_ARRAY, Repetition.EXACTLY_ONE));
        fields.add(new SimpleRecordField("long", FieldType.LONG, Repetition.EXACTLY_ONE));
        fields.add(new SimpleRecordField("string", FieldType.STRING, Repetition.EXACTLY_ONE));
        fields.add(new SimpleRecordField("long string", FieldType.LONG_STRING, Repetition.EXACTLY_ONE));
        fields.add(new ComplexRecordField("complex", Repetition.EXACTLY_ONE,
            new SimpleRecordField("key", FieldType.STRING, Repetition.EXACTLY_ONE),
            new SimpleRecordField("value", FieldType.STRING, Repetition.EXACTLY_ONE)));
        fields.add(new MapRecordField("map",
            new SimpleRecordField("key", FieldType.STRING, Repetition.EXACTLY_ONE),
            new SimpleRecordField("value", FieldType.STRING, Repetition.ZERO_OR_ONE), Repetition.EXACTLY_ONE));
        fields.add(new UnionRecordField("union1", Repetition.EXACTLY_ONE, Arrays.asList(new RecordField[] {
            new SimpleRecordField("one", FieldType.STRING, Repetition.EXACTLY_ONE),
            new SimpleRecordField("two", FieldType.INT, Repetition.EXACTLY_ONE)
        })));
        fields.add(new UnionRecordField("union2", Repetition.EXACTLY_ONE, Arrays.asList(new RecordField[] {
            new SimpleRecordField("one", FieldType.STRING, Repetition.EXACTLY_ONE),
            new SimpleRecordField("two", FieldType.INT, Repetition.EXACTLY_ONE)
        })));
        final RecordSchema schema = new RecordSchema(fields);

        final SchemaRecordReader reader = SchemaRecordReader.fromSchema(schema);

        final byte[] buffer;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(baos)) {

            dos.write(1); // sentinel byte
            dos.writeInt(42);
            dos.writeBoolean(true);
            final byte[] array = "hello".getBytes();
            dos.writeInt(array.length);
            dos.write(array);

            dos.writeLong(42L);
            dos.writeUTF("hello");

            final String longString = "hello";
            final byte[] longStringArray = longString.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(longStringArray.length);
            dos.write(longStringArray);

            dos.writeUTF("key");
            dos.writeUTF("value");

            dos.writeInt(2);
            dos.writeUTF("key1");
            dos.writeBoolean(true);
            dos.writeUTF("value1");
            dos.writeUTF("key2");
            dos.writeBoolean(false);

            dos.writeUTF("one");
            dos.writeUTF("hello");

            dos.writeUTF("two");
            dos.writeInt(42);

            buffer = baos.toByteArray();
        }

        try (final ByteArrayInputStream in = new ByteArrayInputStream(buffer)) {
            final Record record = reader.readRecord(in);
            assertNotNull(record);

            assertEquals(42, record.getFieldValue("int"));
            assertTrue((boolean) record.getFieldValue("boolean"));
            assertTrue(Arrays.equals("hello".getBytes(), (byte[]) record.getFieldValue("byte array")));
            assertEquals(42L, record.getFieldValue("long"));
            assertEquals("hello", record.getFieldValue("string"));
            assertEquals("hello", record.getFieldValue("long string"));

            final Record complexRecord = (Record) record.getFieldValue("complex");
            assertEquals("key", complexRecord.getFieldValue(new SimpleRecordField("key", FieldType.STRING, Repetition.EXACTLY_ONE)));
            assertEquals("value", complexRecord.getFieldValue(new SimpleRecordField("value", FieldType.STRING, Repetition.EXACTLY_ONE)));

            final Map<String, String> map = new HashMap<>();
            map.put("key1", "value1");
            map.put("key2", null);
            assertEquals(map, record.getFieldValue("map"));

            assertEquals("hello", record.getFieldValue("union1"));
            assertEquals(42, record.getFieldValue("union2"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReadZeroOrOneFields() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new SimpleRecordField("int", FieldType.INT, Repetition.ZERO_OR_ONE));
        fields.add(new SimpleRecordField("int present", FieldType.INT, Repetition.ZERO_OR_ONE));
        fields.add(new SimpleRecordField("boolean", FieldType.BOOLEAN, Repetition.ZERO_OR_ONE));
        fields.add(new SimpleRecordField("boolean present", FieldType.BOOLEAN, Repetition.ZERO_OR_ONE));
        fields.add(new SimpleRecordField("byte array", FieldType.BYTE_ARRAY, Repetition.ZERO_OR_ONE));
        fields.add(new SimpleRecordField("byte array present", FieldType.BYTE_ARRAY, Repetition.ZERO_OR_ONE));
        fields.add(new SimpleRecordField("long", FieldType.LONG, Repetition.ZERO_OR_ONE));
        fields.add(new SimpleRecordField("long present", FieldType.LONG, Repetition.ZERO_OR_ONE));
        fields.add(new SimpleRecordField("string", FieldType.STRING, Repetition.ZERO_OR_ONE));
        fields.add(new SimpleRecordField("string present", FieldType.STRING, Repetition.ZERO_OR_ONE));
        fields.add(new SimpleRecordField("long string", FieldType.LONG_STRING, Repetition.ZERO_OR_ONE));
        fields.add(new SimpleRecordField("long string present", FieldType.LONG_STRING, Repetition.ZERO_OR_ONE));
        fields.add(new ComplexRecordField("complex", Repetition.ZERO_OR_ONE,
            new SimpleRecordField("key", FieldType.STRING, Repetition.ZERO_OR_ONE),
            new SimpleRecordField("value", FieldType.STRING, Repetition.ZERO_OR_ONE)));
        fields.add(new ComplexRecordField("complex present", Repetition.ZERO_OR_ONE,
            new SimpleRecordField("key", FieldType.STRING, Repetition.ZERO_OR_ONE),
            new SimpleRecordField("value", FieldType.STRING, Repetition.ZERO_OR_ONE)));
        fields.add(new MapRecordField("map",
            new SimpleRecordField("key", FieldType.STRING, Repetition.ZERO_OR_ONE),
            new SimpleRecordField("value", FieldType.STRING, Repetition.ZERO_OR_MORE), Repetition.ZERO_OR_ONE));
        fields.add(new MapRecordField("map present",
            new SimpleRecordField("key", FieldType.STRING, Repetition.ZERO_OR_ONE),
            new SimpleRecordField("value", FieldType.STRING, Repetition.ZERO_OR_MORE), Repetition.ZERO_OR_ONE));
        fields.add(new UnionRecordField("union", Repetition.ZERO_OR_ONE, Arrays.asList(new RecordField[] {
            new SimpleRecordField("one", FieldType.STRING, Repetition.EXACTLY_ONE),
            new SimpleRecordField("two", FieldType.INT, Repetition.EXACTLY_ONE)
        })));
        fields.add(new UnionRecordField("union present", Repetition.ZERO_OR_ONE, Arrays.asList(new RecordField[] {
            new SimpleRecordField("one", FieldType.STRING, Repetition.EXACTLY_ONE),
            new SimpleRecordField("two", FieldType.INT, Repetition.ZERO_OR_MORE)
        })));

        final RecordSchema schema = new RecordSchema(fields);

        final SchemaRecordReader reader = SchemaRecordReader.fromSchema(schema);

        // for each field, make the first one missing and the second one present.
        final byte[] buffer;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(baos)) {

            dos.write(1); // sentinel byte
            dos.write(0);
            dos.writeByte(1);
            dos.writeInt(42);

            dos.write(0);
            dos.writeByte(1);
            dos.writeBoolean(true);

            final byte[] array = "hello".getBytes();
            dos.write(0);
            dos.writeByte(1);
            dos.writeInt(array.length);
            dos.write(array);

            dos.write(0);
            dos.writeByte(1);
            dos.writeLong(42L);

            dos.write(0);
            dos.writeByte(1);
            dos.writeUTF("hello");

            final String longString = "hello";
            final byte[] longStringArray = longString.getBytes(StandardCharsets.UTF_8);
            dos.write(0);
            dos.writeByte(1);
            dos.writeInt(longStringArray.length);
            dos.write(longStringArray);

            dos.write(0);
            dos.writeByte(1);
            dos.writeByte(1);
            dos.writeUTF("key");
            dos.writeByte(0);

            dos.writeBoolean(false); // map not present
            dos.writeBoolean(true); // map present
            dos.writeInt(2);    // 2 entries in the map
            dos.writeBoolean(true); // key present
            dos.writeUTF("key1");
            dos.writeInt(2); // 2 values
            dos.writeUTF("one");
            dos.writeUTF("two");
            dos.writeBoolean(false); // key not present
            dos.writeInt(1);
            dos.writeUTF("three");

            dos.writeBoolean(false);
            dos.writeBoolean(true);
            dos.writeUTF("two");
            dos.writeInt(3);    // 3 entries
            dos.writeInt(1);
            dos.writeInt(2);
            dos.writeInt(3);

            buffer = baos.toByteArray();
        }

        try (final ByteArrayInputStream in = new ByteArrayInputStream(buffer)) {
            final Record record = reader.readRecord(in);
            assertNotNull(record);

            // Read everything into a map and make sure that no value is missing that has a name ending in " present"
            final Map<String, Object> valueMap = new HashMap<>();
            for (final RecordField field : record.getSchema().getFields()) {
                final Object value = record.getFieldValue(field);
                if (value == null) {
                    assertFalse(field.getFieldName().endsWith(" present"));
                    continue;
                }

                valueMap.put(field.getFieldName(), value);
            }

            assertEquals(42, valueMap.get("int present"));
            assertTrue((boolean) valueMap.get("boolean present"));
            assertTrue(Arrays.equals("hello".getBytes(), (byte[]) valueMap.get("byte array present")));
            assertEquals(42L, valueMap.get("long present"));
            assertEquals("hello", valueMap.get("string present"));
            assertEquals("hello", valueMap.get("long string present"));

            final Record complexRecord = (Record) valueMap.get("complex present");
            assertEquals("key", complexRecord.getFieldValue(new SimpleRecordField("key", FieldType.STRING, Repetition.EXACTLY_ONE)));
            assertNull(complexRecord.getFieldValue(new SimpleRecordField("value", FieldType.STRING, Repetition.EXACTLY_ONE)));

            final Map<String, List<String>> map = (Map<String, List<String>>) valueMap.get("map present");
            assertNotNull(map);
            assertEquals(2, map.size());
            assertTrue(map.containsKey(null));
            assertTrue(map.containsKey("key1"));

            final List<String> key1Values = Arrays.asList(new String[] {"one", "two"});
            assertEquals(key1Values, map.get("key1"));
            final List<String> nullKeyValues = Arrays.asList(new String[] {"three"});
            assertEquals(nullKeyValues, map.get(null));

            final List<Integer> unionValues = (List<Integer>) valueMap.get("union present");
            assertEquals(3, unionValues.size());
            assertEquals(1, unionValues.get(0).intValue());
            assertEquals(2, unionValues.get(1).intValue());
            assertEquals(3, unionValues.get(2).intValue());
        }
    }
}
