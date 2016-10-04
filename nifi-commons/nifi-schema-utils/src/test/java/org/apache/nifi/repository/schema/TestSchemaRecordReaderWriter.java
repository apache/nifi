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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class TestSchemaRecordReaderWriter {

    @Test
    @SuppressWarnings("unchecked")
    public void testRoundTrip() throws IOException {
        // Create a 'complex' record that contains two different types of fields - a string and an int.
        final List<RecordField> complexFieldList1 = new ArrayList<>();
        complexFieldList1.add(createField("string field", FieldType.STRING));
        complexFieldList1.add(createField("int field", FieldType.INT));
        final ComplexRecordField complexField1 = new ComplexRecordField("complex1", Repetition.EXACTLY_ONE, complexFieldList1);
        final Map<RecordField, Object> complexMap1 = new LinkedHashMap<>();
        final RecordField stringField = createField("string field", FieldType.STRING);
        final RecordField intField = createField("int field", FieldType.INT);
        complexMap1.put(stringField, "apples");
        complexMap1.put(intField, 100);
        final FieldMapRecord complexRecord1 = new FieldMapRecord(complexMap1, new RecordSchema(stringField, intField));

        // Create another 'complex' record that contains two other types of fields - a long string and a long.
        final List<RecordField> complexFieldList2 = new ArrayList<>();
        complexFieldList2.add(createField("long string field", FieldType.LONG_STRING));
        complexFieldList2.add(createField("long field", FieldType.LONG));
        final ComplexRecordField complexField2 = new ComplexRecordField("complex2", Repetition.EXACTLY_ONE, complexFieldList2);
        final Map<RecordField, Object> complexMap2 = new LinkedHashMap<>();
        final RecordField longStringField = createField("long string field", FieldType.LONG_STRING);
        final RecordField longField = createField("long field", FieldType.LONG);
        complexMap2.put(longStringField, "oranges");
        complexMap2.put(longField, Long.MAX_VALUE);
        final FieldMapRecord complexRecord2 = new FieldMapRecord(complexMap2, new RecordSchema(longStringField, longField));

        // Create a Union Field that indicates that the type could be either 'complex 1' or 'complex 2'
        final UnionRecordField unionRecordField = new UnionRecordField("union", Repetition.ZERO_OR_MORE, Arrays.asList(new RecordField[] {complexField1, complexField2}));

        // Create a Record Schema
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
        fields.add(new ComplexRecordField("complex present", Repetition.EXACTLY_ONE,
            new SimpleRecordField("color", FieldType.STRING, Repetition.ZERO_OR_ONE),
            new SimpleRecordField("fruit", FieldType.STRING, Repetition.ZERO_OR_ONE)));
        fields.add(new MapRecordField("map present",
            new SimpleRecordField("key", FieldType.STRING, Repetition.EXACTLY_ONE),
            new SimpleRecordField("value", FieldType.INT, Repetition.EXACTLY_ONE), Repetition.ZERO_OR_ONE));
        fields.add(unionRecordField);

        final RecordSchema schema = new RecordSchema(fields);

        // Create a 'complex' record that contains two different elements.
        final RecordField colorField = createField("color", FieldType.STRING);
        final RecordField fruitField = createField("fruit", FieldType.STRING);
        final Map<RecordField, Object> complexFieldMap = new LinkedHashMap<>();
        complexFieldMap.put(colorField, "red");
        complexFieldMap.put(fruitField, "apple");

        // Create a simple map that can be used for a Map Field
        final Map<String, Integer> simpleMap = new HashMap<>();
        simpleMap.put("apples", 100);

        // Create a Map of record fields to values, so that we can create a Record to write out
        final Map<RecordField, Object> values = new LinkedHashMap<>();
        values.put(createField("int", FieldType.INT), 42);
        values.put(createField("int present", FieldType.INT), 42);
        values.put(createField("boolean present", FieldType.BOOLEAN), true);
        values.put(createField("byte array present", FieldType.BYTE_ARRAY), "Hello".getBytes());
        values.put(createField("long present", FieldType.LONG), 42L);
        values.put(createField("string present", FieldType.STRING), "Hello");
        values.put(createField("long string present", FieldType.LONG_STRING), "Long Hello");
        values.put(createField("complex present", FieldType.COMPLEX), new FieldMapRecord(complexFieldMap, new RecordSchema(colorField, fruitField)));
        values.put(new MapRecordField("map present", createField("key", FieldType.STRING), createField("value", FieldType.INT), Repetition.EXACTLY_ONE), simpleMap);
        values.put(unionRecordField, Arrays.asList(new NamedValue[] {
            new NamedValue("complex1", complexRecord1),
            new NamedValue("complex2", complexRecord2)}));

        final FieldMapRecord originalRecord = new FieldMapRecord(values, schema);

        // Write out a record and read it back in.
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            // Write the schema to the stream
            schema.writeTo(baos);

            // Write the record twice, to make sure that we're able to read/write multiple sequential records
            final SchemaRecordWriter writer = new SchemaRecordWriter();
            writer.writeRecord(originalRecord, baos);
            writer.writeRecord(originalRecord, baos);

            try (final InputStream in = new ByteArrayInputStream(baos.toByteArray())) {
                // Read the Schema from the stream and create a Record Reader for reading records, based on this schema
                final RecordSchema readSchema = RecordSchema.readFrom(in);
                final SchemaRecordReader reader = SchemaRecordReader.fromSchema(readSchema);

                // Read two records and verify the values.
                for (int i=0; i < 2; i++) {
                    final Record record = reader.readRecord(in);

                    assertNotNull(record);
                    assertEquals(42, record.getFieldValue("int"));
                    assertEquals(42, record.getFieldValue("int present"));
                    assertEquals(true, record.getFieldValue("boolean present"));
                    assertTrue(Arrays.equals("Hello".getBytes(), (byte[]) record.getFieldValue("byte array present")));
                    assertEquals(42L, record.getFieldValue("long present"));
                    assertEquals("Hello", record.getFieldValue("string present"));
                    assertEquals("Long Hello", record.getFieldValue("long string present"));

                    final Record complexRecord = (Record) record.getFieldValue("complex present");
                    assertEquals("red", complexRecord.getFieldValue("color"));
                    assertEquals("apple", complexRecord.getFieldValue("fruit"));

                    assertEquals(simpleMap, record.getFieldValue("map present"));

                    final List<Record> unionRecords = (List<Record>) record.getFieldValue("union");
                    assertNotNull(unionRecords);
                    assertEquals(2, unionRecords.size());

                    final Record unionRecord1 = unionRecords.get(0);
                    assertEquals("apples", unionRecord1.getFieldValue("string field"));
                    assertEquals(100, unionRecord1.getFieldValue("int field"));

                    final Record unionRecord2 = unionRecords.get(1);
                    assertEquals("oranges", unionRecord2.getFieldValue("long string field"));
                    assertEquals(Long.MAX_VALUE, unionRecord2.getFieldValue("long field"));
                }

                // Ensure that there is no more data.
                assertNull(reader.readRecord(in));
            }
        }
    }

    private SimpleRecordField createField(final String fieldName, final FieldType type) {
        return new SimpleRecordField(fieldName, type, Repetition.ZERO_OR_ONE);
    }
}
