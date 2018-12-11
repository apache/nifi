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

package org.apache.nifi.serialization.record;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestDataTypeUtils {
    /**
     * This is a unit test to verify conversion java Date objects to Timestamps. Support for this was
     * required in order to help the MongoDB packages handle date/time logical types in the Record API.
     */
    @Test
    public void testDateToTimestamp() {
        java.util.Date date = new java.util.Date();
        Timestamp ts = DataTypeUtils.toTimestamp(date, null, null);

        assertNotNull(ts);
        assertEquals("Times didn't match", ts.getTime(), date.getTime());

        java.sql.Date sDate = new java.sql.Date(date.getTime());
        ts = DataTypeUtils.toTimestamp(date, null, null);
        assertNotNull(ts);
        assertEquals("Times didn't match", ts.getTime(), sDate.getTime());
    }

    /*
     * This was a bug in NiFi 1.8 where converting from a Timestamp to a Date with the record path API
     * would throw an exception.
     */
    @Test
    public void testTimestampToDate() {
        java.util.Date date = new java.util.Date();
        Timestamp ts = DataTypeUtils.toTimestamp(date, null, null);
        assertNotNull(ts);

        java.sql.Date output = DataTypeUtils.toDate(ts, null, null);
        assertNotNull(output);
        assertEquals("Timestamps didn't match", output.getTime(), ts.getTime());
    }

    @Test
    public void testConvertRecordMapToJavaMap() {
        assertNull(DataTypeUtils.convertRecordMapToJavaMap(null, null));
        assertNull(DataTypeUtils.convertRecordMapToJavaMap(null, RecordFieldType.MAP.getDataType()));
        Map<String,Object> resultMap = DataTypeUtils.convertRecordMapToJavaMap(new HashMap<>(), RecordFieldType.MAP.getDataType());
        assertNotNull(resultMap);
        assertTrue(resultMap.isEmpty());

        int[] intArray = {3,2,1};

        Map<String,Object> inputMap = new HashMap<String,Object>() {{
            put("field1", "hello");
            put("field2", 1);
            put("field3", intArray);
        }};

        resultMap = DataTypeUtils.convertRecordMapToJavaMap(inputMap, RecordFieldType.STRING.getDataType());
        assertNotNull(resultMap);
        assertFalse(resultMap.isEmpty());
        assertEquals("hello", resultMap.get("field1"));
        assertEquals(1, resultMap.get("field2"));
        assertTrue(resultMap.get("field3") instanceof int[]);
        assertNull(resultMap.get("field4"));

    }

    @Test
    public void testConvertRecordArrayToJavaArray() {
        assertNull(DataTypeUtils.convertRecordArrayToJavaArray(null, null));
        assertNull(DataTypeUtils.convertRecordArrayToJavaArray(null, RecordFieldType.STRING.getDataType()));
        String[] stringArray = {"Hello", "World!"};
        Object[] resultArray = DataTypeUtils.convertRecordArrayToJavaArray(stringArray, RecordFieldType.STRING.getDataType());
        assertNotNull(resultArray);
        for(Object o : resultArray) {
            assertTrue(o instanceof String);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testConvertRecordFieldToObject() {
        assertNull(DataTypeUtils.convertRecordFieldtoObject(null, null));
        assertNull(DataTypeUtils.convertRecordFieldtoObject(null, RecordFieldType.MAP.getDataType()));

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("defaultOfHello", RecordFieldType.STRING.getDataType(), "hello"));
        fields.add(new RecordField("noDefault", RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.STRING.getDataType())));
        fields.add(new RecordField("intField", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("intArray", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())));

        // Map of Records with Arrays
        List<RecordField> nestedRecordFields = new ArrayList<>();
        nestedRecordFields.add(new RecordField("a", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())));
        nestedRecordFields.add(new RecordField("b", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));
        RecordSchema nestedRecordSchema = new SimpleRecordSchema(nestedRecordFields);

        fields.add(new RecordField("complex", RecordFieldType.MAP.getMapDataType(RecordFieldType.RECORD.getRecordDataType(nestedRecordSchema))));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Map<String, Object> values = new HashMap<>();
        values.put("noDefault", "world");
        values.put("intField", 5);
        values.put("intArray", new Integer[] {3,2,1});
        final Map<String, Object> complexValues = new HashMap<>();

        final Map<String, Object> complexValueRecord1 = new HashMap<>();
        complexValueRecord1.put("a",new Integer[] {3,2,1});
        complexValueRecord1.put("b",new Integer[] {5,4,3});

        final Map<String, Object> complexValueRecord2 = new HashMap<>();
        complexValueRecord2.put("a",new String[] {"hello","world!"});
        complexValueRecord2.put("b",new String[] {"5","4","3"});

        complexValues.put("complex1", DataTypeUtils.toRecord(complexValueRecord1, nestedRecordSchema, "complex1", StandardCharsets.UTF_8));
        complexValues.put("complex2", DataTypeUtils.toRecord(complexValueRecord2, nestedRecordSchema, "complex2", StandardCharsets.UTF_8));

        values.put("complex", complexValues);
        final Record inputRecord = new MapRecord(schema, values);

        Object o = DataTypeUtils.convertRecordFieldtoObject(inputRecord, RecordFieldType.RECORD.getRecordDataType(schema));
        assertTrue(o instanceof Map);
        Map<String,Object> outputMap = (Map<String,Object>) o;
        assertEquals("hello", outputMap.get("defaultOfHello"));
        assertEquals("world", outputMap.get("noDefault"));
        o = outputMap.get("intField");
        assertEquals(5,o);
        o = outputMap.get("intArray");
        assertTrue(o instanceof Integer[]);
        Integer[] intArray = (Integer[])o;
        assertEquals(3, intArray.length);
        assertEquals((Integer)3, intArray[0]);
        o = outputMap.get("complex");
        assertTrue(o instanceof Map);
        Map<String,Object> nestedOutputMap = (Map<String,Object>)o;
        o = nestedOutputMap.get("complex1");
        assertTrue(o instanceof Map);
        Map<String,Object> complex1 = (Map<String,Object>)o;
        o = complex1.get("a");
        assertTrue(o instanceof Integer[]);
        assertEquals((Integer)2, ((Integer[])o)[1]);
        o = complex1.get("b");
        assertTrue(o instanceof Integer[]);
        assertEquals((Integer)3, ((Integer[])o)[2]);
        o = nestedOutputMap.get("complex2");
        assertTrue(o instanceof Map);
        Map<String,Object> complex2 = (Map<String,Object>)o;
        o = complex2.get("a");
        assertTrue(o instanceof String[]);
        assertEquals("hello", ((String[])o)[0]);
        o = complex2.get("b");
        assertTrue(o instanceof String[]);
        assertEquals("4", ((String[])o)[1]);

    }

    @Test
    public void testToArray() {
        final List<String> list = Arrays.asList("Seven", "Eleven", "Thirteen");

        final Object[] array = DataTypeUtils.toArray(list, "list", null);

        assertEquals(list.size(), array.length);
        for (int i = 0; i < list.size(); i++) {
            assertEquals(list.get(i), array[i]);
        }
    }

    @Test
    public void testStringToBytes() {
        Object bytes = DataTypeUtils.convertType("Hello", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()),null, StandardCharsets.UTF_8);
        assertTrue(bytes instanceof Byte[]);
        assertNotNull(bytes);
        Byte[] b = (Byte[]) bytes;
        assertEquals("Conversion from String to byte[] failed", (long) 72, (long) b[0] );  // H
        assertEquals("Conversion from String to byte[] failed", (long) 101, (long) b[1] ); // e
        assertEquals("Conversion from String to byte[] failed", (long) 108, (long) b[2] ); // l
        assertEquals("Conversion from String to byte[] failed", (long) 108, (long) b[3] ); // l
        assertEquals("Conversion from String to byte[] failed", (long) 111, (long) b[4] ); // o
    }

    @Test
    public void testBytesToString() {
        Object s = DataTypeUtils.convertType("Hello".getBytes(StandardCharsets.UTF_16), RecordFieldType.STRING.getDataType(),null, StandardCharsets.UTF_16);
        assertNotNull(s);
        assertTrue(s instanceof String);
        assertEquals("Conversion from byte[] to String failed", "Hello", s);
    }

    @Test
    public void testBytesToBytes() {
        Object b = DataTypeUtils.convertType("Hello".getBytes(StandardCharsets.UTF_16), RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()),null, StandardCharsets.UTF_16);
        assertNotNull(b);
        assertTrue(b instanceof Byte[]);
        assertEquals("Conversion from byte[] to String failed at char 0", (Object) "Hello".getBytes(StandardCharsets.UTF_16)[0], ((Byte[]) b)[0]);
    }

    @Test
    public void testFloatingPointCompatibility() {
        final String[] prefixes = new String[] {"", "-", "+"};
        final String[] exponents = new String[] {"e0", "e1", "e-1", "E0", "E1", "E-1"};
        final String[] decimals = new String[] {"", ".0", ".1", "."};

        for (final String prefix : prefixes) {
            for (final String decimal : decimals) {
                for (final String exp : exponents) {
                    String toTest = prefix + "100" + decimal + exp;
                    assertTrue(toTest + " not valid float", DataTypeUtils.isFloatTypeCompatible(toTest));
                    assertTrue(toTest + " not valid double", DataTypeUtils.isDoubleTypeCompatible(toTest));

                    Double.parseDouble(toTest); // ensure we can actually parse it
                    Float.parseFloat(toTest);

                    if (decimal.length() > 1) {
                        toTest = prefix + decimal + exp;
                        assertTrue(toTest + " not valid float", DataTypeUtils.isFloatTypeCompatible(toTest));
                        assertTrue(toTest + " not valid double", DataTypeUtils.isDoubleTypeCompatible(toTest));
                        Double.parseDouble(toTest); // ensure we can actually parse it
                        Float.parseFloat(toTest);
                    }
                }
            }
        }
    }
}
