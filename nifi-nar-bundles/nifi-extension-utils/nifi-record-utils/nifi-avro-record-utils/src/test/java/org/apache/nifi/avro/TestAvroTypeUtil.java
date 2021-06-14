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

package org.apache.nifi.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestAvroTypeUtil {

    @Test
    @Ignore("Performance test meant for manually testing only before/after changes in order to measure performance difference caused by changes.")
    public void testCreateAvroRecordPerformance() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        for (int i=0; i < 100; i++) {
            fields.add(new RecordField("field" + i, RecordFieldType.STRING.getDataType(), true));
        }

        final RecordSchema recordSchema = new SimpleRecordSchema(fields);
        final Schema avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);

        final Map<String, Object> values = new HashMap<>();
        for (int i=0; i < 100; i++) {
            // Leave half of the values null
            if (i % 2 == 0) {
                values.put("field" + i, String.valueOf(i));
            }
        }

        final MapRecord record = new MapRecord(recordSchema, values);

        final int iterations = 1_000_000;

        for (int j=0; j < 1_000; j++) {
            final long start = System.currentTimeMillis();

            for (int i = 0; i < iterations; i++) {
                AvroTypeUtil.createAvroRecord(record, avroSchema);
            }

            final long millis = System.currentTimeMillis() - start;
            System.out.println(millis);
        }
    }

    @Test
    public void testCreateAvroSchemaPrimitiveTypes() throws SchemaNotFoundException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("int", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("long", RecordFieldType.LONG.getDataType()));
        fields.add(new RecordField("string", RecordFieldType.STRING.getDataType(), "hola",
                Collections.singleton("greeting")));
        fields.add(new RecordField("byte", RecordFieldType.BYTE.getDataType()));
        fields.add(new RecordField("char", RecordFieldType.CHAR.getDataType()));
        fields.add(new RecordField("short", RecordFieldType.SHORT.getDataType()));
        fields.add(new RecordField("decimal", RecordFieldType.DECIMAL.getDecimalDataType(30, 10)));
        fields.add(new RecordField("double", RecordFieldType.DOUBLE.getDataType()));
        fields.add(new RecordField("float", RecordFieldType.FLOAT.getDataType()));
        fields.add(new RecordField("time", RecordFieldType.TIME.getDataType()));
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));
        fields.add(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()));

        final DataType arrayType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());
        fields.add(new RecordField("strings", arrayType));

        final DataType mapType = RecordFieldType.MAP.getMapDataType(RecordFieldType.LONG.getDataType());
        fields.add(new RecordField("map", mapType));

        final List<RecordField> personFields = new ArrayList<>();
        personFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        personFields.add(new RecordField("dob", RecordFieldType.DATE.getDataType()));
        final RecordSchema personSchema = new SimpleRecordSchema(personFields);
        final DataType personType = RecordFieldType.RECORD.getRecordDataType(personSchema);
        fields.add(new RecordField("person", personType));

        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final Schema avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);

        // everything should be a union, since it's nullable.
        for (final Field field : avroSchema.getFields()) {
            final Schema fieldSchema = field.schema();
            assertEquals(Type.UNION, fieldSchema.getType());
            assertTrue("Field " + field.name() + " does not contain NULL type",
                    fieldSchema.getTypes().contains(Schema.create(Type.NULL)));
        }

        final RecordSchema afterConversion = AvroTypeUtil.createSchema(avroSchema);

        assertEquals(RecordFieldType.INT.getDataType(), afterConversion.getDataType("int").get());
        assertEquals(RecordFieldType.LONG.getDataType(), afterConversion.getDataType("long").get());
        assertEquals(RecordFieldType.STRING.getDataType(), afterConversion.getDataType("string").get());
        assertEquals(RecordFieldType.INT.getDataType(), afterConversion.getDataType("byte").get());
        assertEquals(RecordFieldType.STRING.getDataType(), afterConversion.getDataType("char").get());
        assertEquals(RecordFieldType.INT.getDataType(), afterConversion.getDataType("short").get());
        assertEquals(RecordFieldType.DECIMAL.getDecimalDataType(30, 10), afterConversion.getDataType("decimal").get());
        assertEquals(RecordFieldType.DOUBLE.getDataType(), afterConversion.getDataType("double").get());
        assertEquals(RecordFieldType.FLOAT.getDataType(), afterConversion.getDataType("float").get());
        assertEquals(RecordFieldType.TIME.getDataType(), afterConversion.getDataType("time").get());
        assertEquals(RecordFieldType.DATE.getDataType(), afterConversion.getDataType("date").get());
        assertEquals(RecordFieldType.TIMESTAMP.getDataType(), afterConversion.getDataType("timestamp").get());
        assertEquals(arrayType, afterConversion.getDataType("strings").get());
        assertEquals(mapType, afterConversion.getDataType("map").get());
        assertEquals(personType, afterConversion.getDataType("person").get());

        final RecordField stringField = afterConversion.getField("string").get();
        assertEquals("hola", stringField.getDefaultValue());
        assertEquals(Collections.singleton("greeting"), stringField.getAliases());
    }

    /**
     * The issue consists on having an Avro's schema with a default value in an
     * array. See
     * <a href="https://issues.apache.org/jira/browse/NIFI-4893">NIFI-4893</a>.
     * @throws IOException
     *             schema not found.
     */
    @Test
    public void testDefaultArrayValue1() throws IOException {
        Schema avroSchema = new Schema.Parser().parse(getClass().getResourceAsStream("defaultArrayValue1.json"));
        GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
        Record r = builder.build();
        @SuppressWarnings("unchecked")
        GenericData.Array<Integer> values = (GenericData.Array<Integer>) r.get("listOfInt");
        assertEquals(values.size(), 0);
        RecordSchema record = AvroTypeUtil.createSchema(avroSchema);
        RecordField field = record.getField("listOfInt").get();
        assertEquals(RecordFieldType.ARRAY, field.getDataType().getFieldType());
        assertTrue(field.getDefaultValue() instanceof Object[]);
        assertEquals(0, ((Object[]) field.getDefaultValue()).length);
    }

    /**
     * The issue consists on having an Avro's schema with a default value in an
     * array. See
     * <a href="https://issues.apache.org/jira/browse/NIFI-4893">NIFI-4893</a>.
     * @throws IOException
     *             schema not found.
     */
    @Test
    public void testDefaultArrayValue2() throws IOException {
        Schema avroSchema = new Schema.Parser().parse(getClass().getResourceAsStream("defaultArrayValue2.json"));
        GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
        Record r = builder.build();
        @SuppressWarnings("unchecked")
        GenericData.Array<Integer> values = (GenericData.Array<Integer>) r.get("listOfInt");
        assertArrayEquals(new Object[] { 1, 2 }, values.toArray());
        RecordSchema record = AvroTypeUtil.createSchema(avroSchema);
        RecordField field = record.getField("listOfInt").get();
        assertEquals(RecordFieldType.ARRAY, field.getDataType().getFieldType());
        assertTrue(field.getDefaultValue() instanceof Object[]);
        assertArrayEquals(new Object[] { 1, 2 }, ((Object[]) field.getDefaultValue()));
    }

    /**
     * The issue consists on having an Avro's schema with a default value in an
     * array. See
     * <a href="https://issues.apache.org/jira/browse/NIFI-4893">NIFI-4893</a>.
     * @throws IOException
     *             schema not found.
     */
    @Test
    public void testDefaultArrayValuesInRecordsCase1() throws IOException {
        Schema avroSchema = new Schema.Parser().parse(getClass().getResourceAsStream("defaultArrayInRecords1.json"));
        GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
        Record field1Record = new GenericRecordBuilder(avroSchema.getField("field1").schema()).build();
        builder.set("field1", field1Record);
        Record r = builder.build();

        @SuppressWarnings("unchecked")
        GenericData.Array<Integer> values = (GenericData.Array<Integer>) ((GenericRecord) r.get("field1"))
                .get("listOfInt");
        assertArrayEquals(new Object[] {}, values.toArray());
        RecordSchema record = AvroTypeUtil.createSchema(avroSchema);
        RecordField field = record.getField("field1").get();
        assertEquals(RecordFieldType.RECORD, field.getDataType().getFieldType());
        RecordDataType data = (RecordDataType) field.getDataType();
        RecordSchema childSchema = data.getChildSchema();
        RecordField childField = childSchema.getField("listOfInt").get();
        assertEquals(RecordFieldType.ARRAY, childField.getDataType().getFieldType());
        assertTrue(childField.getDefaultValue() instanceof Object[]);
        assertArrayEquals(new Object[] {}, ((Object[]) childField.getDefaultValue()));
    }

    /**
    * The issue consists on having an Avro's schema with a default value in an
    * array. See
    * <a href="https://issues.apache.org/jira/browse/NIFI-4893">NIFI-4893</a>.
    * @throws IOException
    *             schema not found.
    */
   @Test
   public void testDefaultArrayValuesInRecordsCase2() throws IOException {
       Schema avroSchema = new Schema.Parser().parse(getClass().getResourceAsStream("defaultArrayInRecords2.json"));
       GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
       Record field1Record = new GenericRecordBuilder(avroSchema.getField("field1").schema()).build();
       builder.set("field1", field1Record);
       Record r = builder.build();

       @SuppressWarnings("unchecked")
       GenericData.Array<Integer> values = (GenericData.Array<Integer>) ((GenericRecord) r.get("field1"))
               .get("listOfInt");
       assertArrayEquals(new Object[] {1,2,3}, values.toArray());
       RecordSchema record = AvroTypeUtil.createSchema(avroSchema);
       RecordField field = record.getField("field1").get();
       assertEquals(RecordFieldType.RECORD, field.getDataType().getFieldType());
       RecordDataType data = (RecordDataType) field.getDataType();
       RecordSchema childSchema = data.getChildSchema();
       RecordField childField = childSchema.getField("listOfInt").get();
       assertEquals(RecordFieldType.ARRAY, childField.getDataType().getFieldType());
       assertTrue(childField.getDefaultValue() instanceof Object[]);
       assertArrayEquals(new Object[] {1,2,3}, ((Object[]) childField.getDefaultValue()));
   }
    @Test
    // Simple recursion is a record A composing itself (similar to a LinkedList Node
    // referencing 'next')
    public void testSimpleRecursiveSchema() {
        Schema recursiveSchema = new Schema.Parser().parse("{\n" + "  \"namespace\": \"org.apache.nifi.testing\",\n"
                + "  \"name\": \"NodeRecord\",\n" + "  \"type\": \"record\",\n" + "  \"fields\": [\n" + "    {\n"
                + "      \"name\": \"id\",\n" + "      \"type\": \"int\"\n" + "    },\n" + "    {\n"
                + "      \"name\": \"value\",\n" + "      \"type\": \"string\"\n" + "    },\n" + "    {\n"
                + "      \"name\": \"parent\",\n" + "      \"type\": [\n" + "        \"null\",\n"
                + "        \"NodeRecord\"\n" + "      ]\n" + "    }\n" + "  ]\n" + "}\n");

        // Make sure the following doesn't throw an exception
        RecordSchema result = AvroTypeUtil.createSchema(recursiveSchema);

        // Make sure it parsed correctly
        Assert.assertEquals(3, result.getFieldCount());

        Optional<RecordField> idField = result.getField("id");
        Assert.assertTrue(idField.isPresent());
        Assert.assertEquals(RecordFieldType.INT, idField.get().getDataType().getFieldType());

        Optional<RecordField> valueField = result.getField("value");
        Assert.assertTrue(valueField.isPresent());
        Assert.assertEquals(RecordFieldType.STRING, valueField.get().getDataType().getFieldType());

        Optional<RecordField> parentField = result.getField("parent");
        Assert.assertTrue(parentField.isPresent());
        Assert.assertEquals(RecordFieldType.RECORD, parentField.get().getDataType().getFieldType());

        // The 'parent' field should have a circular schema reference to the top level
        // record schema, similar to how Avro handles this
        Assert.assertEquals(result, ((RecordDataType) parentField.get().getDataType()).getChildSchema());
    }

    @Test
    // Complicated recursion is a record A composing record B, who composes a record
    // A
    public void testComplicatedRecursiveSchema() {
        Schema recursiveSchema = new Schema.Parser().parse("{\n" + "  \"namespace\": \"org.apache.nifi.testing\",\n"
                + "  \"name\": \"Record_A\",\n" + "  \"type\": \"record\",\n" + "  \"fields\": [\n" + "    {\n"
                + "      \"name\": \"id\",\n" + "      \"type\": \"int\"\n" + "    },\n" + "    {\n"
                + "      \"name\": \"value\",\n" + "      \"type\": \"string\"\n" + "    },\n" + "    {\n"
                + "      \"name\": \"child\",\n" + "      \"type\": {\n"
                + "        \"namespace\": \"org.apache.nifi.testing\",\n" + "        \"name\": \"Record_B\",\n"
                + "        \"type\": \"record\",\n" + "        \"fields\": [\n" + "          {\n"
                + "            \"name\": \"id\",\n" + "            \"type\": \"int\"\n" + "          },\n"
                + "          {\n" + "            \"name\": \"value\",\n" + "            \"type\": \"string\"\n"
                + "          },\n" + "          {\n" + "            \"name\": \"parent\",\n"
                + "            \"type\": [\n" + "              \"null\",\n" + "              \"Record_A\"\n"
                + "            ]\n" + "          }\n" + "        ]\n" + "      }\n" + "    }\n" + "  ]\n" + "}\n");

        // Make sure the following doesn't throw an exception
        RecordSchema recordASchema = AvroTypeUtil.createSchema(recursiveSchema);

        // Make sure it parsed correctly
        Assert.assertEquals(3, recordASchema.getFieldCount());

        Optional<RecordField> recordAIdField = recordASchema.getField("id");
        Assert.assertTrue(recordAIdField.isPresent());
        Assert.assertEquals(RecordFieldType.INT, recordAIdField.get().getDataType().getFieldType());

        Optional<RecordField> recordAValueField = recordASchema.getField("value");
        Assert.assertTrue(recordAValueField.isPresent());
        Assert.assertEquals(RecordFieldType.STRING, recordAValueField.get().getDataType().getFieldType());

        Optional<RecordField> recordAChildField = recordASchema.getField("child");
        Assert.assertTrue(recordAChildField.isPresent());
        Assert.assertEquals(RecordFieldType.RECORD, recordAChildField.get().getDataType().getFieldType());

        // Get the child schema
        RecordSchema recordBSchema = ((RecordDataType) recordAChildField.get().getDataType()).getChildSchema();

        // Make sure it parsed correctly
        Assert.assertEquals(3, recordBSchema.getFieldCount());

        Optional<RecordField> recordBIdField = recordBSchema.getField("id");
        Assert.assertTrue(recordBIdField.isPresent());
        Assert.assertEquals(RecordFieldType.INT, recordBIdField.get().getDataType().getFieldType());

        Optional<RecordField> recordBValueField = recordBSchema.getField("value");
        Assert.assertTrue(recordBValueField.isPresent());
        Assert.assertEquals(RecordFieldType.STRING, recordBValueField.get().getDataType().getFieldType());

        Optional<RecordField> recordBParentField = recordBSchema.getField("parent");
        Assert.assertTrue(recordBParentField.isPresent());
        Assert.assertEquals(RecordFieldType.RECORD, recordBParentField.get().getDataType().getFieldType());

        // Make sure the 'parent' field has a schema reference back to the original top
        // level record schema
        Assert.assertEquals(recordASchema, ((RecordDataType) recordBParentField.get().getDataType()).getChildSchema());
    }

    @Test
    public void testMapWithNullSchema() throws IOException {

        Schema recursiveSchema = new Schema.Parser().parse(getClass().getResourceAsStream("schema.json"));

        // Make sure the following doesn't throw an exception
        RecordSchema recordASchema = AvroTypeUtil.createSchema(recursiveSchema.getTypes().get(0));

        // check the fix with the proper file
        try (DataFileStream<GenericRecord> r = new DataFileStream<>(getClass().getResourceAsStream("data.avro"),
                new GenericDatumReader<>())) {
            GenericRecord n = r.next();
            AvroTypeUtil.convertAvroRecordToMap(n, recordASchema, StandardCharsets.UTF_8);
        }
    }

    @Test
    public void testToDecimalConversion() {
        final LogicalTypes.Decimal decimalType = LogicalTypes.decimal(18, 8);
        final Schema fieldSchema = Schema.create(Type.BYTES);
        decimalType.addToSchema(fieldSchema);

        final Map<Object, String> expects = new HashMap<>();

        // Double to Decimal
        expects.put(123d, "123.00000000");
        // Double can not represent exact 1234567890.12345678, so use 1 less digit to
        // test here.
        expects.put(1234567890.12345678d, "1234567890.12345670");
        expects.put(123456789.12345678d, "123456789.12345678");
        expects.put(1234567890123456d, "1234567890123456.00000000");
        // ROUND HALF UP.
        expects.put(0.1234567890123456d, "0.12345679");

        // BigDecimal to BigDecimal
        expects.put(new BigDecimal("123"), "123.00000000");
        expects.put(new BigDecimal("1234567890.12345678"), "1234567890.12345678");
        expects.put(new BigDecimal("123456789012345678"), "123456789012345678.00000000");
        // ROUND HALF UP.
        expects.put(new BigDecimal("0.123456789012345678"), "0.12345679");

        // String to BigDecimal
        expects.put("123", "123.00000000");
        expects.put("1234567890.12345678", "1234567890.12345678");
        expects.put("123456789012345678", "123456789012345678.00000000");
        expects.put("0.1234567890123456", "0.12345679");
        expects.put("Not a number", "java.lang.NumberFormatException");

        // Integer to BigDecimal
        expects.put(123, "123.00000000");
        expects.put(-1234567, "-1234567.00000000");

        // Long to BigDecimal
        expects.put(123L, "123.00000000");
        expects.put(123456789012345678L, "123456789012345678.00000000");

        expects.forEach((rawValue, expect) -> {
            final Object convertedValue;
            try {
                convertedValue = AvroTypeUtil.convertToAvroObject(rawValue, fieldSchema, StandardCharsets.UTF_8);
            } catch (Exception e) {
                if (expect.equals(e.getClass().getCanonicalName())) {
                    // Expected behavior.
                    return;
                }
                fail(String.format("Unexpected exception, %s with %s %s while expecting %s", e,
                        rawValue.getClass().getSimpleName(), rawValue, expect));
                return;
            }

            assertTrue(convertedValue instanceof ByteBuffer);
            final ByteBuffer serializedBytes = (ByteBuffer) convertedValue;

            final BigDecimal bigDecimal = new Conversions.DecimalConversion().fromBytes(serializedBytes, fieldSchema,
                    decimalType);
            assertEquals(String.format("%s %s should be converted to %s", rawValue.getClass().getSimpleName(), rawValue,
                    expect), expect, bigDecimal.toString());
        });

    }

    @Test
    public void testConvertAvroRecordToMapWithFieldTypeOfFixedAndLogicalTypeDecimal() {
       // Create a field schema like {"type":"fixed","name":"amount","size":16,"logicalType":"decimal","precision":18,"scale":8}
       final LogicalTypes.Decimal decimalType = LogicalTypes.decimal(18, 8);
        final Schema fieldSchema = Schema.createFixed("amount", null, null, 16);
        decimalType.addToSchema(fieldSchema);

        // Create a field named "amount" using the field schema above
        final Schema.Field field = new Schema.Field("amount", fieldSchema, null, (Object)null);

        // Create an overall record schema with the amount field
        final Schema avroSchema = Schema.createRecord(Collections.singletonList(field));

        // Create an example Avro record with the amount field of type fixed and a logical type of decimal
        final BigDecimal expectedBigDecimal = new BigDecimal("1234567890.12345678");
        final GenericRecord genericRecord = new GenericData.Record(avroSchema);
        genericRecord.put("amount", new Conversions.DecimalConversion().toFixed(expectedBigDecimal, fieldSchema, decimalType));

        // Convert the Avro schema to a Record schema
        thenConvertAvroSchemaToRecordSchema(avroSchema, expectedBigDecimal, genericRecord);
    }

    @Test
    public void testConvertAvroRecordToMapWithFieldTypeOfBinaryAndLogicalTypeDecimal() {
        // Create a field schema like {"type":"binary","name":"amount","logicalType":"decimal","precision":18,"scale":8}
        final LogicalTypes.Decimal decimalType = LogicalTypes.decimal(18, 8);
        final Schema fieldSchema = Schema.create(Type.BYTES);
        decimalType.addToSchema(fieldSchema);

        // Create a field named "amount" using the field schema above
        final Schema.Field field = new Schema.Field("amount", fieldSchema, null, (Object)null);

        // Create an overall record schema with the amount field
        final Schema avroSchema = Schema.createRecord(Collections.singletonList(field));

        // Create an example Avro record with the amount field of type binary and a logical type of decimal
        final BigDecimal expectedBigDecimal = new BigDecimal("1234567890.12345678");
        final GenericRecord genericRecord = new GenericData.Record(avroSchema);
        genericRecord.put("amount", new Conversions.DecimalConversion().toBytes(expectedBigDecimal, fieldSchema, decimalType));

        // Convert the Avro schema to a Record schema
        thenConvertAvroSchemaToRecordSchema(avroSchema, expectedBigDecimal, genericRecord);
    }

    private void thenConvertAvroSchemaToRecordSchema(Schema avroSchema, BigDecimal expectedBigDecimal, GenericRecord genericRecord) {
        final RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema);

        // Convert the Avro record a Map and verify the object produced is the same BigDecimal that was converted to fixed
        final Map<String, Object> convertedMap = AvroTypeUtil.convertAvroRecordToMap(genericRecord, recordSchema, StandardCharsets.UTF_8);
        assertNotNull(convertedMap);
        assertEquals(1, convertedMap.size());

        final Object resultObject = convertedMap.get("amount");
        assertNotNull(resultObject);
        assertTrue(resultObject instanceof BigDecimal);

        final BigDecimal resultBigDecimal = (BigDecimal) resultObject;
        assertEquals(expectedBigDecimal, resultBigDecimal);
    }

    @Test
    public void testBytesDecimalConversion(){
        final LogicalTypes.Decimal decimalType = LogicalTypes.decimal(18, 8);
        final Schema fieldSchema = Schema.create(Type.BYTES);
        decimalType.addToSchema(fieldSchema);
        final Object convertedValue = AvroTypeUtil.convertToAvroObject("2.5", fieldSchema, StandardCharsets.UTF_8);
        assertTrue(convertedValue instanceof ByteBuffer);
        final ByteBuffer serializedBytes = (ByteBuffer)convertedValue;
        final BigDecimal bigDecimal = new Conversions.DecimalConversion().fromBytes(serializedBytes, fieldSchema, decimalType);
        assertEquals(new BigDecimal("2.5").setScale(8), bigDecimal);
    }

    @Test
    public void testDateConversion() {
        final Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        c.set(2019, Calendar.JANUARY, 1, 0, 0, 0);
        c.set(Calendar.MILLISECOND, 0);
        final long epochMillis = c.getTimeInMillis();

        final LogicalTypes.Date dateType = LogicalTypes.date();
        final Schema fieldSchema = Schema.create(Type.INT);
        dateType.addToSchema(fieldSchema);
        final Object convertedValue = AvroTypeUtil.convertToAvroObject(new Date(epochMillis), fieldSchema);
        assertTrue(convertedValue instanceof Integer);
        assertEquals(LocalDate.of(2019, 1, 1).toEpochDay(), (int) convertedValue);
    }

    @Test
    public void testFixedDecimalConversion(){
        final LogicalTypes.Decimal decimalType = LogicalTypes.decimal(18, 8);
        final Schema fieldSchema = Schema.createFixed("mydecimal", "no doc", "myspace", 18);
        decimalType.addToSchema(fieldSchema);
        final Object convertedValue = AvroTypeUtil.convertToAvroObject("2.5", fieldSchema, StandardCharsets.UTF_8);
        assertTrue(convertedValue instanceof GenericFixed);
        final GenericFixed genericFixed = (GenericFixed)convertedValue;
        final BigDecimal bigDecimal = new Conversions.DecimalConversion().fromFixed(genericFixed, fieldSchema, decimalType);
        assertEquals(new BigDecimal("2.5").setScale(8), bigDecimal);
    }

    @Test
    public void testSchemaNameNotEmpty() throws IOException {
        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("simpleSchema.json"));
        RecordSchema recordSchema = AvroTypeUtil.createSchema(schema);
        Assert.assertTrue(recordSchema.getIdentifier().getName().isPresent());
        Assert.assertEquals(Optional.of("record_name"), recordSchema.getIdentifier().getName());
    }

    @Test
    public void testStringToBytesConversion() {
        Object o = AvroTypeUtil.convertToAvroObject("Hello", Schema.create(Type.BYTES), StandardCharsets.UTF_16);
        assertTrue(o instanceof ByteBuffer);
        assertEquals("Hello", new String(((ByteBuffer) o).array(), StandardCharsets.UTF_16));
    }

    @Test
    public void testStringToNullableBytesConversion() {
        Object o = AvroTypeUtil.convertToAvroObject("Hello", Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.BYTES)), StandardCharsets.UTF_16);
        assertTrue(o instanceof ByteBuffer);
        assertEquals("Hello", new String(((ByteBuffer) o).array(), StandardCharsets.UTF_16));
    }

    @Test
    public void testBytesToStringConversion() {
        final Charset charset = Charset.forName("UTF_32LE");
        Object o = AvroTypeUtil.convertToAvroObject("Hello".getBytes(charset), Schema.create(Type.STRING), charset);
        assertTrue(o instanceof String);
        assertEquals("Hello", o);
    }

    @Test
    public void testAliasCreatedForInvalidField() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("valid", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("$invalid2", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("3invalid3", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("  __ Another ONE!!", RecordFieldType.STRING.getDataType()));

        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final Schema avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
        assertNotNull(avroSchema.getField("valid"));

        assertNull(avroSchema.getField("$invalid"));
        final Field field2 = avroSchema.getField("_invalid2");
        assertNotNull(field2);
        assertEquals("_invalid2", field2.name());
        assertEquals(1, field2.aliases().size());
        assertTrue(field2.aliases().contains("$invalid2"));

        assertNull(avroSchema.getField("$invalid3"));
        final Field field3 = avroSchema.getField("_invalid3");
        assertNotNull(field3);
        assertEquals("_invalid3", field3.name());
        assertEquals(1, field3.aliases().size());
        assertTrue(field3.aliases().contains("3invalid3"));

        assertNull(avroSchema.getField("  __ Another ONE!!"));
        final Field field4 = avroSchema.getField("_____Another_ONE__");
        assertNotNull(field4);
        assertEquals("_____Another_ONE__", field4.name());
        assertEquals(1, field4.aliases().size());
        assertTrue(field4.aliases().contains("  __ Another ONE!!"));
    }

    public void testListToArrayConversion() {
        final Charset charset = Charset.forName("UTF-8");
        Object o = AvroTypeUtil.convertToAvroObject(Collections.singletonList("Hello"), Schema.createArray(Schema.create(Type.STRING)), charset);
        assertTrue(o instanceof List);
        assertEquals(1, ((List) o).size());
        assertEquals("Hello", ((List) o).get(0));
    }

    @Test
    public void testMapToRecordConversion() {
        final Charset charset = Charset.forName("UTF-8");
        Object o = AvroTypeUtil.convertToAvroObject(Collections.singletonMap("Hello", "World"),
                Schema.createRecord(Collections.singletonList(new Field("Hello", Schema.create(Type.STRING), "", ""))), charset);
        assertTrue(o instanceof Record);
        assertEquals("World", ((Record) o).get("Hello"));
    }

    @Test
    public void testListAndMapConversion() {
        Schema s = Schema.createRecord(Arrays.asList(
            new Field("List", Schema.createArray(Schema.createRecord(
                Arrays.asList(
                    new Field("Message", Schema.create(Type.STRING), "", "")
                )
            )), "", null)
        ));

        Map<String, Object> obj = new HashMap<>();
        List<Map<String, Object>> list = new ArrayList<>();
        for (int x = 0; x < 10; x++) {
            list.add(new HashMap<String, Object>(){{
                put("Message", UUID.randomUUID().toString());
            }});
        }
        obj.put("List", list);

        Object o = AvroTypeUtil.convertToAvroObject(obj, s);
        assertTrue(o instanceof Record);
        List innerList = (List)((Record)o).get("List");
        assertNotNull( innerList );
        assertEquals(10, innerList.size());
        for (Object inner : innerList) {
            assertTrue(inner instanceof Record);
            assertNotNull(((Record)inner).get("Message"));
        }
    }

    @Test
    public void testConvertToAvroObjectWhenIntVSUnion_INT_FLOAT_ThenReturnInt() {
        // GIVEN
        List<Schema.Type> schemaTypes = Arrays.asList(
                Schema.Type.INT,
                Schema.Type.FLOAT
        );
        Integer rawValue = 1;

        Object expected = 1;

        // WHEN
        // THEN
        testConvertToAvroObjectAlsoReverseSchemaList(expected, rawValue, schemaTypes);
    }

    @Test
    public void testConvertToAvroObjectWhenFloatVSUnion_INT_FLOAT_ThenReturnFloat() {
        // GIVEN
        List<Schema.Type> schemaTypes = Arrays.asList(
                Schema.Type.INT,
                Schema.Type.FLOAT
        );
        Float rawValue = 1.5f;

        Object expected = 1.5f;

        // WHEN
        // THEN
        testConvertToAvroObjectAlsoReverseSchemaList(expected, rawValue, schemaTypes);
    }

    private void testConvertToAvroObjectAlsoReverseSchemaList(Object expected, Object rawValue, List<Schema.Type> schemaTypes) {
        // GIVEN
        List<Schema> schemaList = schemaTypes.stream()
                .map(Schema::create)
                .collect(Collectors.toList());

        // WHEN
        Object actual = AvroTypeUtil.convertToAvroObject(rawValue, Schema.createUnion(schemaList), StandardCharsets.UTF_16);

        // THEN
        assertEquals(expected, actual);

        // WHEN
        Collections.reverse(schemaList);
        Object actualAfterReverse = AvroTypeUtil.convertToAvroObject(rawValue, Schema.createUnion(schemaList), StandardCharsets.UTF_16);

        // THEN
        assertEquals(expected, actualAfterReverse);
    }

    @Test
    public void testConvertAvroMap() {
        // GIVEN
        Map<?, ?> expected = new HashMap<String, Object>() {{
            put(
                    "nullableMapField",
                    new HashMap<String, Object>() {{
                        put("key1", "value1");
                        put("key2", "value2");
                    }}
            );
        }};

        Schema nullableMapFieldAvroSchema = Schema.createUnion(
                Schema.create(Type.NULL),
                Schema.create(Type.INT),
                Schema.createMap(Schema.create(Type.STRING))
        );

        Schema avroRecordSchema = Schema.createRecord(
                "record", "doc", "namespace", false,
                Arrays.asList(
                        new Field("nullableMapField", nullableMapFieldAvroSchema, "nullable map field", (Object)null)
                )
        );

        Map<?, ?> value = new HashMap<Utf8, Object>(){{
            put(new Utf8("key1"), "value1");
            put(new Utf8("key2"), "value2");
        }};

        Record avroRecord = new GenericRecordBuilder(avroRecordSchema)
                .set("nullableMapField", value)
                .build();

        RecordSchema nifiRecordSchema = new SimpleRecordSchema(
                Arrays.asList(
                        new RecordField("nullableMapField", RecordFieldType.CHOICE.getChoiceDataType(
                                RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType())
                        ))
                )
        );

        // WHEN
        Object actual = AvroTypeUtil.convertAvroRecordToMap(avroRecord, nifiRecordSchema);

        // THEN
        assertEquals(expected, actual);
    }

    @Test
    public void testConvertNifiRecordIntoAvroRecord() throws IOException {
        // given
        final MapRecord nifiRecord = givenRecordContainingNumericMap();
        final Schema avroSchema = givenAvroSchemaContainingNumericMap();

        // when
        final GenericRecord result = AvroTypeUtil.createAvroRecord(nifiRecord, avroSchema);

        // then
        final HashMap<String, Object> numbers = (HashMap<String, Object>) result.get("numbers");
        Assert.assertTrue(Long.class.isInstance(numbers.get("number1")));
        Assert.assertTrue(Long.class.isInstance(numbers.get("number2")));
    }

    @Test
    public void testSchemaWithReoccurringFieldNameWithDifferentFieldNameInChildSchema() throws Exception {
        // GIVEN
        String reoccurringFieldName = "reoccurringFieldNameWithDifferentChildSchema";

        final List<RecordField> childRecord11Fields = Arrays.asList(
            new RecordField("childRecordField1", RecordFieldType.STRING.getDataType())
        );
        final List<RecordField> childRecord21Fields = Arrays.asList(
            new RecordField("childRecordField2", RecordFieldType.STRING.getDataType())
        );

        String expected = "{" +
            "\"type\":\"record\"," +
            "\"name\":\"nifiRecord\"," +
            "\"namespace\":\"org.apache.nifi\"," +
            "\"fields\":[{" +
                "\"name\":\"record1\"," +
                "\"type\":[\"null\",{" +
                    "\"type\":\"record\"," +
                    "\"name\":\"record1Type\"," +
                    "\"fields\":[{" +
                        "\"name\":\"reoccurringFieldNameWithDifferentChildSchema\"," +
                        "\"type\":[\"null\",{" +
                            "\"type\":\"record\"," +
                            "\"name\":\"record1_reoccurringFieldNameWithDifferentChildSchemaType\"," +
                            "\"fields\":[{" +
                                "\"name\":\"childRecordField1\"," +
                                "\"type\":[\"null\",\"string\"]" +
                            "}]" +
                        "}]" +
                    "}]" +
                "}]" +
            "}," +
            "{" +
                "\"name\":\"record2\"," +
                "\"type\":[\"null\",{" +
                    "\"type\":\"record\"," +
                    "\"name\":\"record2Type\"," +
                    "\"fields\":[{" +
                        "\"name\":\"reoccurringFieldNameWithDifferentChildSchema\"," +
                        "\"type\":[\"null\",{" +
                            "\"type\":\"record\"," +
                            "\"name\":\"record2_reoccurringFieldNameWithDifferentChildSchemaType\"," +
                            "\"fields\":[{" +
                                "\"name\":\"childRecordField2\"," +
                                "\"type\":[\"null\",\"string\"]" +
                            "}]" +
                        "}]" +
                    "}]" +
                "}]" +
            "}]" +
        "}";

        // WHEN
        // THEN
        testSchemaWithReoccurringFieldName(reoccurringFieldName, childRecord11Fields, childRecord21Fields, expected);
    }

    @Test
    public void testSchemaWithReoccurringFieldNameWithDifferentTypeInChildSchema() throws Exception {
        // GIVEN
        String reoccurringFieldName = "reoccurringFieldNameWithDifferentChildSchema";
        String reoccurringFieldNameInChildSchema = "childRecordField";

        final List<RecordField> childRecord11Fields = Arrays.asList(
            new RecordField(reoccurringFieldNameInChildSchema, RecordFieldType.STRING.getDataType())
        );
        final List<RecordField> childRecord21Fields = Arrays.asList(
            new RecordField(reoccurringFieldNameInChildSchema, RecordFieldType.BOOLEAN.getDataType())
        );

        String expected = "{" +
            "\"type\":\"record\"," +
            "\"name\":\"nifiRecord\"," +
            "\"namespace\":\"org.apache.nifi\"," +
            "\"fields\":[{" +
                "\"name\":\"record1\"," +
                "\"type\":[\"null\",{" +
                    "\"type\":\"record\"," +
                    "\"name\":\"record1Type\"," +
                    "\"fields\":[{" +
                        "\"name\":\"reoccurringFieldNameWithDifferentChildSchema\"," +
                        "\"type\":[\"null\",{" +
                            "\"type\":\"record\"," +
                            "\"name\":\"record1_reoccurringFieldNameWithDifferentChildSchemaType\"," +
                            "\"fields\":[{" +
                                "\"name\":\"childRecordField\"," +
                                "\"type\":[\"null\",\"string\"]" +
                            "}]" +
                        "}]" +
                    "}]" +
                "}]" +
            "}," +
            "{" +
                "\"name\":\"record2\"," +
                "\"type\":[\"null\",{" +
                    "\"type\":\"record\"," +
                    "\"name\":\"record2Type\"," +
                    "\"fields\":[{" +
                        "\"name\":\"reoccurringFieldNameWithDifferentChildSchema\"," +
                        "\"type\":[\"null\",{" +
                            "\"type\":\"record\"," +
                            "\"name\":\"record2_reoccurringFieldNameWithDifferentChildSchemaType\"," +
                            "\"fields\":[{" +
                                "\"name\":\"childRecordField\"," +
                                "\"type\":[\"null\",\"boolean\"]" +
                            "}]" +
                        "}]" +
                    "}]" +
                "}]" +
            "}]" +
        "}";

        // WHEN
        // THEN
        testSchemaWithReoccurringFieldName(reoccurringFieldName, childRecord11Fields, childRecord21Fields, expected);
    }

    @Test
    public void testSchemaWithArrayOfRecordsThatContainDifferentChildRecordForSameField() throws Exception {
        // GIVEN
        SimpleRecordSchema recordSchema1 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType())
        ));
        SimpleRecordSchema recordSchema2 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("string", RecordFieldType.STRING.getDataType())
        ));

        RecordSchema recordChoiceSchema = new SimpleRecordSchema(Arrays.asList(
            new RecordField("record", RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.RECORD.getRecordDataType(recordSchema1),
                RecordFieldType.RECORD.getRecordDataType(recordSchema2)
            ))
        ));

        RecordSchema schema = new SimpleRecordSchema(Arrays.asList(
            new RecordField("dataCollection", RecordFieldType.ARRAY.getArrayDataType(
                RecordFieldType.RECORD.getRecordDataType(recordChoiceSchema)
            )
        )));

        String expected = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"nifiRecord\",\n" +
            "  \"namespace\": \"org.apache.nifi\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"dataCollection\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        {\n" +
            "          \"type\": \"array\",\n" +
            "          \"items\": {\n" +
            "            \"type\": \"record\",\n" +
            "            \"name\": \"dataCollectionType\",\n" +
            "            \"fields\": [\n" +
            "              {\n" +
            "                \"name\": \"record\",\n" +
            "                \"type\": [\n" +
            "                  {\n" +
            "                    \"type\": \"record\",\n" +
            "                    \"name\": \"dataCollection_recordType\",\n" +
            "                    \"fields\": [\n" +
            "                      {\n" +
            "                        \"name\": \"integer\",\n" +
            "                        \"type\": [\n" +
            "                          \"null\",\n" +
            "                          \"int\"\n" +
            "                        ]\n" +
            "                      },\n" +
            "                      {\n" +
            "                        \"name\": \"boolean\",\n" +
            "                        \"type\": [\n" +
            "                          \"null\",\n" +
            "                          \"boolean\"\n" +
            "                        ]\n" +
            "                      }\n" +
            "                    ]\n" +
            "                  },\n" +
            "                  {\n" +
            "                    \"type\": \"record\",\n" +
            "                    \"name\": \"dataCollection_record2Type\",\n" +
            "                    \"fields\": [\n" +
            "                      {\n" +
            "                        \"name\": \"integer\",\n" +
            "                        \"type\": [\n" +
            "                          \"null\",\n" +
            "                          \"int\"\n" +
            "                        ]\n" +
            "                      },\n" +
            "                      {\n" +
            "                        \"name\": \"string\",\n" +
            "                        \"type\": [\n" +
            "                          \"null\",\n" +
            "                          \"string\"\n" +
            "                        ]\n" +
            "                      }\n" +
            "                    ]\n" +
            "                  },\n" +
            "                  \"null\"\n" +
            "                ]\n" +
            "              }\n" +
            "            ]\n" +
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  ]\n" +
            "}";

        // WHEN
        Schema actual = AvroTypeUtil.extractAvroSchema(schema);

        // THEN
        ObjectMapper mapper = new ObjectMapper();
        assertEquals(mapper.readTree(expected), mapper.readTree(actual.toString()));
    }

    private void testSchemaWithReoccurringFieldName(String reoccurringFieldName, List<RecordField> childRecord11Fields, List<RecordField> childRecord21Fields, String expected) {
        // GIVEN
        final List<RecordField> fields = new ArrayList<>();

        final List<RecordField> record1Fields = Arrays.asList(
            new RecordField(reoccurringFieldName, RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(childRecord11Fields)))
        );
        final DataType recordType1 = RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(record1Fields));

        final List<RecordField> record2Fields = Arrays.asList(
            new RecordField(reoccurringFieldName, RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(childRecord21Fields)))
        );
        final DataType recordType2 = RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(record2Fields));

        fields.add(new RecordField("record1", recordType1));
        fields.add(new RecordField("record2", recordType2));

        RecordSchema recordSchema = new SimpleRecordSchema(fields);

        // WHEN
        Schema actual = AvroTypeUtil.extractAvroSchema(recordSchema);

        // THEN
        assertEquals(expected, actual.toString());
    }

    @Test
    public void testSchemaWithMultiLevelRecord() throws Exception {
        // GIVEN
        final List<RecordField> fields = new ArrayList<>();

        final List<RecordField> level3RecordFields = Arrays.asList(
            new RecordField("stringField", RecordFieldType.STRING.getDataType())
        );
        final List<RecordField> level2RecordFields = Arrays.asList(
            new RecordField("level3Record", RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(level3RecordFields)))
        );
        final List<RecordField> multiLevelRecordFields = Arrays.asList(
            new RecordField("level2Record", RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(level2RecordFields)))
        );
        final DataType multiLevelRecordType = RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(multiLevelRecordFields));

        fields.add(new RecordField("multiLevelRecord", multiLevelRecordType));

        RecordSchema recordSchema = new SimpleRecordSchema(fields);

        String expected = "{" +
            "\"type\":\"record\"," +
            "\"name\":\"nifiRecord\"," +
            "\"namespace\":\"org.apache.nifi\"," +
            "\"fields\":[{" +
                "\"name\":\"multiLevelRecord\"," +
                "\"type\":[\"null\",{" +
                    "\"type\":\"record\"," +
                    "\"name\":\"multiLevelRecordType\"," +
                    "\"fields\":[{" +
                        "\"name\":\"level2Record\"," +
                        "\"type\":[\"null\",{" +
                            "\"type\":\"record\"," +
                            "\"name\":\"multiLevelRecord_level2RecordType\"," +
                            "\"fields\":[{" +
                                "\"name\":\"level3Record\"," +
                                "\"type\":[\"null\",{" +
                                    "\"type\":\"record\"," +
                                    "\"name\":\"multiLevelRecord_level2Record_level3RecordType\"," +
                                    "\"fields\":[{" +
                                        "\"name\":\"stringField\"," +
                                        "\"type\":[\"null\",\"string\"]" +
                                    "}]" +
                                "}]" +
                            "}]" +
                        "}]" +
                    "}]" +
                "}]" +
            "}]" +
        "}";

        // WHEN
        Schema actual = AvroTypeUtil.extractAvroSchema(recordSchema);

        // THEN
        assertEquals(expected, actual.toString());
    }

    private MapRecord givenRecordContainingNumericMap() {

        final Map<String, Object> numberValues = new HashMap<>();
        numberValues.put("number1", 123); // Intentionally an Integer as validation accepts it
        numberValues.put("number2", 123L);

        final List<RecordField> numberFields = Arrays.asList(
            new RecordField("number1", RecordFieldType.LONG.getDataType()),
            new RecordField("number2", RecordFieldType.LONG.getDataType())
        );

        final RecordSchema nifiNumberSchema = new SimpleRecordSchema(numberFields);
        final MapRecord numberRecord = new MapRecord(new SimpleRecordSchema(numberFields), numberValues);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 1);
        values.put("numbers", numberRecord);

        final List<RecordField> fields = Arrays.asList(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("numbers", RecordFieldType.RECORD.getRecordDataType(nifiNumberSchema))
        );

        return new MapRecord(new SimpleRecordSchema(fields), values);
    }

    private Schema givenAvroSchemaContainingNumericMap() {
        final List<Field> avroFields = Arrays.asList(
                new Field("id", Schema.create(Type.INT), "", ""),
                new Field("numbers", Schema.createMap(Schema.create(Type.LONG)), "", "")
        );

        return Schema.createRecord(avroFields);
    }
}
