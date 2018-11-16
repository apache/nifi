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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.Assert;
import org.junit.Test;

public class TestAvroTypeUtil {

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
}
