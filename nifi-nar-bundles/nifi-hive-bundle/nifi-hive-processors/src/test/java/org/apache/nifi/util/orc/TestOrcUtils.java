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
package org.apache.nifi.util.orc;


import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the OrcUtils helper class
 */
public class TestOrcUtils {

    @Test
    public void test_getOrcField_primitive() throws Exception {
        // Expected ORC types
        TypeDescription[] expectedTypes = {
                TypeDescription.createInt(),
                TypeDescription.createLong(),
                TypeDescription.createBoolean(),
                TypeDescription.createFloat(),
                TypeDescription.createDouble(),
                TypeDescription.createBinary(),
                TypeDescription.createString(),
        };

        // Build a fake Avro record with all types
        Schema testSchema = buildPrimitiveAvroSchema();
        List<Schema.Field> fields = testSchema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            assertEquals(expectedTypes[i], OrcUtils.getOrcField(fields.get(i).schema()));
        }

    }

    @Test
    public void test_getOrcField_union_optional_type() throws Exception {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("union").type().unionOf().nullBuilder().endNull().and().booleanType().endUnion().noDefault();
        Schema testSchema = builder.endRecord();
        TypeDescription orcType = OrcUtils.getOrcField(testSchema.getField("union").schema());
        assertEquals(TypeDescription.createBoolean(), orcType);
    }

    @Test
    public void test_getOrcField_union() throws Exception {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("union").type().unionOf().intType().and().booleanType().endUnion().noDefault();
        Schema testSchema = builder.endRecord();
        TypeDescription orcType = OrcUtils.getOrcField(testSchema.getField("union").schema());
        assertEquals(
                TypeDescription.createUnion()
                        .addUnionChild(TypeDescription.createInt())
                        .addUnionChild(TypeDescription.createBoolean()),
                orcType);
    }

    @Test
    public void test_getOrcField_map() throws Exception {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("map").type().map().values().doubleType().noDefault();
        Schema testSchema = builder.endRecord();
        TypeDescription orcType = OrcUtils.getOrcField(testSchema.getField("map").schema());
        assertEquals(
                TypeDescription.createMap(TypeDescription.createString(), TypeDescription.createDouble()),
                orcType);
    }

    @Test
    public void test_getOrcField_nested_map() throws Exception {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("map").type().map().values().map().values().doubleType().noDefault();
        Schema testSchema = builder.endRecord();
        TypeDescription orcType = OrcUtils.getOrcField(testSchema.getField("map").schema());
        assertEquals(
                TypeDescription.createMap(TypeDescription.createString(),
                        TypeDescription.createMap(TypeDescription.createString(), TypeDescription.createDouble())),
                orcType);
    }

    @Test
    public void test_getOrcField_array() throws Exception {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("array").type().array().items().longType().noDefault();
        Schema testSchema = builder.endRecord();
        TypeDescription orcType = OrcUtils.getOrcField(testSchema.getField("array").schema());
        assertEquals(
                TypeDescription.createList(TypeDescription.createLong()),
                orcType);
    }

    @Test
    public void test_getOrcField_complex_array() throws Exception {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("array").type().array().items().map().values().floatType().noDefault();
        Schema testSchema = builder.endRecord();
        TypeDescription orcType = OrcUtils.getOrcField(testSchema.getField("array").schema());
        assertEquals(
                TypeDescription.createList(TypeDescription.createMap(TypeDescription.createString(), TypeDescription.createFloat())),
                orcType);
    }

    @Test
    public void test_getOrcField_record() throws Exception {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("int").type().intType().noDefault();
        builder.name("long").type().longType().longDefault(1L);
        builder.name("array").type().array().items().stringType().noDefault();
        Schema testSchema = builder.endRecord();
        TypeDescription orcType = OrcUtils.getOrcField(testSchema);
        assertEquals(
                TypeDescription.createStruct()
                        .addField("int", TypeDescription.createInt())
                        .addField("long", TypeDescription.createLong())
                        .addField("array", TypeDescription.createList(TypeDescription.createString())),
                orcType);
    }

    @Test
    public void test_getOrcField_enum() throws Exception {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("enumField").type().enumeration("enum").symbols("a", "b", "c").enumDefault("a");
        Schema testSchema = builder.endRecord();
        TypeDescription orcType = OrcUtils.getOrcField(testSchema.getField("enumField").schema());
        assertEquals(TypeDescription.createString(), orcType);
    }

    @Test
    public void test_getPrimitiveOrcTypeFromPrimitiveAvroType() throws Exception {
        // Expected ORC types
        TypeDescription[] expectedTypes = {
                TypeDescription.createInt(),
                TypeDescription.createLong(),
                TypeDescription.createBoolean(),
                TypeDescription.createFloat(),
                TypeDescription.createDouble(),
                TypeDescription.createBinary(),
                TypeDescription.createString(),
        };

        Schema testSchema = buildPrimitiveAvroSchema();
        List<Schema.Field> fields = testSchema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            assertEquals(expectedTypes[i], OrcUtils.getPrimitiveOrcTypeFromPrimitiveAvroType(fields.get(i).schema().getType()));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_getPrimitiveOrcTypeFromPrimitiveAvroType_badType() throws Exception {
        Schema.Type nonPrimitiveType = Schema.Type.ARRAY;
        OrcUtils.getPrimitiveOrcTypeFromPrimitiveAvroType(nonPrimitiveType);
    }

    @Test
    public void test_putRowToBatch() {
        TypeDescription orcSchema = buildPrimitiveOrcSchema();
        VectorizedRowBatch batch = orcSchema.createRowBatch();
        Schema avroSchema = buildPrimitiveAvroSchema();
        List<Schema.Field> fields = avroSchema.getFields();
        GenericData.Record record = buildPrimitiveAvroRecord(1, 2L, false, 1.0f, 3.0, ByteBuffer.wrap("Hello".getBytes()), "World");
        for (int i = 0; i < fields.size(); i++) {
            OrcUtils.putToRowBatch(batch.cols[i], new MutableInt(0), 0, fields.get(i).schema(), record.get(i));
        }

        assertEquals(1, ((LongColumnVector) batch.cols[0]).vector[0]);
        assertEquals(2, ((LongColumnVector) batch.cols[1]).vector[0]);
        assertEquals(0, ((LongColumnVector) batch.cols[2]).vector[0]);
        assertEquals(1.0, ((DoubleColumnVector) batch.cols[3]).vector[0], Double.MIN_NORMAL);
        assertEquals(3.0, ((DoubleColumnVector) batch.cols[4]).vector[0], Double.MIN_NORMAL);
        assertEquals("Hello", ((BytesColumnVector) batch.cols[5]).toString(0));
        assertEquals("World", ((BytesColumnVector) batch.cols[6]).toString(0));

    }

    @Test
    public void test_putRowToBatch_union() {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("union").type().unionOf().intType().and().floatType().endUnion().noDefault();
        Schema testSchema = builder.endRecord();

        GenericData.Record row = new GenericData.Record(testSchema);
        row.put("union", 2);

        TypeDescription orcSchema = TypeDescription.createUnion()
                .addUnionChild(TypeDescription.createInt())
                .addUnionChild(TypeDescription.createFloat());

        VectorizedRowBatch batch = orcSchema.createRowBatch();
        batch.ensureSize(2);
        OrcUtils.putToRowBatch(batch.cols[0], new MutableInt(0), 0, testSchema.getField("union").schema(), row.get("union"));

        UnionColumnVector union = ((UnionColumnVector) batch.cols[0]);
        // verify the value is in the union field of type 'int'
        assertEquals(2, ((LongColumnVector) union.fields[0]).vector[0]);
        assertEquals(0.0, ((DoubleColumnVector) union.fields[1]).vector[0], Double.MIN_NORMAL);

        row.put("union", 2.0f);
        OrcUtils.putToRowBatch(batch.cols[0], new MutableInt(0), 1, testSchema.getField("union").schema(), row.get("union"));

        union = ((UnionColumnVector) batch.cols[0]);
        // verify the value is in the union field of type 'double'
        assertEquals(0, ((LongColumnVector) union.fields[0]).vector[1]);
        assertEquals(2.0, ((DoubleColumnVector) union.fields[1]).vector[1], Double.MIN_NORMAL);
    }

    @Test
    public void test_putRowToBatch_optional_union() {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("union").type().unionOf().nullType().and().floatType().endUnion().noDefault();
        Schema testSchema = builder.endRecord();

        GenericData.Record row = new GenericData.Record(testSchema);
        row.put("union", 2.0f);

        TypeDescription orcSchema = TypeDescription.createFloat();

        VectorizedRowBatch batch = orcSchema.createRowBatch();
        batch.ensureSize(2);
        OrcUtils.putToRowBatch(batch.cols[0], new MutableInt(0), 0, testSchema.getField("union").schema(), row.get("union"));

        assertTrue(batch.cols[0] instanceof DoubleColumnVector);

        DoubleColumnVector union = ((DoubleColumnVector) batch.cols[0]);
        // verify the value is in the union field of type 'int'
        assertEquals(2.0, union.vector[0], Double.MIN_NORMAL);

    }

    @Test
    public void test_putRowToBatch_array_ints() throws Exception {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("array").type().array().items().intType().noDefault();
        Schema testSchema = builder.endRecord();

        GenericData.Record row = new GenericData.Record(testSchema);
        int[] data1 = {1, 2, 3, 4, 5};
        row.put("array", data1);

        TypeDescription orcSchema = OrcUtils.getOrcField(testSchema.getField("array").schema());
        VectorizedRowBatch batch = orcSchema.createRowBatch();
        batch.ensureSize(2);
        MutableInt vectorOffset = new MutableInt(0);
        OrcUtils.putToRowBatch(batch.cols[0], vectorOffset, 0, testSchema.getField("array").schema(), row.get("array"));

        int[] data2 = {10, 20, 30, 40};
        row.put("array", data2);
        OrcUtils.putToRowBatch(batch.cols[0], vectorOffset, 1, testSchema.getField("array").schema(), row.get("array"));

        ListColumnVector array = ((ListColumnVector) batch.cols[0]);
        LongColumnVector dataColumn = ((LongColumnVector) array.child);
        // Check the first row, entries 0..4 should have values 1..5
        for (int i = 0; i < 5; i++) {
            assertEquals(i + 1, dataColumn.vector[i]);
        }
        // Check the second row, entries 5..8 should have values 10..40 (by tens)
        for (int i = 0; i < 4; i++) {
            assertEquals((i + 1) * 10, dataColumn.vector[(int) array.offsets[1] + i]);
        }
        assertEquals(0, dataColumn.vector[9]);
    }

    @Test
    public void test_putRowToBatch_array_floats() throws Exception {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("array").type().array().items().floatType().noDefault();
        Schema testSchema = builder.endRecord();

        GenericData.Record row = new GenericData.Record(testSchema);
        float[] data1 = {1.0f, 2.0f, 3.0f};
        row.put("array", data1);

        TypeDescription orcSchema = OrcUtils.getOrcField(testSchema.getField("array").schema());
        VectorizedRowBatch batch = orcSchema.createRowBatch();
        batch.ensureSize(2);
        MutableInt vectorOffset = new MutableInt(0);
        OrcUtils.putToRowBatch(batch.cols[0], vectorOffset, 0, testSchema.getField("array").schema(), row.get("array"));

        float[] data2 = {40.0f, 41.0f, 42.0f, 43.0f};
        row.put("array", data2);
        OrcUtils.putToRowBatch(batch.cols[0], vectorOffset, 1, testSchema.getField("array").schema(), row.get("array"));

        ListColumnVector array = ((ListColumnVector) batch.cols[0]);
        DoubleColumnVector dataColumn = ((DoubleColumnVector) array.child);
        // Check the first row, entries 0..4 should have values 1..5
        for (int i = 0; i < 3; i++) {
            assertEquals(i + 1.0f, dataColumn.vector[i], Float.MIN_NORMAL);
        }
        // Check the second row, entries 5..8 should have values 10..40 (by tens)
        for (int i = 0; i < 4; i++) {
            assertEquals((i + 40.0f), dataColumn.vector[(int) array.offsets[1] + i], Float.MIN_NORMAL);
        }
        assertEquals(0.0f, dataColumn.vector[9], Float.MIN_NORMAL);
    }

    @Test
    public void test_putRowToBatch_list_doubles() throws Exception {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("array").type().array().items().doubleType().noDefault();
        Schema testSchema = builder.endRecord();

        GenericData.Record row = new GenericData.Record(testSchema);
        List<Double> data1 = Arrays.asList(1.0, 2.0, 3.0);
        row.put("array", data1);

        TypeDescription orcSchema = OrcUtils.getOrcField(testSchema.getField("array").schema());
        VectorizedRowBatch batch = orcSchema.createRowBatch();
        batch.ensureSize(2);
        MutableInt vectorOffset = new MutableInt(0);
        OrcUtils.putToRowBatch(batch.cols[0], vectorOffset, 0, testSchema.getField("array").schema(), row.get("array"));

        List<Double> data2 = Arrays.asList(40.0, 41.0, 42.0, 43.0);
        row.put("array", data2);
        OrcUtils.putToRowBatch(batch.cols[0], vectorOffset, 1, testSchema.getField("array").schema(), row.get("array"));

        ListColumnVector array = ((ListColumnVector) batch.cols[0]);
        DoubleColumnVector dataColumn = ((DoubleColumnVector) array.child);
        // Check the first row, entries 0..4 should have values 1..5
        for (int i = 0; i < 3; i++) {
            assertEquals(i + 1.0f, dataColumn.vector[i], Float.MIN_NORMAL);
        }
        // Check the second row, entries 5..8 should have values 10..40 (by tens)
        for (int i = 0; i < 4; i++) {
            assertEquals((i + 40.0), dataColumn.vector[(int) array.offsets[1] + i], Float.MIN_NORMAL);
        }
        assertEquals(0.0, dataColumn.vector[9], Float.MIN_NORMAL);
    }

    @Test
    public void test_putRowToBatch_array_of_maps() throws Exception {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("array").type().array().items().map().values().floatType().noDefault();
        Schema testSchema = builder.endRecord();

        Map<String, Float> map1 = new TreeMap<String, Float>() {{
            put("key10", 10.0f);
            put("key20", 20.0f);
        }};

        Map<String, Float> map2 = new TreeMap<String, Float>() {{
            put("key101", 101.0f);
            put("key202", 202.0f);
        }};

        Map[] maps = new Map[]{map1, map2, null};
        GenericData.Record row = new GenericData.Record(testSchema);
        row.put("array", maps);

        TypeDescription orcSchema = OrcUtils.getOrcField(testSchema.getField("array").schema());
        VectorizedRowBatch batch = orcSchema.createRowBatch();
        OrcUtils.putToRowBatch(batch.cols[0], new MutableInt(0), 0, testSchema.getField("array").schema(), row.get("array"));

        ListColumnVector array = ((ListColumnVector) batch.cols[0]);
        MapColumnVector map = ((MapColumnVector) array.child);
        StringBuilder buffer = new StringBuilder();
        map.stringifyValue(buffer, 0);
        assertEquals("[{\"key\": \"key10\", \"value\": 10.0}, {\"key\": \"key20\", \"value\": 20.0}]", buffer.toString());
        buffer = new StringBuilder();
        map.stringifyValue(buffer, 1);
        assertEquals("[{\"key\": \"key101\", \"value\": 101.0}, {\"key\": \"key202\", \"value\": 202.0}]", buffer.toString());

    }

    @Test
    public void test_putRowToBatch_primitive_map() throws Exception {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("map").type().map().values().longType().noDefault();
        Schema testSchema = builder.endRecord();

        Map<String, Long> mapData1 = new TreeMap<String, Long>() {{
            put("key10", 100L);
            put("key20", 200L);
        }};

        GenericData.Record row = new GenericData.Record(testSchema);
        row.put("map", mapData1);

        TypeDescription orcSchema = OrcUtils.getOrcField(testSchema.getField("map").schema());
        VectorizedRowBatch batch = orcSchema.createRowBatch();
        batch.ensureSize(2);
        MutableInt vectorOffset = new MutableInt(0);
        OrcUtils.putToRowBatch(batch.cols[0], vectorOffset, 0, testSchema.getField("map").schema(), row.get("map"));

        Map<String, Long> mapData2 = new TreeMap<String, Long>() {{
            put("key1000", 1000L);
            put("key2000", 2000L);
        }};

        OrcUtils.putToRowBatch(batch.cols[0], vectorOffset, 1, testSchema.getField("map").schema(), mapData2);

        MapColumnVector map = ((MapColumnVector) batch.cols[0]);
        StringBuilder buffer = new StringBuilder();
        map.stringifyValue(buffer, 0);
        assertEquals("[{\"key\": \"key10\", \"value\": 100}, {\"key\": \"key20\", \"value\": 200}]", buffer.toString());
        buffer = new StringBuilder();
        map.stringifyValue(buffer, 1);
        assertEquals("[{\"key\": \"key1000\", \"value\": 1000}, {\"key\": \"key2000\", \"value\": 2000}]", buffer.toString());

    }

    @Test
    public void test_getHiveTypeFromAvroType_primitive() throws Exception {
        // Expected ORC types
        String[] expectedTypes = {
                "INT",
                "BIGINT",
                "BOOLEAN",
                "FLOAT",
                "DOUBLE",
                "BINARY",
                "STRING",
        };

        Schema testSchema = buildPrimitiveAvroSchema();
        List<Schema.Field> fields = testSchema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            assertEquals(expectedTypes[i], OrcUtils.getHiveTypeFromAvroType(fields.get(i).schema()));
        }
    }

    @Test
    public void test_getHiveTypeFromAvroType_complex() throws Exception {
        // Expected ORC types
        String[] expectedTypes = {
                "INT",
                "MAP<STRING, DOUBLE>",
                "STRING",
                "UNIONTYPE<BIGINT, FLOAT>",
                "ARRAY<INT>"
        };

        Schema testSchema = buildComplexAvroSchema();
        List<Schema.Field> fields = testSchema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            assertEquals(expectedTypes[i], OrcUtils.getHiveTypeFromAvroType(fields.get(i).schema()));
        }

        assertEquals("STRUCT<myInt:INT, myMap:MAP<STRING, DOUBLE>, myEnum:STRING, myLongOrFloat:UNIONTYPE<BIGINT, FLOAT>, myIntList:ARRAY<INT>>",
                OrcUtils.getHiveTypeFromAvroType(testSchema));
    }

    @Test
    public void test_generateHiveDDL_primitive() throws Exception {
        Schema avroSchema = buildPrimitiveAvroSchema();
        String ddl = OrcUtils.generateHiveDDL(avroSchema, "myHiveTable");
        assertEquals("CREATE EXTERNAL TABLE IF NOT EXISTS myHiveTable (int INT, long BIGINT, boolean BOOLEAN, float FLOAT, double DOUBLE, bytes BINARY, string STRING)"
                + " STORED AS ORC", ddl);
    }

    @Test
    public void test_generateHiveDDL_complex() throws Exception {
        Schema avroSchema = buildComplexAvroSchema();
        String ddl = OrcUtils.generateHiveDDL(avroSchema, "myHiveTable");
        assertEquals("CREATE EXTERNAL TABLE IF NOT EXISTS myHiveTable "
                + "(myInt INT, myMap MAP<STRING, DOUBLE>, myEnum STRING, myLongOrFloat UNIONTYPE<BIGINT, FLOAT>, myIntList ARRAY<INT>)"
                + " STORED AS ORC", ddl);
    }


    //////////////////
    // Helper methods
    //////////////////

    public static Schema buildPrimitiveAvroSchema() {
        // Build a fake Avro record with all primitive types
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("test.record").namespace("any.data").fields();
        builder.name("int").type().intType().noDefault();
        builder.name("long").type().longType().longDefault(1L);
        builder.name("boolean").type().booleanType().booleanDefault(true);
        builder.name("float").type().floatType().floatDefault(0.0f);
        builder.name("double").type().doubleType().doubleDefault(0.0);
        builder.name("bytes").type().bytesType().noDefault();
        builder.name("string").type().stringType().stringDefault("default");
        return builder.endRecord();
    }

    public static GenericData.Record buildPrimitiveAvroRecord(int i, long l, boolean b, float f, double d, ByteBuffer bytes, String string) {
        Schema schema = buildPrimitiveAvroSchema();
        GenericData.Record row = new GenericData.Record(schema);
        row.put("int", i);
        row.put("long", l);
        row.put("boolean", b);
        row.put("float", f);
        row.put("double", d);
        row.put("bytes", bytes);
        row.put("string", string);
        return row;
    }

    public static TypeDescription buildPrimitiveOrcSchema() {
        return TypeDescription.createStruct()
                .addField("int", TypeDescription.createInt())
                .addField("long", TypeDescription.createLong())
                .addField("boolean", TypeDescription.createBoolean())
                .addField("float", TypeDescription.createFloat())
                .addField("double", TypeDescription.createDouble())
                .addField("bytes", TypeDescription.createBinary())
                .addField("string", TypeDescription.createString());
    }

    public static Schema buildComplexAvroSchema() {
        // Build a fake Avro record with nested  types
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("complex.record").namespace("any.data").fields();
        builder.name("myInt").type().unionOf().nullType().and().intType().endUnion().nullDefault();
        builder.name("myMap").type().map().values().doubleType().noDefault();
        builder.name("myEnum").type().enumeration("myEnum").symbols("ABC", "DEF", "XYZ").enumDefault("ABC");
        builder.name("myLongOrFloat").type().unionOf().longType().and().floatType().endUnion().noDefault();
        builder.name("myIntList").type().array().items().intType().noDefault();
        return builder.endRecord();
    }

    public static GenericData.Record buildComplexAvroRecord(Integer i, Map<String, Double> m, String e, Object unionVal, List<Integer> intArray) {
        Schema schema = buildComplexAvroSchema();
        GenericData.Record row = new GenericData.Record(schema);
        row.put("myInt", i);
        row.put("myMap", m);
        row.put("myEnum", e);
        row.put("myLongOrFloat", unionVal);
        row.put("myIntList", intArray);
        return row;
    }
}