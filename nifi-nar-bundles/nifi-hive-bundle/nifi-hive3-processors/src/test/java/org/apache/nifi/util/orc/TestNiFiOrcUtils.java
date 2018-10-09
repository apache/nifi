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
import org.apache.hadoop.hive.ql.io.orc.NiFiOrcUtils;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the NiFiOrcUtils helper class
 */
public class TestNiFiOrcUtils {

    @Test
    public void test_getOrcField_primitive() {
        // Expected ORC types
        TypeInfo[] expectedTypes = {
                TypeInfoFactory.getPrimitiveTypeInfo("int"),
                TypeInfoFactory.getPrimitiveTypeInfo("bigint"),
                TypeInfoFactory.getPrimitiveTypeInfo("boolean"),
                TypeInfoFactory.getPrimitiveTypeInfo("float"),
                TypeInfoFactory.getPrimitiveTypeInfo("double"),
                TypeInfoFactory.getPrimitiveTypeInfo("binary"),
                TypeInfoFactory.getPrimitiveTypeInfo("string")
        };

        // Build a fake Avro record with all types
        RecordSchema testSchema = buildPrimitiveRecordSchema();
        List<RecordField> fields = testSchema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            assertEquals(expectedTypes[i], NiFiOrcUtils.getOrcField(fields.get(i).getDataType(), false));
        }
    }

    @Test
    public void test_getOrcField_union_optional_type() {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("union").type().unionOf().nullBuilder().endNull().and().booleanType().endUnion().noDefault();
        RecordSchema testSchema = AvroTypeUtil.createSchema(builder.endRecord());
        TypeInfo orcType = NiFiOrcUtils.getOrcField(testSchema.getField("union").get().getDataType(), false);
        assertEquals(TypeInfoCreator.createBoolean(), orcType);
    }

    @Test
    public void test_getOrcField_union() {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("union").type().unionOf().intType().and().booleanType().endUnion().noDefault();
        RecordSchema testSchema = AvroTypeUtil.createSchema(builder.endRecord());
        TypeInfo orcType = NiFiOrcUtils.getOrcField(testSchema.getField("union").get().getDataType(), false);
        assertEquals(
                TypeInfoFactory.getUnionTypeInfo(Arrays.asList(
                        TypeInfoCreator.createInt(),
                        TypeInfoCreator.createBoolean())),
                orcType);
    }

    @Test
    public void test_getOrcField_map() {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("map").type().map().values().doubleType().noDefault();
        RecordSchema testSchema = AvroTypeUtil.createSchema(builder.endRecord());
        TypeInfo orcType = NiFiOrcUtils.getOrcField(testSchema.getField("map").get().getDataType(), true);
        assertEquals(
                TypeInfoFactory.getMapTypeInfo(
                        TypeInfoCreator.createString(),
                        TypeInfoCreator.createDouble()),
                orcType);
    }

    @Test
    public void test_getOrcField_nested_map() {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("map").type().map().values().map().values().doubleType().noDefault();
        RecordSchema testSchema = AvroTypeUtil.createSchema(builder.endRecord());
        TypeInfo orcType = NiFiOrcUtils.getOrcField(testSchema.getField("map").get().getDataType(), false);
        assertEquals(
                TypeInfoFactory.getMapTypeInfo(TypeInfoCreator.createString(),
                        TypeInfoFactory.getMapTypeInfo(TypeInfoCreator.createString(), TypeInfoCreator.createDouble())),
                orcType);
    }

    @Test
    public void test_getOrcField_array() {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("array").type().array().items().longType().noDefault();
        RecordSchema testSchema = AvroTypeUtil.createSchema(builder.endRecord());
        TypeInfo orcType = NiFiOrcUtils.getOrcField(testSchema.getField("array").get().getDataType(), false);
        assertEquals(
                TypeInfoFactory.getListTypeInfo(TypeInfoCreator.createLong()),
                orcType);
    }

    @Test
    public void test_getOrcField_complex_array() {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("Array").type().array().items().map().values().floatType().noDefault();
        RecordSchema testSchema = AvroTypeUtil.createSchema(builder.endRecord());
        TypeInfo orcType = NiFiOrcUtils.getOrcField(testSchema.getField("Array").get().getDataType(), true);
        assertEquals(
                TypeInfoFactory.getListTypeInfo(TypeInfoFactory.getMapTypeInfo(TypeInfoCreator.createString(), TypeInfoCreator.createFloat())),
                orcType);
    }

    @Test
    public void test_getOrcField_record() {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("Int").type().intType().noDefault();
        builder.name("Long").type().longType().longDefault(1L);
        builder.name("Array").type().array().items().stringType().noDefault();
        RecordSchema testSchema = AvroTypeUtil.createSchema(builder.endRecord());
        // Normalize field names for Hive, assert that their names are now lowercase
        TypeInfo orcType = NiFiOrcUtils.getOrcSchema(testSchema, true);
        assertEquals(
                TypeInfoFactory.getStructTypeInfo(
                        Arrays.asList("int", "long", "array"),
                        Arrays.asList(
                                TypeInfoCreator.createInt(),
                                TypeInfoCreator.createLong(),
                                TypeInfoFactory.getListTypeInfo(TypeInfoCreator.createString()))),
                orcType);
    }

    @Test
    public void test_getOrcField_enum() {
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data").fields();
        builder.name("enumField").type().enumeration("enum").symbols("a", "b", "c").enumDefault("a");
        RecordSchema testSchema = AvroTypeUtil.createSchema(builder.endRecord());
        TypeInfo orcType = NiFiOrcUtils.getOrcField(testSchema.getField("enumField").get().getDataType(), true);
        assertEquals(TypeInfoCreator.createString(), orcType);
    }

    @Test
    public void test_getPrimitiveOrcTypeFromPrimitiveFieldType() {
        // Expected ORC types
        TypeInfo[] expectedTypes = {
                TypeInfoCreator.createInt(),
                TypeInfoCreator.createLong(),
                TypeInfoCreator.createBoolean(),
                TypeInfoCreator.createFloat(),
                TypeInfoCreator.createDouble(),
                TypeInfoCreator.createBinary(),
                TypeInfoCreator.createString(),
        };

        RecordSchema testSchema = buildPrimitiveRecordSchema();
        List<RecordField> fields = testSchema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            // Skip Binary as it is a primitive type in Avro but a complex type (array[byte]) in the NiFi Record API
            if (i == 5) {
                continue;
            }
            assertEquals(expectedTypes[i], NiFiOrcUtils.getPrimitiveOrcTypeFromPrimitiveFieldType(fields.get(i).getDataType()));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_getPrimitiveOrcTypeFromPrimitiveFieldType_badType() {
        NiFiOrcUtils.getPrimitiveOrcTypeFromPrimitiveFieldType(RecordFieldType.ARRAY.getDataType());
    }

    @Test
    public void test_getWritable() throws Exception {
        assertTrue(NiFiOrcUtils.convertToORCObject(null, 1, true) instanceof IntWritable);
        assertTrue(NiFiOrcUtils.convertToORCObject(null, 1L, true) instanceof LongWritable);
        assertTrue(NiFiOrcUtils.convertToORCObject(null, 1.0f, true) instanceof FloatWritable);
        assertTrue(NiFiOrcUtils.convertToORCObject(null, 1.0, true) instanceof DoubleWritable);
        assertTrue(NiFiOrcUtils.convertToORCObject(null, new int[]{1, 2, 3}, true) instanceof List);
        assertTrue(NiFiOrcUtils.convertToORCObject(null, Arrays.asList(1, 2, 3), true) instanceof List);
        Map<String, Float> map = new HashMap<>();
        map.put("Hello", 1.0f);
        map.put("World", 2.0f);

        Object convMap = NiFiOrcUtils.convertToORCObject(TypeInfoUtils.getTypeInfoFromTypeString("map<string,float>"), map, true);
        assertTrue(convMap instanceof Map);
        ((Map) convMap).forEach((key, value) -> {
            assertTrue(key instanceof Text);
            assertTrue(value instanceof FloatWritable);
        });
    }

    @Test
    public void test_getHiveTypeFromFieldType_primitive() {
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

        RecordSchema testSchema = buildPrimitiveRecordSchema();
        List<RecordField> fields = testSchema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            assertEquals(expectedTypes[i], NiFiOrcUtils.getHiveTypeFromFieldType(fields.get(i).getDataType(), false));
        }
    }

    @Test
    public void test_getHiveTypeFromFieldType_complex() {
        // Expected ORC types
        String[] expectedTypes = {
                "INT",
                "MAP<STRING, DOUBLE>",
                "STRING",
                "UNIONTYPE<BIGINT, FLOAT>",
                "ARRAY<INT>"
        };

        RecordSchema testSchema = buildComplexRecordSchema();
        List<RecordField> fields = testSchema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            assertEquals(expectedTypes[i], NiFiOrcUtils.getHiveTypeFromFieldType(fields.get(i).getDataType(), false));
        }

        assertEquals("STRUCT<myInt:INT, myMap:MAP<STRING, DOUBLE>, myEnum:STRING, myLongOrFloat:UNIONTYPE<BIGINT, FLOAT>, myIntList:ARRAY<INT>>",
                NiFiOrcUtils.getHiveSchema(testSchema, false));
    }

    @Test
    public void test_generateHiveDDL_primitive() {
        RecordSchema schema = buildPrimitiveRecordSchema();
        String ddl = NiFiOrcUtils.generateHiveDDL(schema, "myHiveTable", false);
        assertEquals("CREATE EXTERNAL TABLE IF NOT EXISTS `myHiveTable` (`int` INT, `long` BIGINT, `boolean` BOOLEAN, `float` FLOAT, `double` DOUBLE, `bytes` BINARY, `string` STRING)"
                + " STORED AS ORC", ddl);
    }

    @Test
    public void test_generateHiveDDL_complex() {
        RecordSchema schema = buildComplexRecordSchema();
        String ddl = NiFiOrcUtils.generateHiveDDL(schema, "myHiveTable", false);
        assertEquals("CREATE EXTERNAL TABLE IF NOT EXISTS `myHiveTable` "
                + "(`myInt` INT, `myMap` MAP<STRING, DOUBLE>, `myEnum` STRING, `myLongOrFloat` UNIONTYPE<BIGINT, FLOAT>, `myIntList` ARRAY<INT>)"
                + " STORED AS ORC", ddl);
    }

    @Test
    public void test_generateHiveDDL_complex_normalize() {
        RecordSchema schema = buildComplexRecordSchema();
        String ddl = NiFiOrcUtils.generateHiveDDL(schema, "myHiveTable", true);
        assertEquals("CREATE EXTERNAL TABLE IF NOT EXISTS `myHiveTable` "
                + "(`myint` INT, `mymap` MAP<STRING, DOUBLE>, `myenum` STRING, `mylongorfloat` UNIONTYPE<BIGINT, FLOAT>, `myintlist` ARRAY<INT>)"
                + " STORED AS ORC", ddl);
    }

    @Test
    public void test_convertToORCObject() {
        List<Object> objects = Arrays.asList("Hello", "x");
        objects.forEach((avroObject) -> {
            Object o = NiFiOrcUtils.convertToORCObject(TypeInfoUtils.getTypeInfoFromTypeString("uniontype<bigint,string>"), avroObject, true);
            assertTrue(o instanceof UnionObject);
            UnionObject uo = (UnionObject) o;
            assertTrue(uo.getObject() instanceof Text);
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_convertToORCObjectBadUnion() {
        NiFiOrcUtils.convertToORCObject(TypeInfoUtils.getTypeInfoFromTypeString("uniontype<bigint,long>"), "Hello", true);
    }

    @Test
    public void test_getHiveTypeFromFieldType_complex_normalize() {
        // Expected ORC types
        String[] expectedTypes = {
                "INT",
                "MAP<STRING, DOUBLE>",
                "STRING",
                "UNIONTYPE<BIGINT, FLOAT>",
                "ARRAY<INT>"
        };

        RecordSchema testSchema = buildComplexRecordSchema();
        List<RecordField> fields = testSchema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            assertEquals(expectedTypes[i], NiFiOrcUtils.getHiveTypeFromFieldType(fields.get(i).getDataType(), true));
        }

        assertEquals("STRUCT<myint:INT, mymap:MAP<STRING, DOUBLE>, myenum:STRING, mylongorfloat:UNIONTYPE<BIGINT, FLOAT>, myintlist:ARRAY<INT>>",
                NiFiOrcUtils.getHiveSchema(testSchema, true));
    }

    //////////////////
    // Helper methods
    //////////////////

    public static RecordSchema buildPrimitiveRecordSchema() {
        // Build a fake record with all primitive types
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("test.record").namespace("any.data").fields();
        builder.name("int").type().intType().noDefault();
        builder.name("long").type().longType().longDefault(1L);
        builder.name("boolean").type().booleanType().booleanDefault(true);
        builder.name("float").type().floatType().floatDefault(0.0f);
        builder.name("double").type().doubleType().doubleDefault(0.0);
        builder.name("bytes").type().bytesType().noDefault();
        builder.name("string").type().stringType().stringDefault("default");
        return AvroTypeUtil.createSchema(builder.endRecord());
    }

    public static Record buildPrimitiveRecord(int i, long l, boolean b, float f, double d, ByteBuffer bytes, String string) {
        RecordSchema schema = buildPrimitiveRecordSchema();
        Map<String, Object> row = new HashMap<>();
        row.put("int", i);
        row.put("long", l);
        row.put("boolean", b);
        row.put("float", f);
        row.put("double", d);
        row.put("bytes", bytes);
        row.put("string", string);
        return new MapRecord(schema, row);
    }

    public static TypeInfo buildPrimitiveOrcSchema() {
        return TypeInfoFactory.getStructTypeInfo(Arrays.asList("int", "long", "boolean", "float", "double", "bytes", "string"),
                Arrays.asList(
                        TypeInfoCreator.createInt(),
                        TypeInfoCreator.createLong(),
                        TypeInfoCreator.createBoolean(),
                        TypeInfoCreator.createFloat(),
                        TypeInfoCreator.createDouble(),
                        TypeInfoCreator.createBinary(),
                        TypeInfoCreator.createString()));
    }

    public static RecordSchema buildComplexRecordSchema() {
        // Build a fake Avro record with nested  types
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("complex.record").namespace("any.data").fields();
        builder.name("myInt").type().unionOf().nullType().and().intType().endUnion().nullDefault();
        builder.name("myMap").type().map().values().doubleType().noDefault();
        builder.name("myEnum").type().enumeration("myEnum").symbols("ABC", "DEF", "XYZ").enumDefault("ABC");
        builder.name("myLongOrFloat").type().unionOf().longType().and().floatType().endUnion().noDefault();
        builder.name("myIntList").type().array().items().intType().noDefault();
        return AvroTypeUtil.createSchema(builder.endRecord());
    }

    public static Record buildComplexAvroRecord(Integer i, Map<String, Double> m, String e, Object unionVal, List<Integer> intArray) {
        RecordSchema schema = buildComplexRecordSchema();
        Map<String, Object> row = new HashMap<>();
        row.put("myInt", i);
        row.put("myMap", m);
        row.put("myEnum", e);
        row.put("myLongOrFloat", unionVal);
        row.put("myIntList", intArray);
        return new MapRecord(schema, row);
    }

    public static TypeInfo buildComplexOrcSchema() {
        return TypeInfoUtils.getTypeInfoFromTypeString("struct<myInt:int,myMap:map<string,double>,myEnum:string,myLongOrFloat:uniontype<int>,myIntList:array<int>>");
    }

    public static Schema buildNestedComplexAvroSchema() {
        // Build a fake Avro record with nested complex types
        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("nested.complex.record").namespace("any.data").fields();
        builder.name("myMapOfArray").type().map().values().array().items().doubleType().noDefault();
        builder.name("myArrayOfMap").type().array().items().map().values().stringType().noDefault();
        return builder.endRecord();
    }

    public static GenericData.Record buildNestedComplexAvroRecord(Map<String, List<Double>> m, List<Map<String, String>> a) {
        Schema schema = buildNestedComplexAvroSchema();
        GenericData.Record row = new GenericData.Record(schema);
        row.put("myMapOfArray", m);
        row.put("myArrayOfMap", a);
        return row;
    }

    public static TypeInfo buildNestedComplexOrcSchema() {
        return TypeInfoUtils.getTypeInfoFromTypeString("struct<myMapOfArray:map<string,array<double>>,myArrayOfMap:array<map<string,string>>>");
    }

    private static class TypeInfoCreator {
        static TypeInfo createInt() {
            return TypeInfoFactory.getPrimitiveTypeInfo("int");
        }

        static TypeInfo createLong() {
            return TypeInfoFactory.getPrimitiveTypeInfo("bigint");
        }

        static TypeInfo createBoolean() {
            return TypeInfoFactory.getPrimitiveTypeInfo("boolean");
        }

        static TypeInfo createFloat() {
            return TypeInfoFactory.getPrimitiveTypeInfo("float");
        }

        static TypeInfo createDouble() {
            return TypeInfoFactory.getPrimitiveTypeInfo("double");
        }

        static TypeInfo createBinary() {
            return TypeInfoFactory.getPrimitiveTypeInfo("binary");
        }

        static TypeInfo createString() {
            return TypeInfoFactory.getPrimitiveTypeInfo("string");
        }
    }
}