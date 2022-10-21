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
package org.apache.nifi.processors.iceberg;

import org.apache.avro.file.DataFileWriter;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.nifi.processors.iceberg.converter.ArrayElementGetter;
import org.apache.nifi.processors.iceberg.converter.IcebergRecordConverter;
import org.apache.nifi.processors.iceberg.converter.RecordFieldGetter;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.io.File.createTempFile;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

public class TestIcebergRecordConverter {

    private OutputFile tempFile;

    @BeforeEach
    public void setUp() throws Exception {
        tempFile = Files.localOutput(createTempFile("test", null));
    }

    @AfterEach
    public void tearDown() {
        File file = new File(tempFile.location());
        file.deleteOnExit();
    }

    private static final Schema STRUCT = new Schema(
            Types.NestedField.required(0, "struct", Types.StructType.of(
                    Types.NestedField.required(1, "nested_struct", Types.StructType.of(
                            Types.NestedField.required(2, "string", Types.StringType.get()),
                            Types.NestedField.required(3, "integer", Types.IntegerType.get()))
                    )
            ))
    );

    private static final Schema LIST = new Schema(
            Types.NestedField.required(0, "list", Types.ListType.ofRequired(
                            1, Types.ListType.ofRequired(
                                    2, Types.StringType.get())
                    )
            )
    );

    private static final Schema MAP = new Schema(
            Types.NestedField.required(0, "map", Types.MapType.ofRequired(
                    1, 2, Types.StringType.get(), Types.MapType.ofRequired(
                            3, 4, Types.StringType.get(), Types.LongType.get()
                    )
            ))
    );

    private static final Schema PRIMITIVES = new Schema(
            Types.NestedField.optional(0, "string", Types.StringType.get()),
            Types.NestedField.optional(1, "integer", Types.IntegerType.get()),
            Types.NestedField.optional(2, "float", Types.FloatType.get()),
            Types.NestedField.optional(3, "long", Types.LongType.get()),
            Types.NestedField.optional(4, "double", Types.DoubleType.get()),
            Types.NestedField.optional(5, "decimal", Types.DecimalType.of(10, 2)),
            Types.NestedField.optional(6, "boolean", Types.BooleanType.get()),
            Types.NestedField.optional(7, "fixed", Types.FixedType.ofLength(5)),
            Types.NestedField.optional(8, "binary", Types.BinaryType.get()),
            Types.NestedField.optional(9, "date", Types.DateType.get()),
            Types.NestedField.optional(10, "time", Types.TimeType.get()),
            Types.NestedField.optional(11, "timestamp", Types.TimestampType.withZone()),
            Types.NestedField.optional(12, "timestampTz", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(13, "uuid", Types.UUIDType.get()),
            Types.NestedField.optional(14, "choice", Types.IntegerType.get())
    );

    private static RecordSchema getStructSchema() {
        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("struct", new RecordDataType(getNestedStructSchema())));

        return new SimpleRecordSchema(fields);
    }

    private static RecordSchema getNestedStructSchema() {
        List<RecordField> nestedFields = new ArrayList<>();
        nestedFields.add(new RecordField("nested_struct", new RecordDataType(getNestedStructSchema2())));

        return new SimpleRecordSchema(nestedFields);
    }

    private static RecordSchema getNestedStructSchema2() {
        List<RecordField> nestedFields2 = new ArrayList<>();
        nestedFields2.add(new RecordField("string", RecordFieldType.STRING.getDataType()));
        nestedFields2.add(new RecordField("integer", RecordFieldType.INT.getDataType()));

        return new SimpleRecordSchema(nestedFields2);
    }

    private static RecordSchema getListSchema() {
        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("list", new ArrayDataType(
                new ArrayDataType(RecordFieldType.STRING.getDataType()))));

        return new SimpleRecordSchema(fields);
    }

    private static RecordSchema getMapSchema() {
        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("map", new MapDataType(
                new MapDataType(RecordFieldType.LONG.getDataType()))));

        return new SimpleRecordSchema(fields);
    }

    private static RecordSchema getPrimitivesSchema() {
        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("string", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("integer", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("float", RecordFieldType.FLOAT.getDataType()));
        fields.add(new RecordField("long", RecordFieldType.LONG.getDataType()));
        fields.add(new RecordField("double", RecordFieldType.DOUBLE.getDataType()));
        fields.add(new RecordField("decimal", RecordFieldType.DECIMAL.getDecimalDataType(10, 2)));
        fields.add(new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType()));
        fields.add(new RecordField("fixed", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())));
        fields.add(new RecordField("binary", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())));
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));
        fields.add(new RecordField("time", RecordFieldType.TIME.getDataType()));
        fields.add(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()));
        fields.add(new RecordField("timestampTz", RecordFieldType.TIMESTAMP.getDataType()));
        fields.add(new RecordField("uuid", RecordFieldType.UUID.getDataType()));
        fields.add(new RecordField("choice", RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.STRING.getDataType(), RecordFieldType.INT.getDataType())));

        return new SimpleRecordSchema(fields);
    }

    private static RecordSchema getChoiceSchema() {
        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("string", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("integer", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("float", RecordFieldType.LONG.getDataType()));

        return new SimpleRecordSchema(fields);
    }

    private static Record setupStructTestRecord() {
        Map<String, Object> nestedValues2 = new HashMap<>();
        nestedValues2.put("string", "Test String");
        nestedValues2.put("integer", 10);
        MapRecord nestedRecord2 = new MapRecord(getNestedStructSchema2(), nestedValues2);

        Map<String, Object> nestedValues = new HashMap<>();
        nestedValues.put("nested_struct", nestedRecord2);
        MapRecord nestedRecord = new MapRecord(getNestedStructSchema(), nestedValues);

        Map<String, Object> values = new HashMap<>();
        values.put("struct", nestedRecord);
        return new MapRecord(getStructSchema(), values);
    }

    private static Record setupListTestRecord() {
        List<String> nestedList = new ArrayList<>();
        nestedList.add("Test String");

        List<Collection> list = new ArrayList<>();
        list.add(nestedList);

        Map<String, Object> values = new HashMap<>();
        values.put("list", list);

        return new MapRecord(getListSchema(), values);
    }

    private static Record setupMapTestRecord() {
        Map<String, Long> nestedMap = new HashMap<>();
        nestedMap.put("nested_key", 42L);

        Map<String, Map> map = new HashMap<>();
        map.put("key", nestedMap);

        Map<String, Object> values = new HashMap<>();
        values.put("map", map);

        return new MapRecord(getMapSchema(), values);
    }

    private static Record setupPrimitivesTestRecord() {
        LocalDate localDate = LocalDate.of(2017, 4, 4);
        LocalTime localTime = LocalTime.of(14, 20, 33);
        LocalDateTime localDateTime = LocalDateTime.of(2017, 4, 4, 14, 20, 33, 789000000);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, ZoneOffset.ofHours(-5));

        Map<String, Object> values = new HashMap<>();
        values.put("string", "Test String");
        values.put("integer", 8);
        values.put("float", 1.23456F);
        values.put("long", 42L);
        values.put("double", 3.14159D);
        values.put("decimal", new BigDecimal("12345678.12"));
        values.put("boolean", true);
        values.put("fixed", "hello".getBytes());
        values.put("binary", "hello".getBytes());
        values.put("date", localDate);
        values.put("time", Time.valueOf(localTime));
        values.put("timestamp", Timestamp.from(offsetDateTime.toInstant()));
        values.put("timestampTz", Timestamp.valueOf(localDateTime));
        values.put("uuid", UUID.fromString("0000-00-00-00-000000"));
        values.put("choice", "10");

        return new MapRecord(getPrimitivesSchema(), values);
    }

    private static Record setupChoiceTestRecord() {
        Map<String, Object> values = new HashMap<>();
        values.put("choice1", "20");
        values.put("choice2", "30a");
        values.put("choice3", String.valueOf(Long.MAX_VALUE));

        return new MapRecord(getChoiceSchema(), values);
    }

    @Test
    public void testPrimitivesAvro() throws IOException {
        RecordSchema nifiSchema = getPrimitivesSchema();
        Record record = setupPrimitivesTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(PRIMITIVES, nifiSchema, FileFormat.AVRO);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeToAvro(PRIMITIVES, genericRecord, tempFile);

        List<GenericRecord> results = readFromAvro(PRIMITIVES, tempFile.toInputFile());

        Assertions.assertEquals(results.size(), 1);
        GenericRecord resultRecord = results.get(0);

        LocalDateTime localDateTime = LocalDateTime.of(2017, 4, 4, 14, 20, 33, 789000000);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, ZoneOffset.ofHours(-5));

        Assertions.assertEquals(resultRecord.get(0, String.class), "Test String");
        Assertions.assertEquals(resultRecord.get(1, Integer.class), new Integer(8));
        Assertions.assertEquals(resultRecord.get(2, Float.class), new Float(1.23456F));
        Assertions.assertEquals(resultRecord.get(3, Long.class), new Long(42L));
        Assertions.assertEquals(resultRecord.get(4, Double.class), new Double(3.14159D));
        Assertions.assertEquals(resultRecord.get(5, BigDecimal.class), new BigDecimal("12345678.12"));
        Assertions.assertEquals(resultRecord.get(6, Boolean.class), Boolean.TRUE);
        Assertions.assertArrayEquals(resultRecord.get(7, byte[].class), new byte[]{104, 101, 108, 108, 111});
        Assertions.assertArrayEquals(resultRecord.get(8, ByteBuffer.class).array(), new byte[]{104, 101, 108, 108, 111});
        Assertions.assertEquals(resultRecord.get(9, LocalDate.class), LocalDate.of(2017, 4, 4));
        Assertions.assertEquals(resultRecord.get(10, LocalTime.class), LocalTime.of(14, 20, 33));
        Assertions.assertEquals(resultRecord.get(11, OffsetDateTime.class), offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC));
        Assertions.assertEquals(resultRecord.get(12, LocalDateTime.class), LocalDateTime.of(2017, 4, 4, 14, 20, 33, 789000000));
        Assertions.assertEquals(resultRecord.get(13, UUID.class), UUID.fromString("0000-00-00-00-000000"));
        Assertions.assertEquals(resultRecord.get(14, Integer.class), new Integer(10));
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testPrimitivesOrc() throws IOException {
        RecordSchema nifiSchema = getPrimitivesSchema();
        Record record = setupPrimitivesTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(PRIMITIVES, nifiSchema, FileFormat.ORC);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeToOrc(PRIMITIVES, genericRecord, tempFile);

        List<GenericRecord> results = readFromOrc(PRIMITIVES, tempFile.toInputFile());

        Assertions.assertEquals(results.size(), 1);
        GenericRecord resultRecord = results.get(0);

        LocalDateTime localDateTime = LocalDateTime.of(2017, 4, 4, 14, 20, 33, 789000000);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, ZoneOffset.ofHours(-5));

        Assertions.assertEquals(resultRecord.get(0, String.class), "Test String");
        Assertions.assertEquals(resultRecord.get(1, Integer.class), new Integer(8));
        Assertions.assertEquals(resultRecord.get(2, Float.class), new Float(1.23456F));
        Assertions.assertEquals(resultRecord.get(3, Long.class), new Long(42L));
        Assertions.assertEquals(resultRecord.get(4, Double.class), new Double(3.14159D));
        Assertions.assertEquals(resultRecord.get(5, BigDecimal.class), new BigDecimal("12345678.12"));
        Assertions.assertEquals(resultRecord.get(6, Boolean.class), Boolean.TRUE);
        Assertions.assertArrayEquals(resultRecord.get(7, byte[].class), new byte[]{104, 101, 108, 108, 111});
        Assertions.assertArrayEquals(resultRecord.get(8, ByteBuffer.class).array(), new byte[]{104, 101, 108, 108, 111});
        Assertions.assertEquals(resultRecord.get(9, LocalDate.class), LocalDate.of(2017, 4, 4));
        Assertions.assertEquals(resultRecord.get(10, LocalTime.class), LocalTime.of(14, 20, 33));
        Assertions.assertEquals(resultRecord.get(11, OffsetDateTime.class), offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC));
        Assertions.assertEquals(resultRecord.get(12, LocalDateTime.class), LocalDateTime.of(2017, 4, 4, 14, 20, 33, 789000000));
        Assertions.assertEquals(resultRecord.get(13, UUID.class), UUID.fromString("0000-00-00-00-000000"));
        Assertions.assertEquals(resultRecord.get(14, Integer.class), new Integer(10));
    }

    @Test
    public void testPrimitivesParquet() throws IOException {
        RecordSchema nifiSchema = getPrimitivesSchema();
        Record record = setupPrimitivesTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(PRIMITIVES, nifiSchema, FileFormat.PARQUET);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeToParquet(PRIMITIVES, genericRecord, tempFile);

        List<GenericRecord> results = readFromParquet(PRIMITIVES, tempFile.toInputFile());

        Assertions.assertEquals(results.size(), 1);
        GenericRecord resultRecord = results.get(0);

        LocalDateTime localDateTime = LocalDateTime.of(2017, 4, 4, 14, 20, 33, 789000000);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, ZoneOffset.ofHours(-5));

        Assertions.assertEquals(resultRecord.get(0, String.class), "Test String");
        Assertions.assertEquals(resultRecord.get(1, Integer.class), new Integer(8));
        Assertions.assertEquals(resultRecord.get(2, Float.class), new Float(1.23456F));
        Assertions.assertEquals(resultRecord.get(3, Long.class), new Long(42L));
        Assertions.assertEquals(resultRecord.get(4, Double.class), new Double(3.14159D));
        Assertions.assertEquals(resultRecord.get(5, BigDecimal.class), new BigDecimal("12345678.12"));
        Assertions.assertEquals(resultRecord.get(6, Boolean.class), Boolean.TRUE);
        Assertions.assertArrayEquals(resultRecord.get(7, byte[].class), new byte[]{104, 101, 108, 108, 111});
        Assertions.assertArrayEquals(resultRecord.get(8, ByteBuffer.class).array(), new byte[]{104, 101, 108, 108, 111});
        Assertions.assertEquals(resultRecord.get(9, LocalDate.class), LocalDate.of(2017, 4, 4));
        Assertions.assertEquals(resultRecord.get(10, LocalTime.class), LocalTime.of(14, 20, 33));
        Assertions.assertEquals(resultRecord.get(11, OffsetDateTime.class), offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC));
        Assertions.assertEquals(resultRecord.get(12, LocalDateTime.class), LocalDateTime.of(2017, 4, 4, 14, 20, 33, 789000000));
        Assertions.assertArrayEquals(resultRecord.get(13, byte[].class), new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        Assertions.assertEquals(resultRecord.get(14, Integer.class), new Integer(10));
    }

    @Test
    public void testStructAvro() throws IOException {
        RecordSchema nifiSchema = getStructSchema();
        Record record = setupStructTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(STRUCT, nifiSchema, FileFormat.AVRO);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeToAvro(STRUCT, genericRecord, tempFile);

        List<GenericRecord> results = readFromAvro(STRUCT, tempFile.toInputFile());

        Assertions.assertEquals(results.size(), 1);
        Assertions.assertInstanceOf(GenericRecord.class, results.get(0));
        GenericRecord resultRecord = results.get(0);

        Assertions.assertEquals(resultRecord.size(), 1);
        Assertions.assertInstanceOf(GenericRecord.class, resultRecord.get(0));
        GenericRecord nestedRecord = (GenericRecord) resultRecord.get(0);

        Assertions.assertEquals(nestedRecord.size(), 1);
        Assertions.assertInstanceOf(GenericRecord.class, nestedRecord.get(0));
        GenericRecord baseRecord = (GenericRecord) nestedRecord.get(0);

        Assertions.assertEquals(baseRecord.get(0, String.class), "Test String");
        Assertions.assertEquals(baseRecord.get(1, Integer.class), new Integer(10));
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testStructOrc() throws IOException {
        RecordSchema nifiSchema = getStructSchema();
        Record record = setupStructTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(STRUCT, nifiSchema, FileFormat.ORC);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeToOrc(STRUCT, genericRecord, tempFile);

        List<GenericRecord> results = readFromOrc(STRUCT, tempFile.toInputFile());

        Assertions.assertEquals(results.size(), 1);
        Assertions.assertInstanceOf(GenericRecord.class, results.get(0));
        GenericRecord resultRecord = results.get(0);

        Assertions.assertEquals(resultRecord.size(), 1);
        Assertions.assertInstanceOf(GenericRecord.class, resultRecord.get(0));
        GenericRecord nestedRecord = (GenericRecord) resultRecord.get(0);

        Assertions.assertEquals(nestedRecord.size(), 1);
        Assertions.assertInstanceOf(GenericRecord.class, nestedRecord.get(0));
        GenericRecord baseRecord = (GenericRecord) nestedRecord.get(0);

        Assertions.assertEquals(baseRecord.get(0, String.class), "Test String");
        Assertions.assertEquals(baseRecord.get(1, Integer.class), new Integer(10));
    }

    @Test
    public void testStructParquet() throws IOException {
        RecordSchema nifiSchema = getStructSchema();
        Record record = setupStructTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(STRUCT, nifiSchema, FileFormat.PARQUET);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeToParquet(STRUCT, genericRecord, tempFile);

        List<GenericRecord> results = readFromParquet(STRUCT, tempFile.toInputFile());

        Assertions.assertEquals(results.size(), 1);
        Assertions.assertInstanceOf(GenericRecord.class, results.get(0));
        GenericRecord resultRecord = results.get(0);

        Assertions.assertEquals(resultRecord.size(), 1);
        Assertions.assertInstanceOf(GenericRecord.class, resultRecord.get(0));
        GenericRecord nestedRecord = (GenericRecord) resultRecord.get(0);

        Assertions.assertEquals(nestedRecord.size(), 1);
        Assertions.assertInstanceOf(GenericRecord.class, nestedRecord.get(0));
        GenericRecord baseRecord = (GenericRecord) nestedRecord.get(0);

        Assertions.assertEquals(baseRecord.get(0, String.class), "Test String");
        Assertions.assertEquals(baseRecord.get(1, Integer.class), new Integer(10));
    }

    @Test
    public void testListAvro() throws IOException {
        RecordSchema nifiSchema = getListSchema();
        Record record = setupListTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(LIST, nifiSchema, FileFormat.AVRO);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeToAvro(LIST, genericRecord, tempFile);

        List<GenericRecord> results = readFromAvro(LIST, tempFile.toInputFile());

        Assertions.assertEquals(results.size(), 1);
        Assertions.assertInstanceOf(GenericRecord.class, results.get(0));
        GenericRecord resultRecord = results.get(0);

        Assertions.assertEquals(resultRecord.size(), 1);
        Assertions.assertInstanceOf(List.class, resultRecord.get(0));
        List nestedList = (List) resultRecord.get(0);

        Assertions.assertEquals(nestedList.size(), 1);
        Assertions.assertInstanceOf(List.class, nestedList.get(0));
        List baseList = (List) nestedList.get(0);

        Assertions.assertEquals(baseList.get(0), "Test String");
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testListOrc() throws IOException {
        RecordSchema nifiSchema = getListSchema();
        Record record = setupListTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(LIST, nifiSchema, FileFormat.ORC);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeToOrc(LIST, genericRecord, tempFile);

        List<GenericRecord> results = readFromOrc(LIST, tempFile.toInputFile());

        Assertions.assertEquals(results.size(), 1);
        Assertions.assertInstanceOf(GenericRecord.class, results.get(0));
        GenericRecord resultRecord = results.get(0);

        Assertions.assertEquals(resultRecord.size(), 1);
        Assertions.assertInstanceOf(List.class, resultRecord.get(0));
        List nestedList = (List) resultRecord.get(0);

        Assertions.assertEquals(nestedList.size(), 1);
        Assertions.assertInstanceOf(List.class, nestedList.get(0));
        List baseList = (List) nestedList.get(0);

        Assertions.assertEquals(baseList.get(0), "Test String");
    }

    @Test
    public void testListParquet() throws IOException {
        RecordSchema nifiSchema = getListSchema();
        Record record = setupListTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(LIST, nifiSchema, FileFormat.PARQUET);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeToParquet(LIST, genericRecord, tempFile);

        List<GenericRecord> results = readFromParquet(LIST, tempFile.toInputFile());

        Assertions.assertEquals(results.size(), 1);
        Assertions.assertInstanceOf(GenericRecord.class, results.get(0));
        GenericRecord resultRecord = results.get(0);

        Assertions.assertEquals(resultRecord.size(), 1);
        Assertions.assertInstanceOf(List.class, resultRecord.get(0));
        List nestedList = (List) resultRecord.get(0);

        Assertions.assertEquals(nestedList.size(), 1);
        Assertions.assertInstanceOf(List.class, nestedList.get(0));
        List baseList = (List) nestedList.get(0);

        Assertions.assertEquals(baseList.get(0), "Test String");
    }

    @Test
    public void testMapAvro() throws IOException {
        RecordSchema nifiSchema = getMapSchema();
        Record record = setupMapTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(MAP, nifiSchema, FileFormat.AVRO);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeToAvro(MAP, genericRecord, tempFile);

        List<GenericRecord> results = readFromAvro(MAP, tempFile.toInputFile());

        Assertions.assertEquals(results.size(), 1);
        Assertions.assertInstanceOf(GenericRecord.class, results.get(0));
        GenericRecord resultRecord = results.get(0);

        Assertions.assertEquals(resultRecord.size(), 1);
        Assertions.assertInstanceOf(Map.class, resultRecord.get(0));
        Map nestedMap = (Map) resultRecord.get(0);

        Assertions.assertEquals(nestedMap.size(), 1);
        Assertions.assertInstanceOf(Map.class, nestedMap.get("key"));
        Map baseMap = (Map) nestedMap.get("key");

        Assertions.assertEquals(baseMap.get("nested_key"), 42L);
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testMapOrc() throws IOException {
        RecordSchema nifiSchema = getMapSchema();
        Record record = setupMapTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(MAP, nifiSchema, FileFormat.ORC);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeToOrc(MAP, genericRecord, tempFile);

        List<GenericRecord> results = readFromOrc(MAP, tempFile.toInputFile());

        Assertions.assertEquals(results.size(), 1);
        Assertions.assertInstanceOf(GenericRecord.class, results.get(0));
        GenericRecord resultRecord = results.get(0);

        Assertions.assertEquals(resultRecord.size(), 1);
        Assertions.assertInstanceOf(Map.class, resultRecord.get(0));
        Map nestedMap = (Map) resultRecord.get(0);

        Assertions.assertEquals(nestedMap.size(), 1);
        Assertions.assertInstanceOf(Map.class, nestedMap.get("key"));
        Map baseMap = (Map) nestedMap.get("key");

        Assertions.assertEquals(baseMap.get("nested_key"), 42L);
    }

    @Test
    public void testMapParquet() throws IOException {
        RecordSchema nifiSchema = getMapSchema();
        Record record = setupMapTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(MAP, nifiSchema, FileFormat.PARQUET);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeToParquet(MAP, genericRecord, tempFile);

        List<GenericRecord> results = readFromParquet(MAP, tempFile.toInputFile());

        Assertions.assertEquals(results.size(), 1);
        Assertions.assertInstanceOf(GenericRecord.class, results.get(0));
        GenericRecord resultRecord = results.get(0);

        Assertions.assertEquals(resultRecord.size(), 1);
        Assertions.assertInstanceOf(Map.class, resultRecord.get(0));
        Map nestedMap = (Map) resultRecord.get(0);

        Assertions.assertEquals(nestedMap.size(), 1);
        Assertions.assertInstanceOf(Map.class, nestedMap.get("key"));
        Map baseMap = (Map) nestedMap.get("key");

        Assertions.assertEquals(baseMap.get("nested_key"), 42L);
    }

    @Test
    public void testSchemaMismatchAvro() {
        RecordSchema nifiSchema = getListSchema();
        Record record = setupListTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(LIST, nifiSchema, FileFormat.AVRO);
        GenericRecord genericRecord = recordConverter.convert(record);

        DataFileWriter.AppendWriteException e = assertThrows(DataFileWriter.AppendWriteException.class, () -> writeToAvro(STRUCT, genericRecord, tempFile));
        assertTrue(e.getMessage().contains("java.util.ArrayList cannot be cast"), e.getMessage());
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testSchemaMismatchOrc() {
        RecordSchema nifiSchema = getListSchema();
        Record record = setupListTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(LIST, nifiSchema, FileFormat.ORC);
        GenericRecord genericRecord = recordConverter.convert(record);

        ClassCastException e = assertThrows(ClassCastException.class, () -> writeToOrc(STRUCT, genericRecord, tempFile));
        assertTrue(e.getMessage().contains("java.util.ArrayList cannot be cast"));
    }

    @Test
    public void testSchemaMismatchParquet() {
        RecordSchema nifiSchema = getListSchema();
        Record record = setupListTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(LIST, nifiSchema, FileFormat.PARQUET);
        GenericRecord genericRecord = recordConverter.convert(record);

        ClassCastException e = assertThrows(ClassCastException.class, () -> writeToParquet(STRUCT, genericRecord, tempFile));
        assertTrue(e.getMessage().contains("java.util.ArrayList cannot be cast"));
    }

    @Test
    public void testChoiceDataTypeInRecord() {
        Record record = setupChoiceTestRecord();
        DataType dataType = RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.STRING.getDataType(), RecordFieldType.INT.getDataType(), RecordFieldType.LONG.getDataType());

        RecordFieldGetter.FieldGetter fieldGetter1 = RecordFieldGetter.createFieldGetter(dataType, "choice1", true);
        RecordFieldGetter.FieldGetter fieldGetter2 = RecordFieldGetter.createFieldGetter(dataType, "choice2", true);
        RecordFieldGetter.FieldGetter fieldGetter3 = RecordFieldGetter.createFieldGetter(dataType, "choice3", true);

        Assertions.assertInstanceOf(Integer.class, fieldGetter1.getFieldOrNull(record));
        Assertions.assertInstanceOf(String.class, fieldGetter2.getFieldOrNull(record));
        Assertions.assertInstanceOf(Long.class, fieldGetter3.getFieldOrNull(record));
    }

    @Test
    public void testChoiceDataTypeInArray() {
        DataType dataType = RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.STRING.getDataType(), RecordFieldType.INT.getDataType(), RecordFieldType.LONG.getDataType());
        ArrayElementGetter.ElementGetter elementGetter = ArrayElementGetter.createElementGetter(dataType);

        String[] testArray = {"20", "30a", String.valueOf(Long.MAX_VALUE)};

        Assertions.assertInstanceOf(Integer.class, elementGetter.getElementOrNull(testArray, 0));
        Assertions.assertInstanceOf(String.class, elementGetter.getElementOrNull(testArray, 1));
        Assertions.assertInstanceOf(Long.class, elementGetter.getElementOrNull(testArray, 2));
    }

    public void writeToAvro(Schema schema, GenericRecord record, OutputFile outputFile) throws IOException {
        try (FileAppender<GenericRecord> appender = Avro.write(outputFile)
                .schema(schema)
                .createWriterFunc(DataWriter::create)
                .overwrite()
                .build()) {
            appender.add(record);
        }
    }

    public ArrayList<GenericRecord> readFromAvro(Schema schema, InputFile inputFile) throws IOException {
        try (AvroIterable<GenericRecord> reader = Avro.read(inputFile)
                .project(schema)
                .createReaderFunc(DataReader::create)
                .build()) {
            return Lists.newArrayList(reader);
        }
    }

    public void writeToOrc(Schema schema, GenericRecord record, OutputFile outputFile) throws IOException {
        try (FileAppender<GenericRecord> appender = ORC.write(outputFile)
                .schema(schema)
                .createWriterFunc(GenericOrcWriter::buildWriter)
                .overwrite()
                .build()) {
            appender.add(record);
        }
    }

    public ArrayList<GenericRecord> readFromOrc(Schema schema, InputFile inputFile) throws IOException {
        try (CloseableIterable<GenericRecord> reader = ORC.read(inputFile)
                .project(schema)
                .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
                .build()) {
            return Lists.newArrayList(reader);
        }
    }

    public void writeToParquet(Schema schema, GenericRecord record, OutputFile outputFile) throws IOException {
        try (FileAppender<GenericRecord> appender = Parquet.write(outputFile)
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .overwrite()
                .build()) {
            appender.add(record);
        }
    }

    public ArrayList<GenericRecord> readFromParquet(Schema schema, InputFile inputFile) throws IOException {
        try (CloseableIterable<GenericRecord> reader = Parquet.read(inputFile)
                .project(schema)
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
                .build()) {
            return Lists.newArrayList(reader);
        }
    }
}