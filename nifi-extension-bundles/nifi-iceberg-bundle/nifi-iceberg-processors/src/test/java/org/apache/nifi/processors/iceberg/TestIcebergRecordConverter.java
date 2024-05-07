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
import org.apache.nifi.logging.ComponentLog;
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
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.io.File.createTempFile;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

public class TestIcebergRecordConverter {

    private static final LocalDateTime LOCAL_DATE_TIME = LocalDateTime.of(2017, 4, 4, 14, 20, 33, 789000000);

    private OutputFile tempFile;

    private ComponentLog logger;

    @BeforeEach
    public void setUp() throws Exception {
        tempFile = Files.localOutput(createTempFile("test", null));
        logger = new MockComponentLog("id", "TestIcebergRecordConverter");
    }

    @AfterEach
    public void tearDown() {
        File file = new File(tempFile.location());
        file.deleteOnExit();
    }

    private static final Schema STRUCT_SCHEMA = new Schema(
            Types.NestedField.required(0, "struct", Types.StructType.of(
                    Types.NestedField.required(1, "nested_struct", Types.StructType.of(
                            Types.NestedField.required(2, "string", Types.StringType.get()),
                            Types.NestedField.required(3, "integer", Types.IntegerType.get()))
                    )
            ))
    );

    private static final Schema LIST_SCHEMA = new Schema(
            Types.NestedField.required(0, "list", Types.ListType.ofRequired(
                            1, Types.ListType.ofRequired(2, Types.StringType.get())
                    )
            )
    );

    private static final Schema MAP_SCHEMA = new Schema(
            Types.NestedField.required(0, "map", Types.MapType.ofRequired(
                    1, 2, Types.StringType.get(), Types.MapType.ofRequired(
                            3, 4, Types.StringType.get(), Types.LongType.get()
                    )
            ))
    );

    private static final Schema RECORD_IN_LIST_SCHEMA = new Schema(
            Types.NestedField.required(0, "list", Types.ListType.ofRequired(
                    1, Types.StructType.of(
                            Types.NestedField.required(2, "string", Types.StringType.get()),
                            Types.NestedField.required(3, "integer", Types.IntegerType.get())))
            )
    );

    private static final Schema RECORD_IN_MAP_SCHEMA = new Schema(
            Types.NestedField.required(0, "map", Types.MapType.ofRequired(
                    1, 2, Types.StringType.get(), Types.StructType.of(
                            Types.NestedField.required(3, "string", Types.StringType.get()),
                            Types.NestedField.required(4, "integer", Types.IntegerType.get())))
            )
    );

    private static final Schema PRIMITIVES_SCHEMA = new Schema(
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
            Types.NestedField.optional(14, "choice", Types.IntegerType.get()),
            Types.NestedField.optional(15, "enum", Types.StringType.get())
    );

    private static final Schema PRIMITIVES_SCHEMA_WITH_REQUIRED_FIELDS = new Schema(
            Types.NestedField.optional(0, "string", Types.StringType.get()),
            Types.NestedField.required(1, "integer", Types.IntegerType.get()),
            Types.NestedField.required(2, "float", Types.FloatType.get()),
            Types.NestedField.required(3, "long", Types.LongType.get()),
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

    private static final Schema COMPATIBLE_PRIMITIVES_SCHEMA = new Schema(
            Types.NestedField.optional(0, "string", Types.StringType.get()),
            Types.NestedField.optional(1, "integer", Types.IntegerType.get()),
            Types.NestedField.optional(2, "float", Types.FloatType.get()),
            Types.NestedField.optional(3, "long", Types.LongType.get()),
            Types.NestedField.optional(4, "double", Types.DoubleType.get()),
            Types.NestedField.optional(5, "decimal", Types.DecimalType.of(10, 2)),
            Types.NestedField.optional(7, "fixed", Types.FixedType.ofLength(5)),
            Types.NestedField.optional(8, "binary", Types.BinaryType.get()),
            Types.NestedField.optional(9, "date", Types.DateType.get()),
            Types.NestedField.optional(10, "time", Types.TimeType.get()),
            Types.NestedField.optional(11, "timestamp", Types.TimestampType.withZone()),
            Types.NestedField.optional(12, "timestampTz", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(13, "uuid", Types.UUIDType.get()),
            Types.NestedField.optional(14, "choice", Types.IntegerType.get())
    );

    private static final Schema CASE_INSENSITIVE_SCHEMA = new Schema(
            Types.NestedField.optional(0, "FIELD1", Types.StringType.get()),
            Types.NestedField.optional(1, "Field2", Types.StringType.get()),
            Types.NestedField.optional(2, "fielD3", Types.StringType.get()),
            Types.NestedField.optional(3, "field4", Types.StringType.get())
    );

    private static final Schema UNORDERED_SCHEMA = new Schema(
            Types.NestedField.optional(0, "field1", Types.StringType.get()),
            Types.NestedField.required(1, "field2", Types.StructType.of(
                    Types.NestedField.required(2, "field3", Types.StringType.get()),
                    Types.NestedField.required(3, "field4", Types.ListType.ofRequired(4, Types.StringType.get()))
            )),
            Types.NestedField.optional(5, "field5", Types.StringType.get()),
            Types.NestedField.required(6, "field6", Types.MapType.ofRequired(
                    7, 8, Types.StringType.get(), Types.StringType.get()
            ))
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

    private static RecordSchema getRecordInListSchema() {
        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("list", new ArrayDataType(
                new RecordDataType(getNestedStructSchema2()))));

        return new SimpleRecordSchema(fields);
    }

    private static RecordSchema getRecordInMapSchema() {
        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("map", new MapDataType(
                new RecordDataType(getNestedStructSchema2()))));

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
        fields.add(new RecordField("enum", RecordFieldType.ENUM.getEnumDataType(Arrays.asList("red", "blue", "yellow"))));

        return new SimpleRecordSchema(fields);
    }

    private static RecordSchema getPrimitivesSchemaMissingFields() {
        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("string", RecordFieldType.STRING.getDataType()));
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

    private static RecordSchema getPrimitivesAsCompatiblesSchema() {
        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("string", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("integer", RecordFieldType.SHORT.getDataType()));
        fields.add(new RecordField("float", RecordFieldType.DOUBLE.getDataType()));
        fields.add(new RecordField("long", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("double", RecordFieldType.FLOAT.getDataType()));
        fields.add(new RecordField("decimal", RecordFieldType.DOUBLE.getDataType()));
        fields.add(new RecordField("fixed", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())));
        fields.add(new RecordField("binary", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())));
        fields.add(new RecordField("date", RecordFieldType.STRING.getDataType("yyyy-MM-dd")));
        fields.add(new RecordField("time", RecordFieldType.STRING.getDataType("HH:mm:ss.SSS")));
        fields.add(new RecordField("timestamp", RecordFieldType.STRING.getDataType("yyyy-MM-dd HH:mm:ss.SSSZ")));
        fields.add(new RecordField("timestampTz", RecordFieldType.STRING.getDataType("yyyy-MM-dd HH:mm:ss.SSSZ")));
        fields.add(new RecordField("uuid", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("choice", RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.STRING.getDataType(), RecordFieldType.INT.getDataType())));

        return new SimpleRecordSchema(fields);
    }

    private static RecordSchema getCaseInsensitiveSchema() {
        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("field1", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("FIELD2", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("Field3", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("fielD4", RecordFieldType.STRING.getDataType()));

        return new SimpleRecordSchema(fields);
    }

    private static RecordSchema getNestedUnorderedSchema() {
        List<RecordField> nestedFields = new ArrayList<>();
        nestedFields.add(new RecordField("FIELD4", new ArrayDataType(RecordFieldType.STRING.getDataType())));
        nestedFields.add(new RecordField("FIELD3", RecordFieldType.STRING.getDataType()));

        return new SimpleRecordSchema(nestedFields);
    }

    private static RecordSchema getUnorderedSchema() {
        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("FIELD6", new MapDataType(RecordFieldType.STRING.getDataType())));
        fields.add(new RecordField("FIELD5", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("FIELD1", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("FIELD2", new RecordDataType(getNestedUnorderedSchema())));

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
        List<String> nestedList1 = new ArrayList<>();
        nestedList1.add("Test String1");
        nestedList1.add("Test String2");

        List<String> nestedList2 = new ArrayList<>();
        nestedList2.add("Test String3");
        nestedList2.add("Test String4");

        List<Collection<String>> list = new ArrayList<>();
        list.add(nestedList1);
        list.add(nestedList2);

        Map<String, Object> values = new HashMap<>();
        values.put("list", list);

        return new MapRecord(getListSchema(), values);
    }

    private static Record setupMapTestRecord() {
        Map<String, Long> nestedMap = new HashMap<>();
        nestedMap.put("nested_key", 42L);

        Map<String, Map<String, Long>> map = new HashMap<>();
        map.put("key", nestedMap);

        Map<String, Object> values = new HashMap<>();
        values.put("map", map);

        return new MapRecord(getMapSchema(), values);
    }

    private static Record setupRecordInListTestRecord() {
        Map<String, Object> struct1 = new HashMap<>();
        struct1.put("string", "Test String 1");
        struct1.put("integer", 10);

        Map<String, Object> struct2 = new HashMap<>();
        struct2.put("string", "Test String 2");
        struct2.put("integer", 20);

        return new MapRecord(getRecordInListSchema(), Collections.singletonMap("list", Arrays.asList(struct1, struct2)));
    }

    private static Record setupRecordInMapTestRecord() {
        Map<String, Object> struct1 = new HashMap<>();
        struct1.put("string", "Test String 1");
        struct1.put("integer", 10);

        Map<String, Object> struct2 = new HashMap<>();
        struct2.put("string", "Test String 2");
        struct2.put("integer", 20);

        Map<String, Map<String, Object>> map = new HashMap<>();
        map.put("key1", struct1);
        map.put("key2", struct2);

        return new MapRecord(getMapSchema(), Collections.singletonMap("map", map));
    }

    private static Record setupPrimitivesTestRecord() {
        LocalDate localDate = LocalDate.of(2017, 4, 4);
        LocalTime localTime = LocalTime.of(14, 20, 33);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(LOCAL_DATE_TIME, ZoneOffset.ofHours(-5));

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
        values.put("timestampTz", Timestamp.valueOf(LOCAL_DATE_TIME));
        values.put("uuid", UUID.fromString("0000-00-00-00-000000"));
        values.put("choice", "10");
        values.put("enum", "blue");

        return new MapRecord(getPrimitivesSchema(), values);
    }

    private static Record setupPrimitivesTestRecordMissingFields() {
        LocalDate localDate = LocalDate.of(2017, 4, 4);
        LocalTime localTime = LocalTime.of(14, 20, 33);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(LOCAL_DATE_TIME, ZoneOffset.ofHours(-5));

        Map<String, Object> values = new HashMap<>();
        values.put("string", "Test String");
        values.put("double", 3.14159D);
        values.put("decimal", new BigDecimal("12345678.12"));
        values.put("boolean", true);
        values.put("fixed", "hello".getBytes());
        values.put("binary", "hello".getBytes());
        values.put("date", localDate);
        values.put("time", Time.valueOf(localTime));
        values.put("timestamp", Timestamp.from(offsetDateTime.toInstant()));
        values.put("timestampTz", Timestamp.valueOf(LOCAL_DATE_TIME));
        values.put("uuid", UUID.fromString("0000-00-00-00-000000"));
        values.put("choice", "10");

        return new MapRecord(getPrimitivesSchemaMissingFields(), values);
    }

    private static Record setupCompatiblePrimitivesTestRecord() {

        Map<String, Object> values = new HashMap<>();
        values.put("string", 123);
        values.put("integer", 8);
        values.put("float", 1.23456);
        values.put("long", 42L);
        values.put("double", 3.14159);
        values.put("decimal", 12345678.12);
        values.put("fixed", "hello".getBytes());
        values.put("binary", "hello".getBytes());
        values.put("date", "2017-04-04");
        values.put("time", "14:20:33.000");
        values.put("timestamp", "2017-04-04 14:20:33.789-0500");
        values.put("timestampTz", "2017-04-04 14:20:33.789-0500");
        values.put("uuid", "0000-00-00-00-000000");
        values.put("choice", "10");

        return new MapRecord(getPrimitivesAsCompatiblesSchema(), values);
    }

    private static Record setupCaseInsensitiveTestRecord() {
        Map<String, Object> values = new HashMap<>();
        values.put("field1", "Text1");
        values.put("FIELD2", "Text2");
        values.put("Field3", "Text3");
        values.put("fielD4", "Text4");

        return new MapRecord(getCaseInsensitiveSchema(), values);
    }

    private static Record setupUnorderedTestRecord() {
        List<String> listValues = new ArrayList<>();
        listValues.add("list value2");
        listValues.add("list value1");

        Map<String, String> mapValues = new HashMap<>();
        mapValues.put("key2", "map value2");
        mapValues.put("key1", "map value1");

        Map<String, Object> nestedValues = new HashMap<>();
        nestedValues.put("FIELD4", listValues);
        nestedValues.put("FIELD3", "value3");
        MapRecord nestedRecord = new MapRecord(getNestedUnorderedSchema(), nestedValues);

        Map<String, Object> values = new HashMap<>();
        values.put("FIELD6", mapValues);
        values.put("FIELD5", "value5");
        values.put("FIELD1", "value1");
        values.put("FIELD2", nestedRecord);
        return new MapRecord(getStructSchema(), values);
    }

    private static Record setupChoiceTestRecord() {
        Map<String, Object> values = new HashMap<>();
        values.put("choice1", "20");
        values.put("choice2", "30a");
        values.put("choice3", String.valueOf(Long.MAX_VALUE));

        return new MapRecord(getChoiceSchema(), values);
    }

    @DisabledOnOs(WINDOWS)
    @ParameterizedTest
    @EnumSource(value = FileFormat.class, names = {"AVRO", "ORC", "PARQUET"})
    public void testPrimitives(FileFormat format) throws IOException {
        RecordSchema nifiSchema = getPrimitivesSchema();
        Record record = setupPrimitivesTestRecord();

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(PRIMITIVES_SCHEMA, nifiSchema, format, UnmatchedColumnBehavior.IGNORE_UNMATCHED_COLUMN, logger);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeTo(format, PRIMITIVES_SCHEMA, genericRecord, tempFile);

        List<GenericRecord> results = readFrom(format, PRIMITIVES_SCHEMA, tempFile.toInputFile());

        assertEquals(results.size(), 1);
        GenericRecord resultRecord = results.getFirst();

        OffsetDateTime offsetDateTime = OffsetDateTime.of(LOCAL_DATE_TIME, ZoneOffset.ofHours(-5));

        assertEquals("Test String", resultRecord.get(0, String.class));
        assertEquals(Integer.valueOf(8), resultRecord.get(1, Integer.class));
        assertEquals(Float.valueOf(1.23456F), resultRecord.get(2, Float.class));
        assertEquals(Long.valueOf(42L), resultRecord.get(3, Long.class));
        assertEquals(Double.valueOf(3.14159D), resultRecord.get(4, Double.class));
        assertEquals(new BigDecimal("12345678.12"), resultRecord.get(5, BigDecimal.class));
        assertEquals(Boolean.TRUE, resultRecord.get(6, Boolean.class));
        assertArrayEquals(new byte[]{104, 101, 108, 108, 111}, resultRecord.get(7, byte[].class));
        assertArrayEquals(new byte[]{104, 101, 108, 108, 111}, resultRecord.get(8, ByteBuffer.class).array());
        assertEquals(LocalDate.of(2017, 4, 4), resultRecord.get(9, LocalDate.class));
        assertEquals(LocalTime.of(14, 20, 33), resultRecord.get(10, LocalTime.class));
        assertEquals(offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC), resultRecord.get(11, OffsetDateTime.class));
        assertEquals(LOCAL_DATE_TIME, resultRecord.get(12, LocalDateTime.class));
        assertEquals(Integer.valueOf(10), resultRecord.get(14, Integer.class));
        assertEquals("blue", resultRecord.get(15, String.class));

        if (format.equals(PARQUET)) {
            assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, resultRecord.get(13, byte[].class));
        } else {
            assertEquals(UUID.fromString("0000-00-00-00-000000"), resultRecord.get(13, UUID.class));
        }
    }

    @DisabledOnOs(WINDOWS)
    @ParameterizedTest
    @EnumSource(value = FileFormat.class, names = {"AVRO", "ORC", "PARQUET"})
    public void testPrimitivesIgnoreMissingFields(FileFormat format) throws IOException {
        RecordSchema nifiSchema = getPrimitivesSchemaMissingFields();
        Record record = setupPrimitivesTestRecordMissingFields();
        MockComponentLog mockComponentLogger = new MockComponentLog("id", "TestIcebergRecordConverter");

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(PRIMITIVES_SCHEMA, nifiSchema, format, UnmatchedColumnBehavior.IGNORE_UNMATCHED_COLUMN, mockComponentLogger);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeTo(format, PRIMITIVES_SCHEMA, genericRecord, tempFile);

        List<GenericRecord> results = readFrom(format, PRIMITIVES_SCHEMA, tempFile.toInputFile());

        assertEquals(results.size(), 1);
        GenericRecord resultRecord = results.getFirst();

        OffsetDateTime offsetDateTime = OffsetDateTime.of(LOCAL_DATE_TIME, ZoneOffset.ofHours(-5));

        assertEquals("Test String", resultRecord.get(0, String.class));
        assertNull(resultRecord.get(1, Integer.class));
        assertNull(resultRecord.get(2, Float.class));
        assertNull(resultRecord.get(3, Long.class));
        assertEquals(Double.valueOf(3.14159D), resultRecord.get(4, Double.class));
        assertEquals(new BigDecimal("12345678.12"), resultRecord.get(5, BigDecimal.class));
        assertEquals(Boolean.TRUE, resultRecord.get(6, Boolean.class));
        assertArrayEquals(new byte[]{104, 101, 108, 108, 111}, resultRecord.get(7, byte[].class));
        assertArrayEquals(new byte[]{104, 101, 108, 108, 111}, resultRecord.get(8, ByteBuffer.class).array());
        assertEquals(LocalDate.of(2017, 4, 4), resultRecord.get(9, LocalDate.class));
        assertEquals(LocalTime.of(14, 20, 33), resultRecord.get(10, LocalTime.class));
        assertEquals(offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC), resultRecord.get(11, OffsetDateTime.class));
        assertEquals(LOCAL_DATE_TIME, resultRecord.get(12, LocalDateTime.class));
        assertEquals(Integer.valueOf(10), resultRecord.get(14, Integer.class));

        if (format.equals(FileFormat.PARQUET)) {
            // Parquet uses a conversion to the byte values of numeric characters such as "0" -> byte value 0
            UUID uuid = UUID.fromString("0000-00-00-00-000000");
            ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
            byteBuffer.putLong(uuid.getMostSignificantBits());
            byteBuffer.putLong(uuid.getLeastSignificantBits());
            assertArrayEquals(byteBuffer.array(), resultRecord.get(13, byte[].class));
        } else {
            assertEquals(UUID.fromString("0000-00-00-00-000000"), resultRecord.get(13, UUID.class));
        }

        // Test null values
        for (String fieldName : record.getRawFieldNames()) {
            record.setValue(fieldName, null);
        }

        genericRecord = recordConverter.convert(record);

        writeTo(format, PRIMITIVES_SCHEMA, genericRecord, tempFile);

        results = readFrom(format, PRIMITIVES_SCHEMA, tempFile.toInputFile());

        assertEquals(results.size(), 1);
        resultRecord = results.getFirst();
        assertNull(resultRecord.get(0, String.class));
        assertNull(resultRecord.get(1, Integer.class));
        assertNull(resultRecord.get(2, Float.class));
        assertNull(resultRecord.get(3, Long.class));
        assertNull(resultRecord.get(4, Double.class));
        assertNull(resultRecord.get(5, BigDecimal.class));
        assertNull(resultRecord.get(6, Boolean.class));
        assertNull(resultRecord.get(7));
        assertNull(resultRecord.get(8));
        assertNull(resultRecord.get(9, LocalDate.class));
        assertNull(resultRecord.get(10, LocalTime.class));
        assertNull(resultRecord.get(11, OffsetDateTime.class));
        assertNull(resultRecord.get(14, Integer.class));
    }

    @DisabledOnOs(WINDOWS)
    @ParameterizedTest
    @EnumSource(value = FileFormat.class, names = {"AVRO", "ORC", "PARQUET"})
    public void testPrimitivesMissingRequiredFields(FileFormat format) {
        RecordSchema nifiSchema = getPrimitivesSchemaMissingFields();
        MockComponentLog mockComponentLogger = new MockComponentLog("id", "TestIcebergRecordConverter");

        assertThrows(IllegalArgumentException.class,
                () -> new IcebergRecordConverter(PRIMITIVES_SCHEMA_WITH_REQUIRED_FIELDS, nifiSchema, format, UnmatchedColumnBehavior.IGNORE_UNMATCHED_COLUMN, mockComponentLogger));
    }

    @DisabledOnOs(WINDOWS)
    @ParameterizedTest
    @EnumSource(value = FileFormat.class, names = {"AVRO", "ORC", "PARQUET"})
    public void testPrimitivesWarnMissingFields(FileFormat format) throws IOException {
        RecordSchema nifiSchema = getPrimitivesSchemaMissingFields();
        Record record = setupPrimitivesTestRecordMissingFields();
        MockComponentLog mockComponentLogger = new MockComponentLog("id", "TestIcebergRecordConverter");

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(PRIMITIVES_SCHEMA, nifiSchema, format, UnmatchedColumnBehavior.WARNING_UNMATCHED_COLUMN, mockComponentLogger);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeTo(format, PRIMITIVES_SCHEMA, genericRecord, tempFile);

        List<GenericRecord> results = readFrom(format, PRIMITIVES_SCHEMA, tempFile.toInputFile());

        assertEquals(results.size(), 1);
        GenericRecord resultRecord = results.getFirst();

        OffsetDateTime offsetDateTime = OffsetDateTime.of(LOCAL_DATE_TIME, ZoneOffset.ofHours(-5));

        assertEquals("Test String", resultRecord.get(0, String.class));
        assertNull(resultRecord.get(1, Integer.class));
        assertNull(resultRecord.get(2, Float.class));
        assertNull(resultRecord.get(3, Long.class));
        assertEquals(Double.valueOf(3.14159D), resultRecord.get(4, Double.class));
        assertEquals(new BigDecimal("12345678.12"), resultRecord.get(5, BigDecimal.class));
        assertEquals(Boolean.TRUE, resultRecord.get(6, Boolean.class));
        assertArrayEquals(new byte[]{104, 101, 108, 108, 111}, resultRecord.get(7, byte[].class));
        assertArrayEquals(new byte[]{104, 101, 108, 108, 111}, resultRecord.get(8, ByteBuffer.class).array());
        assertEquals(LocalDate.of(2017, 4, 4), resultRecord.get(9, LocalDate.class));
        assertEquals(LocalTime.of(14, 20, 33), resultRecord.get(10, LocalTime.class));
        assertEquals(offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC), resultRecord.get(11, OffsetDateTime.class));
        assertEquals(LOCAL_DATE_TIME, resultRecord.get(12, LocalDateTime.class));
        assertEquals(Integer.valueOf(10), resultRecord.get(14, Integer.class));

        if (format.equals(FileFormat.PARQUET)) {
            // Parquet uses a conversion to the byte values of numeric characters such as "0" -> byte value 0
            UUID uuid = UUID.fromString("0000-00-00-00-000000");
            ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
            byteBuffer.putLong(uuid.getMostSignificantBits());
            byteBuffer.putLong(uuid.getLeastSignificantBits());
            assertArrayEquals(byteBuffer.array(), resultRecord.get(13, byte[].class));
        } else {
            assertEquals(UUID.fromString("0000-00-00-00-000000"), resultRecord.get(13, UUID.class));
        }
    }

    @DisabledOnOs(WINDOWS)
    @ParameterizedTest
    @EnumSource(value = FileFormat.class, names = {"AVRO", "ORC", "PARQUET"})
    public void testPrimitivesFailMissingFields(FileFormat format) {
        RecordSchema nifiSchema = getPrimitivesSchemaMissingFields();
        MockComponentLog mockComponentLogger = new MockComponentLog("id", "TestIcebergRecordConverter");

        assertThrows(IllegalArgumentException.class,
                () -> new IcebergRecordConverter(PRIMITIVES_SCHEMA, nifiSchema, format, UnmatchedColumnBehavior.FAIL_UNMATCHED_COLUMN, mockComponentLogger));
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testCompatiblePrimitives() throws IOException {
        RecordSchema nifiSchema = getPrimitivesAsCompatiblesSchema();
        Record record = setupCompatiblePrimitivesTestRecord();
        final FileFormat format = PARQUET;

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(COMPATIBLE_PRIMITIVES_SCHEMA, nifiSchema, format, UnmatchedColumnBehavior.FAIL_UNMATCHED_COLUMN, logger);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeTo(format, COMPATIBLE_PRIMITIVES_SCHEMA, genericRecord, tempFile);

        List<GenericRecord> results = readFrom(format, COMPATIBLE_PRIMITIVES_SCHEMA, tempFile.toInputFile());

        assertEquals(results.size(), 1);
        GenericRecord resultRecord = results.getFirst();

        OffsetDateTime offsetDateTime = OffsetDateTime.of(LOCAL_DATE_TIME, ZoneOffset.ofHours(-5));

        assertEquals("123", resultRecord.get(0, String.class));
        assertEquals(Integer.valueOf(8), resultRecord.get(1, Integer.class));
        assertEquals(Float.valueOf(1.23456F), resultRecord.get(2, Float.class));
        assertEquals(Long.valueOf(42L), resultRecord.get(3, Long.class));
        assertEquals(Double.valueOf(3.141590118408203), resultRecord.get(4, Double.class));
        assertEquals(new BigDecimal("12345678.12"), resultRecord.get(5, BigDecimal.class));
        assertArrayEquals(new byte[]{104, 101, 108, 108, 111}, resultRecord.get(6, byte[].class));
        assertArrayEquals(new byte[]{104, 101, 108, 108, 111}, resultRecord.get(7, ByteBuffer.class).array());
        assertEquals(LocalDate.of(2017, 4, 4), resultRecord.get(8, LocalDate.class));
        assertEquals(LocalTime.of(14, 20, 33), resultRecord.get(9, LocalTime.class));
        assertEquals(offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC), resultRecord.get(10, OffsetDateTime.class));
        assertEquals(LOCAL_DATE_TIME, resultRecord.get(11, LocalDateTime.class));
        assertEquals(Integer.valueOf(10), resultRecord.get(13, Integer.class));

        assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, resultRecord.get(12, byte[].class));
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testStruct() throws IOException {
        RecordSchema nifiSchema = getStructSchema();
        Record record = setupStructTestRecord();
        final FileFormat format = FileFormat.ORC;

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(STRUCT_SCHEMA, nifiSchema, format, UnmatchedColumnBehavior.IGNORE_UNMATCHED_COLUMN, logger);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeTo(format, STRUCT_SCHEMA, genericRecord, tempFile);

        List<GenericRecord> results = readFrom(format, STRUCT_SCHEMA, tempFile.toInputFile());

        assertEquals(1, results.size());
        GenericRecord resultRecord = results.getFirst();

        assertEquals(1, resultRecord.size());
        assertInstanceOf(GenericRecord.class, resultRecord.get(0));
        GenericRecord nestedRecord = (GenericRecord) resultRecord.get(0);

        assertEquals(1, nestedRecord.size());
        assertInstanceOf(GenericRecord.class, nestedRecord.get(0));
        GenericRecord baseRecord = (GenericRecord) nestedRecord.get(0);

        assertEquals("Test String", baseRecord.get(0, String.class));
        assertEquals(Integer.valueOf(10), baseRecord.get(1, Integer.class));
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testList() throws IOException {
        RecordSchema nifiSchema = getListSchema();
        Record record = setupListTestRecord();
        final FileFormat format = FileFormat.AVRO;

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(LIST_SCHEMA, nifiSchema, format, UnmatchedColumnBehavior.IGNORE_UNMATCHED_COLUMN, logger);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeTo(format, LIST_SCHEMA, genericRecord, tempFile);

        List<GenericRecord> results = readFrom(format, LIST_SCHEMA, tempFile.toInputFile());

        assertEquals(1, results.size());
        GenericRecord resultRecord = results.getFirst();

        assertEquals(1, resultRecord.size());
        assertInstanceOf(List.class, resultRecord.get(0));
        List<?> nestedList = resultRecord.get(0, List.class);

        assertEquals(2, nestedList.size());
        assertInstanceOf(List.class, nestedList.get(0));
        assertInstanceOf(List.class, nestedList.get(1));

        assertTrue(((List<String>) nestedList.get(0)).containsAll(List.of("Test String1", "Test String2")));
        assertTrue(((List<String>) nestedList.get(1)).containsAll(List.of("Test String3", "Test String4")));
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testMap() throws IOException {
        RecordSchema nifiSchema = getMapSchema();
        Record record = setupMapTestRecord();
        final FileFormat format = PARQUET;

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(MAP_SCHEMA, nifiSchema, format, UnmatchedColumnBehavior.IGNORE_UNMATCHED_COLUMN, logger);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeTo(format, MAP_SCHEMA, genericRecord, tempFile);

        List<GenericRecord> results = readFrom(format, MAP_SCHEMA, tempFile.toInputFile());

        assertEquals(1, results.size());
        GenericRecord resultRecord = results.getFirst();

        assertEquals(1, resultRecord.size());
        assertInstanceOf(Map.class, resultRecord.get(0));
        Map nestedMap = resultRecord.get(0, Map.class);

        assertEquals(1, nestedMap.size());
        assertInstanceOf(Map.class, nestedMap.get("key"));
        Map baseMap = (Map) nestedMap.get("key");

        assertEquals(42L, baseMap.get("nested_key"));
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testRecordInList() throws IOException {
        RecordSchema nifiSchema = getRecordInListSchema();
        Record record = setupRecordInListTestRecord();
        final FileFormat format = FileFormat.AVRO;

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(RECORD_IN_LIST_SCHEMA, nifiSchema, format, UnmatchedColumnBehavior.IGNORE_UNMATCHED_COLUMN, logger);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeTo(format, RECORD_IN_LIST_SCHEMA, genericRecord, tempFile);

        List<GenericRecord> results = readFrom(format, RECORD_IN_LIST_SCHEMA, tempFile.toInputFile());

        assertEquals(1, results.size());
        assertInstanceOf(GenericRecord.class, results.get(0));
        GenericRecord resultRecord = results.get(0);

        assertEquals(1, resultRecord.size());
        assertInstanceOf(List.class, resultRecord.get(0));
        List<?> fieldList = resultRecord.get(0, List.class);

        assertEquals(2, fieldList.size());
        assertInstanceOf(GenericRecord.class, fieldList.get(0));
        assertInstanceOf(GenericRecord.class, fieldList.get(1));

        GenericRecord record1 = (GenericRecord) fieldList.get(0);
        GenericRecord record2 = (GenericRecord) fieldList.get(1);

        assertEquals("Test String 1", record1.get(0, String.class));
        assertEquals(Integer.valueOf(10), record1.get(1, Integer.class));

        assertEquals("Test String 2", record2.get(0, String.class));
        assertEquals(Integer.valueOf(20), record2.get(1, Integer.class));
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testRecordInMap() throws IOException {
        RecordSchema nifiSchema = getRecordInMapSchema();
        Record record = setupRecordInMapTestRecord();
        final FileFormat format = FileFormat.ORC;

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(RECORD_IN_MAP_SCHEMA, nifiSchema, format, UnmatchedColumnBehavior.IGNORE_UNMATCHED_COLUMN, logger);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeTo(format, RECORD_IN_MAP_SCHEMA, genericRecord, tempFile);

        List<GenericRecord> results = readFrom(format, RECORD_IN_MAP_SCHEMA, tempFile.toInputFile());

        assertEquals(1, results.size());
        assertInstanceOf(GenericRecord.class, results.get(0));
        GenericRecord resultRecord = results.get(0);

        assertEquals(1, resultRecord.size());
        assertInstanceOf(Map.class, resultRecord.get(0));
        Map recordMap = resultRecord.get(0, Map.class);

        assertEquals(2, recordMap.size());
        assertInstanceOf(GenericRecord.class, recordMap.get("key1"));
        assertInstanceOf(GenericRecord.class, recordMap.get("key2"));

        GenericRecord record1 = (GenericRecord) recordMap.get("key1");
        GenericRecord record2 = (GenericRecord) recordMap.get("key2");

        assertEquals("Test String 1", record1.get(0, String.class));
        assertEquals(Integer.valueOf(10), record1.get(1, Integer.class));

        assertEquals("Test String 2", record2.get(0, String.class));
        assertEquals(Integer.valueOf(20), record2.get(1, Integer.class));
    }

    @DisabledOnOs(WINDOWS)
    @ParameterizedTest
    @EnumSource(value = FileFormat.class, names = {"AVRO", "ORC", "PARQUET"})
    public void testSchemaMismatch(FileFormat format) {
        RecordSchema nifiSchema = getPrimitivesSchemaMissingFields();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new IcebergRecordConverter(PRIMITIVES_SCHEMA_WITH_REQUIRED_FIELDS, nifiSchema, format, UnmatchedColumnBehavior.IGNORE_UNMATCHED_COLUMN, logger));
        assertTrue(e.getMessage().contains("Iceberg requires a non-null value for required fields"), e.getMessage());
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testCaseInsensitiveFieldMapping() throws IOException {
        RecordSchema nifiSchema = getCaseInsensitiveSchema();
        Record record = setupCaseInsensitiveTestRecord();
        final FileFormat format = FileFormat.AVRO;

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(CASE_INSENSITIVE_SCHEMA, nifiSchema, format, UnmatchedColumnBehavior.IGNORE_UNMATCHED_COLUMN, logger);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeTo(format, CASE_INSENSITIVE_SCHEMA, genericRecord, tempFile);

        List<GenericRecord> results = readFrom(format, CASE_INSENSITIVE_SCHEMA, tempFile.toInputFile());

        assertEquals(1, results.size());
        GenericRecord resultRecord = results.getFirst();

        assertEquals("Text1", resultRecord.get(0, String.class));
        assertEquals("Text2", resultRecord.get(1, String.class));
        assertEquals("Text3", resultRecord.get(2, String.class));
        assertEquals("Text4", resultRecord.get(3, String.class));
    }

    @DisabledOnOs(WINDOWS)
    @Test
    public void testUnorderedFieldMapping() throws IOException {
        RecordSchema nifiSchema = getUnorderedSchema();
        Record record = setupUnorderedTestRecord();
        final FileFormat format = PARQUET;

        IcebergRecordConverter recordConverter = new IcebergRecordConverter(UNORDERED_SCHEMA, nifiSchema, format, UnmatchedColumnBehavior.IGNORE_UNMATCHED_COLUMN, logger);
        GenericRecord genericRecord = recordConverter.convert(record);

        writeTo(format, UNORDERED_SCHEMA, genericRecord, tempFile);

        List<GenericRecord> results = readFrom(format, UNORDERED_SCHEMA, tempFile.toInputFile());

        assertEquals(1, results.size());
        GenericRecord resultRecord = results.getFirst();

        assertEquals("value1", resultRecord.get(0, String.class));

        assertInstanceOf(GenericRecord.class, resultRecord.get(1));
        GenericRecord nestedRecord = (GenericRecord) resultRecord.get(1);

        assertEquals("value3", nestedRecord.get(0, String.class));

        assertInstanceOf(List.class, nestedRecord.get(1));
        List<String> nestedList = nestedRecord.get(1, List.class);
        assertTrue(nestedList.containsAll(List.of("list value1", "list value2")));

        assertEquals("value5", resultRecord.get(2, String.class));

        assertInstanceOf(Map.class, resultRecord.get(3));
        Map<?,?> map = resultRecord.get(3, Map.class);
        assertEquals("map value1", map.get("key1"));
        assertEquals("map value2", map.get("key2"));
    }

    @Test
    public void testChoiceDataTypeInRecord() {
        Record record = setupChoiceTestRecord();
        DataType dataType = RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.STRING.getDataType(), RecordFieldType.INT.getDataType(), RecordFieldType.LONG.getDataType());

        RecordFieldGetter.FieldGetter fieldGetter1 = RecordFieldGetter.createFieldGetter(dataType, "choice1", true);
        RecordFieldGetter.FieldGetter fieldGetter2 = RecordFieldGetter.createFieldGetter(dataType, "choice2", true);
        RecordFieldGetter.FieldGetter fieldGetter3 = RecordFieldGetter.createFieldGetter(dataType, "choice3", true);

        assertInstanceOf(Integer.class, fieldGetter1.getFieldOrNull(record));
        assertInstanceOf(String.class, fieldGetter2.getFieldOrNull(record));
        assertInstanceOf(Long.class, fieldGetter3.getFieldOrNull(record));
    }

    @Test
    public void testChoiceDataTypeInArray() {
        DataType dataType = RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.STRING.getDataType(), RecordFieldType.INT.getDataType(), RecordFieldType.LONG.getDataType());
        ArrayElementGetter.ElementGetter elementGetter = ArrayElementGetter.createElementGetter(dataType);

        String[] testArray = {"20", "30a", String.valueOf(Long.MAX_VALUE)};

        assertInstanceOf(Integer.class, elementGetter.getElementOrNull(testArray[0]));
        assertInstanceOf(String.class, elementGetter.getElementOrNull(testArray[1]));
        assertInstanceOf(Long.class, elementGetter.getElementOrNull(testArray[2]));
    }

    private void writeTo(FileFormat format, Schema schema, GenericRecord record, OutputFile outputFile) throws IOException {
        switch (format) {
            case AVRO:
                writeToAvro(schema, record, outputFile);
                break;
            case ORC:
                writeToOrc(schema, record, outputFile);
                break;
            case PARQUET:
                writeToParquet(schema, record, outputFile);
                break;
        }
    }

    private ArrayList<GenericRecord> readFrom(FileFormat format, Schema schema, InputFile inputFile) throws IOException {
        return switch (format) {
            case AVRO -> readFromAvro(schema, inputFile);
            case ORC -> readFromOrc(schema, inputFile);
            case PARQUET -> readFromParquet(schema, inputFile);
            default -> throw new IOException("Unknown file format: " + format);
        };
    }

    private void writeToAvro(Schema schema, GenericRecord record, OutputFile outputFile) throws IOException {
        try (FileAppender<GenericRecord> appender = Avro.write(outputFile)
                .schema(schema)
                .createWriterFunc(DataWriter::create)
                .overwrite()
                .build()) {
            appender.add(record);
        }
    }

    private ArrayList<GenericRecord> readFromAvro(Schema schema, InputFile inputFile) throws IOException {
        try (AvroIterable<GenericRecord> reader = Avro.read(inputFile)
                .project(schema)
                .createReaderFunc(DataReader::create)
                .build()) {
            return Lists.newArrayList(reader);
        }
    }

    private void writeToOrc(Schema schema, GenericRecord record, OutputFile outputFile) throws IOException {
        try (FileAppender<GenericRecord> appender = ORC.write(outputFile)
                .schema(schema)
                .createWriterFunc(GenericOrcWriter::buildWriter)
                .overwrite()
                .build()) {
            appender.add(record);
        }
    }

    private ArrayList<GenericRecord> readFromOrc(Schema schema, InputFile inputFile) throws IOException {
        try (CloseableIterable<GenericRecord> reader = ORC.read(inputFile)
                .project(schema)
                .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
                .build()) {
            return Lists.newArrayList(reader);
        }
    }

    private void writeToParquet(Schema schema, GenericRecord record, OutputFile outputFile) throws IOException {
        try (FileAppender<GenericRecord> appender = Parquet.write(outputFile)
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .overwrite()
                .build()) {
            appender.add(record);
        }
    }

    private ArrayList<GenericRecord> readFromParquet(Schema schema, InputFile inputFile) throws IOException {
        try (CloseableIterable<GenericRecord> reader = Parquet.read(inputFile)
                .project(schema)
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
                .build()) {
            return Lists.newArrayList(reader);
        }
    }
}