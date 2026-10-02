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
package org.apache.nifi.processors.iceberg.record;

import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

class RecordConverterTest {

    private static final String CITY_FIELD_NAME = "city";

    private static final String CREATED_FIELD_NAME = "created";

    private static final String NAME_FIELD_NAME = "name";

    private static final String ITEMS_FIELD_NAME = "items";

    private static final String ADDRESS_FIELD_NAME = "address";

    private static final String ID_FIELD_NAME = "id";

    private static final String DATA_FIELD_NAME = "data";

    private static final String IDENTIFIER_FIELD_NAME = "identifier";

    private static final String CITY_FIELD_VALUE = "Berlin";

    private static final String NAME_FIELD_VALUE = "widget";

    private static final String ID_FIELD_VALUE = "row-1";

    private static final byte[] DATA_FIELD_VALUE = new byte[] {1, 2, 3, 4};

    private static final UUID IDENTIFIER_FIELD_VALUE = UUID.fromString("0f8fad5b-d9cb-469f-a165-70867728950e");

    private static final LocalDateTime CREATED_LOCAL_DATE_TIME = LocalDateTime.of(2026, 1, 1, 12, 30, 45);

    @Test
    void testConvertPrimitiveArrayToList() {
        final Types.ListType listType = Types.ListType.ofOptional(1, Types.StringType.get());
        final Object[] array = new Object[] {"a", "b", "c"};

        final Object converted = RecordConverter.convertValue(array, listType);

        final List<?> list = assertInstanceOf(List.class, converted);
        assertEquals(List.of("a", "b", "c"), list);
    }

    @Test
    void testConvertArrayElementDateTime() {
        final Types.ListType listType = Types.ListType.ofOptional(1, Types.DateType.get());
        final Object[] array = new Object[] {java.sql.Date.valueOf("2026-02-03")};

        final Object converted = RecordConverter.convertValue(array, listType);

        final List<?> list = assertInstanceOf(List.class, converted);
        assertEquals(List.of(LocalDate.of(2026, 2, 3)), list);
    }

    @Test
    void testConvertNestedRecordToStructLike() {
        final Types.StructType structType = Types.StructType.of(
                Types.NestedField.optional(1, CITY_FIELD_NAME, Types.StringType.get())
        );

        final RecordSchema nestedSchema = new SimpleRecordSchema(List.of(
                new RecordField(CITY_FIELD_NAME, RecordFieldType.STRING.getDataType())
        ));
        final Map<String, Object> nestedValues = new LinkedHashMap<>();
        nestedValues.put(CITY_FIELD_NAME, CITY_FIELD_VALUE);
        final Record nestedRecord = new MapRecord(nestedSchema, nestedValues);

        final Object converted = RecordConverter.convertValue(nestedRecord, structType);

        final StructLike struct = assertInstanceOf(StructLike.class, converted);
        assertEquals(CITY_FIELD_VALUE, struct.get(0, String.class));
    }

    @Test
    void testConvertNestedRecordDateTimeField() {
        final Types.StructType structType = Types.StructType.of(
                Types.NestedField.optional(1, CREATED_FIELD_NAME, Types.TimestampType.withoutZone())
        );

        final RecordSchema nestedSchema = new SimpleRecordSchema(List.of(
                new RecordField(CREATED_FIELD_NAME, RecordFieldType.TIMESTAMP.getDataType())
        ));
        final Map<String, Object> nestedValues = new LinkedHashMap<>();
        nestedValues.put(CREATED_FIELD_NAME, Timestamp.valueOf("2026-01-01 12:30:45"));
        final Record nestedRecord = new MapRecord(nestedSchema, nestedValues);

        final Object converted = RecordConverter.convertValue(nestedRecord, structType);

        final StructLike struct = assertInstanceOf(StructLike.class, converted);
        assertEquals(LocalDateTime.of(2026, 1, 1, 12, 30, 45), struct.get(0, LocalDateTime.class));
    }

    /**
     * Iceberg Types not adjusted to UTC require a LocalDateTime. The Iceberg Type is not resolved for every field,
     * so an unknown Type must retain the same conversion.
     */
    @ParameterizedTest
    @MethodSource
    void testConvertTimestampNotAdjustedToUtc(final Type icebergType) {
        final Timestamp timestamp = Timestamp.valueOf(CREATED_LOCAL_DATE_TIME);

        final Object converted = RecordConverter.convertValue(timestamp, icebergType);

        assertEquals(CREATED_LOCAL_DATE_TIME, converted);
    }

    private static Stream<Arguments> testConvertTimestampNotAdjustedToUtc() {
        return Stream.of(
                Arguments.of(Types.TimestampType.withoutZone()),
                Arguments.of(Types.TimestampNanoType.withoutZone()),
                Arguments.of((Type) null)
        );
    }

    /**
     * Iceberg timestamptz columns require an OffsetDateTime rather than a LocalDateTime. A Timestamp identifies an
     * instant, so the converted value must describe that same instant expressed at UTC.
     */
    @ParameterizedTest
    @MethodSource
    void testConvertTimestampAdjustedToUtc(final Type icebergType) {
        final Timestamp timestamp = Timestamp.valueOf(CREATED_LOCAL_DATE_TIME);

        final Object converted = RecordConverter.convertValue(timestamp, icebergType);

        final OffsetDateTime offsetDateTime = assertInstanceOf(OffsetDateTime.class, converted);
        assertEquals(ZoneOffset.UTC, offsetDateTime.getOffset());
        assertEquals(timestamp.toInstant(), offsetDateTime.toInstant());
    }

    private static Stream<Arguments> testConvertTimestampAdjustedToUtc() {
        return Stream.of(
                Arguments.of(Types.TimestampType.withZone()),
                Arguments.of(Types.TimestampNanoType.withZone())
        );
    }

    /**
     * A timestamptz column nested inside a struct must be converted through the recursive path, which requires the
     * Iceberg Type of the nested field to be resolved and passed down.
     */
    @Test
    void testGetConvertedRecordNestedTimestampWithZone() {
        final Types.StructType structType = Types.StructType.of(
                Types.NestedField.optional(1, CREATED_FIELD_NAME, Types.TimestampType.withZone())
        );

        final RecordSchema nestedSchema = new SimpleRecordSchema(List.of(
                new RecordField(CREATED_FIELD_NAME, RecordFieldType.TIMESTAMP.getDataType())
        ));
        final Timestamp timestamp = Timestamp.valueOf(CREATED_LOCAL_DATE_TIME);
        final Map<String, Object> nestedValues = new LinkedHashMap<>();
        nestedValues.put(CREATED_FIELD_NAME, timestamp);
        final Record nestedRecord = new MapRecord(nestedSchema, nestedValues);

        final Object converted = RecordConverter.convertValue(nestedRecord, structType);

        final StructLike struct = assertInstanceOf(StructLike.class, converted);
        assertEquals(timestamp.toInstant(), struct.get(0, OffsetDateTime.class).toInstant());
    }

    @Test
    void testConvertMapValues() {
        final Types.MapType mapType = Types.MapType.ofOptional(
                1, 2, Types.StringType.get(), Types.StringType.get()
        );
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put(CITY_FIELD_NAME, CITY_FIELD_VALUE);
        map.put(NAME_FIELD_NAME, NAME_FIELD_VALUE);

        final Object converted = RecordConverter.convertValue(map, mapType);

        final Map<?, ?> resultMap = assertInstanceOf(Map.class, converted);
        assertEquals(CITY_FIELD_VALUE, resultMap.get(CITY_FIELD_NAME));
        assertEquals(NAME_FIELD_VALUE, resultMap.get(NAME_FIELD_NAME));
    }

    @Test
    void testConvertMapDateTimeValue() {
        final Types.MapType mapType = Types.MapType.ofOptional(
                1, 2, Types.StringType.get(), Types.DateType.get()
        );
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put(CREATED_FIELD_NAME, java.sql.Date.valueOf("2026-02-03"));

        final Object converted = RecordConverter.convertValue(map, mapType);

        final Map<?, ?> resultMap = assertInstanceOf(Map.class, converted);
        assertEquals(LocalDate.of(2026, 2, 3), resultMap.get(CREATED_FIELD_NAME));
    }

    @Test
    void testGetConvertedRecordArrayOfStructs() {
        final Types.StructType elementStruct = Types.StructType.of(
                Types.NestedField.optional(2, NAME_FIELD_NAME, Types.StringType.get())
        );
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(1, ITEMS_FIELD_NAME,
                        Types.ListType.ofOptional(3, elementStruct))
        );

        final RecordSchema elementSchema = new SimpleRecordSchema(List.of(
                new RecordField(NAME_FIELD_NAME, RecordFieldType.STRING.getDataType())
        ));
        final Map<String, Object> elementValues = new LinkedHashMap<>();
        elementValues.put(NAME_FIELD_NAME, NAME_FIELD_VALUE);
        final Record element = new MapRecord(elementSchema, elementValues);

        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField(ITEMS_FIELD_NAME,
                        RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(elementSchema)))
        ));
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(ITEMS_FIELD_NAME, new Object[] {element});
        final Record record = new MapRecord(schema, values);

        final org.apache.iceberg.data.Record converted = new DelegatedRecord(record, struct);
        final Object items = converted.getField(ITEMS_FIELD_NAME);

        final List<?> list = assertInstanceOf(List.class, items);
        final StructLike first = assertInstanceOf(StructLike.class, list.get(0));
        assertEquals(NAME_FIELD_VALUE, first.get(0, String.class));
    }

    /**
     * A Record field declared as CHOICE, as schema inference produces when a field is an object in some Records and a
     * scalar in others, must still be converted. Conversion is driven by the Iceberg type rather than the Record field
     * type, so the CHOICE needs no dedicated handling, but it must not short circuit conversion of the whole Record.
     */
    @Test
    void testGetConvertedRecordChoiceFieldWithScalarSiblings() {
        final Types.StructType nestedStruct = Types.StructType.of(
                Types.NestedField.optional(2, CITY_FIELD_NAME, Types.StringType.get())
        );
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(1, ADDRESS_FIELD_NAME, nestedStruct),
                Types.NestedField.optional(3, ID_FIELD_NAME, Types.StringType.get())
        );

        final RecordSchema nestedSchema = new SimpleRecordSchema(List.of(
                new RecordField(CITY_FIELD_NAME, RecordFieldType.STRING.getDataType())
        ));
        final Map<String, Object> nestedValues = new LinkedHashMap<>();
        nestedValues.put(CITY_FIELD_NAME, CITY_FIELD_VALUE);
        final Record nestedRecord = new MapRecord(nestedSchema, nestedValues);

        // Every field other than the CHOICE is a scalar, so the CHOICE alone must require conversion
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField(ADDRESS_FIELD_NAME, RecordFieldType.CHOICE.getChoiceDataType(
                        RecordFieldType.RECORD.getRecordDataType(nestedSchema),
                        RecordFieldType.STRING.getDataType())),
                new RecordField(ID_FIELD_NAME, RecordFieldType.STRING.getDataType())
        ));
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(ADDRESS_FIELD_NAME, nestedRecord);
        values.put(ID_FIELD_NAME, ID_FIELD_VALUE);
        final Record record = new MapRecord(schema, values);

        final org.apache.iceberg.data.Record converted = new DelegatedRecord(record, struct);
        final Object address = converted.getField(ADDRESS_FIELD_NAME);

        final StructLike addressStruct = assertInstanceOf(StructLike.class, address);
        assertEquals(CITY_FIELD_VALUE, addressStruct.get(0, String.class));
        assertEquals(ID_FIELD_VALUE, converted.getField(ID_FIELD_NAME));
    }

    /**
     * Record Readers provide bytes as byte arrays or as Object arrays of Byte elements, and Iceberg binary, geometry, and
     * geography columns require ByteBuffer values.
     */
    @ParameterizedTest
    @MethodSource
    void testConvertBytesToByteBuffer(final Type icebergType, final Object value) {
        final Object converted = RecordConverter.convertValue(value, icebergType);

        final ByteBuffer buffer = assertInstanceOf(ByteBuffer.class, converted);
        assertEquals(ByteBuffer.wrap(DATA_FIELD_VALUE), buffer);
    }

    private static Stream<Arguments> testConvertBytesToByteBuffer() {
        return Stream.of(Types.BinaryType.get(), Types.GeometryType.crs84(), Types.GeographyType.crs84())
                .flatMap(icebergType -> Stream.of(
                        Arguments.of(icebergType, DATA_FIELD_VALUE.clone()),
                        Arguments.of(icebergType, getObjectArray(DATA_FIELD_VALUE)),
                        Arguments.of(icebergType, ByteBuffer.wrap(DATA_FIELD_VALUE))
                ));
    }

    /**
     * Iceberg fixed columns require byte arrays.
     */
    @ParameterizedTest
    @MethodSource
    void testConvertBytesToFixed(final Object value) {
        final Types.FixedType fixedType = Types.FixedType.ofLength(DATA_FIELD_VALUE.length);

        final Object converted = RecordConverter.convertValue(value, fixedType);

        final byte[] bytes = assertInstanceOf(byte[].class, converted);
        assertArrayEquals(DATA_FIELD_VALUE, bytes);
    }

    private static Stream<Arguments> testConvertBytesToFixed() {
        return Stream.of(
                Arguments.of(DATA_FIELD_VALUE.clone()),
                Arguments.of((Object) getObjectArray(DATA_FIELD_VALUE)),
                Arguments.of(ByteBuffer.wrap(DATA_FIELD_VALUE))
        );
    }

    /**
     * Iceberg uuid columns require UUID values, which Record Readers can provide as Strings or as byte arrays.
     */
    @ParameterizedTest
    @MethodSource
    void testConvertUuid(final Object value) {
        final Object converted = RecordConverter.convertValue(value, Types.UUIDType.get());

        assertEquals(IDENTIFIER_FIELD_VALUE, converted);
    }

    private static Stream<Arguments> testConvertUuid() {
        final byte[] identifierBytes = ByteBuffer.allocate(16)
                .putLong(IDENTIFIER_FIELD_VALUE.getMostSignificantBits())
                .putLong(IDENTIFIER_FIELD_VALUE.getLeastSignificantBits())
                .array();

        return Stream.of(
                Arguments.of(IDENTIFIER_FIELD_VALUE),
                Arguments.of(IDENTIFIER_FIELD_VALUE.toString()),
                Arguments.of(identifierBytes),
                Arguments.of((Object) getObjectArray(identifierBytes))
        );
    }

    @ParameterizedTest
    @MethodSource
    void testConvertNullBytesUuid(final Type icebergType) {
        final Object converted = RecordConverter.convertValue(null, icebergType);

        assertNull(converted);
    }

    private static Stream<Arguments> testConvertNullBytesUuid() {
        return Stream.of(
                Arguments.of(Types.BinaryType.get()),
                Arguments.of(Types.FixedType.ofLength(DATA_FIELD_VALUE.length)),
                Arguments.of(Types.UUIDType.get()),
                Arguments.of(Types.GeometryType.crs84()),
                Arguments.of(Types.GeographyType.crs84())
        );
    }

    @Test
    void testConvertArrayElementBytes() {
        final Types.ListType listType = Types.ListType.ofOptional(1, Types.BinaryType.get());
        final Object[] array = new Object[] {getObjectArray(DATA_FIELD_VALUE)};

        final Object converted = RecordConverter.convertValue(array, listType);

        final List<?> list = assertInstanceOf(List.class, converted);
        assertEquals(List.of(ByteBuffer.wrap(DATA_FIELD_VALUE)), list);
    }

    @Test
    void testConvertMapBytesValue() {
        final Types.MapType mapType = Types.MapType.ofOptional(
                1, 2, Types.StringType.get(), Types.BinaryType.get()
        );
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put(DATA_FIELD_NAME, getObjectArray(DATA_FIELD_VALUE));

        final Object converted = RecordConverter.convertValue(map, mapType);

        final Map<?, ?> resultMap = assertInstanceOf(Map.class, converted);
        assertEquals(ByteBuffer.wrap(DATA_FIELD_VALUE), resultMap.get(DATA_FIELD_NAME));
    }

    @Test
    void testConvertNestedRecordBytesField() {
        final Types.StructType structType = Types.StructType.of(
                Types.NestedField.optional(1, DATA_FIELD_NAME, Types.BinaryType.get())
        );

        final RecordSchema nestedSchema = new SimpleRecordSchema(List.of(
                new RecordField(DATA_FIELD_NAME, RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()))
        ));
        final Map<String, Object> nestedValues = new LinkedHashMap<>();
        nestedValues.put(DATA_FIELD_NAME, getObjectArray(DATA_FIELD_VALUE));
        final Record nestedRecord = new MapRecord(nestedSchema, nestedValues);

        final Object converted = RecordConverter.convertValue(nestedRecord, structType);

        final StructLike struct = assertInstanceOf(StructLike.class, converted);
        assertEquals(ByteBuffer.wrap(DATA_FIELD_VALUE), struct.get(0, ByteBuffer.class));
    }

    /**
     * UUID values read as Strings must be converted for Iceberg uuid columns, even when no other Record field requires
     * conversion.
     */
    @Test
    void testGetConvertedRecordUuidString() {
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(1, IDENTIFIER_FIELD_NAME, Types.UUIDType.get())
        );

        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField(IDENTIFIER_FIELD_NAME, RecordFieldType.STRING.getDataType())
        ));
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(IDENTIFIER_FIELD_NAME, IDENTIFIER_FIELD_VALUE.toString());
        final Record record = new MapRecord(schema, values);

        final Record converted = RecordConverter.getConvertedRecord(record, struct);

        assertEquals(IDENTIFIER_FIELD_VALUE, converted.getValue(IDENTIFIER_FIELD_NAME));
    }

    private static Object[] getObjectArray(final byte[] bytes) {
        final Object[] array = new Object[bytes.length];
        for (int index = 0; index < bytes.length; index++) {
            array[index] = bytes[index];
        }

        return array;
    }
}
