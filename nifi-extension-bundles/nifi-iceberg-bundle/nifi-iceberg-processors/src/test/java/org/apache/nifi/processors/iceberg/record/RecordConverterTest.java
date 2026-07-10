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
import org.apache.iceberg.types.Types;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class RecordConverterTest {

    private static final String CITY_FIELD_NAME = "city";

    private static final String CREATED_FIELD_NAME = "created";

    private static final String NAME_FIELD_NAME = "name";

    private static final String ITEMS_FIELD_NAME = "items";

    private static final String CITY_FIELD_VALUE = "Berlin";

    private static final String NAME_FIELD_VALUE = "widget";

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
        nestedValues.put(CREATED_FIELD_NAME, java.sql.Timestamp.valueOf("2026-01-01 12:30:45"));
        final Record nestedRecord = new MapRecord(nestedSchema, nestedValues);

        final Object converted = RecordConverter.convertValue(nestedRecord, structType);

        final StructLike struct = assertInstanceOf(StructLike.class, converted);
        assertEquals(LocalDateTime.of(2026, 1, 1, 12, 30, 45), struct.get(0, LocalDateTime.class));
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
}
