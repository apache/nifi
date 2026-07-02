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
                Types.NestedField.optional(1, "city", Types.StringType.get())
        );

        final RecordSchema nestedSchema = new SimpleRecordSchema(List.of(
                new RecordField("city", RecordFieldType.STRING.getDataType())
        ));
        final Map<String, Object> nestedValues = new LinkedHashMap<>();
        nestedValues.put("city", "Berlin");
        final Record nestedRecord = new MapRecord(nestedSchema, nestedValues);

        final Object converted = RecordConverter.convertValue(nestedRecord, structType);

        final StructLike struct = assertInstanceOf(StructLike.class, converted);
        assertEquals("Berlin", struct.get(0, String.class));
    }

    @Test
    void testConvertNestedRecordDateTimeField() {
        final Types.StructType structType = Types.StructType.of(
                Types.NestedField.optional(1, "created", Types.TimestampType.withoutZone())
        );

        final RecordSchema nestedSchema = new SimpleRecordSchema(List.of(
                new RecordField("created", RecordFieldType.TIMESTAMP.getDataType())
        ));
        final Map<String, Object> nestedValues = new LinkedHashMap<>();
        nestedValues.put("created", java.sql.Timestamp.valueOf("2026-01-01 12:30:45"));
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
        map.put("k1", "v1");
        map.put("k2", "v2");

        final Object converted = RecordConverter.convertValue(map, mapType);

        final Map<?, ?> resultMap = assertInstanceOf(Map.class, converted);
        assertEquals("v1", resultMap.get("k1"));
        assertEquals("v2", resultMap.get("k2"));
    }

    @Test
    void testConvertMapDateTimeValue() {
        final Types.MapType mapType = Types.MapType.ofOptional(
                1, 2, Types.StringType.get(), Types.DateType.get()
        );
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("day", java.sql.Date.valueOf("2026-02-03"));

        final Object converted = RecordConverter.convertValue(map, mapType);

        final Map<?, ?> resultMap = assertInstanceOf(Map.class, converted);
        assertEquals(LocalDate.of(2026, 2, 3), resultMap.get("day"));
    }

    @Test
    void testGetConvertedRecordArrayOfStructs() {
        final Types.StructType elementStruct = Types.StructType.of(
                Types.NestedField.optional(2, "name", Types.StringType.get())
        );
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.optional(1, "items",
                        Types.ListType.ofOptional(3, elementStruct))
        );

        final RecordSchema elementSchema = new SimpleRecordSchema(List.of(
                new RecordField("name", RecordFieldType.STRING.getDataType())
        ));
        final Map<String, Object> elementValues = new LinkedHashMap<>();
        elementValues.put("name", "widget");
        final Record element = new MapRecord(elementSchema, elementValues);

        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("items",
                        RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(elementSchema)))
        ));
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("items", new Object[] {element});
        final Record record = new MapRecord(schema, values);

        final org.apache.iceberg.data.Record converted = new DelegatedRecord(record, struct);
        final Object items = converted.getField("items");

        final List<?> list = assertInstanceOf(List.class, items);
        final StructLike first = assertInstanceOf(StructLike.class, list.get(0));
        assertEquals("widget", first.get(0, String.class));
    }
}
