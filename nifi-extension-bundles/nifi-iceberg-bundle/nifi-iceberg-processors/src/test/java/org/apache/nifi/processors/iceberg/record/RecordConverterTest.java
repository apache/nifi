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

import org.apache.iceberg.types.Types;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class RecordConverterTest {

    private static final String FIELD_NAME = "field";

    @Test
    void testConvertTimestampWithoutZone() {
        final Timestamp timestamp = Timestamp.valueOf("2026-01-01 10:00:00");
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.required(1, FIELD_NAME, Types.TimestampType.withoutZone())
        );

        final Record converted = convertSingleField(RecordFieldType.TIMESTAMP, timestamp, struct);

        assertEquals(timestamp.toLocalDateTime(), converted.getValue(FIELD_NAME));
    }

    @Test
    void testConvertTimestampWithZone() {
        final Timestamp timestamp = Timestamp.valueOf("2026-01-01 10:00:00");
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.required(1, FIELD_NAME, Types.TimestampType.withZone())
        );

        final Record converted = convertSingleField(RecordFieldType.TIMESTAMP, timestamp, struct);

        final OffsetDateTime expected = timestamp.toLocalDateTime().atOffset(ZoneOffset.UTC);
        assertEquals(expected, converted.getValue(FIELD_NAME));
    }

    @Test
    void testConvertDate() {
        final Date date = Date.valueOf("2026-02-03");
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.required(1, FIELD_NAME, Types.DateType.get())
        );

        final Record converted = convertSingleField(RecordFieldType.DATE, date, struct);

        assertEquals(date.toLocalDate(), converted.getValue(FIELD_NAME));
    }

    @Test
    void testConvertTime() {
        final Time time = Time.valueOf("23:30:45");
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.required(1, FIELD_NAME, Types.TimeType.get())
        );

        final Record converted = convertSingleField(RecordFieldType.TIME, time, struct);

        assertEquals(time.toLocalTime(), converted.getValue(FIELD_NAME));
    }

    @Test
    void testConvertUuidPassesThroughExistingUuid() {
        final UUID uuid = UUID.randomUUID();
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.required(1, FIELD_NAME, Types.UUIDType.get())
        );

        final Record converted = convertSingleField(RecordFieldType.UUID, uuid, struct);

        assertSame(uuid, converted.getValue(FIELD_NAME));
    }

    @Test
    void testConvertUuidFromString() {
        final UUID uuid = UUID.randomUUID();
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.required(1, FIELD_NAME, Types.UUIDType.get())
        );

        final Record converted = convertSingleField(RecordFieldType.UUID, uuid.toString(), struct);

        assertEquals(uuid, converted.getValue(FIELD_NAME));
    }

    @Test
    void testConvertFixed() {
        final Object[] boxedBytes = {(byte) 1, (byte) 2, (byte) 3};
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.required(1, FIELD_NAME, Types.FixedType.ofLength(3))
        );

        final Record converted = convertSingleField(RecordFieldType.ARRAY, boxedBytes, struct);

        assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) converted.getValue(FIELD_NAME));
    }

    @Test
    void testConvertBinary() {
        final Object[] boxedBytes = {(byte) 4, (byte) 5};
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.required(1, FIELD_NAME, Types.BinaryType.get())
        );

        final Record converted = convertSingleField(RecordFieldType.ARRAY, boxedBytes, struct);

        assertEquals(ByteBuffer.wrap(new byte[] {4, 5}), converted.getValue(FIELD_NAME));
    }

    @Test
    void testConvertListRecursivelyConvertsElements() {
        final Timestamp first = Timestamp.valueOf("2026-01-01 01:00:00");
        final Timestamp second = Timestamp.valueOf("2026-01-02 02:00:00");
        final Object[] values = {first, second};
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.required(1, FIELD_NAME,
                        Types.ListType.ofRequired(2, Types.TimestampType.withoutZone()))
        );

        final Record converted = convertSingleField(RecordFieldType.ARRAY, values, struct);

        final List<?> list = (List<?>) converted.getValue(FIELD_NAME);
        assertEquals(List.of(first.toLocalDateTime(), second.toLocalDateTime()), list);
    }

    @Test
    void testConvertMapRecursivelyConvertsValues() {
        final Timestamp timestamp = Timestamp.valueOf("2026-01-01 01:00:00");
        final Map<Object, Object> value = new LinkedHashMap<>();
        value.put("key", timestamp);
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.required(1, FIELD_NAME,
                        Types.MapType.ofRequired(2, 3, Types.StringType.get(), Types.TimestampType.withoutZone()))
        );

        final Record converted = convertSingleField(RecordFieldType.MAP, value, struct);

        final Map<?, ?> convertedMap = (Map<?, ?>) converted.getValue(FIELD_NAME);
        assertEquals(timestamp.toLocalDateTime(), convertedMap.get("key"));
    }

    @Test
    void testConvertStructWrapsNestedRecordAndConvertsItsFields() {
        final String innerField = "innerTimestamp";
        final Timestamp innerTimestamp = Timestamp.valueOf("2026-03-04 08:15:30");

        final RecordSchema nestedSchema = new SimpleRecordSchema(
                List.of(new RecordField(innerField, RecordFieldType.TIMESTAMP.getDataType()))
        );
        final Map<String, Object> nestedValues = new LinkedHashMap<>();
        nestedValues.put(innerField, innerTimestamp);
        final Record nestedRecord = new MapRecord(nestedSchema, nestedValues);

        final Types.StructType nestedStruct = Types.StructType.of(
                Types.NestedField.required(2, innerField, Types.TimestampType.withoutZone())
        );
        final Types.StructType struct = Types.StructType.of(
                Types.NestedField.required(1, FIELD_NAME, nestedStruct)
        );

        final Record converted = convertSingleField(RecordFieldType.RECORD, nestedRecord, struct);

        final Object structValue = converted.getValue(FIELD_NAME);
        final org.apache.iceberg.data.Record delegatedRecord = assertInstanceOf(org.apache.iceberg.data.Record.class, structValue);
        assertEquals(innerTimestamp.toLocalDateTime(), delegatedRecord.getField(innerField));
    }

    @Test
    void testFieldWithoutMatchingIcebergFieldPassesThroughUnchanged() {
        final String value = "unmapped-value";
        final Types.StructType struct = Types.StructType.of();

        final Record converted = convertSingleField(RecordFieldType.STRING, value, struct);

        assertEquals(value, converted.getValue(FIELD_NAME));
    }

    private static Record convertSingleField(final RecordFieldType fieldType, final Object value, final Types.StructType struct) {
        final RecordSchema recordSchema = new SimpleRecordSchema(
                List.of(new RecordField(FIELD_NAME, fieldType.getDataType()))
        );
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(FIELD_NAME, value);
        final Record record = new MapRecord(recordSchema, values);

        return RecordConverter.getConvertedRecord(record, struct);
    }
}
