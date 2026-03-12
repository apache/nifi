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
package org.apache.nifi.services.iceberg.parquet.io;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PartitionKeyRecordTest {

    private static final LocalDateTime CREATED = LocalDateTime.of(LocalDate.ofEpochDay(0), LocalTime.ofSecondOfDay(0));
    private static final OffsetDateTime CREATED_ZONED = OffsetDateTime.of(CREATED, ZoneOffset.UTC);
    private static final LocalDate UPDATED = LocalDate.ofEpochDay(0);
    private static final LocalTime STOPPED = LocalTime.ofSecondOfDay(300);

    private static final String FIRST_FIELD_NAME = "firstField";

    private static final int FIRST_FIELD_POSITION = 0;

    @Test
    void testTimestampNanoTypeConverted() {
        final Record record = setFirstField(Types.TimestampNanoType.withoutZone(), CREATED);
        final Object wrappedFirstField = getWrappedFirstField(record);

        final long expected = DateTimeUtil.nanosFromTimestamp(CREATED);
        assertEquals(expected, wrappedFirstField);
    }

    @Test
    void testTimestampNanoTypeZonedConverted() {
        final Record record = setFirstField(Types.TimestampNanoType.withZone(), CREATED_ZONED);
        final Object wrappedFirstField = getWrappedFirstField(record);

        final long expected = DateTimeUtil.nanosFromTimestamptz(CREATED_ZONED);
        assertEquals(expected, wrappedFirstField);
    }

    @Test
    void testTimestampTypeConverted() {
        final Record record = setFirstField(Types.TimestampType.withoutZone(), CREATED);
        final Object wrappedFirstField = getWrappedFirstField(record);

        final long expected = DateTimeUtil.microsFromTimestamp(CREATED);
        assertEquals(expected, wrappedFirstField);
    }

    @Test
    void testTimestampTypeZonedConverted() {
        final Record record = setFirstField(Types.TimestampType.withZone(), CREATED_ZONED);
        final Object wrappedFirstField = getWrappedFirstField(record);

        final long expected = DateTimeUtil.microsFromTimestamptz(CREATED_ZONED);
        assertEquals(expected, wrappedFirstField);
    }

    @Test
    void testDateTypeConverted() {
        final Record record = setFirstField(Types.DateType.get(), UPDATED);
        final Object wrappedFirstField = getWrappedFirstField(record);

        final int expected = DateTimeUtil.daysFromDate(UPDATED);
        assertEquals(expected, wrappedFirstField);
    }

    @Test
    void testTimeTypeConverted() {
        final Record record = setFirstField(Types.TimeType.get(), STOPPED);
        final Object wrappedFirstField = getWrappedFirstField(record);

        final long expected = DateTimeUtil.microsFromTime(STOPPED);
        assertEquals(expected, wrappedFirstField);
    }

    @Test
    void testFixedTypeConverted() {
        final byte[] bytes = String.class.getSimpleName().getBytes(StandardCharsets.UTF_8);

        final Record record = setFirstField(Types.FixedType.ofLength(bytes.length), bytes);
        final Object wrappedFirstField = getWrappedFirstField(record);

        final ByteBuffer expected = ByteBuffer.wrap(bytes);
        assertEquals(expected, wrappedFirstField);
    }

    @Test
    void testStringTypeUnchanged() {
        final String expected = String.class.getName();

        final Record record = setFirstField(Types.StringType.get(), expected);
        final Object wrappedFirstField = getWrappedFirstField(record);

        assertEquals(expected, wrappedFirstField);
    }

    private Object getWrappedFirstField(final Record record) {
        final PartitionKeyRecord partitionKeyRecord = new PartitionKeyRecord(record.struct());
        partitionKeyRecord.wrap(record);
        return partitionKeyRecord.get(FIRST_FIELD_POSITION, Object.class);
    }

    private Record setFirstField(final Type firstFieldType, final Object firstField) {
        final Schema schema = getSchema(firstFieldType);
        final Record record = GenericRecord.create(schema);
        record.set(FIRST_FIELD_POSITION, firstField);
        return record;
    }

    private Schema getSchema(final Type firstFieldType) {
        final Types.NestedField field = Types.NestedField.builder()
                .ofType(firstFieldType)
                .asRequired()
                .withId(FIRST_FIELD_POSITION)
                .withName(FIRST_FIELD_NAME)
                .build();
        final List<Types.NestedField> fields = List.of(field);
        return new Schema(fields);
    }
}
