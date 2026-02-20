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
package org.apache.nifi.services.iceberg.parquet.io.recordconverters;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
class IcebergParquetRecordWriterConverterTest {

    private IcebergParquetRecordWriterConverter converter;

    @BeforeEach
    void setUp() {
        converter = new IcebergParquetRecordWriterConverter();
    }

    @Test
    void testConvertTimestampToLocalDateTime() {
        final Schema schema = new Schema(
                Types.NestedField.required(1, "ts_field", Types.TimestampType.withoutZone())
        );
        final Timestamp timestamp = Timestamp.valueOf("2026-06-06 06:06:06");
        final Record record = GenericRecord.create(schema);
        record.set(0, timestamp);

        converter.convertRecord(record);

        Object result = record.get(0);
        assertInstanceOf(LocalDateTime.class, result);
        assertEquals(timestamp.toLocalDateTime(), result);
    }

    @Test
    void testConvertNestedRecord() {
        final Schema innerSchema = new Schema(
                Types.NestedField.required(1, "inner_ts", Types.TimestampType.withoutZone())
        );
        final Schema outerSchema = new Schema(
                Types.NestedField.required(2, "nested_record", innerSchema.asStruct())
        );

        final Record innerRecord = GenericRecord.create(innerSchema);
        final Timestamp timestamp = Timestamp.valueOf("2026-06-06 06:06:06");
        innerRecord.set(0, timestamp);

        final Record outerRecord = GenericRecord.create(outerSchema);
        outerRecord.set(0, innerRecord);

        converter.convertRecord(outerRecord);

        Record resultInner = (Record) outerRecord.get(0);
        assertInstanceOf(LocalDateTime.class, resultInner.get(0));
    }

    @Test
    void testConvertListWithRecords() {
        final Schema elementSchema = new Schema(
                Types.NestedField.required(1, "ts", Types.TimestampType.withoutZone())
        );
        final Record recordInList = GenericRecord.create(elementSchema);
        recordInList.set(0, Timestamp.valueOf("2026-06-06 06:06:06"));

        final List<Record> list = Collections.singletonList(recordInList);

        converter.convertList(list);

        assertInstanceOf(LocalDateTime.class, recordInList.get(0));
    }

    @Test
    void testNullValueHandling() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "nullable_field", Types.TimestampType.withoutZone())
        );
        final Record record = GenericRecord.create(schema);
        record.set(0, null);

        converter.convertRecord(record);

        assertNull(record.get(0));
    }
}
