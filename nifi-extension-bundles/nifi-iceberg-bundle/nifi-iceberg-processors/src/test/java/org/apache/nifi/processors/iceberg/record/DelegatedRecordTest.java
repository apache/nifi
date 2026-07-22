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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DelegatedRecordTest {

    private static final String LABEL_FIELD = "label";
    private static final String LABEL = "delegated-record";
    private static final String LABEL_MODIFIED = "modified-record";

    private static final String CREATED_FIELD = "created";
    private static final String UPDATED_FIELD = "updated";
    private static final String STOPPED_FIELD = "stopped";
    private static final Timestamp CREATED = Timestamp.valueOf("2026-01-01 12:30:45.123456789");
    private static final LocalDateTime CREATED_CONVERTED = CREATED.toLocalDateTime();
    private static final Date UPDATED = Date.valueOf("2026-02-03");
    private static final LocalDate UPDATED_CONVERTED = UPDATED.toLocalDate();
    private static final Time STOPPED = Time.valueOf("23:30:45");
    private static final LocalTime STOPPED_CONVERTED = STOPPED.toLocalTime();

    private static final String ID_FIELD = "id";
    private static final String AMOUNT_FIELD = "amount";
    private static final int ID = 7;
    private static final int ID_MODIFIED = 99;
    private static final BigDecimal AMOUNT = new BigDecimal("12.34");

    private static final Types.StructType TABLE_STRUCT = Types.StructType.of(
            Types.NestedField.required(1, ID_FIELD, Types.IntegerType.get()),
            Types.NestedField.optional(2, AMOUNT_FIELD, Types.DecimalType.of(10, 2)),
            Types.NestedField.optional(3, LABEL_FIELD, Types.StringType.get())
    );

    @Test
    void testCopyEmptyRecord() {
        final List<RecordField> recordFields = List.of();
        final RecordSchema recordSchema = new SimpleRecordSchema(recordFields);
        final Record record = new MapRecord(recordSchema, new LinkedHashMap<>());

        final Types.StructType structType = Types.StructType.of();
        final DelegatedRecord delegatedRecord = new DelegatedRecord(record, structType);

        assertEquals(recordSchema.getFieldCount(), delegatedRecord.size());

        final org.apache.iceberg.data.Record copiedRecord = delegatedRecord.copy();
        assertEquals(delegatedRecord, copiedRecord);

        assertEquals(delegatedRecord.hashCode(), record.hashCode());
    }

    @Test
    void testSetGetStringField() {
        final RecordSchema recordSchema = new SimpleRecordSchema(
                List.of(
                        new RecordField(LABEL_FIELD, RecordFieldType.STRING.getDataType())
                )
        );
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(LABEL_FIELD, LABEL);

        final Record record = new MapRecord(recordSchema, values);

        final Types.StructType structType = Types.StructType.of(
                Types.NestedField.optional(1, LABEL_FIELD, Types.StringType.get())
        );
        final DelegatedRecord delegatedRecord = new DelegatedRecord(record, structType);

        final Types.StructType recordStruct = delegatedRecord.struct();
        assertEquals(structType, recordStruct);
        assertEquals(recordSchema.getFieldCount(), delegatedRecord.size());

        final Object field = delegatedRecord.getField(LABEL_FIELD);
        assertEquals(LABEL, field);

        final Object firstField = delegatedRecord.get(0);
        assertEquals(LABEL, firstField);

        final String firstStringField = delegatedRecord.get(0, String.class);
        assertEquals(LABEL, firstStringField);

        delegatedRecord.setField(LABEL_FIELD, LABEL_MODIFIED);
        final Object fieldModified = delegatedRecord.getField(LABEL_FIELD);
        assertEquals(LABEL_MODIFIED, fieldModified);

        delegatedRecord.set(0, LABEL);
        final Object fieldReverted = delegatedRecord.getField(LABEL_FIELD);
        assertEquals(LABEL, fieldReverted);
    }

    @Test
    void testGetTimestampDateTimeFields() {
        final RecordSchema recordSchema = new SimpleRecordSchema(
                List.of(
                        new RecordField(CREATED_FIELD, RecordFieldType.TIMESTAMP.getDataType()),
                        new RecordField(UPDATED_FIELD, RecordFieldType.DATE.getDataType()),
                        new RecordField(STOPPED_FIELD, RecordFieldType.TIME.getDataType())
                )
        );
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(CREATED_FIELD, CREATED);
        values.put(UPDATED_FIELD, UPDATED);
        values.put(STOPPED_FIELD, STOPPED);

        final Record record = new MapRecord(recordSchema, values);

        final Types.StructType structType = Types.StructType.of();
        final DelegatedRecord delegatedRecord = new DelegatedRecord(record, structType);

        final Object created = delegatedRecord.getField(CREATED_FIELD);
        assertEquals(CREATED_CONVERTED, created);

        final Object updated = delegatedRecord.getField(UPDATED_FIELD);
        assertEquals(UPDATED_CONVERTED, updated);

        final Object stopped = delegatedRecord.getField(STOPPED_FIELD);
        assertEquals(STOPPED_CONVERTED, stopped);
    }

    /**
     * Iceberg writers read values positionally against the table struct, so position 0 must always return the value of
     * the table's first column ("id"), independent of the field ordering in the incoming Record schema.
     */
    @Test
    void testGetByPositionMatchesTableColumnNameRegardlessOfInputOrder() {
        final DelegatedRecord delegatedRecord = new DelegatedRecord(shuffledInputRecord(), TABLE_STRUCT);

        assertEquals(ID, delegatedRecord.get(0));
        assertEquals(AMOUNT, delegatedRecord.get(1));
        assertEquals(LABEL, delegatedRecord.get(2));
    }

    /**
     * When the incoming Record does not contain a column present in the table schema, positional access must return
     * null for that column rather than shifting subsequent input values into it.
     */
    @Test
    void testGetByPositionReturnsNullForColumnMissingFromInput() {
        final RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField(ID_FIELD, RecordFieldType.INT.getDataType()),
                new RecordField(LABEL_FIELD, RecordFieldType.STRING.getDataType())
        ));
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(ID_FIELD, ID);
        values.put(LABEL_FIELD, LABEL);
        final DelegatedRecord delegatedRecord = new DelegatedRecord(new MapRecord(recordSchema, values), TABLE_STRUCT);

        assertEquals(ID, delegatedRecord.get(0));
        assertNull(delegatedRecord.get(1), "Missing '" + AMOUNT_FIELD + "' column must be null, not shifted input data");
        assertEquals(LABEL, delegatedRecord.get(2));
    }

    /**
     * Positional set must resolve the target field by the Iceberg table column name for the given position, independent
     * of the incoming Record field ordering, so that set(position) is symmetric with get(position).
     */
    @Test
    void testSetByPositionMatchesTableColumnNameRegardlessOfInputOrder() {
        final DelegatedRecord delegatedRecord = new DelegatedRecord(shuffledInputRecord(), TABLE_STRUCT);

        delegatedRecord.set(0, ID_MODIFIED);

        assertEquals(ID_MODIFIED, delegatedRecord.getField(ID_FIELD));
        assertEquals(AMOUNT, delegatedRecord.getField(AMOUNT_FIELD));
        assertEquals(LABEL, delegatedRecord.getField(LABEL_FIELD));
    }

    /**
     * Build an incoming Record whose field ordering (amount, label, id) intentionally differs from the
     * {@link #TABLE_STRUCT} ordering (id, amount, label) so positional access must resolve by column name.
     */
    private static Record shuffledInputRecord() {
        final RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField(AMOUNT_FIELD, RecordFieldType.DECIMAL.getDataType()),
                new RecordField(LABEL_FIELD, RecordFieldType.STRING.getDataType()),
                new RecordField(ID_FIELD, RecordFieldType.INT.getDataType())
        ));
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(AMOUNT_FIELD, AMOUNT);
        values.put(LABEL_FIELD, LABEL);
        values.put(ID_FIELD, ID);
        return new MapRecord(recordSchema, values);
    }
}
