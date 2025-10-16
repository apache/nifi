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

import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Standard implementation of Iceberg Record wrapping NiFi Record
 */
public class DelegatedRecord implements Record {
    private final org.apache.nifi.serialization.record.Record record;

    private final Types.StructType struct;

    public DelegatedRecord(
            final org.apache.nifi.serialization.record.Record record,
            final Types.StructType struct
    ) {
        this.record = Objects.requireNonNull(record);
        this.struct = Objects.requireNonNull(struct);
    }

    @Override
    public Types.StructType struct() {
        return struct;
    }

    /**
     * Get Field value for field name from supporting Record
     *
     * @param fieldName Field Name for value requested
     * @return Field Value or null when not found
     */
    @Override
    public Object getField(final String fieldName) {
        return record.getValue(fieldName);
    }

    /**
     * Set Field value for field name in supporting Record
     *
     * @param fieldName Field Name to be added
     * @param fieldValue Field Value to be added
     */
    @Override
    public void setField(final String fieldName, final Object fieldValue) {
        record.setValue(fieldName, fieldValue);
    }

    /**
     * Get Field value for specified position from supporting Record
     *
     * @param position Field position
     * @return Field value or null when not found
     */
    @Override
    public Object get(final int position) {
        final RecordField recordField = record.getSchema().getField(position);
        return record.getValue(recordField);
    }

    /**
     * Create and return a copy of the Record
     *
     * @return Copy of the Record
     */
    @Override
    public Record copy() {
        return copy(Collections.emptyMap());
    }

    /**
     * Create and return a copy of the Record
     *
     * @param overrides Fields and values to override in the copied Record
     * @return Copy of the Record
     */
    @Override
    public Record copy(final Map<String, Object> overrides) {
        final Map<String, Object> values = record.toMap();
        values.putAll(overrides);
        final MapRecord mapRecord = new MapRecord(record.getSchema(), values);
        return new DelegatedRecord(mapRecord, struct);
    }

    /**
     * Get count of fields in the supporting Record
     *
     * @return Count of fields
     */
    @Override
    public int size() {
        return record.getSchema().getFieldCount();
    }

    /**
     * Get Field value for specified position cast to specified value class
     *
     * @param position Field position
     * @param valueClass Field value class
     * @return Field value or null when not found
     * @param <T> Field Value Type
     */
    @Override
    public <T> T get(final int position, final Class<T> valueClass) {
        final Object value = get(position);
        if (value == null || valueClass.isInstance(value)) {
            return valueClass.cast(value);
        }
        throw new IllegalStateException(String.format("Field [%d] value not an instance of [%s]", position, valueClass));
    }

    /**
     * Set Field value for specified position
     *
     * @param position Field position
     * @param value Field value
     * @param <T> Field Value Type
     */
    @Override
    public <T> void set(final int position, final T value) {
        final RecordField recordField = record.getSchema().getField(position);
        record.setValue(recordField, value);
    }

    @Override
    public boolean equals(final Object other) {
        final boolean equals;

        if (other instanceof DelegatedRecord otherRecord) {
            equals = record.equals(otherRecord.record);
        } else {
            equals = false;
        }

        return equals;
    }

    @Override
    public int hashCode() {
        return record.hashCode();
    }
}
