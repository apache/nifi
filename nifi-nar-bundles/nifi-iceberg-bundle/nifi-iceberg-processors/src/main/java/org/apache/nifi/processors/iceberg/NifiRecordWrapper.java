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

import org.apache.iceberg.StructLike;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * Class to wrap and adapt {@link Record} to Iceberg {@link StructLike} for partition handling usage like {@link
 * org.apache.iceberg.PartitionKey#partition(StructLike)}
 */
public class NifiRecordWrapper implements StructLike {

    private final DataType[] types;

    private final BiFunction<Record, Integer, ?>[] getters;

    private Record record = null;

    @SuppressWarnings("unchecked")
    public NifiRecordWrapper(RecordSchema schema) {
        this.types = schema.getDataTypes().toArray(new DataType[0]);
        this.getters = Stream.of(types).map(NifiRecordWrapper::getter).toArray(BiFunction[]::new);
    }

    public NifiRecordWrapper wrap(Record record) {
        this.record = record;
        return this;
    }

    @Override
    public int size() {
        return types.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        if (record.getSchema().getField(pos) == null) {
            return null;
        } else if (getters[pos] != null) {
            return javaClass.cast(getters[pos].apply(record, pos));
        }

        return javaClass.cast(record.getValue(record.getSchema().getField(pos)));
    }

    @Override
    public <T> void set(int i, T t) {
        throw new UnsupportedOperationException("Record wrapper update is unsupported.");
    }

    private static BiFunction<Record, Integer, ?> getter(DataType type) {
        if (type.equals(RecordFieldType.TIMESTAMP.getDataType())) {
            return (row, pos) -> {
                final RecordField field = row.getSchema().getField(pos);
                final Object value = row.getValue(field);

                Timestamp timestamp = DataTypeUtils.toTimestamp(value, () -> DataTypeUtils.getDateFormat(type.getFormat()), field.getFieldName());
                return timestamp.getTime() * 1000L;
            };
        } else if (type.equals(RecordFieldType.DATE.getDataType())) {
            return (row, pos) -> {
                final RecordField field = row.getSchema().getField(pos);
                final Object value = row.getValue(field);

                final LocalDate localDate = DataTypeUtils.toLocalDate(value, () -> DataTypeUtils.getDateTimeFormatter(type.getFormat(), ZoneId.systemDefault()), field.getFieldName());
                return (int) localDate.toEpochDay();
            };
        } else if (type.equals(RecordFieldType.TIME.getDataType())) {
            return (row, pos) -> {
                final RecordField field = row.getSchema().getField(pos);
                final Object value = row.getValue(field);

                final Time time = DataTypeUtils.toTime(value, () -> DataTypeUtils.getDateFormat(type.getFormat()), field.getFieldName());
                final Date date = new Date(time.getTime());
                final Duration duration = Duration.between(date.toInstant().truncatedTo(ChronoUnit.DAYS), date.toInstant());
                return duration.toMillis() * 1000L;
            };
        } else if (type.equals(RecordFieldType.RECORD.getDataType())) {
            RecordDataType structType = (RecordDataType) type;
            NifiRecordWrapper nestedWrapper = new NifiRecordWrapper(structType.getChildSchema());
            return (row, pos) -> nestedWrapper.wrap(row.getAsRecord(row.getSchema().getField(pos).getFieldName(), row.getSchema()));
        }

        return null;
    }
}
