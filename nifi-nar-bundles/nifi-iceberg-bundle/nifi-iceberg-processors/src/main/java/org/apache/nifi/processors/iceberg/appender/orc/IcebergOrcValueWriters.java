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
package org.apache.nifi.processors.iceberg.appender.orc;

import com.google.common.collect.Lists;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.data.orc.GenericOrcWriters;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.nifi.processors.iceberg.appender.ArrayElementGetter;
import org.apache.nifi.processors.iceberg.appender.RecordFieldGetter;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.nifi.processors.iceberg.appender.RecordFieldGetter.createFieldGetter;

/**
 * This class contains data type specific value writing logic for Orc files.
 */
public class IcebergOrcValueWriters {

    private IcebergOrcValueWriters() {
    }

    static OrcValueWriter<UUID> uuids() {
        return UUIDWriter.INSTANCE;
    }

    static OrcValueWriter<Byte[]> byteArray() {
        return ByteArrayWriter.INSTANCE;
    }

    public static OrcValueWriter<BigDecimal> decimal(int precision, int scale) {
        if (precision <= 18) {
            return new Decimal18Writer(precision, scale);
        } else if (precision <= 38) {
            return new Decimal38Writer(precision, scale);
        } else {
            throw new IllegalArgumentException("Invalid precision: " + precision);
        }
    }

    static OrcValueWriter<LocalDate> dates() {
        return DateWriter.INSTANCE;
    }

    static OrcValueWriter<Time> times() {
        return TimeWriter.INSTANCE;
    }

    static OrcValueWriter<Timestamp> timestamps() {
        return TimestampWriter.INSTANCE;
    }

    static OrcValueWriter<Timestamp> timestampTzs() {
        return TimestampTzWriter.INSTANCE;
    }

    static <T> OrcValueWriter<Object[]> list(OrcValueWriter<T> elementWriter, DataType elementType) {
        return new ListWriter<>(elementWriter, elementType);
    }

    static <K, V> OrcValueWriter<Map<K, V>> map(OrcValueWriter<K> keyWriter, OrcValueWriter<V> valueWriter, DataType keyType, DataType valueType) {
        return new MapWriter<>(keyWriter, valueWriter, keyType, valueType);
    }

    static OrcValueWriter<Record> struct(List<OrcValueWriter<?>> writers, List<RecordField> recordFields) {
        return new RowDataWriter(writers, recordFields);
    }

    private static class UUIDWriter implements OrcValueWriter<UUID> {

        private static final UUIDWriter INSTANCE = new UUIDWriter();

        @Override
        public void nonNullWrite(int rowId, UUID data, ColumnVector output) {
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putLong(data.getMostSignificantBits());
            buffer.putLong(data.getLeastSignificantBits());
            ((BytesColumnVector) output).setRef(rowId, buffer.array(), 0, buffer.array().length);
        }
    }

    private static class ByteArrayWriter implements OrcValueWriter<Byte[]> {

        private static final ByteArrayWriter INSTANCE = new ByteArrayWriter();

        @Override
        public void nonNullWrite(int rowId, Byte[] data, ColumnVector output) {
            ((BytesColumnVector) output).setRef(rowId, ArrayUtils.toPrimitive(data), 0, data.length);
        }
    }

    private static class Decimal18Writer implements OrcValueWriter<BigDecimal> {

        private final int precision;
        private final int scale;

        Decimal18Writer(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public void nonNullWrite(int rowId, BigDecimal data, ColumnVector output) {
            Validate.isTrue(data.scale() == scale, String.format("Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, data));
            Validate.isTrue(data.precision() <= precision, String.format("Cannot write value as decimal(%s,%s), invalid precision: %s", precision, scale, data));

            ((DecimalColumnVector) output).vector[rowId].setFromLongAndScale(data.unscaledValue().longValueExact(), data.scale());
        }
    }

    private static class Decimal38Writer implements OrcValueWriter<BigDecimal> {

        private final int precision;
        private final int scale;

        Decimal38Writer(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public void nonNullWrite(int rowId, BigDecimal data, ColumnVector output) {
            Validate.isTrue(data.scale() == scale, String.format("Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, data));
            Validate.isTrue(data.precision() <= precision, String.format("Cannot write value as decimal(%s,%s), invalid precision: %s", precision, scale, data));

            ((DecimalColumnVector) output).vector[rowId].set(HiveDecimal.create(data, false));
        }
    }

    private static class DateWriter implements OrcValueWriter<LocalDate> {

        private static final DateWriter INSTANCE = new DateWriter();

        @Override
        public void nonNullWrite(int rowId, LocalDate data, ColumnVector output) {
            ((LongColumnVector) output).vector[rowId] = data.toEpochDay();
        }
    }

    private static class TimeWriter implements OrcValueWriter<Time> {

        private static final TimeWriter INSTANCE = new TimeWriter();

        @Override
        public void nonNullWrite(int rowId, Time data, ColumnVector output) {
            final Date date = new Date(data.getTime());
            final Duration duration = Duration.between(date.toInstant().truncatedTo(ChronoUnit.DAYS), date.toInstant());
            // The time in java.sql.Time is in millisecond, while the standard time in iceberg is microsecond, so it needs to be transformed into microsecond.
            ((LongColumnVector) output).vector[rowId] = duration.toMillis() * 1000L;
        }
    }

    private static class TimestampWriter implements OrcValueWriter<Timestamp> {

        private static final TimestampWriter INSTANCE = new TimestampWriter();

        @Override
        public void nonNullWrite(int rowId, Timestamp data, ColumnVector output) {
            TimestampColumnVector columnVector = (TimestampColumnVector) output;
            columnVector.setIsUTC(true);

            final OffsetDateTime offsetDateTime = data.toInstant().atOffset(ZoneOffset.UTC);
            columnVector.time[rowId] = offsetDateTime.toEpochSecond() * 1_000 + offsetDateTime.getNano() / 1_000_000;
            // truncate nanos to only keep microsecond precision.
            columnVector.nanos[rowId] = (offsetDateTime.getNano() / 1_000) * 1_000;
        }
    }

    private static class TimestampTzWriter implements OrcValueWriter<Timestamp> {

        private static final TimestampTzWriter INSTANCE = new TimestampTzWriter();

        @Override
        public void nonNullWrite(int rowId, Timestamp data, ColumnVector output) {
            TimestampColumnVector columnVector = (TimestampColumnVector) output;

            final Instant instant = data.toInstant();
            columnVector.time[rowId] = instant.toEpochMilli();
            // truncate nanos to only keep microsecond precision.
            columnVector.nanos[rowId] = (instant.getNano() / 1_000) * 1_000;
        }
    }

    static class ListWriter<T> implements OrcValueWriter<Object[]> {
        private final OrcValueWriter<T> elementWriter;
        private final ArrayElementGetter.ElementGetter elementGetter;

        ListWriter(OrcValueWriter<T> elementWriter, DataType elementType) {
            this.elementWriter = elementWriter;
            this.elementGetter = ArrayElementGetter.createElementGetter(elementType);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void nonNullWrite(int rowId, Object[] data, ColumnVector output) {
            ListColumnVector cv = (ListColumnVector) output;
            cv.lengths[rowId] = data.length;
            cv.offsets[rowId] = cv.childCount;
            cv.childCount = (int) (cv.childCount + cv.lengths[rowId]);
            // make sure the child is big enough.
            growColumnVector(cv.child, cv.childCount);

            for (int e = 0; e < cv.lengths[rowId]; ++e) {
                final Object value = elementGetter.getElementOrNull(data, e);
                elementWriter.write((int) (e + cv.offsets[rowId]), (T) value, cv.child);
            }
        }

        @Override
        public Stream<FieldMetrics<?>> metrics() {
            return elementWriter.metrics();
        }
    }

    static class MapWriter<K, V> implements OrcValueWriter<Map<K, V>> {
        private final OrcValueWriter<K> keyWriter;
        private final OrcValueWriter<V> valueWriter;
        private final ArrayElementGetter.ElementGetter keyGetter;
        private final ArrayElementGetter.ElementGetter valueGetter;

        MapWriter(OrcValueWriter<K> keyWriter, OrcValueWriter<V> valueWriter, DataType keyType, DataType valueType) {
            this.keyWriter = keyWriter;
            this.valueWriter = valueWriter;
            this.keyGetter = ArrayElementGetter.createElementGetter(keyType);
            this.valueGetter = ArrayElementGetter.createElementGetter(valueType);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void nonNullWrite(int rowId, Map<K, V> data, ColumnVector output) {
            MapColumnVector cv = (MapColumnVector) output;
            final Object[] keyArray = data.keySet().toArray();
            final Object[] valueArray = data.values().toArray();

            // record the length and start of the list elements
            cv.lengths[rowId] = data.size();
            cv.offsets[rowId] = cv.childCount;
            cv.childCount = (int) (cv.childCount + cv.lengths[rowId]);
            // make sure the child is big enough
            growColumnVector(cv.keys, cv.childCount);
            growColumnVector(cv.values, cv.childCount);
            // Add each element
            for (int e = 0; e < cv.lengths[rowId]; ++e) {
                final int pos = (int) (e + cv.offsets[rowId]);
                keyWriter.write(pos, (K) keyGetter.getElementOrNull(keyArray, e), cv.keys);
                valueWriter.write(pos, (V) valueGetter.getElementOrNull(valueArray, e), cv.values);
            }
        }

        @Override
        public Stream<FieldMetrics<?>> metrics() {
            return Stream.concat(keyWriter.metrics(), valueWriter.metrics());
        }
    }

    static class RowDataWriter extends GenericOrcWriters.StructWriter<Record> {

        private final List<RecordFieldGetter.FieldGetter> fieldGetters;

        RowDataWriter(List<OrcValueWriter<?>> writers, List<RecordField> recordFields) {
            super(writers);
            this.fieldGetters = Lists.newArrayListWithExpectedSize(recordFields.size());
            for (RecordField recordField : recordFields) {
                fieldGetters.add(createFieldGetter(recordField.getDataType(), recordField.getFieldName(), recordField.isNullable()));
            }
        }

        @Override
        protected Object get(Record record, int index) {
            return fieldGetters.get(index).getFieldOrNull(record);
        }
    }

    private static void growColumnVector(ColumnVector cv, int requestedSize) {
        if (cv.isNull.length < requestedSize) {
            // Use growth factor of 3 to avoid frequent array allocations
            cv.ensureSize(requestedSize * 3, true);
        }
    }
}
