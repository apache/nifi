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
package org.apache.nifi.processors.iceberg.appender.avro;

import org.apache.avro.io.Encoder;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.avro.ValueWriter;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.DecimalUtil;
import org.apache.nifi.processors.iceberg.appender.ArrayElementGetter;
import org.apache.nifi.processors.iceberg.appender.RecordFieldGetter;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.iceberg.appender.RecordFieldGetter.createFieldGetter;

/**
 * This class contains data type specific value writing logic for Avro files.
 */
public class IcebergAvroValueWriters {

    private IcebergAvroValueWriters() {
    }

    static ValueWriter<Byte[]> fixed(int length) {
        return new FixedWriter(length);
    }

    static ValueWriter<Byte[]> bytes() {
        return BytesWriter.INSTANCE;
    }

    static ValueWriter<BigDecimal> decimals(int precision, int scale) {
        return new DecimalWriter(precision, scale);
    }

    static ValueWriter<LocalDate> dates() {
        return DateWriter.INSTANCE;
    }

    static ValueWriter<Time> timeMicros() {
        return TimeMicrosWriter.INSTANCE;
    }

    static ValueWriter<Timestamp> timestampMicros() {
        return TimestampMicrosWriter.INSTANCE;
    }

    static <T> ValueWriter<T[]> array(ValueWriter<T> elementWriter, DataType dataType) {
        return new ArrayWriter<>(elementWriter, dataType);
    }

    static <K, V> ValueWriter<Map<K, V>> arrayMap(ValueWriter<K> keyWriter, DataType keyType, ValueWriter<V> valueWriter, DataType valueType) {
        return new ArrayMapWriter<>(keyWriter, keyType, valueWriter, valueType);
    }

    static <K, V> ValueWriter<Map<K, V>> map(ValueWriter<K> keyWriter, DataType keyType, ValueWriter<V> valueWriter, DataType valueType) {
        return new MapWriter<>(keyWriter, keyType, valueWriter, valueType);
    }

    static ValueWriter<Record> record(List<ValueWriter<?>> writers, List<RecordField> recordFields) {
        return new RecordWriter(writers, recordFields);
    }

    private static class FixedWriter implements ValueWriter<Byte[]> {

        private final int length;

        private FixedWriter(int length) {
            this.length = length;
        }

        @Override
        public void write(Byte[] value, Encoder encoder) throws IOException {
            Validate.isTrue(value.length == length, String.format("Cannot write byte array of length %s as fixed[%s]", value.length, length));

            encoder.writeFixed(ArrayUtils.toPrimitive(value));
        }
    }

    private static class BytesWriter implements ValueWriter<Byte[]> {

        private static final BytesWriter INSTANCE = new BytesWriter();

        @Override
        public void write(Byte[] value, Encoder encoder) throws IOException {
            encoder.writeBytes(ArrayUtils.toPrimitive(value));
        }
    }

    public static class DecimalWriter implements ValueWriter<BigDecimal> {

        private final int precision;
        private final int scale;
        private final ThreadLocal<byte[]> bytes;

        private DecimalWriter(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
            this.bytes = ThreadLocal.withInitial(() -> new byte[TypeUtil.decimalRequiredBytes(precision)]);
        }

        @Override
        public void write(BigDecimal value, Encoder encoder) throws IOException {
            encoder.writeFixed(DecimalUtil.toReusedFixLengthBytes(precision, scale, value, bytes.get()));
        }
    }

    private static class TimeMicrosWriter implements ValueWriter<Time> {

        private static final TimeMicrosWriter INSTANCE = new TimeMicrosWriter();

        @Override
        public void write(Time value, Encoder encoder) throws IOException {
            final Date date = new Date(value.getTime());
            final Duration duration = Duration.between(date.toInstant().truncatedTo(ChronoUnit.DAYS), date.toInstant());
            // Iceberg stores time in microseconds, so we need to transform the millisecond values.
            encoder.writeLong(duration.toMillis() * 1000L);
        }
    }

    private static class DateWriter implements ValueWriter<LocalDate> {

        private static final DateWriter INSTANCE = new DateWriter();

        @Override
        public void write(LocalDate value, Encoder encoder) throws IOException {
            encoder.writeInt((int) value.toEpochDay());
        }
    }

    private static class TimestampMicrosWriter implements ValueWriter<Timestamp> {

        private static final TimestampMicrosWriter INSTANCE = new TimestampMicrosWriter();

        @Override
        public void write(Timestamp value, Encoder encoder) throws IOException {
            // Iceberg stores timestamp in microseconds, so we need to transform the millisecond values.
            encoder.writeLong(value.getTime() * 1000L);
        }
    }

    private static class ArrayWriter<T> implements ValueWriter<T[]> {
        private final ValueWriter<T> elementWriter;
        private final ArrayElementGetter.ElementGetter elementGetter;

        private ArrayWriter(ValueWriter<T> elementWriter, DataType dataType) {
            this.elementWriter = elementWriter;
            this.elementGetter = ArrayElementGetter.createElementGetter(dataType);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void write(T[] array, Encoder encoder) throws IOException {
            encoder.writeArrayStart();
            final int numElements = array.length;
            encoder.setItemCount(numElements);
            for (int i = 0; i < numElements; i += 1) {
                encoder.startItem();
                elementWriter.write((T) elementGetter.getElementOrNull(array, i), encoder);
            }
            encoder.writeArrayEnd();
        }
    }

    private static class ArrayMapWriter<K, V> implements ValueWriter<Map<K, V>> {
        private final ValueWriter<K> keyWriter;
        private final ValueWriter<V> valueWriter;
        private final ArrayElementGetter.ElementGetter keyGetter;
        private final ArrayElementGetter.ElementGetter valueGetter;

        private ArrayMapWriter(ValueWriter<K> keyWriter, DataType keyType, ValueWriter<V> valueWriter, DataType valueType) {
            this.keyWriter = keyWriter;
            this.keyGetter = ArrayElementGetter.createElementGetter(keyType);
            this.valueWriter = valueWriter;
            this.valueGetter = ArrayElementGetter.createElementGetter(valueType);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void write(Map<K, V> map, Encoder encoder) throws IOException {
            encoder.writeArrayStart();
            final int numElements = map.size();
            encoder.setItemCount(numElements);
            final Object[] keyArray = map.keySet().toArray();
            final Object[] valueArray = map.values().toArray();
            for (int i = 0; i < numElements; i += 1) {
                encoder.startItem();
                keyWriter.write((K) keyGetter.getElementOrNull(keyArray, i), encoder);
                valueWriter.write((V) valueGetter.getElementOrNull(valueArray, i), encoder);
            }
            encoder.writeArrayEnd();
        }
    }

    private static class MapWriter<K, V> implements ValueWriter<Map<K, V>> {
        private final ValueWriter<K> keyWriter;
        private final ValueWriter<V> valueWriter;
        private final ArrayElementGetter.ElementGetter keyGetter;
        private final ArrayElementGetter.ElementGetter valueGetter;

        private MapWriter(ValueWriter<K> keyWriter, DataType keyType, ValueWriter<V> valueWriter, DataType valueType) {
            this.keyWriter = keyWriter;
            this.keyGetter = ArrayElementGetter.createElementGetter(keyType);
            this.valueWriter = valueWriter;
            this.valueGetter = ArrayElementGetter.createElementGetter(valueType);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void write(Map<K, V> map, Encoder encoder) throws IOException {
            encoder.writeMapStart();
            final int numElements = map.size();
            encoder.setItemCount(numElements);
            final Object[] keyArray = map.keySet().toArray();
            final Object[] valueArray = map.values().toArray();
            for (int i = 0; i < numElements; i += 1) {
                encoder.startItem();
                keyWriter.write((K) keyGetter.getElementOrNull(keyArray, i), encoder);
                valueWriter.write((V) valueGetter.getElementOrNull(valueArray, i), encoder);
            }
            encoder.writeMapEnd();
        }
    }

    static class RecordWriter implements ValueWriter<Record> {

        private final ValueWriter<?>[] writers;
        private final RecordFieldGetter.FieldGetter[] getters;

        private RecordWriter(List<ValueWriter<?>> writers, List<RecordField> recordFields) {
            this.writers = (ValueWriter<?>[]) Array.newInstance(ValueWriter.class, writers.size());
            this.getters = new RecordFieldGetter.FieldGetter[writers.size()];
            for (int i = 0; i < writers.size(); i += 1) {
                final RecordField recordField = recordFields.get(i);
                this.writers[i] = writers.get(i);
                this.getters[i] = createFieldGetter(recordField.getDataType(), recordField.getFieldName(), recordField.isNullable());
            }
        }

        @Override
        public void write(Record record, Encoder encoder) throws IOException {
            for (int i = 0; i < writers.length; i += 1) {
                if (record.getValue(record.getSchema().getField(i)) == null) {
                    writers[i].write(null, encoder);
                } else {
                    write(record, i, writers[i], encoder);
                }
            }
        }

        @SuppressWarnings("unchecked")
        private <T> void write(Record record, int pos, ValueWriter<T> writer, Encoder encoder) throws IOException {
            writer.write((T) getters[pos].getFieldOrNull(record), encoder);
        }
    }
}
