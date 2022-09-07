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
package org.apache.nifi.processors.iceberg.appender.parquet;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.DecimalUtil;
import org.apache.nifi.processors.iceberg.appender.ArrayElementGetter;
import org.apache.nifi.processors.iceberg.appender.RecordFieldGetter;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.apache.nifi.processors.iceberg.appender.RecordFieldGetter.createFieldGetter;

/**
 * This class contains data type specific value writing logic for Parquet files.
 */
public class IcebergParquetValueWriters {

    private IcebergParquetValueWriters() {
    }

    static ParquetValueWriters.PrimitiveWriter<?> ints(DataType type, ColumnDescriptor desc) {
        if (type.getFieldType().equals(RecordFieldType.BYTE)) {
            return ParquetValueWriters.tinyints(desc);
        } else if (type.getFieldType().equals(RecordFieldType.SHORT)) {
            return ParquetValueWriters.shorts(desc);
        }
        return ParquetValueWriters.ints(desc);
    }

    static ParquetValueWriters.PrimitiveWriter<LocalDate> dates(ColumnDescriptor desc) {
        return new DateWriter(desc);
    }

    static ParquetValueWriters.PrimitiveWriter<Time> timeMicros(ColumnDescriptor desc) {
        return new TimeMicrosWriter(desc);
    }

    static ParquetValueWriters.PrimitiveWriter<Timestamp> timestampMicros(ColumnDescriptor desc) {
        return new TimestampMicrosWriter(desc);
    }

    static ParquetValueWriters.PrimitiveWriter<BigDecimal> decimalAsInteger(ColumnDescriptor desc, int precision, int scale) {
        Validate.isTrue(precision <= 9, String.format("Cannot write decimal value as integer with precision larger than 9, wrong precision %s", precision));
        return new IntegerDecimalWriter(desc, precision, scale);
    }

    static ParquetValueWriters.PrimitiveWriter<BigDecimal> decimalAsLong(ColumnDescriptor desc, int precision, int scale) {
        Validate.isTrue(precision <= 18, String.format("Cannot write decimal value as long with precision larger than 18, wrong precision %s", precision));
        return new LongDecimalWriter(desc, precision, scale);
    }

    static ParquetValueWriters.PrimitiveWriter<BigDecimal> decimalAsFixed(ColumnDescriptor desc, int precision, int scale) {
        return new FixedDecimalWriter(desc, precision, scale);
    }

    static ParquetValueWriters.PrimitiveWriter<Byte[]> byteArrays(ColumnDescriptor desc) {
        return new ByteArrayWriter(desc);
    }

    static ParquetValueWriters.PrimitiveWriter<UUID> uuids(ColumnDescriptor desc) {
        return new UUIDWriter(desc);
    }

    private static class UUIDWriter extends ParquetValueWriters.PrimitiveWriter<UUID> {

        private UUIDWriter(ColumnDescriptor desc) {
            super(desc);
        }

        @Override
        public void write(int repetitionLevel, UUID value) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
            byteBuffer.putLong(value.getMostSignificantBits());
            byteBuffer.putLong(value.getLeastSignificantBits());
            column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(byteBuffer.array()));
        }
    }

    private static class ByteArrayWriter extends ParquetValueWriters.PrimitiveWriter<Byte[]> {

        private ByteArrayWriter(ColumnDescriptor desc) {
            super(desc);
        }

        @Override
        public void write(int repetitionLevel, Byte[] bytes) {
            column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(ArrayUtils.toPrimitive(bytes)));
        }
    }

    private static class IntegerDecimalWriter extends ParquetValueWriters.PrimitiveWriter<BigDecimal> {

        private final int precision;
        private final int scale;

        private IntegerDecimalWriter(ColumnDescriptor desc, int precision, int scale) {
            super(desc);
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public void write(int repetitionLevel, BigDecimal value) {
            Validate.isTrue(value.scale() == scale, String.format("Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, value));
            Validate.isTrue(value.precision() <= precision, String.format("Cannot write value as decimal(%s,%s), too large: %s", precision, scale, value));

            column.writeInteger(repetitionLevel, (int) value.unscaledValue().longValueExact());
        }
    }

    private static class LongDecimalWriter extends ParquetValueWriters.PrimitiveWriter<BigDecimal> {

        private final int precision;
        private final int scale;

        private LongDecimalWriter(ColumnDescriptor desc, int precision, int scale) {
            super(desc);
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public void write(int repetitionLevel, BigDecimal value) {
            Validate.isTrue(value.scale() == scale, String.format("Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, value));
            Validate.isTrue(value.precision() <= precision, String.format("Cannot write value as decimal(%s,%s), too large: %s", precision, scale, value));

            column.writeLong(repetitionLevel, value.unscaledValue().longValueExact());
        }
    }

    private static class FixedDecimalWriter extends ParquetValueWriters.PrimitiveWriter<BigDecimal> {

        private final int precision;
        private final int scale;
        private final ThreadLocal<byte[]> bytes;

        private FixedDecimalWriter(ColumnDescriptor desc, int precision, int scale) {
            super(desc);
            this.precision = precision;
            this.scale = scale;
            this.bytes = ThreadLocal.withInitial(() -> new byte[TypeUtil.decimalRequiredBytes(precision)]);
        }

        @Override
        public void write(int repetitionLevel, BigDecimal value) {
            final byte[] binary = DecimalUtil.toReusedFixLengthBytes(precision, scale, value, bytes.get());
            column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(binary));
        }
    }

    private static class DateWriter extends ParquetValueWriters.PrimitiveWriter<LocalDate> {

        private DateWriter(ColumnDescriptor desc) {
            super(desc);
        }

        @Override
        public void write(int repetitionLevel, LocalDate value) {
            column.writeInteger(repetitionLevel, (int) value.toEpochDay());
        }
    }

    private static class TimeMicrosWriter extends ParquetValueWriters.PrimitiveWriter<Time> {

        private TimeMicrosWriter(ColumnDescriptor desc) {
            super(desc);
        }

        @Override
        public void write(int repetitionLevel, Time value) {
            final Date date = new Date(value.getTime());
            final Duration duration = Duration.between(date.toInstant().truncatedTo(ChronoUnit.DAYS), date.toInstant());
            // Iceberg stores time in microseconds, so we need to transform the millisecond values.
            column.writeLong(repetitionLevel, duration.toMillis() * 1000L);
        }
    }

    private static class TimestampMicrosWriter extends ParquetValueWriters.PrimitiveWriter<Timestamp> {

        private TimestampMicrosWriter(ColumnDescriptor desc) {
            super(desc);
        }

        @Override
        public void write(int repetitionLevel, Timestamp value) {
            // Iceberg stores timestamp in microseconds, so we need to transform the millisecond values.
            column.writeLong(repetitionLevel, value.getTime() * 1000L);
        }
    }

    static class ArrayDataWriter<E> extends ParquetValueWriters.RepeatedWriter<Object[], E> {
        private final DataType elementType;

        ArrayDataWriter(int definitionLevel, int repetitionLevel, ParquetValueWriter<E> writer, DataType elementType) {
            super(definitionLevel, repetitionLevel, writer);
            this.elementType = elementType;
        }

        @Override
        protected Iterator<E> elements(Object[] list) {
            return new ElementIterator<>(list);
        }

        private class ElementIterator<E> implements Iterator<E> {
            private final int size;
            private final Object[] list;
            private final ArrayElementGetter.ElementGetter getter;
            private int index;

            private ElementIterator(Object[] list) {
                this.list = list;
                size = list.length;
                getter = ArrayElementGetter.createElementGetter(elementType);
                index = 0;
            }

            @Override
            public boolean hasNext() {
                return index != size;
            }

            @Override
            @SuppressWarnings("unchecked")
            public E next() {
                if (index >= size) {
                    throw new NoSuchElementException();
                }

                E element = (E) getter.getElementOrNull(list, index);
                index += 1;

                return element;
            }
        }
    }

    static class MapDataWriter<K, V> extends ParquetValueWriters.RepeatedKeyValueWriter<Map<K, V>, K, V> {
        private final DataType keyType;
        private final DataType valueType;

        MapDataWriter(int definitionLevel, int repetitionLevel, ParquetValueWriter<K> keyWriter, ParquetValueWriter<V> valueWriter,
                      DataType keyType, DataType valueType) {
            super(definitionLevel, repetitionLevel, keyWriter, valueWriter);
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        protected Iterator<Map.Entry<K, V>> pairs(Map<K, V> map) {
            return new EntryIterator<>(map);
        }

        private class EntryIterator<K, V> implements Iterator<Map.Entry<K, V>> {
            private final int size;
            private final Object[] keys;
            private final Object[] values;
            private final ParquetValueReaders.ReusableEntry<K, V> entry;
            private final ArrayElementGetter.ElementGetter keyGetter;
            private final ArrayElementGetter.ElementGetter valueGetter;
            private int index;

            private EntryIterator(Map<K, V> map) {
                size = map.size();
                keys = map.keySet().toArray();
                values = map.values().toArray();
                entry = new ParquetValueReaders.ReusableEntry<>();
                keyGetter = ArrayElementGetter.createElementGetter(keyType);
                valueGetter = ArrayElementGetter.createElementGetter(valueType);
                index = 0;
            }

            @Override
            public boolean hasNext() {
                return index != size;
            }

            @Override
            @SuppressWarnings("unchecked")
            public Map.Entry<K, V> next() {
                if (index >= size) {
                    throw new NoSuchElementException();
                }

                entry.set((K) keyGetter.getElementOrNull(keys, index), (V) valueGetter.getElementOrNull(values, index));
                index += 1;

                return entry;
            }
        }
    }

    static class RecordWriter extends ParquetValueWriters.StructWriter<Record> {

        private final RecordFieldGetter.FieldGetter[] fieldGetter;

        RecordWriter(List<ParquetValueWriter<?>> writers, List<RecordField> recordFields) {
            super(writers);
            fieldGetter = new RecordFieldGetter.FieldGetter[recordFields.size()];
            for (int i = 0; i < recordFields.size(); i += 1) {
                final RecordField recordField = recordFields.get(i);
                fieldGetter[i] = createFieldGetter(recordField.getDataType(), recordField.getFieldName(), recordField.isNullable());
            }
        }

        @Override
        protected Object get(Record record, int index) {
            return fieldGetter[index].getFieldOrNull(record);
        }
    }

}
