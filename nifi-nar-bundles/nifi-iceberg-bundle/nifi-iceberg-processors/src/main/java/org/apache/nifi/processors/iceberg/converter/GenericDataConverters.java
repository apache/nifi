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
package org.apache.nifi.processors.iceberg.converter;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.nifi.processors.iceberg.converter.RecordFieldGetter.createFieldGetter;

/**
 * Data converter implementations for different data types.
 */
public class GenericDataConverters {

    static class SameTypeConverter implements DataConverter<Object, Object> {

        static final SameTypeConverter INSTANCE = new SameTypeConverter();

        @Override
        public Object convert(Object data) {
            return data;
        }
    }

    static class TimeConverter implements DataConverter<Time, LocalTime> {

        static final TimeConverter INSTANCE = new TimeConverter();

        @Override
        public LocalTime convert(Time data) {
            return data.toLocalTime();
        }
    }

    static class TimestampConverter implements DataConverter<Timestamp, LocalDateTime> {

        static final TimestampConverter INSTANCE = new TimestampConverter();

        @Override
        public LocalDateTime convert(Timestamp data) {
            return data.toLocalDateTime();
        }
    }

    static class TimestampWithTimezoneConverter implements DataConverter<Timestamp, OffsetDateTime> {

        static final TimestampWithTimezoneConverter INSTANCE = new TimestampWithTimezoneConverter();

        @Override
        public OffsetDateTime convert(Timestamp data) {
            return OffsetDateTime.ofInstant(data.toInstant(), ZoneId.of("UTC"));
        }
    }

    static class UUIDtoByteArrayConverter implements DataConverter<UUID, byte[]> {

        static final UUIDtoByteArrayConverter INSTANCE = new UUIDtoByteArrayConverter();

        @Override
        public byte[] convert(UUID data) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
            byteBuffer.putLong(data.getMostSignificantBits());
            byteBuffer.putLong(data.getLeastSignificantBits());
            return byteBuffer.array();
        }
    }

    static class FixedConverter implements DataConverter<Byte[], byte[]> {

        private final int length;

        FixedConverter(int length) {
            this.length = length;
        }

        @Override
        public byte[] convert(Byte[] data) {
            Validate.isTrue(data.length == length, String.format("Cannot write byte array of length %s as fixed[%s]", data.length, length));
            return ArrayUtils.toPrimitive(data);
        }
    }

    static class BinaryConverter implements DataConverter<Byte[], ByteBuffer> {

        static final BinaryConverter INSTANCE = new BinaryConverter();

        @Override
        public ByteBuffer convert(Byte[] data) {
            return ByteBuffer.wrap(ArrayUtils.toPrimitive(data));
        }
    }

    static class BigDecimalConverter implements DataConverter<BigDecimal, BigDecimal> {

        private final int precision;
        private final int scale;

        BigDecimalConverter(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public BigDecimal convert(BigDecimal data) {
            Validate.isTrue(data.scale() == scale, "Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, data);
            Validate.isTrue(data.precision() <= precision, "Cannot write value as decimal(%s,%s), invalid precision: %s", precision, scale, data);
            return data;
        }
    }

    static class ArrayConverter<T, S> implements DataConverter<T[], List<S>> {
        private final DataConverter<T, S> fieldConverter;
        private final ArrayElementGetter.ElementGetter elementGetter;

        ArrayConverter(DataConverter<T, S> elementConverter, DataType dataType) {
            this.fieldConverter = elementConverter;
            this.elementGetter = ArrayElementGetter.createElementGetter(dataType);
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<S> convert(T[] data) {
            final int numElements = data.length;
            List<S> result = new ArrayList<>(numElements);
            for (int i = 0; i < numElements; i += 1) {
                result.add(i, fieldConverter.convert((T) elementGetter.getElementOrNull(data, i)));
            }
            return result;
        }
    }

    static class MapConverter<K, V, L, B> implements DataConverter<Map<K, V>, Map<L, B>> {
        private final DataConverter<K, L> keyConverter;
        private final DataConverter<V, B> valueConverter;
        private final ArrayElementGetter.ElementGetter keyGetter;
        private final ArrayElementGetter.ElementGetter valueGetter;

        MapConverter(DataConverter<K, L> keyConverter, DataType keyType, DataConverter<V, B> valueConverter, DataType valueType) {
            this.keyConverter = keyConverter;
            this.keyGetter = ArrayElementGetter.createElementGetter(keyType);
            this.valueConverter = valueConverter;
            this.valueGetter = ArrayElementGetter.createElementGetter(valueType);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Map<L, B> convert(Map<K, V> data) {
            final int mapSize = data.size();
            final Object[] keyArray = data.keySet().toArray();
            final Object[] valueArray = data.values().toArray();
            Map<L, B> result = new HashMap<>(mapSize);
            for (int i = 0; i < mapSize; i += 1) {
                result.put(keyConverter.convert((K) keyGetter.getElementOrNull(keyArray, i)), valueConverter.convert((V) valueGetter.getElementOrNull(valueArray, i)));
            }

            return result;
        }
    }

    static class RecordConverter implements DataConverter<Record, GenericRecord> {

        private final DataConverter<?, ?>[] converters;
        private final RecordFieldGetter.FieldGetter[] getters;

        private final Types.StructType schema;

        RecordConverter(List<DataConverter<?, ?>> converters, List<RecordField> recordFields, Types.StructType schema) {
            this.schema = schema;
            this.converters = (DataConverter<?, ?>[]) Array.newInstance(DataConverter.class, converters.size());
            this.getters = new RecordFieldGetter.FieldGetter[converters.size()];
            for (int i = 0; i < converters.size(); i += 1) {
                final RecordField recordField = recordFields.get(i);
                this.converters[i] = converters.get(i);
                this.getters[i] = createFieldGetter(recordField.getDataType(), recordField.getFieldName(), recordField.isNullable());
            }
        }

        @Override
        public GenericRecord convert(Record data) {
            final GenericRecord template = GenericRecord.create(schema);
            // GenericRecord.copy() is more performant then GenericRecord.create(StructType) since NAME_MAP_CACHE access is eliminated. Using copy here to gain performance.
            final GenericRecord result = template.copy();

            for (int i = 0; i < converters.length; i += 1) {
                result.set(i, convert(data, i, converters[i]));
            }

            return result;
        }

        @SuppressWarnings("unchecked")
        private <T, S> S convert(Record record, int pos, DataConverter<T, S> converter) {
            return converter.convert((T) getters[pos].getFieldOrNull(record));
        }
    }
}
