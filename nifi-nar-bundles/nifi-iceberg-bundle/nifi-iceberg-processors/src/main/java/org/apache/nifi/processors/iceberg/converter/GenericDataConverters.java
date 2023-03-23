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
import org.apache.nifi.serialization.record.RecordSchema;

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
import java.util.Optional;
import java.util.UUID;

import static org.apache.nifi.processors.iceberg.converter.RecordFieldGetter.createFieldGetter;

/**
 * Data converter implementations for different data types.
 */
public class GenericDataConverters {

    static class SameTypeConverter extends DataConverter<Object, Object> {

        @Override
        public Object convert(Object data) {
            return data;
        }
    }

    static class TimeConverter extends DataConverter<Time, LocalTime> {

        @Override
        public LocalTime convert(Time data) {
            return data.toLocalTime();
        }
    }

    static class TimestampConverter extends DataConverter<Timestamp, LocalDateTime> {

        @Override
        public LocalDateTime convert(Timestamp data) {
            return data.toLocalDateTime();
        }
    }

    static class TimestampWithTimezoneConverter extends DataConverter<Timestamp, OffsetDateTime> {

        @Override
        public OffsetDateTime convert(Timestamp data) {
            return OffsetDateTime.ofInstant(data.toInstant(), ZoneId.of("UTC"));
        }
    }

    static class UUIDtoByteArrayConverter extends DataConverter<UUID, byte[]> {

        @Override
        public byte[] convert(UUID data) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
            byteBuffer.putLong(data.getMostSignificantBits());
            byteBuffer.putLong(data.getLeastSignificantBits());
            return byteBuffer.array();
        }
    }

    static class FixedConverter extends DataConverter<Byte[], byte[]> {

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

    static class BinaryConverter extends DataConverter<Byte[], ByteBuffer> {

        @Override
        public ByteBuffer convert(Byte[] data) {
            return ByteBuffer.wrap(ArrayUtils.toPrimitive(data));
        }
    }

    static class BigDecimalConverter extends DataConverter<BigDecimal, BigDecimal> {
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

    static class ArrayConverter<S, T> extends DataConverter<S[], List<T>> {
        private final DataConverter<S, T> fieldConverter;
        private final ArrayElementGetter.ElementGetter elementGetter;

        ArrayConverter(DataConverter<S, T> elementConverter, DataType dataType) {
            this.fieldConverter = elementConverter;
            this.elementGetter = ArrayElementGetter.createElementGetter(dataType);
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<T> convert(S[] data) {
            final int numElements = data.length;
            final List<T> result = new ArrayList<>(numElements);
            for (int i = 0; i < numElements; i += 1) {
                result.add(i, fieldConverter.convert((S) elementGetter.getElementOrNull(data[i])));
            }
            return result;
        }
    }

    static class MapConverter<SK, SV, TK, TV> extends DataConverter<Map<SK, SV>, Map<TK, TV>> {
        private final DataConverter<SK, TK> keyConverter;
        private final DataConverter<SV, TV> valueConverter;
        private final ArrayElementGetter.ElementGetter keyGetter;
        private final ArrayElementGetter.ElementGetter valueGetter;

        MapConverter(DataConverter<SK, TK> keyConverter, DataType keyType, DataConverter<SV, TV> valueConverter, DataType valueType) {
            this.keyConverter = keyConverter;
            this.keyGetter = ArrayElementGetter.createElementGetter(keyType);
            this.valueConverter = valueConverter;
            this.valueGetter = ArrayElementGetter.createElementGetter(valueType);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Map<TK, TV> convert(Map<SK, SV> data) {
            final int mapSize = data.size();
            final Object[] keyArray = data.keySet().toArray();
            final Object[] valueArray = data.values().toArray();
            final Map<TK, TV> result = new HashMap<>(mapSize);
            for (int i = 0; i < mapSize; i += 1) {
                result.put(keyConverter.convert((SK) keyGetter.getElementOrNull(keyArray[i])), valueConverter.convert((SV) valueGetter.getElementOrNull(valueArray[i])));
            }

            return result;
        }
    }

    static class RecordConverter extends DataConverter<Record, GenericRecord> {

        private final List<DataConverter<?, ?>> converters;
        private final Map<String, RecordFieldGetter.FieldGetter> getters;

        private final Types.StructType schema;

        RecordConverter(List<DataConverter<?, ?>> converters, RecordSchema recordSchema, Types.StructType schema) {
            this.schema = schema;
            this.converters = converters;
            this.getters = new HashMap<>(converters.size());

            for (DataConverter<?, ?> converter : converters) {
                final Optional<RecordField> recordField = recordSchema.getField(converter.getSourceFieldName());
                final RecordField field = recordField.get();
                // creates a record field accessor for every data converter
                getters.put(converter.getTargetFieldName(), createFieldGetter(field.getDataType(), field.getFieldName(), field.isNullable()));
            }
        }

        @Override
        public GenericRecord convert(Record data) {
            final GenericRecord record = GenericRecord.create(schema);

            for (DataConverter<?, ?> converter : converters) {
                record.setField(converter.getTargetFieldName(), convert(data, converter));
            }

            return record;
        }

        @SuppressWarnings("unchecked")
        private <S, T> T convert(Record record, DataConverter<S, T> converter) {
            return converter.convert((S) getters.get(converter.getTargetFieldName()).getFieldOrNull(record));
        }
    }
}
