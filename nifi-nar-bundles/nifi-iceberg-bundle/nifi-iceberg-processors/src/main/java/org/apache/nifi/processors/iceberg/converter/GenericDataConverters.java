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
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
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

    static class PrimitiveTypeConverter extends DataConverter<Object, Object> {
        final Type.PrimitiveType targetType;
        final DataType sourceType;

        public PrimitiveTypeConverter(final Type.PrimitiveType type, final DataType dataType) {
            targetType = type;
            sourceType = dataType;
        }

        @Override
        public Object convert(Object data) {
            switch (targetType.typeId()) {
                case BOOLEAN:
                    return DataTypeUtils.toBoolean(data, null);
                case INTEGER:
                    return DataTypeUtils.toInteger(data, null);
                case LONG:
                    return DataTypeUtils.toLong(data, null);
                case FLOAT:
                    return DataTypeUtils.toFloat(data, null);
                case DOUBLE:
                    return DataTypeUtils.toDouble(data, null);
                case DATE:
                    return DataTypeUtils.toLocalDate(data, () -> DataTypeUtils.getDateTimeFormatter(sourceType.getFormat(), ZoneId.systemDefault()), null);
                case UUID:
                    return DataTypeUtils.toUUID(data);
                case STRING:
                default:
                    return DataTypeUtils.toString(data, () -> null);
            }
        }
    }

    static class TimeConverter extends DataConverter<Object, LocalTime> {

        private final String timeFormat;

        public TimeConverter(final String format) {
            this.timeFormat = format;
        }

        @Override
        public LocalTime convert(Object data) {
            return DataTypeUtils.toTime(data, () -> DataTypeUtils.getDateFormat(timeFormat), null).toLocalTime();
        }
    }

    static class TimestampConverter extends DataConverter<Object, LocalDateTime> {

        private final DataType dataType;

        public TimestampConverter(final DataType dataType) {
            this.dataType = dataType;
        }

        @Override
        public LocalDateTime convert(Object data) {
            final Timestamp convertedTimestamp = DataTypeUtils.toTimestamp(data, () -> DataTypeUtils.getDateFormat(dataType.getFormat()), null);
            return convertedTimestamp.toLocalDateTime();
        }
    }

    static class TimestampWithTimezoneConverter extends DataConverter<Object, OffsetDateTime> {

        private final DataType dataType;

        public TimestampWithTimezoneConverter(final DataType dataType) {
            this.dataType = dataType;
        }

        @Override
        public OffsetDateTime convert(Object data) {
            final Timestamp convertedTimestamp = DataTypeUtils.toTimestamp(data, () -> DataTypeUtils.getDateFormat(dataType.getFormat()), null);
            return OffsetDateTime.ofInstant(convertedTimestamp.toInstant(), ZoneId.of("UTC"));
        }
    }

    static class UUIDtoByteArrayConverter extends DataConverter<Object, byte[]> {

        @Override
        public byte[] convert(Object data) {
            final UUID uuid = DataTypeUtils.toUUID(data);
            ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
            byteBuffer.putLong(uuid.getMostSignificantBits());
            byteBuffer.putLong(uuid.getLeastSignificantBits());
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

    static class BigDecimalConverter extends DataConverter<Object, BigDecimal> {
        private final int precision;
        private final int scale;

        BigDecimalConverter(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public BigDecimal convert(Object data) {
            if (data instanceof BigDecimal) {
                BigDecimal bigDecimal = (BigDecimal) data;
                Validate.isTrue(bigDecimal.scale() == scale, "Cannot write value as decimal(%s,%s), wrong scale %s for value: %s", precision, scale, bigDecimal.scale(), data);
                Validate.isTrue(bigDecimal.precision() <= precision, "Cannot write value as decimal(%s,%s), invalid precision %s for value: %s",
                        precision, scale, bigDecimal.precision(), data);
                return bigDecimal;
            }
            return DataTypeUtils.toBigDecimal(data, null);
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
