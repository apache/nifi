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
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.field.FieldConverter;
import org.apache.nifi.serialization.record.field.StandardFieldConverterRegistry;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
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
                    final FieldConverter<Object, LocalDate> converter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(LocalDate.class);
                    return converter.convertField(data, Optional.ofNullable(sourceType.getFormat()), null);
                case UUID:
                    return DataTypeUtils.toUUID(data);
                case STRING:
                default:
                    return StandardFieldConverterRegistry.getRegistry().getFieldConverter(String.class).convertField(data, Optional.empty(), null);
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
            final FieldConverter<Object, LocalTime> converter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(LocalTime.class);
            return converter.convertField(data, Optional.ofNullable(timeFormat), null);
        }
    }

    static class TimestampConverter extends DataConverter<Object, LocalDateTime> {

        private final DataType dataType;

        public TimestampConverter(final DataType dataType) {
            this.dataType = dataType;
        }

        @Override
        public LocalDateTime convert(Object data) {
            final FieldConverter<Object, LocalDateTime> converter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(LocalDateTime.class);
            return converter.convertField(data, Optional.ofNullable(dataType.getFormat()), null);
        }
    }

    static class TimestampWithTimezoneConverter extends DataConverter<Object, OffsetDateTime> {

        private final DataType dataType;

        public TimestampWithTimezoneConverter(final DataType dataType) {
            this.dataType = dataType;
        }

        @Override
        public OffsetDateTime convert(Object data) {
            final FieldConverter<Object, OffsetDateTime> converter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(OffsetDateTime.class);
            return converter.convertField(data, Optional.ofNullable(dataType.getFormat()), null);
        }
    }

    static class UUIDtoByteArrayConverter extends DataConverter<Object, byte[]> {

        @Override
        public byte[] convert(Object data) {
            if (data == null) {
                return null;
            }
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
            if (data == null) {
                return null;
            }
            Validate.isTrue(data.length == length, String.format("Cannot write byte array of length %s as fixed[%s]", data.length, length));
            return ArrayUtils.toPrimitive(data);
        }
    }

    static class BinaryConverter extends DataConverter<Byte[], ByteBuffer> {

        @Override
        public ByteBuffer convert(Byte[] data) {
            if (data == null) {
                return null;
            }
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
            if (data == null) {
                return null;
            }
            if (data instanceof BigDecimal bigDecimal) {
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
            if (data == null) {
                return null;
            }
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
            if (data == null) {
                return null;
            }
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
                if (recordField.isEmpty()) {
                    final Types.NestedField missingField = schema.field(converter.getTargetFieldName());
                    if (missingField != null) {
                        getters.put(converter.getTargetFieldName(), createFieldGetter(convertSchemaTypeToDataType(missingField.type()), missingField.name(), missingField.isOptional()));
                    }
                } else {
                    final RecordField field = recordField.get();
                    // creates a record field accessor for every data converter
                    getters.put(converter.getTargetFieldName(), createFieldGetter(field.getDataType(), field.getFieldName(), field.isNullable()));
                }
            }
        }

        @Override
        public GenericRecord convert(Record data) {
            if (data == null) {
                return null;
            }
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

    public static DataType convertSchemaTypeToDataType(Type schemaType) {
        switch (schemaType.typeId()) {
            case BOOLEAN:
                return RecordFieldType.BOOLEAN.getDataType();
            case INTEGER:
                return RecordFieldType.INT.getDataType();
            case LONG:
                return RecordFieldType.LONG.getDataType();
            case FLOAT:
                return RecordFieldType.FLOAT.getDataType();
            case DOUBLE:
                return RecordFieldType.DOUBLE.getDataType();
            case DATE:
                return RecordFieldType.DATE.getDataType();
            case TIME:
                return RecordFieldType.TIME.getDataType();
            case TIMESTAMP:
                return RecordFieldType.TIMESTAMP.getDataType();
            case STRING:
                return RecordFieldType.STRING.getDataType();
            case UUID:
                return RecordFieldType.UUID.getDataType();
            case FIXED:
            case BINARY:
                return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
            case DECIMAL:
                return RecordFieldType.DECIMAL.getDataType();
            case STRUCT:
                // Build a record type from the struct type
                Types.StructType structType = schemaType.asStructType();
                List<Types.NestedField> fields = structType.fields();
                List<RecordField> recordFields = new ArrayList<>(fields.size());
                for (Types.NestedField field : fields) {
                    DataType dataType = convertSchemaTypeToDataType(field.type());
                    recordFields.add(new RecordField(field.name(), dataType, field.isOptional()));
                }
                RecordSchema recordSchema = new SimpleRecordSchema(recordFields);
                return RecordFieldType.RECORD.getRecordDataType(recordSchema);
            case LIST:
                // Build a list type from the elements
                Types.ListType listType = schemaType.asListType();
                return RecordFieldType.ARRAY.getArrayDataType(convertSchemaTypeToDataType(listType.elementType()), listType.isElementOptional());
            case MAP:
                // Build a map type from the elements
                Types.MapType mapType = schemaType.asMapType();
                return RecordFieldType.MAP.getMapDataType(convertSchemaTypeToDataType(mapType.valueType()), mapType.isValueOptional());
        }
        throw new IllegalArgumentException("Invalid or unsupported type: " + schemaType);
    }
}
