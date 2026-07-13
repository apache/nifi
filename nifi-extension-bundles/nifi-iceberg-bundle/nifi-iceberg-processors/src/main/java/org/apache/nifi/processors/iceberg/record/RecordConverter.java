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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.types.Types;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Record Converter handles translating field values to types compatible with Apache Iceberg Records
 */
class RecordConverter {

    private static final Set<RecordFieldType> CONVERSION_REQUIRED_FIELD_TYPES = Set.of(
            RecordFieldType.TIMESTAMP,
            RecordFieldType.DATE,
            RecordFieldType.TIME,
            RecordFieldType.UUID,
            RecordFieldType.ARRAY,
            RecordFieldType.RECORD,
            RecordFieldType.MAP
    );

    /**
     * Get Converted Record with conditional handling for field values requiring translation
     *
     * @param inputRecord Input Record to be converted
     * @return Input Record or new Record with converted field values
     */
    static Record getConvertedRecord(final Record inputRecord, final Types.StructType struct) {
        final Record convertedRecord;

        if (isConversionRequired(inputRecord.getSchema())) {
            convertedRecord = convertRecord(inputRecord, struct);
        } else {
            convertedRecord = inputRecord;
        }

        return convertedRecord;
    }

    static Record convertRecord(final Record inputRecord, final Types.StructType struct) {
        final RecordSchema recordSchema = inputRecord.getSchema();
        final Map<String, Object> values = inputRecord.toMap();
        final Map<String, Object> convertedValues = new LinkedHashMap<>();

        for (final RecordField field : recordSchema.getFields()) {
            final String fieldName = field.getFieldName();
            final Types.NestedField icebergField = struct.field(fieldName);
            final Object rawValue = values.get(fieldName);
            if (icebergField == null) {
                convertedValues.put(fieldName, rawValue);
            }
            else {
                convertedValues.put(fieldName, convertValue(rawValue, icebergField.type()));
            }
        }

        return new MapRecord(recordSchema, convertedValues);
    }

    private static Object convertValue(final Object value, final org.apache.iceberg.types.Type icebergType) {
        if (value == null) {
            return null;
        }

        return switch (icebergType.typeId()) {
            case TIMESTAMP -> {
                final LocalDateTime local = ((Timestamp) value).toLocalDateTime();
                yield ((Types.TimestampType) icebergType).shouldAdjustToUTC()
                        ? local.atOffset(ZoneOffset.UTC)
                        : local;
            }
            case UUID -> value instanceof java.util.UUID ? value : java.util.UUID.fromString(value.toString());
            case FIXED -> toByteArray(value);
            case BINARY -> ByteBuffer.wrap(toByteArray(value));
            case LIST -> {
                final Iterable<?> elements = value instanceof Object[] array ? Arrays.asList(array) : (List<?>) value;
                final List<Object> list = new ArrayList<>();
                for (final Object element : elements) {
                    list.add(convertValue(element, ((Types.ListType) icebergType).elementType()));
                }
                yield list;
            }
            case STRUCT -> value instanceof final org.apache.iceberg.data.Record convertedStruct
                    ? convertedStruct
                    : new DelegatedRecord(
                            (org.apache.nifi.serialization.record.Record) value,
                            (Types.StructType) icebergType
                    );
            case MAP -> {
                final Types.MapType mapType = (Types.MapType) icebergType;
                final Map<Object, Object> converted = new LinkedHashMap<>();
                for (final Map.Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
                    final Object convertedKey = convertValue(entry.getKey(), mapType.keyType());
                    final Object convertedValue = convertValue(entry.getValue(), mapType.valueType());
                    converted.put(convertedKey, convertedValue);
                }
                yield converted;
            }
            case DATE -> ((Date) value).toLocalDate();
            case TIME -> ((Time) value).toLocalTime();
            default -> value;
        };
    }

    private static byte[] toByteArray(final Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        final Object[] boxed = (Object[]) value;
        final Byte[] boxedBytes = Arrays.copyOf(boxed, boxed.length, Byte[].class);
        return ArrayUtils.toPrimitive(boxedBytes);
    }

    private static boolean isConversionRequired(final RecordSchema recordSchema) {
        final List<RecordField> fields = recordSchema.getFields();

        for (final RecordField field : fields) {
            final DataType dataType = field.getDataType();
            final RecordFieldType recordFieldType = dataType.getFieldType();
            if (CONVERSION_REQUIRED_FIELD_TYPES.contains(recordFieldType)) {
                return true;
            }
        }

        return false;
    }
}
