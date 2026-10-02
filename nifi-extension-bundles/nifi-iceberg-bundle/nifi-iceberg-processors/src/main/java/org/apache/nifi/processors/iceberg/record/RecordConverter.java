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

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
            RecordFieldType.ARRAY,
            RecordFieldType.RECORD,
            RecordFieldType.MAP,
            // CHOICE can wrap any of the above, so it must also trigger conversion.
            RecordFieldType.CHOICE
    );

    /**
     * Get Converted Record with recursive, schema-aware handling for field values requiring translation
     *
     * @param inputRecord Input Record to be converted
     * @param struct Iceberg Struct Type describing the target field types (may be null for scalar-only conversion)
     * @return Input Record or new Record with converted field values
     */
    static Record getConvertedRecord(final Record inputRecord, final Types.StructType struct) {
        final Record convertedRecord;

        final RecordSchema recordSchema = inputRecord.getSchema();
        if (isConversionRequired(recordSchema) || isUuidConversionRequired(struct)) {
            final Map<String, Object> values = inputRecord.toMap();
            final Map<String, Object> convertedValues = new LinkedHashMap<>(values.size());
            for (final Map.Entry<String, Object> entry : values.entrySet()) {
                final String field = entry.getKey();
                final Type fieldType = fieldType(struct, field);
                convertedValues.put(field, convertValue(entry.getValue(), fieldType));
            }
            convertedRecord = new MapRecord(recordSchema, convertedValues);
        } else {
            convertedRecord = inputRecord;
        }

        return convertedRecord;
    }

    static Object convertValue(final Object value, final Type icebergType) {
        return switch (value) {
            // Convert java.sql types to corresponding java.time types for Apache Iceberg
            case Timestamp timestamp -> convertTimestamp(timestamp, icebergType);
            case Date date -> date.toLocalDate();
            case Time time -> time.toLocalTime();
            // Recursively convert complex types and convert binary and UUID values against the matching Iceberg type
            case null, default -> convertComplexValue(value, icebergType);
        };
    }

    /**
     * Convert a Timestamp to the java.time type required by the target Iceberg Type. Iceberg Types declaring an
     * adjustment to UTC require an OffsetDateTime, and other Types require a LocalDateTime. A Timestamp identifies
     * an instant, so the adjusted conversion preserves that instant expressed at UTC
     *
     * @param timestamp Timestamp to be converted
     * @param icebergType Iceberg Type describing the target field type (may be null when not resolved)
     * @return OffsetDateTime at UTC for Iceberg Types adjusted to UTC or LocalDateTime for other Types
     */
    private static Object convertTimestamp(final Timestamp timestamp, final Type icebergType) {
        return shouldAdjustToUtc(icebergType) ? timestamp.toInstant().atOffset(ZoneOffset.UTC) : timestamp.toLocalDateTime();
    }

    /**
     * Determine whether the Iceberg Type declares an adjustment to UTC, which Apache Iceberg requires for the
     * timestamptz and timestamptz_ns column types
     *
     * @param icebergType Iceberg Type describing the target field type (may be null when not resolved)
     * @return Adjustment to UTC required status
     */
    private static boolean shouldAdjustToUtc(final Type icebergType) {
        return switch (icebergType) {
            case Types.TimestampType timestampType -> timestampType.shouldAdjustToUTC();
            case Types.TimestampNanoType timestampNanoType -> timestampNanoType.shouldAdjustToUTC();
            case null, default -> false;
        };
    }

    /**
     * Recursively convert array, collection, nested record, and map values against the matching Iceberg type, and convert
     * values for Iceberg primitive types that require specific Java types
     *
     * @param value Field value to be converted
     * @param icebergType Iceberg Type describing the target field type (may be null when not resolved)
     * @return Converted value or the input value when the Iceberg Type is unknown or does not require conversion of the
     * value
     */
    private static Object convertComplexValue(final Object value, final Type icebergType) {
        final Object convertedValue;

        if (icebergType == null) {
            convertedValue = value;
        } else if (icebergType.isListType()) {
            convertedValue = convertListValue(value, icebergType.asListType());
        } else if (icebergType.isStructType() && value instanceof Record nestedRecord) {
            convertedValue = new DelegatedRecord(nestedRecord, icebergType.asStructType());
        } else if (icebergType.isMapType() && value instanceof Map<?, ?> map) {
            convertedValue = convertMap(map, icebergType.asMapType());
        } else {
            convertedValue = convertPrimitiveValue(value, icebergType);
        }

        return convertedValue;
    }

    /**
     * Convert an array or collection value to the List required for Apache Iceberg with elements converted against
     * the Iceberg element type
     *
     * @param value Field value to be converted
     * @param listType Iceberg List Type describing the target element type
     * @return Converted List or the input value when the value is neither an array nor a collection
     */
    private static Object convertListValue(final Object value, final Types.ListType listType) {
        final Type elementType = listType.elementType();
        return switch (value) {
            case Object[] array -> convertList(Arrays.asList(array), elementType);
            case Collection<?> collection -> convertList(collection, elementType);
            case null, default -> value;
        };
    }

    private static List<Object> convertList(final Collection<?> collection, final Type elementType) {
        final List<Object> converted = new ArrayList<>(collection.size());
        for (final Object element : collection) {
            converted.add(convertValue(element, elementType));
        }
        return converted;
    }

    private static Map<Object, Object> convertMap(final Map<?, ?> map, final Types.MapType mapType) {
        // Using LinkedHashMap here to keep input ordering for deterministic flows.
        final Map<Object, Object> converted = new LinkedHashMap<>(map.size());
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            final Object key = convertValue(entry.getKey(), mapType.keyType());
            final Object mappedValue = convertValue(entry.getValue(), mapType.valueType());
            converted.put(key, mappedValue);
        }
        return converted;
    }

    /**
     * Convert byte array and UUID values to the Java types that Apache Iceberg requires for the matching primitive type.
     * Record Readers provide bytes as byte arrays or as Object arrays of Byte elements, and UUIDs as Strings or byte arrays.
     *
     * @param value Field value to be converted
     * @param icebergType Iceberg Type describing the target field type
     * @return ByteBuffer for binary, geometry, and geography types, byte array for fixed types, UUID for uuid types, or the
     * input value when the Iceberg Type does not require conversion of the value
     */
    private static Object convertPrimitiveValue(final Object value, final Type icebergType) {
        return switch (icebergType.typeId()) {
            // Geometry and geography values are encoded as Well-Known Binary
            case BINARY, GEOMETRY, GEOGRAPHY -> switch (value) {
                case byte[] bytes -> ByteBuffer.wrap(bytes);
                case Object[] array -> ByteBuffer.wrap(toByteArray(array));
                case null, default -> value;
            };
            case FIXED -> switch (value) {
                case Object[] array -> toByteArray(array);
                case ByteBuffer buffer -> ByteBuffers.toByteArray(buffer);
                case null, default -> value;
            };
            case UUID -> switch (value) {
                case String string -> DataTypeUtils.toUUID(string);
                case byte[] bytes -> DataTypeUtils.toUUID(bytes);
                case Object[] array -> DataTypeUtils.toUUID(toByteArray(array));
                case null, default -> value;
            };
            default -> value;
        };
    }

    private static byte[] toByteArray(final Object[] array) {
        final byte[] bytes = new byte[array.length];
        for (int index = 0; index < array.length; index++) {
            bytes[index] = (Byte) array[index];
        }

        return bytes;
    }

    private static Type fieldType(final Types.StructType struct, final String fieldName) {
        final Types.NestedField nestedField = struct == null ? null : struct.field(fieldName);
        return nestedField == null ? null : nestedField.type();
    }

    private static boolean isConversionRequired(final RecordSchema recordSchema) {
        return recordSchema.getFields().stream()
                .map(RecordField::getDataType)
                .map(DataType::getFieldType)
                .anyMatch(CONVERSION_REQUIRED_FIELD_TYPES::contains);
    }

    /**
     * Determine whether the Iceberg Struct Type contains uuid fields, which require conversion of String values that do
     * not otherwise require Record conversion
     *
     * @param struct Iceberg Struct Type describing the target field types (may be null for scalar-only conversion)
     * @return UUID conversion required status
     */
    private static boolean isUuidConversionRequired(final Types.StructType struct) {
        return struct != null && struct.fields().stream()
                .map(Types.NestedField::type)
                .anyMatch(type -> type.typeId() == Type.TypeID.UUID);
    }
}
