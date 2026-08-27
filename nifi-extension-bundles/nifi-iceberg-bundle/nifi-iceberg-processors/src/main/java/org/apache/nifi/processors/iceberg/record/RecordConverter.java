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
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
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
        if (isConversionRequired(recordSchema)) {
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
            case Timestamp timestamp -> timestamp.toLocalDateTime();
            case Date date -> date.toLocalDate();
            case Time time -> time.toLocalTime();
            // Recursively convert complex types against the matching Iceberg type
            case null, default -> convertComplexValue(value, icebergType);
        };
    }

    /**
     * Recursively convert array, collection, nested record, and map values against the matching Iceberg type
     *
     * @param value Field value to be converted
     * @param icebergType Iceberg Type describing the target field type (may be null when not resolved)
     * @return Converted value or the input value when the Iceberg Type is unknown or does not describe a complex
     * type matching the value
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
            convertedValue = value;
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
}
