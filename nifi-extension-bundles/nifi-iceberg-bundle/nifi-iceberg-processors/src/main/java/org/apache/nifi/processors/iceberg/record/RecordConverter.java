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
            RecordFieldType.MAP
    );

    /**
     * Get Converted Record with recursive, schema-aware handling for field values requiring translation
     *
     * @param inputRecord Input Record to be converted
     * @param struct Iceberg Struct Type describing the target field types (may be null for scalar-only conversion)
     * @return Input Record or new Record with converted field values
     */
    static Record getConvertedRecord(final Record inputRecord, final Types.StructType struct) {
        final RecordSchema recordSchema = inputRecord.getSchema();
        if (!isConversionRequired(recordSchema)) {
            return inputRecord;
        }

        final Map<String, Object> values = inputRecord.toMap();
        final Map<String, Object> convertedValues = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : values.entrySet()) {
            final String field = entry.getKey();
            final Type fieldType = fieldType(struct, field);
            convertedValues.put(field, convertValue(entry.getValue(), fieldType));
        }

        return new MapRecord(recordSchema, convertedValues);
    }

    static Object convertValue(final Object value, final Type icebergType) {
        return switch (value) {
            // Convert java.sql types to corresponding java.time types for Apache Iceberg
            case Timestamp timestamp -> timestamp.toLocalDateTime();
            case Date date -> date.toLocalDate();
            case Time time -> time.toLocalTime();
            case Object[] array when icebergType != null && icebergType.isListType() ->
                convertList(Arrays.asList(array), icebergType.asListType().elementType());
            case Collection<?> collection when icebergType != null && icebergType.isListType() ->
                convertList(collection, icebergType.asListType().elementType());
            case Record nestedRecord when icebergType != null && icebergType.isStructType() ->
                new DelegatedRecord(nestedRecord, icebergType.asStructType());
            case Map<?, ?> map when icebergType != null && icebergType.isMapType() ->
                convertMap(map, icebergType.asMapType());
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
        final Map<Object, Object> converted = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            final Object key = convertValue(entry.getKey(), mapType.keyType());
            final Object mappedValue = convertValue(entry.getValue(), mapType.valueType());
            converted.put(key, mappedValue);
        }
        return converted;
    }

    private static Type fieldType(final Types.StructType struct, final String fieldName) {
        if (struct == null) {
            return null;
        }
        final Types.NestedField nestedField = struct.field(fieldName);
        return nestedField == null ? null : nestedField.type();
    }

    private static boolean isConversionRequired(final RecordSchema recordSchema) {
        for (final RecordField field : recordSchema.getFields()) {
            final RecordFieldType recordFieldType = field.getDataType().getFieldType();
            if (CONVERSION_REQUIRED_FIELD_TYPES.contains(recordFieldType)) {
                return true;
            }
        }
        return false;
    }
}
