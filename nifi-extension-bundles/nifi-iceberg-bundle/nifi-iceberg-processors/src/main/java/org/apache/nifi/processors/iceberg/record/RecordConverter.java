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

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
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
            RecordFieldType.TIME
    );

    /**
     * Get Converted Record with conditional handling for field values requiring translation
     *
     * @param inputRecord Input Record to be converted
     * @return Input Record or new Record with converted field values
     */
    static Record getConvertedRecord(final Record inputRecord) {
        final Record convertedRecord;

        final RecordSchema recordSchema = inputRecord.getSchema();
        if (isConversionRequired(recordSchema)) {
            final Map<String, Object> values = inputRecord.toMap();
            convertedRecord = getConvertedRecord(recordSchema, values);
        } else {
            convertedRecord = inputRecord;
        }

        return convertedRecord;
    }

    private static Record getConvertedRecord(final RecordSchema recordSchema, final Map<String, Object> values) {
        final Map<String, Object> convertedValues = new LinkedHashMap<>();

        for (final Map.Entry<String, Object> entry : values.entrySet()) {
            final String field = entry.getKey();
            final Object value = entry.getValue();
            final Object converted = getConvertedValue(value);
            convertedValues.put(field, converted);
        }

        return new MapRecord(recordSchema, convertedValues);
    }

    private static Object getConvertedValue(final Object value) {
        return switch (value) {
            // Convert java.sql types to corresponding java.time types for Apache Iceberg
            case Timestamp timestamp -> timestamp.toLocalDateTime();
            case Date date -> date.toLocalDate();
            case Time time -> time.toLocalTime();
            case null, default -> value;
        };
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
