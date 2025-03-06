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
package org.apache.nifi.processors.snowflake.snowpipe.streaming.converter;

import net.snowflake.ingest.streaming.internal.ColumnProperties;
import org.apache.nifi.serialization.SchemaValidationException;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Standard implementation of converter from NiFi Record to Snowpipe Streaming Row Map using Logical Data Types
 */
public class StandardRecordRowConverter implements RecordRowConverter {
    private static final ObjectLogicalDataTypeConverter objectLogicalDataTypeConverter = new ObjectLogicalDataTypeConverter();

    private final Map<String, LogicalDataType> logicalDataTypes = new HashMap<>();

    private final Map<String, String> fieldColumnNames = new HashMap<>();

    public StandardRecordRowConverter(final RecordSchema recordSchema, final Map<String, ColumnProperties> namedColumnProperties) {
        Objects.requireNonNull(recordSchema, "Record Schema required");
        Objects.requireNonNull(namedColumnProperties, "Named ColumnProperties required");

        for (final RecordField field : recordSchema.getFields()) {
            final String fieldName = field.getFieldName();
            final String columnName = getColumnName(fieldName, namedColumnProperties);
            fieldColumnNames.put(fieldName, columnName);

            final String logicalType = namedColumnProperties.get(columnName).getLogicalType();

            LogicalDataType logicalDataType = LogicalDataType.UNKNOWN;
            try {
                logicalDataType = LogicalDataType.valueOf(logicalType);
            } catch (final IllegalArgumentException ignored) {

            }
            logicalDataTypes.put(fieldName, logicalDataType);
        }
    }

    /**
     * Convert Record to Snowpipe Streaming Row using Logical Data Type conversion
     *
     * @param record Record to be converted
     * @param row Snowpipe Streaming Row Map for converted fields
     */
    @Override
    public void convertRecord(final Record record, final Map<String, Object> row) {
        for (final RecordField field : record.getSchema().getFields()) {
            final String fieldName = field.getFieldName();
            final Object value = record.getValue(fieldName);
            final LogicalDataType logicalDataType = logicalDataTypes.get(fieldName);
            final Object convertedObject = objectLogicalDataTypeConverter.getConvertedObject(value, logicalDataType);

            final String columnName = fieldColumnNames.get(fieldName);
            row.put(columnName, convertedObject);
        }
    }

    private String getColumnName(final String recordFieldName, final Map<String, ColumnProperties> namedColumnProperties) {
        if (namedColumnProperties.containsKey(recordFieldName)) {
            return recordFieldName;
        }

        final String quoted = '"' + recordFieldName.replace("\"", "\"\"") + '"';
        if (namedColumnProperties.containsKey(quoted)) {
            return quoted;
        }

        final String fieldNameUpperCased = recordFieldName.toUpperCase();
        if (namedColumnProperties.containsKey(fieldNameUpperCased)) {
            return fieldNameUpperCased;
        }

        throw new SchemaValidationException("Column Name not found for Record Field [%s]".formatted(recordFieldName));
    }
}
