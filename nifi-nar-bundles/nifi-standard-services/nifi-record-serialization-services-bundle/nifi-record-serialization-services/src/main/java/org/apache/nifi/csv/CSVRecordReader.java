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

package org.apache.nifi.csv;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;


public class CSVRecordReader implements RecordReader {
    private final CSVParser csvParser;
    private final RecordSchema schema;

    private final Supplier<DateFormat> LAZY_DATE_FORMAT;
    private final Supplier<DateFormat> LAZY_TIME_FORMAT;
    private final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;

    private List<String> rawFieldNames;

    public CSVRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema, final CSVFormat csvFormat, final boolean hasHeader, final boolean ignoreHeader,
        final String dateFormat, final String timeFormat, final String timestampFormat) throws IOException {

        this.schema = schema;
        final DateFormat df = dateFormat == null ? null : DataTypeUtils.getDateFormat(dateFormat);
        final DateFormat tf = timeFormat == null ? null : DataTypeUtils.getDateFormat(timeFormat);
        final DateFormat tsf = timestampFormat == null ? null : DataTypeUtils.getDateFormat(timestampFormat);

        LAZY_DATE_FORMAT = () -> df;
        LAZY_TIME_FORMAT = () -> tf;
        LAZY_TIMESTAMP_FORMAT = () -> tsf;

        final Reader reader = new InputStreamReader(new BOMInputStream(in));

        CSVFormat withHeader;
        if (hasHeader) {
            withHeader = csvFormat.withSkipHeaderRecord();

            if (ignoreHeader) {
                withHeader = withHeader.withHeader(schema.getFieldNames().toArray(new String[0]));
            }
        } else {
            withHeader = csvFormat.withHeader(schema.getFieldNames().toArray(new String[0]));
        }

        csvParser = new CSVParser(reader, withHeader);
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        final RecordSchema schema = getSchema();

        final List<String> rawFieldNames = getRawFieldNames();
        final int numFieldNames = rawFieldNames.size();

        for (final CSVRecord csvRecord : csvParser) {
            final Map<String, Object> values = new LinkedHashMap<>();
            for (int i = 0; i < csvRecord.size(); i++) {
                final String rawFieldName = numFieldNames <= i ? "unknown_field_index_" + i : rawFieldNames.get(i);
                final String rawValue = csvRecord.get(i);

                final Optional<DataType> dataTypeOption = schema.getDataType(rawFieldName);

                if (!dataTypeOption.isPresent() && dropUnknownFields) {
                    continue;
                }

                final Object value;
                if (coerceTypes && dataTypeOption.isPresent()) {
                    value = convert(rawValue, dataTypeOption.get(), rawFieldName);
                } else if (dataTypeOption.isPresent()) {
                    // The CSV Reader is going to return all fields as Strings, because CSV doesn't have any way to
                    // dictate a field type. As a result, we will use the schema that we have to attempt to convert
                    // the value into the desired type if it's a simple type.
                    value = convertSimpleIfPossible(rawValue, dataTypeOption.get(), rawFieldName);
                } else {
                    value = rawValue;
                }

                values.put(rawFieldName, value);
            }

            return new MapRecord(schema, values, coerceTypes, dropUnknownFields);
        }

        return null;
    }


    private List<String> getRawFieldNames() {
        if (this.rawFieldNames != null) {
            return this.rawFieldNames;
        }

        // Use a SortedMap keyed by index of the field so that we can get a List of field names in the correct order
        final SortedMap<Integer, String> sortedMap = new TreeMap<>();
        for (final Map.Entry<String, Integer> entry : csvParser.getHeaderMap().entrySet()) {
            sortedMap.put(entry.getValue(), entry.getKey());
        }

        this.rawFieldNames = new ArrayList<>(sortedMap.values());
        return this.rawFieldNames;
    }


    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    protected Object convert(final String value, final DataType dataType, final String fieldName) {
        if (dataType == null || value == null) {
            return value;
        }

        final String trimmed = value.startsWith("\"") && value.endsWith("\"") ? value.substring(1, value.length() - 1) : value;
        if (trimmed.isEmpty()) {
            return null;
        }

        return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
    }

    private Object convertSimpleIfPossible(final String value, final DataType dataType, final String fieldName) {
        if (dataType == null || value == null) {
            return value;
        }

        final String trimmed = value.startsWith("\"") && value.endsWith("\"") ? value.substring(1, value.length() - 1) : value;
        if (trimmed.isEmpty()) {
            return null;
        }

        switch (dataType.getFieldType()) {
            case STRING:
                return value;
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BYTE:
            case CHAR:
            case SHORT:
            case TIME:
            case TIMESTAMP:
            case DATE:
                if (DataTypeUtils.isCompatibleDataType(trimmed, dataType)) {
                    return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                } else {
                    return value;
                }
        }

        return value;
    }

    @Override
    public void close() throws IOException {
        csvParser.close();
    }
}
