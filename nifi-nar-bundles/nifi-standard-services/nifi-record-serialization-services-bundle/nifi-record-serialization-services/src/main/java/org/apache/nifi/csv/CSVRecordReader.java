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
import java.util.HashMap;
import java.util.Map;
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
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;


public class CSVRecordReader implements RecordReader {
    private final CSVParser csvParser;
    private final RecordSchema schema;

    private final Supplier<DateFormat> LAZY_DATE_FORMAT;
    private final Supplier<DateFormat> LAZY_TIME_FORMAT;
    private final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;

    public CSVRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema, final CSVFormat csvFormat,
        final String dateFormat, final String timeFormat, final String timestampFormat) throws IOException {

        this.schema = schema;
        final DateFormat df = dateFormat == null ? null : DataTypeUtils.getDateFormat(dateFormat);
        final DateFormat tf = timeFormat == null ? null : DataTypeUtils.getDateFormat(timeFormat);
        final DateFormat tsf = timestampFormat == null ? null : DataTypeUtils.getDateFormat(timestampFormat);

        LAZY_DATE_FORMAT = () -> df;
        LAZY_TIME_FORMAT = () -> tf;
        LAZY_TIMESTAMP_FORMAT = () -> tsf;

        final Reader reader = new InputStreamReader(new BOMInputStream(in));
        final CSVFormat withHeader = csvFormat.withHeader(schema.getFieldNames().toArray(new String[0]));
        csvParser = new CSVParser(reader, withHeader);
    }

    @Override
    public Record nextRecord() throws IOException, MalformedRecordException {
        final RecordSchema schema = getSchema();

        for (final CSVRecord csvRecord : csvParser) {
            final Map<String, Object> rowValues = new HashMap<>(schema.getFieldCount());

            for (final RecordField recordField : schema.getFields()) {
                String rawValue = null;
                final String fieldName = recordField.getFieldName();
                if (csvRecord.isSet(fieldName)) {
                    rawValue = csvRecord.get(fieldName);
                } else {
                    for (final String alias : recordField.getAliases()) {
                        if (csvRecord.isSet(alias)) {
                            rawValue = csvRecord.get(alias);
                            break;
                        }
                    }
                }

                if (rawValue == null) {
                    rowValues.put(fieldName, null);
                    continue;
                }

                final Object converted = convert(rawValue, recordField.getDataType(), fieldName);
                if (converted != null) {
                    rowValues.put(fieldName, converted);
                }
            }

            return new MapRecord(schema, rowValues);
        }

        return null;
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

    @Override
    public void close() throws IOException {
        csvParser.close();
    }
}
