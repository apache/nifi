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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.DateFormat;
import java.util.function.Supplier;

abstract public class AbstractCSVRecordReader implements RecordReader {

    protected final ComponentLog logger;
    protected final boolean hasHeader;
    protected final boolean ignoreHeader;
    private final boolean trimDoubleQuote;

    protected final int skipTopRows;

    protected final Supplier<DateFormat> LAZY_DATE_FORMAT;
    protected final Supplier<DateFormat> LAZY_TIME_FORMAT;
    protected final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;

    protected final String dateFormat;
    protected final String timeFormat;
    protected final String timestampFormat;

    protected final RecordSchema schema;

    protected final Reader inputStreamReader;

    AbstractCSVRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema, final CSVFormat csvFormat, final boolean hasHeader, final boolean ignoreHeader,
                            final String dateFormat, final String timeFormat, final String timestampFormat, final String encoding, final boolean trimDoubleQuote, final int skipTopRows)
            throws IOException {

        this.logger = logger;
        this.schema = schema;
        this.hasHeader = hasHeader;
        this.ignoreHeader = ignoreHeader;
        this.trimDoubleQuote = trimDoubleQuote;
        this.skipTopRows = skipTopRows;

        if (dateFormat == null || dateFormat.isEmpty()) {
            this.dateFormat = null;
            LAZY_DATE_FORMAT = null;
        } else {
            this.dateFormat = dateFormat;
            LAZY_DATE_FORMAT = () -> DataTypeUtils.getDateFormat(dateFormat);
        }

        if (timeFormat == null || timeFormat.isEmpty()) {
            this.timeFormat = null;
            LAZY_TIME_FORMAT = null;
        } else {
            this.timeFormat = timeFormat;
            LAZY_TIME_FORMAT = () -> DataTypeUtils.getDateFormat(timeFormat);
        }

        if (timestampFormat == null || timestampFormat.isEmpty()) {
            this.timestampFormat = null;
            LAZY_TIMESTAMP_FORMAT = null;
        } else {
            this.timestampFormat = timestampFormat;
            LAZY_TIMESTAMP_FORMAT = () -> DataTypeUtils.getDateFormat(timestampFormat);
        }

        final InputStream bomInputStream = BOMInputStream.builder().setInputStream(in).get();
        inputStreamReader = new InputStreamReader(bomInputStream, encoding);

        // Skip the number of rows at the "top" as specified
        for (int i = 0; i < skipTopRows; i++) {
            readNextRecord(inputStreamReader, csvFormat.getRecordSeparator());
        }
    }

    protected final Object convert(final String value, final DataType dataType, final String fieldName) {
        if (dataType == null || value == null) {
            return value;
        }

        final String trimmed;
        final RecordFieldType type = dataType.getFieldType();

        if (!trimDoubleQuote && (type.equals(RecordFieldType.STRING) || type.equals(RecordFieldType.CHOICE))) {
            trimmed = value;
        } else {
            trimmed = trim(value);
        }

        if (trimmed.isEmpty()) {
            return null;
        }

        return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
    }

    protected final Object convertSimpleIfPossible(final String value, final DataType dataType, final String fieldName) {
        if (dataType == null || value == null) {
            return value;
        }

        final String trimmed;

        if (!trimDoubleQuote && dataType.getFieldType().equals(RecordFieldType.STRING)) {
            trimmed = value;
        } else {
            trimmed = trim(value);
        }

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
            case DECIMAL:
            case BYTE:
            case CHAR:
            case SHORT:
                if (DataTypeUtils.isCompatibleDataType(trimmed, dataType)) {
                    return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                }
                break;
            case DATE:
                if (DataTypeUtils.isDateTypeCompatible(trimmed, dateFormat)) {
                    return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                }
                break;
            case TIME:
                if (DataTypeUtils.isTimeTypeCompatible(trimmed, timeFormat)) {
                    return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                }
                break;
            case TIMESTAMP:
                if (DataTypeUtils.isTimestampTypeCompatible(trimmed, timestampFormat)) {
                    return DataTypeUtils.convertType(trimmed, dataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
                }
                break;
        }

        return value;
    }

    protected String trim(String value) {
        return (value.length() > 1) && value.startsWith("\"") && value.endsWith("\"") ? value.substring(1, value.length() - 1) : value;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    /**
     * This method searches using the specified Reader character-by-character until the
     * record separator is found.
     * @param reader the Reader providing the input
     * @param recordSeparator the String specifying the end of a record in the input
     * @throws IOException if an error occurs during reading, including not finding the record separator in the input
     */
    protected void readNextRecord(Reader reader, String recordSeparator) throws IOException {
        int indexIntoSeparator = 0;
        int recordSeparatorLength = recordSeparator.length();
        int code = reader.read();
        while (code != -1) {
            char nextChar = (char)code;
            if (recordSeparator.charAt(indexIntoSeparator) == nextChar) {
                if (++indexIntoSeparator == recordSeparatorLength) {
                    // We have matched the separator, return the string built so far
                    return;
                }
            } else {
                // The character didn't match the expected one in the record separator, reset the separator matcher
                // and check if it is the first character of the separator.
                indexIntoSeparator = 0;
                if (recordSeparator.charAt(indexIntoSeparator) == nextChar) {
                    // This character is the beginning of the record separator, keep it
                    if (++indexIntoSeparator == recordSeparatorLength) {
                        // We have matched the separator, return the string built so far
                        return;
                    }
                }
            }
            // This defensive check limits a record size to 2GB, this prevents out-of-memory errors if the record separator
            // is not present in the input (or at least in the first 2GB)
            if (indexIntoSeparator == Integer.MAX_VALUE) {
                throw new IOException("2GB input threshold reached, the record is either larger than 2GB or the separator "
                        + "is not found in the first 2GB of input. Ensure the Record Separator is correct for this FlowFile.");
            }
            code = reader.read();
        }
        // If the input ends without finding the record separator, an exception is thrown
        throw new IOException("Record separator not found");
    }
}
