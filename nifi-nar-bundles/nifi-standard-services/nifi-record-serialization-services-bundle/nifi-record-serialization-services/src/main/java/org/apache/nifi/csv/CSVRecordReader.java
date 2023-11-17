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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

public class CSVRecordReader extends AbstractCSVRecordReader {
    private final CSVParser csvParser;

    private List<RecordField> recordFields;

    public CSVRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema, final CSVFormat csvFormat, final boolean hasHeader, final boolean ignoreHeader,
                           final String dateFormat, final String timeFormat, final String timestampFormat, final String encoding, final boolean trimDoubleQuote, final int skipTopRows)
            throws IOException {
        super(logger, schema, hasHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, trimDoubleQuote, skipTopRows);

        final InputStream bomInputStream = BOMInputStream.builder().setInputStream(in).get();
        final Reader inputStreamReader = new InputStreamReader(bomInputStream, encoding);

        // Skip the number of rows at the "top" as specified
        for (int i = 0; i < skipTopRows; i++) {
            readNextRecord(inputStreamReader, csvFormat.getRecordSeparator());
        }

        CSVFormat.Builder withHeader;
        if (hasHeader) {
            withHeader = csvFormat.builder().setSkipHeaderRecord(true);

            if (ignoreHeader) {
                withHeader = withHeader.setHeader(schema.getFieldNames().toArray(new String[0]));
            } else {
                withHeader = withHeader.setHeader();
            }
        } else {
            withHeader = csvFormat.builder().setHeader(schema.getFieldNames().toArray(new String[0]));
        }

        csvParser = new CSVParser(inputStreamReader, withHeader.build());
    }

    public CSVRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema, final CSVFormat csvFormat, final boolean hasHeader, final boolean ignoreHeader,
                           final String dateFormat, final String timeFormat, final String timestampFormat, final String encoding) throws IOException {
        this(in, logger, schema, csvFormat, hasHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, encoding, true, 0);
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {

        try {
            final RecordSchema schema = getSchema();

            final List<RecordField> recordFields = getRecordFields();
            final int numFieldNames = recordFields.size();
            for (final CSVRecord csvRecord : csvParser) {
                final Map<String, Object> values = new LinkedHashMap<>(recordFields.size() * 2);
                for (int i = 0; i < csvRecord.size(); i++) {
                    final String rawValue = csvRecord.get(i);

                    final String rawFieldName;
                    final DataType dataType;
                    if (i >= numFieldNames) {
                        if (!dropUnknownFields) {
                            values.put("unknown_field_index_" + i, rawValue);
                        }
                        continue;
                    } else {
                        final RecordField recordField = recordFields.get(i);
                        rawFieldName = recordField.getFieldName();
                        dataType = recordField.getDataType();
                    }

                    final Object value;
                    if (coerceTypes) {
                        value = convert(rawValue, dataType, rawFieldName);
                    } else {
                        // The CSV Reader is going to return all fields as Strings, because CSV doesn't have any way to
                        // dictate a field type. As a result, we will use the schema that we have to attempt to convert
                        // the value into the desired type if it's a simple type.
                        value = convertSimpleIfPossible(rawValue, dataType, rawFieldName);
                    }

                    values.put(rawFieldName, value);
                }

                return new MapRecord(schema, values, coerceTypes, dropUnknownFields);
            }
        } catch (Exception e) {
            throw new MalformedRecordException("Error while getting next record", e);
        }

        return null;
    }


    private List<RecordField> getRecordFields() {
        if (this.recordFields != null) {
            return this.recordFields;
        }

        // Use a SortedMap keyed by index of the field so that we can get a List of field names in the correct order
        final SortedMap<Integer, String> sortedMap = new TreeMap<>();
        for (final Map.Entry<String, Integer> entry : csvParser.getHeaderMap().entrySet()) {
            sortedMap.put(entry.getValue(), entry.getKey());
        }

        final List<RecordField> fields = new ArrayList<>();
        final List<String> rawFieldNames = new ArrayList<>(sortedMap.values());
        for (final String rawFieldName : rawFieldNames) {
            final Optional<RecordField> option = schema.getField(rawFieldName);
            if (option.isPresent()) {
                fields.add(option.get());
            } else {
                fields.add(new RecordField(rawFieldName, RecordFieldType.STRING.getDataType()));
            }
        }

        this.recordFields = fields;
        return fields;
    }

    @Override
    public void close() throws IOException {
        csvParser.close();
    }

    /**
     * This method builds a text representation of the CSV record by searching character-by-character until the
     * record separator is found. Because we never want to consume input we don't use, the method attempts to match
     * the separator separately, and as it is not matched, the characters are added to the returned string.
     * @param reader the Reader providing the input
     * @param recordSeparator the String specifying the end of a record in the input
     * @return a String created from the input until the record separator is reached.
     * @throws IOException if an error occurs during reading
     */
    protected String readNextRecord(Reader reader, String recordSeparator) throws IOException {
        int indexIntoSeparator = 0;
        int recordSeparatorLength = recordSeparator.length();
        StringBuilder lineBuilder = new StringBuilder();
        StringBuilder separatorBuilder = new StringBuilder();
        int code = reader.read();
        while (code != -1) {
            char nextChar = (char)code;
            if (recordSeparator.charAt(indexIntoSeparator) == nextChar) {
                separatorBuilder.append(nextChar);
                if (++indexIntoSeparator == recordSeparatorLength) {
                    // We have matched the separator, return the string built so far
                    lineBuilder.append(separatorBuilder);
                    return lineBuilder.toString();
                }
            } else {
                // The character didn't match the expected one in the record separator, reset the separator matcher
                // and check if it is the first character of the separator.
                indexIntoSeparator = 0;
                if (recordSeparator.charAt(indexIntoSeparator) == nextChar) {
                    // This character is the beginning of the record separator, keep it
                    separatorBuilder = new StringBuilder();
                    separatorBuilder.append(nextChar);
                    if (++indexIntoSeparator == recordSeparatorLength) {
                        // We have matched the separator, return the string built so far
                        return lineBuilder.toString();
                    }
                } else {
                    // This character is not the beginning of the record separator, add it to the return string
                    lineBuilder.append(nextChar);
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

        // The end of input has been reached without the record separator being found, throw an exception with the string so far

        return lineBuilder.toString();
    }
}
