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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;


public class JacksonCSVRecordReader extends AbstractCSVRecordReader {
    private final MappingIterator<String[]> recordStream;
    private List<String> rawFieldNames = null;

    private volatile static CsvMapper mapper = new CsvMapper().enable(CsvParser.Feature.WRAP_AS_ARRAY);

    public JacksonCSVRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema, final CSVFormat csvFormat, final boolean hasHeader, final boolean ignoreHeader,
                                  final String dateFormat, final String timeFormat, final String timestampFormat, final String encoding) throws IOException {
        super(logger, schema, hasHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat);

        final Reader reader = new InputStreamReader(new BOMInputStream(in));

        CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder()
                .setColumnSeparator(csvFormat.getDelimiter())
                .setLineSeparator(csvFormat.getRecordSeparator())
                // Can only use comments in Jackson CSV if the correct marker is set
                .setAllowComments("#" .equals(CharUtils.toString(csvFormat.getCommentMarker())))
                // The call to setUseHeader(false) in all code paths is due to the way Jackson does data binding/mapping. Missing or extra columns may not
                // be handled correctly when using the header for mapping.
                .setUseHeader(false);

        csvSchemaBuilder = (csvFormat.getQuoteCharacter() == null) ? csvSchemaBuilder : csvSchemaBuilder.setQuoteChar(csvFormat.getQuoteCharacter());
        csvSchemaBuilder = (csvFormat.getEscapeCharacter() == null) ? csvSchemaBuilder : csvSchemaBuilder.setEscapeChar(csvFormat.getEscapeCharacter());

        if (hasHeader) {
            if (ignoreHeader) {
                csvSchemaBuilder = csvSchemaBuilder.setSkipFirstDataRow(true);
            }
        }

        CsvSchema csvSchema = csvSchemaBuilder.build();

        // Add remaining config options to the mapper
        List<CsvParser.Feature> features = new ArrayList<>();
        features.add(CsvParser.Feature.INSERT_NULLS_FOR_MISSING_COLUMNS);
        if (csvFormat.getIgnoreEmptyLines()) {
            features.add(CsvParser.Feature.SKIP_EMPTY_LINES);
        }
        if (csvFormat.getTrim()) {
            features.add(CsvParser.Feature.TRIM_SPACES);
        }

        ObjectReader objReader = mapper.readerFor(String[].class)
                .with(csvSchema)
                .withFeatures(features.toArray(new CsvParser.Feature[features.size()]));

        recordStream = objReader.readValues(reader);
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        final RecordSchema schema = getSchema();

        if (recordStream.hasNext()) {
            String[] csvRecord = recordStream.next();

            // If the first record is the header names (and we're using them), store those off for use in creating the value map on the next iterations
            if (rawFieldNames == null) {
                if (!hasHeader || ignoreHeader) {
                    rawFieldNames = schema.getFieldNames();
                } else {
                    rawFieldNames = Arrays.asList(csvRecord);

                    // Advance the stream to keep the record count correct
                    if (recordStream.hasNext()) {
                        csvRecord = recordStream.next();
                    } else {
                        return null;
                    }
                }
            }

            // Check for empty lines and ignore them
            boolean foundRecord = true;
            if (csvRecord == null || (csvRecord.length == 1 && StringUtils.isEmpty(csvRecord[0]))) {
                foundRecord = false;
                while (recordStream.hasNext()) {
                    csvRecord = recordStream.next();

                    if (csvRecord != null && !(csvRecord.length == 1 && StringUtils.isEmpty(csvRecord[0]))) {
                        // This is a non-empty record/row, so continue processing
                        foundRecord = true;
                        break;
                    }
                }
            }

            // If we didn't find a record, then the end of the file was comprised of empty lines, so we have no record to return
            if (!foundRecord) {
                return null;
            }

            final Map<String, Object> values = new HashMap<>(rawFieldNames.size() * 2);
            final int numFieldNames = rawFieldNames.size();
            for (int i = 0; i < csvRecord.length; i++) {
                final String rawFieldName = numFieldNames <= i ? "unknown_field_index_" + i : rawFieldNames.get(i);
                String rawValue = (i >= csvRecord.length) ? null : csvRecord[i];

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

    @Override
    public void close() throws IOException {
        recordStream.close();
    }
}
