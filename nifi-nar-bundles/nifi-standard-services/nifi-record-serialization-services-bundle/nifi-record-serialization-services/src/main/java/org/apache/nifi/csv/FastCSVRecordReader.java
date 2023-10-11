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

import de.siegmar.fastcsv.reader.CommentStrategy;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

public class FastCSVRecordReader extends AbstractCSVRecordReader {
    private final CsvReader csvReader;
    private final Iterator<CsvRow> csvRowIterator;

    private List<RecordField> recordFields;

    private Map<String, Integer> headerMap;

    private final boolean ignoreHeader;
    private final boolean trimDoubleQuote;
    private final CSVFormat csvFormat;

    public FastCSVRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema, final CSVFormat csvFormat, final boolean hasHeader, final boolean ignoreHeader,
                               final String dateFormat, final String timeFormat, final String timestampFormat, final String encoding, final boolean trimDoubleQuote) throws IOException {
        super(logger, schema, hasHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, trimDoubleQuote);
        this.ignoreHeader = ignoreHeader;
        this.trimDoubleQuote = trimDoubleQuote;
        this.csvFormat = csvFormat;

        CsvReader.CsvReaderBuilder builder = CsvReader.builder()
            .fieldSeparator(csvFormat.getDelimiterString().charAt(0))
                .quoteCharacter(csvFormat.getQuoteCharacter())
                .commentStrategy(CommentStrategy.SKIP)
                .skipEmptyRows(csvFormat.getIgnoreEmptyLines())
                .errorOnDifferentFieldCount(!csvFormat.getAllowMissingColumnNames());

        if (csvFormat.getCommentMarker() != null) {
            builder.commentCharacter(csvFormat.getCommentMarker());
        }

        if (hasHeader && !ignoreHeader) {
            headerMap = null;
        } else {
            headerMap = new HashMap<>();
            for (int i = 0; i < schema.getFieldCount(); i++) {
                headerMap.put(schema.getField(i).getFieldName(), i);
            }
        }

        csvReader = builder.build(new InputStreamReader(in, encoding));
        csvRowIterator = csvReader.iterator();
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {

        try {
            final RecordSchema schema = getSchema();

            final List<RecordField> recordFields = getRecordFields();
            final int numFieldNames = recordFields.size();
            if (!csvRowIterator.hasNext()) {
                return null;
            }
            final CsvRow csvRecord = csvRowIterator.next();
            final Map<String, Object> values = new LinkedHashMap<>(recordFields.size() * 2);
            for (int i = 0; i < csvRecord.getFieldCount(); i++) {
                String rawValue = csvRecord.getField(i);
                if (csvFormat.getTrim()) {
                    rawValue = rawValue.trim();
                }
                if (trimDoubleQuote) {
                    rawValue = trim(rawValue);
                }

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

                values.putIfAbsent(rawFieldName, value);
            }

            return new MapRecord(schema, values, coerceTypes, dropUnknownFields);
        } catch (Exception e) {
            throw new MalformedRecordException("Error while getting next record", e);
        }
    }


    private List<RecordField> getRecordFields() {
        if (this.recordFields != null) {
            return this.recordFields;
        }

        if (ignoreHeader) {
            logger.debug("With 'Ignore Header' set to true, FastCSV still reads the header and keeps track "
                    + "of the number of fields in the header. This will cause an error if the provided schema does not "
                    + "have the same number of fields, as this is not conformant to RFC-4180");
        }

        // When getting the field names from the first record, it has to be read in
        if (!csvRowIterator.hasNext()) {
            return Collections.emptyList();
        }
        CsvRow headerRow = csvRowIterator.next();
        headerMap = new HashMap<>();
        for (int i = 0; i < headerRow.getFieldCount(); i++) {
            String rawValue = headerRow.getField(i);
            if (csvFormat.getTrim()) {
                rawValue = rawValue.trim();
            }
            if (this.trimDoubleQuote) {
                rawValue = trim(rawValue);
            }
            headerMap.put(rawValue, i);
        }


        // Use a SortedMap keyed by index of the field so that we can get a List of field names in the correct order
        final SortedMap<Integer, String> sortedMap = new TreeMap<>();
        for (final Map.Entry<String, Integer> entry : headerMap.entrySet()) {
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
        csvReader.close();
    }
}
