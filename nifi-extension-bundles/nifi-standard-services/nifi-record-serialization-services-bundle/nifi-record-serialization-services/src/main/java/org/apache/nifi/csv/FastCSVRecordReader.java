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
import de.siegmar.fastcsv.reader.CsvRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

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

public class FastCSVRecordReader extends AbstractCSVRecordReader {

    private final CsvReader<CsvRecord> csvReader;
    private final Iterator<CsvRecord> csvRecordIterator;

    private List<RecordField> recordFields;
    private Map<String, Integer> headerMap;

    private final boolean ignoreHeader;
    private final boolean trimDoubleQuote;
    private final CSVFormat csvFormat;

    public FastCSVRecordReader(final InputStream in,
            final ComponentLog logger,
            final RecordSchema schema,
            final CSVFormat csvFormat,
            final boolean hasHeader,
            final boolean ignoreHeader,
            final String dateFormat,
            final String timeFormat,
            final String timestampFormat,
            final String encoding,
            final boolean trimDoubleQuote) throws IOException {
        super(logger, schema, hasHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, trimDoubleQuote);
        this.ignoreHeader = ignoreHeader;
        this.trimDoubleQuote = trimDoubleQuote;
        this.csvFormat = csvFormat;

        CsvReader.CsvReaderBuilder builder = CsvReader.builder()
            .fieldSeparator(csvFormat.getDelimiterString().charAt(0))
                .quoteCharacter(csvFormat.getQuoteCharacter())
                .commentStrategy(CommentStrategy.SKIP)
                .skipEmptyLines(csvFormat.getIgnoreEmptyLines())
                .ignoreDifferentFieldCount(csvFormat.getAllowMissingColumnNames());

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

        this.csvReader = builder.ofCsvRecord(new InputStreamReader(in, encoding));
        this.csvRecordIterator = csvReader.iterator();
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields)
            throws IOException, MalformedRecordException {

        try {
            final RecordSchema schema = getSchema();
            final List<RecordField> recordFields = getRecordFields();
            final int numFieldNames = recordFields.size();

            if (!csvRecordIterator.hasNext()) {
                return null;
            }

            final CsvRecord csvRecord = csvRecordIterator.next();
            final Map<String, Object> values = new LinkedHashMap<>(recordFields.size() * 2);

            for (int i = 0; i < csvRecord.getFieldCount(); i++) {
                String rawValue = csvRecord.getField(i);
                if (csvFormat.getTrim()) {
                    rawValue = rawValue.trim();
                }
                if (trimDoubleQuote) {
                    rawValue = trim(rawValue);
                }

                if (i >= numFieldNames) {
                    if (!dropUnknownFields) {
                        values.put("unknown_field_index_" + i, rawValue);
                    }
                    continue;
                }

                final RecordField recordField = recordFields.get(i);
                final String rawFieldName = recordField.getFieldName();
                final DataType dataType = recordField.getDataType();

                final Object value = coerceTypes
                        ? convert(rawValue, dataType, rawFieldName)
                        : convertSimpleIfPossible(rawValue, dataType, rawFieldName);

                values.putIfAbsent(rawFieldName, value);
            }

            return new MapRecord(schema, values, coerceTypes, dropUnknownFields);
        } catch (Exception e) {
            throw new MalformedRecordException("Error while getting next record", e);
        }
    }

    private List<RecordField> getRecordFields() {
        if (recordFields != null) {
            return recordFields;
        }

        if (ignoreHeader) {
            logger.debug("With 'Ignore Header' set to true, FastCSV still reads the header and keeps track "
                    + "of the number of fields in the header. This will cause an error if the provided schema does not "
                    + "have the same number of fields, as this is not conformant to RFC-4180");
        }

        if (!csvRecordIterator.hasNext()) {
            return Collections.emptyList();
        }

        // read header row
        CsvRecord headerRecord = csvRecordIterator.next();
        headerMap = new HashMap<>();
        for (int i = 0; i < headerRecord.getFieldCount(); i++) {
            String rawValue = headerRecord.getField(i);
            if (csvFormat.getTrim()) {
                rawValue = rawValue.trim();
            }
            if (trimDoubleQuote) {
                rawValue = trim(rawValue);
            }
            headerMap.put(rawValue, i);
        }

        SortedMap<Integer, String> sortedMap = new TreeMap<>();
        for (Map.Entry<String, Integer> entry : headerMap.entrySet()) {
            sortedMap.put(entry.getValue(), entry.getKey());
        }

        List<RecordField> fields = new ArrayList<>();
        for (String rawFieldName : new ArrayList<>(sortedMap.values())) {
            Optional<RecordField> optField = getSchema().getField(rawFieldName);
            fields.add(optField.orElseGet(() -> new RecordField(rawFieldName, RecordFieldType.STRING.getDataType())));
        }

        this.recordFields = fields;
        return fields;
    }

    @Override
    public void close() throws IOException {
        csvReader.close();
    }
}
