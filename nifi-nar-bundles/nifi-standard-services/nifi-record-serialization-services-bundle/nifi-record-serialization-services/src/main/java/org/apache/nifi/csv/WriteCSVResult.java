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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Collections;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.stream.io.NonCloseableOutputStream;

public class WriteCSVResult implements RecordSetWriter {
    private final CSVFormat csvFormat;
    private final RecordSchema recordSchema;
    private final SchemaAccessWriter schemaWriter;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;
    private final boolean includeHeaderLine;

    public WriteCSVResult(final CSVFormat csvFormat, final RecordSchema recordSchema, final SchemaAccessWriter schemaWriter,
        final String dateFormat, final String timeFormat, final String timestampFormat, final boolean includeHeaderLine) {
        this.csvFormat = csvFormat;
        this.recordSchema = recordSchema;
        this.schemaWriter = schemaWriter;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
        this.includeHeaderLine = includeHeaderLine;
    }

    private String getFormat(final RecordField field) {
        final DataType dataType = field.getDataType();
        switch (dataType.getFieldType()) {
            case DATE:
                return dateFormat;
            case TIME:
                return timeFormat;
            case TIMESTAMP:
                return timestampFormat;
        }

        return dataType.getFormat();
    }

    @Override
    public WriteResult write(final RecordSet rs, final OutputStream rawOut) throws IOException {
        int count = 0;

        final String[] columnNames = recordSchema.getFieldNames().toArray(new String[0]);
        final CSVFormat formatWithHeader = csvFormat.withHeader(columnNames).withSkipHeaderRecord(!includeHeaderLine);

        schemaWriter.writeHeader(recordSchema, rawOut);

        try (final OutputStream nonCloseable = new NonCloseableOutputStream(rawOut);
            final OutputStreamWriter streamWriter = new OutputStreamWriter(nonCloseable);
            final CSVPrinter printer = new CSVPrinter(streamWriter, formatWithHeader)) {

            try {
                Record record;
                while ((record = rs.next()) != null) {
                    final Object[] colVals = new Object[recordSchema.getFieldCount()];
                    int i = 0;
                    for (final RecordField recordField : recordSchema.getFields()) {
                        colVals[i++] = record.getAsString(recordField, getFormat(recordField));
                    }

                    printer.printRecord(colVals);
                    count++;
                }
            } catch (final Exception e) {
                throw new IOException("Failed to serialize results", e);
            }
        }

        return WriteResult.of(count, schemaWriter.getAttributes(recordSchema));
    }

    @Override
    public WriteResult write(final Record record, final OutputStream rawOut) throws IOException {

        try (final OutputStream nonCloseable = new NonCloseableOutputStream(rawOut);
            final OutputStreamWriter streamWriter = new OutputStreamWriter(nonCloseable);
            final CSVPrinter printer = new CSVPrinter(streamWriter, csvFormat)) {

            try {
                final RecordSchema schema = record.getSchema();
                final Object[] colVals = new Object[schema.getFieldCount()];
                int i = 0;
                for (final RecordField recordField : schema.getFields()) {
                    colVals[i++] = record.getAsString(recordField, getFormat(recordField));
                }

                printer.printRecord(colVals);
            } catch (final Exception e) {
                throw new IOException("Failed to serialize results", e);
            }
        }

        return WriteResult.of(1, Collections.emptyMap());
    }

    @Override
    public String getMimeType() {
        return "text/csv";
    }
}
