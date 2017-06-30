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
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RawRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;

public class WriteCSVResult extends AbstractRecordSetWriter implements RecordSetWriter, RawRecordWriter {
    private final RecordSchema recordSchema;
    private final SchemaAccessWriter schemaWriter;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;
    private final CSVPrinter printer;
    private final Object[] fieldValues;
    private final boolean includeHeaderLine;
    private boolean headerWritten = false;
    private String[] fieldNames;

    public WriteCSVResult(final CSVFormat csvFormat, final RecordSchema recordSchema, final SchemaAccessWriter schemaWriter, final OutputStream out,
        final String dateFormat, final String timeFormat, final String timestampFormat, final boolean includeHeaderLine) throws IOException {

        super(out);
        this.recordSchema = recordSchema;
        this.schemaWriter = schemaWriter;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
        this.includeHeaderLine = includeHeaderLine;

        final CSVFormat formatWithHeader = csvFormat.withSkipHeaderRecord(true);
        final OutputStreamWriter streamWriter = new OutputStreamWriter(out);
        printer = new CSVPrinter(streamWriter, formatWithHeader);

        fieldValues = new Object[recordSchema.getFieldCount()];
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
    protected void onBeginRecordSet() throws IOException {
        schemaWriter.writeHeader(recordSchema, getOutputStream());
    }

    @Override
    protected Map<String, String> onFinishRecordSet() throws IOException {
        return schemaWriter.getAttributes(recordSchema);
    }

    @Override
    public void close() throws IOException {
        printer.close();
    }

    @Override
    public void flush() throws IOException {
        printer.flush();
    }

    private String[] getFieldNames(final Record record) {
        if (fieldNames != null) {
            return fieldNames;
        }

        final Set<String> allFields = new LinkedHashSet<>();
        allFields.addAll(record.getRawFieldNames());
        allFields.addAll(recordSchema.getFieldNames());
        fieldNames = allFields.toArray(new String[0]);
        return fieldNames;
    }

    private void includeHeaderIfNecessary(final Record record, final boolean includeOnlySchemaFields) throws IOException {
        if (headerWritten || !includeHeaderLine) {
            return;
        }

        final Object[] fieldNames;
        if (includeOnlySchemaFields) {
            fieldNames = recordSchema.getFieldNames().toArray(new Object[0]);
        } else {
            fieldNames = getFieldNames(record);
        }

        printer.printRecord(fieldNames);
        headerWritten = true;
    }

    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        // If we are not writing an active record set, then we need to ensure that we write the
        // schema information.
        if (!isActiveRecordSet()) {
            schemaWriter.writeHeader(recordSchema, getOutputStream());
        }

        includeHeaderIfNecessary(record, true);

        int i = 0;
        for (final RecordField recordField : recordSchema.getFields()) {
            fieldValues[i++] = record.getAsString(recordField, getFormat(recordField));
        }

        printer.printRecord(fieldValues);
        return schemaWriter.getAttributes(recordSchema);
    }

    @Override
    public WriteResult writeRawRecord(final Record record) throws IOException {
        // If we are not writing an active record set, then we need to ensure that we write the
        // schema information.
        if (!isActiveRecordSet()) {
            schemaWriter.writeHeader(recordSchema, getOutputStream());
        }

        includeHeaderIfNecessary(record, false);

        final String[] fieldNames = getFieldNames(record);
        // Avoid creating a new Object[] for every Record if we can. But if the record has a different number of columns than does our
        // schema, we don't have a lot of options here, so we just create a new Object[] in that case.
        final Object[] recordFieldValues = (fieldNames.length == this.fieldValues.length) ? this.fieldValues : new String[fieldNames.length];

        int i = 0;
        for (final String fieldName : fieldNames) {
            final Optional<RecordField> recordField = recordSchema.getField(fieldName);
            if (recordField.isPresent()) {
                recordFieldValues[i++] = record.getAsString(fieldName, getFormat(recordField.get()));
            } else {
                recordFieldValues[i++] = record.getAsString(fieldName);
            }
        }

        printer.printRecord(recordFieldValues);
        final Map<String, String> attributes = schemaWriter.getAttributes(recordSchema);
        return WriteResult.of(incrementRecordCount(), attributes);
    }

    @Override
    public String getMimeType() {
        return "text/csv";
    }
}
