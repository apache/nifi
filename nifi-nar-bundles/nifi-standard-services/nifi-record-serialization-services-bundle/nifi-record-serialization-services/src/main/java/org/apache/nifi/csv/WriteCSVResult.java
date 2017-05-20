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
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;

public class WriteCSVResult extends AbstractRecordSetWriter implements RecordSetWriter {
    private final RecordSchema recordSchema;
    private final SchemaAccessWriter schemaWriter;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;
    private final CSVPrinter printer;
    private final Object[] fieldValues;

    public WriteCSVResult(final CSVFormat csvFormat, final RecordSchema recordSchema, final SchemaAccessWriter schemaWriter, final OutputStream out,
        final String dateFormat, final String timeFormat, final String timestampFormat, final boolean includeHeaderLine) throws IOException {

        super(out);
        this.recordSchema = recordSchema;
        this.schemaWriter = schemaWriter;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;

        final String[] columnNames = recordSchema.getFieldNames().toArray(new String[0]);
        final CSVFormat formatWithHeader = csvFormat.withHeader(columnNames).withSkipHeaderRecord(!includeHeaderLine);
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

    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        int i = 0;
        for (final RecordField recordField : recordSchema.getFields()) {
            fieldValues[i++] = record.getAsString(recordField, getFormat(recordField));
        }

        printer.printRecord(fieldValues);
        return schemaWriter.getAttributes(recordSchema);
    }

    @Override
    public String getMimeType() {
        return "text/csv";
    }
}
