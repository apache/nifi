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

import de.siegmar.fastcsv.writer.CsvWriter;
import de.siegmar.fastcsv.writer.LineDelimiter;
import de.siegmar.fastcsv.writer.QuoteStrategies;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RawRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.commons.csv.QuoteMode.MINIMAL;

public class WriteFastCSVResult extends AbstractRecordSetWriter implements RecordSetWriter, RawRecordWriter {
    private final RecordSchema recordSchema;
    private final SchemaAccessWriter schemaWriter;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;

    private final CsvWriter csvWriter;
    private final OutputStreamWriter streamWriter;

    private final String[] fieldValues;
    private final boolean includeHeaderLine;
    private boolean headerWritten = false;
    private String[] fieldNames;

    public WriteFastCSVResult(final CSVFormat csvFormat,
            final RecordSchema recordSchema,
            final SchemaAccessWriter schemaWriter,
            final OutputStream out,
            final String dateFormat,
            final String timeFormat,
            final String timestampFormat,
            final boolean includeHeaderLine,
            final String charSet) throws IOException {

        super(out);
        this.recordSchema = recordSchema;
        this.schemaWriter = schemaWriter;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
        this.includeHeaderLine = includeHeaderLine;

        this.streamWriter = new OutputStreamWriter(out, charSet);

        CsvWriter.CsvWriterBuilder builder = CsvWriter.builder()
            .fieldSeparator(csvFormat.getDelimiterString().charAt(0))
                .quoteCharacter(csvFormat.getQuoteCharacter());

        QuoteMode quoteMode = (csvFormat.getQuoteMode() == null) ? MINIMAL : csvFormat.getQuoteMode();
        switch (quoteMode) {
            case ALL:
                builder.quoteStrategy(QuoteStrategies.ALWAYS);
                break;
            case ALL_NON_NULL:
            case NON_NUMERIC:
                builder.quoteStrategy(QuoteStrategies.NON_EMPTY);
                break;
            default:
                // MINIMAL or NONE → FastCSV's default (required)
                break;
        }

        try {
            LineDelimiter lineDelimiter = LineDelimiter.of(csvFormat.getRecordSeparator());
            builder.lineDelimiter(lineDelimiter);
        } catch (IllegalArgumentException iae) {
            throw new IOException("Line delimiter is not supported, must use LF, CR, or CRLF", iae);
        }

        if (csvFormat.getEscapeCharacter() != null && csvFormat.getEscapeCharacter() != '\"') {
            throw new IOException("Escape character must be a double-quote character (\") per RFC-4180");
        }

        this.csvWriter = builder.build(streamWriter);
        this.fieldValues = new String[recordSchema.getFieldCount()];
    }

    private String getFormat(final RecordField field) {
        final DataType dataType = field.getDataType();
        return switch (dataType.getFieldType()) {
        case DATE -> dateFormat;
        case TIME -> timeFormat;
            case TIMESTAMP -> timestampFormat;
        default -> dataType.getFormat();
        };
    }

    @Override
    protected void onBeginRecordSet() throws IOException {
        schemaWriter.writeHeader(recordSchema, getOutputStream());
    }

    @Override
    protected Map<String, String> onFinishRecordSet() throws IOException {
        includeHeaderIfNecessary(null, true);
        return schemaWriter.getAttributes(recordSchema);
    }

    @Override
    public void flush() throws IOException {
        streamWriter.flush();
    }

    @Override
    public void close() throws IOException {
        csvWriter.close();
    }

    private String[] getFieldNames(final Record record) {
        if (fieldNames != null) {
            return fieldNames;
        }
        Set<String> allFields = new LinkedHashSet<>();
        allFields.addAll(recordSchema.getFieldNames());
        allFields.addAll(record.getRawFieldNames());
        fieldNames = allFields.toArray(new String[0]);
        return fieldNames;
    }

    private void includeHeaderIfNecessary(final Record record, final boolean schemaOnly) throws IOException {
        if (headerWritten || !includeHeaderLine) {
            return;
        }
        String[] names = schemaOnly
                ? recordSchema.getFieldNames().toArray(new String[0])
                : getFieldNames(record);

        csvWriter.writeRecord(names);
        headerWritten = true;
    }

    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        if (!isActiveRecordSet()) {
            schemaWriter.writeHeader(recordSchema, getOutputStream());
        }
        includeHeaderIfNecessary(record, true);

        int i = 0;
        for (RecordField field : recordSchema.getFields()) {
            fieldValues[i++] = record.getAsString(field, getFormat(field));
        }

        csvWriter.writeRecord(fieldValues);
        return schemaWriter.getAttributes(recordSchema);
    }

    @Override
    public WriteResult writeRawRecord(final Record record) throws IOException {
        if (!isActiveRecordSet()) {
            schemaWriter.writeHeader(recordSchema, getOutputStream());
        }
        includeHeaderIfNecessary(record, false);

        String[] names = getFieldNames(record);
        String[] values = (names.length == fieldValues.length)
                ? fieldValues
                : new String[names.length];

        int i = 0;
        for (String name : names) {
            Optional<RecordField> rf = recordSchema.getField(name);
            values[i++] = rf
                    .map(f -> record.getAsString(f, getFormat(f)))
                    .orElseGet(() -> record.getAsString(name));
        }

        csvWriter.writeRecord(values);
        return WriteResult.of(incrementRecordCount(), schemaWriter.getAttributes(recordSchema));
    }

    @Override
    public String getMimeType() {
        return "text/csv";
    }
}
