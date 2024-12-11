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
import de.siegmar.fastcsv.writer.QuoteStrategy;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

import static org.apache.commons.csv.QuoteMode.MINIMAL;

public class WriteFastCSVResult extends AbstractRecordSetWriter implements RecordSetWriter, RawRecordWriter {
    private final RecordSchema recordSchema;
    private final SchemaAccessWriter schemaWriter;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;

    CsvWriter csvWriter;

    //Need to call flush() on the underlying writer
    final OutputStreamWriter streamWriter;

    private final String[] fieldValues;
    private final boolean includeHeaderLine;
    private boolean headerWritten = false;
    private String[] fieldNames;

    public WriteFastCSVResult(final CSVFormat csvFormat, final RecordSchema recordSchema, final SchemaAccessWriter schemaWriter, final OutputStream out,
                                 final String dateFormat, final String timeFormat, final String timestampFormat, final boolean includeHeaderLine, final String charSet) throws IOException {

        super(out);
        this.recordSchema = recordSchema;
        this.schemaWriter = schemaWriter;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
        this.includeHeaderLine = includeHeaderLine;

        streamWriter = new OutputStreamWriter(out, charSet);
        CsvWriter.CsvWriterBuilder builder = CsvWriter.builder()
            .fieldSeparator(csvFormat.getDelimiterString().charAt(0))
                .quoteCharacter(csvFormat.getQuoteCharacter());

        QuoteMode quoteMode = (csvFormat.getQuoteMode() == null) ? MINIMAL : csvFormat.getQuoteMode();
        switch (quoteMode) {
            case ALL:
                builder.quoteStrategy(QuoteStrategy.ALWAYS);
                break;
            case NON_NUMERIC:
                builder.quoteStrategy(QuoteStrategy.NON_EMPTY);
                break;
            case MINIMAL:
            case NONE:
                builder.quoteStrategy(QuoteStrategy.REQUIRED);
        }

        try {
            LineDelimiter lineDelimiter = LineDelimiter.of(csvFormat.getRecordSeparator());
            builder.lineDelimiter(lineDelimiter);
        } catch (IllegalArgumentException iae) {
            throw new IOException("Line delimiter is not supported, must use LF, CR, or CRLF", iae);
        }

        if (csvFormat.getEscapeCharacter() != null && csvFormat.getEscapeCharacter() != '\"') {
            throw new IOException("Escape character must be a double-quote character (\") per the FastCSV conformance to the RFC4180 specification");
        }

        csvWriter = builder.build(streamWriter);
        fieldValues = new String[recordSchema.getFieldCount()];
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
        // If the header has not yet been written (but should be), write it out now
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

        final Set<String> allFields = new LinkedHashSet<>();
        // The fields defined in the schema should be written first followed by extra ones.
        allFields.addAll(recordSchema.getFieldNames());
        allFields.addAll(record.getRawFieldNames());
        fieldNames = allFields.toArray(new String[0]);
        return fieldNames;
    }

    private void includeHeaderIfNecessary(final Record record, final boolean includeOnlySchemaFields) throws IOException {
        if (headerWritten || !includeHeaderLine) {
            return;
        }

        final String[] fieldNames;
        if (includeOnlySchemaFields) {
            fieldNames = recordSchema.getFieldNames().toArray(new String[0]);
        } else {
            fieldNames = getFieldNames(record);
        }

        csvWriter.writeRow(fieldNames);
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
            String fieldValue = getFieldValue(record, recordField);
            fieldValues[i++] = fieldValue;
        }

        csvWriter.writeRow(fieldValues);
        return schemaWriter.getAttributes(recordSchema);
    }

    private String getFieldValue(final Record record, final RecordField recordField) {
        return record.getAsString(recordField, getFormat(recordField));
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
        final String[] recordFieldValues = (fieldNames.length == this.fieldValues.length) ? this.fieldValues : new String[fieldNames.length];

        int i = 0;
        for (final String fieldName : fieldNames) {
            final Optional<RecordField> recordField = recordSchema.getField(fieldName);
            if (recordField.isPresent()) {
                recordFieldValues[i++] = record.getAsString(fieldName, getFormat(recordField.get()));
            } else {
                recordFieldValues[i++] = record.getAsString(fieldName);
            }
        }

        csvWriter.writeRow(recordFieldValues);
        final Map<String, String> attributes = schemaWriter.getAttributes(recordSchema);
        return WriteResult.of(incrementRecordCount(), attributes);
    }

    @Override
    public String getMimeType() {
        return "text/csv";
    }
}
