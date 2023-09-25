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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.DateTimeTextRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Tags({"csv", "result", "set", "recordset", "record", "writer", "serializer", "row", "tsv", "tab", "separated", "delimited"})
@CapabilityDescription("Writes the contents of a RecordSet as CSV data. The first line written "
    + "will be the column names (unless the 'Include Header Line' property is false). All subsequent lines will be the values "
    + "corresponding to the record fields.")
public class CSVRecordSetWriter extends DateTimeTextRecordSetWriter implements RecordSetWriterFactory {

    // CSV writer implementations
    public static final AllowableValue APACHE_COMMONS_CSV = new AllowableValue("commons-csv", "Apache Commons CSV",
            "The CSV writer implementation from the Apache Commons CSV library.");

    public static final AllowableValue FAST_CSV = new AllowableValue("fast-csv", "FastCSV",
            "The CSV writer implementation from the FastCSV library. NOTE: This writer only officially supports RFC-4180, so it recommended to "
                    + "set the 'CSV Format' property to 'RFC 4180'. It does handle some non-compliant CSV data, for that case set the 'CSV Format' property to "
                    + "'CUSTOM' and the other custom format properties (such as 'Trim Fields', 'Trim double quote', etc.) as appropriate. Be aware that this "
                    + "may cause errors if FastCSV doesn't handle the property settings correctly (such as 'Quote Mode'), but otherwise may process the output as expected even "
                    + "if the data is not fully RFC-4180 compliant.");
    public static final PropertyDescriptor CSV_WRITER = new PropertyDescriptor.Builder()
            .name("csv-writer")
            .displayName("CSV Writer")
            .description("Specifies which writer implementation to use to write CSV records. NOTE: Different writers may support different subsets of functionality "
                    + "and may also exhibit different levels of performance.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(APACHE_COMMONS_CSV, FAST_CSV)
            .defaultValue(APACHE_COMMONS_CSV.getValue())
            .required(true)
            .build();

    private volatile ConfigurationContext context;

    private volatile boolean includeHeader;
    private volatile String csvWriter;
    private volatile String charSet;

    // it will be initialized only if there are no dynamic csv formatting properties
    private volatile CSVFormat csvFormat;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(CSVUtils.CSV_FORMAT);
        properties.add(CSV_WRITER);
        properties.add(CSVUtils.VALUE_SEPARATOR);
        properties.add(CSVUtils.INCLUDE_HEADER_LINE);
        properties.add(CSVUtils.QUOTE_CHAR);
        properties.add(CSVUtils.ESCAPE_CHAR);
        properties.add(CSVUtils.COMMENT_MARKER);
        properties.add(CSVUtils.NULL_STRING);
        properties.add(CSVUtils.TRIM_FIELDS);
        properties.add(CSVUtils.QUOTE_MODE);
        properties.add(CSVUtils.RECORD_SEPARATOR);
        properties.add(CSVUtils.TRAILING_DELIMITER);
        properties.add(CSVUtils.CHARSET);
        return properties;
    }

    @OnEnabled
    public void storeStaticProperties(final ConfigurationContext context) {
        this.context = context;

        this.includeHeader = context.getProperty(CSVUtils.INCLUDE_HEADER_LINE).asBoolean();
        this.charSet = context.getProperty(CSVUtils.CHARSET).getValue();

        if (!CSVUtils.isDynamicCSVFormat(context)) {
            this.csvFormat = CSVUtils.createCSVFormat(context, Collections.emptyMap());
        } else {
            this.csvFormat = null;
        }

        this.csvWriter = context.getProperty(CSV_WRITER).getValue();
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out, final Map<String, String> variables) throws SchemaNotFoundException, IOException {
        CSVFormat csvFormat;
        if (this.csvFormat != null) {
            csvFormat = this.csvFormat;
        } else {
            csvFormat = CSVUtils.createCSVFormat(context, variables);
        }

        if (APACHE_COMMONS_CSV.getValue().equals(csvWriter)) {
            return new WriteCSVResult(csvFormat, schema, getSchemaAccessWriter(schema, variables), out,
                    getDateFormat().orElse(null), getTimeFormat().orElse(null), getTimestampFormat().orElse(null), includeHeader, charSet);
        } else if (FAST_CSV.getValue().equals(csvWriter)) {
            return new WriteFastCSVResult(csvFormat, schema, getSchemaAccessWriter(schema, variables), out,
                    getDateFormat().orElse(null), getTimeFormat().orElse(null), getTimestampFormat().orElse(null), includeHeader, charSet);
        } else {
            throw new IOException("Parser not supported");
        }
    }
}
