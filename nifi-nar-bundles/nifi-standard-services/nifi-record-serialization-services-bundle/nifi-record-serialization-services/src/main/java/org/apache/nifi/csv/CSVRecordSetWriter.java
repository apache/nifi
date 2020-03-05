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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
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

    private volatile ConfigurationContext context;

    private volatile boolean includeHeader;
    private volatile String charSet;

    // it will be initialized only if there are no dynamic csv formatting properties
    private volatile CSVFormat csvFormat;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(CSVUtils.CSV_FORMAT);
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
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out, final Map<String, String> variables) throws SchemaNotFoundException, IOException {
        CSVFormat csvFormat;
        if (this.csvFormat != null) {
            csvFormat = this.csvFormat;
        } else {
            csvFormat = CSVUtils.createCSVFormat(context, variables);
        }

        return new WriteCSVResult(csvFormat, schema, getSchemaAccessWriter(schema, variables), out,
            getDateFormat().orElse(null), getTimeFormat().orElse(null), getTimestampFormat().orElse(null), includeHeader, charSet);
    }
}
