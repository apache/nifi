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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.stream.io.NonCloseableInputStream;

@Tags({"csv", "parse", "record", "row", "reader", "delimited", "comma", "separated", "values"})
@CapabilityDescription("Parses CSV-formatted data, returning each row in the CSV file as a separate record. "
    + "This reader assumes that the first line in the content is the column names and all subsequent lines are "
    + "the values. See Controller Service's Usage for further documentation.")
public class CSVReader extends SchemaRegistryService implements RecordReaderFactory {

    private final AllowableValue headerDerivedAllowableValue = new AllowableValue("csv-header-derived", "Use String Fields From Header",
        "The first non-comment line of the CSV file is a header line that contains the names of the columns. The schema will be derived by using the "
            + "column names in the header and assuming that all columns are of type String.");

    private volatile CSVFormat csvFormat;
    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;
    private volatile boolean firstLineIsHeader;
    private volatile boolean ignoreHeader;


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        properties.add(CSVUtils.CSV_FORMAT);
        properties.add(CSVUtils.VALUE_SEPARATOR);
        properties.add(CSVUtils.FIRST_LINE_IS_HEADER);
        properties.add(CSVUtils.IGNORE_CSV_HEADER);
        properties.add(CSVUtils.QUOTE_CHAR);
        properties.add(CSVUtils.ESCAPE_CHAR);
        properties.add(CSVUtils.COMMENT_MARKER);
        properties.add(CSVUtils.NULL_STRING);
        properties.add(CSVUtils.TRIM_FIELDS);
        return properties;
    }

    @OnEnabled
    public void storeCsvFormat(final ConfigurationContext context) {
        this.csvFormat = CSVUtils.createCSVFormat(context);
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
        this.firstLineIsHeader = context.getProperty(CSVUtils.FIRST_LINE_IS_HEADER).asBoolean();
        this.ignoreHeader = context.getProperty(CSVUtils.IGNORE_CSV_HEADER).asBoolean();

        // Ensure that if we are deriving schema from header that we always treat the first line as a header,
        // regardless of the 'First Line is Header' property
        final String accessStrategy = context.getProperty(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY).getValue();
        if (headerDerivedAllowableValue.getValue().equals(accessStrategy)) {
            this.csvFormat = this.csvFormat.withFirstRecordAsHeader();
            this.firstLineIsHeader = true;
        }
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final ComponentLog logger) throws IOException, SchemaNotFoundException {
        // Use Mark/Reset of a BufferedInputStream in case we read from the Input Stream for the header.
        final BufferedInputStream bufferedIn = new BufferedInputStream(in);
        bufferedIn.mark(1024 * 1024);
        final RecordSchema schema = getSchema(variables, new NonCloseableInputStream(bufferedIn), null);
        bufferedIn.reset();

        return new CSVRecordReader(bufferedIn, logger, schema, csvFormat, firstLineIsHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat);
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final SchemaRegistry schemaRegistry, final ConfigurationContext context) {
        if (strategy.equalsIgnoreCase(headerDerivedAllowableValue.getValue())) {
            return new CSVHeaderSchemaStrategy(context);
        }

        return super.getSchemaAccessStrategy(strategy, schemaRegistry, context);
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String allowableValue, final SchemaRegistry schemaRegistry, final ValidationContext context) {
        if (allowableValue.equalsIgnoreCase(headerDerivedAllowableValue.getValue())) {
            return new CSVHeaderSchemaStrategy(context);
        }

        return super.getSchemaAccessStrategy(allowableValue, schemaRegistry, context);
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(headerDerivedAllowableValue);
        return allowableValues;
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return headerDerivedAllowableValue;
    }
}
