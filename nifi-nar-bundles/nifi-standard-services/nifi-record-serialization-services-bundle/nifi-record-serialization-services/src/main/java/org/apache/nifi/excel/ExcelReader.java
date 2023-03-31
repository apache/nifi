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
package org.apache.nifi.excel;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.InferSchemaAccessStrategy;
import org.apache.nifi.schema.inference.RecordSourceFactory;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.stream.io.NonCloseableInputStream;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;

@Tags({"excel", "spreadsheet", "xlsx", "parse", "record", "row", "reader", "values", "cell"})
@CapabilityDescription("Parses a Microsoft Excel document returning each row in each sheet as a separate record. "
        + "This reader allows for inferring a schema either based on the first line of an Excel sheet if a 'header line' is "
        + "present or from all the desired sheets, or providing an explicit schema "
        + "for interpreting the values. See Controller Service's Usage for further documentation. "
        + "This reader is currently only capable of processing .xlsx "
        + "(XSSF 2007 OOXML file format) Excel documents and not older .xls (HSSF '97(-2007) file format) documents.)")
public class ExcelReader extends SchemaRegistryService implements RecordReaderFactory {

    private static final AllowableValue HEADER_DERIVED = new AllowableValue("excel-header-derived", "Use fields From Header",
            "The first chosen row of the Excel sheet is a header row that contains the columns representative of all the rows " +
                    "in the desired sheets. The schema will be derived by using those columns in the header.");
    public static final PropertyDescriptor DESIRED_SHEETS = new PropertyDescriptor
            .Builder().name("extract-sheets")
            .displayName("Sheets to Extract")
            .description("Comma separated list of Excel document sheet names whose rows should be extracted from the excel document. If this property" +
                    " is left blank then all the rows from all the sheets will be extracted from the Excel document. The list of names is case in-sensitive. Any sheets not" +
                    " specified in this value will be ignored. A bulletin will be generated if a specified sheet(s) are not found.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FIRST_ROW_NUM = new PropertyDescriptor
            .Builder().name("excel-extract-first-row")
            .displayName("Row number to start from")
            .description("The row number of the first row to start processing (One based)."
                    + " Use this to skip over rows of data at the top of a worksheet that are not part of the dataset.")
            .required(true)
            .defaultValue("0")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    private AtomicReferenceArray<String> desiredSheets;
    private volatile int firstRow;
    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.firstRow = getFirstRow(context);
        String[] rawDesiredSheets = getRawDesiredSheets(context);
        this.desiredSheets = new AtomicReferenceArray<>(rawDesiredSheets.length);
        IntStream.range(0, rawDesiredSheets.length)
                .forEach(index -> this.desiredSheets.set(index, rawDesiredSheets[index]));
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
    }

    private int getFirstRow(final PropertyContext context) {
        int rawFirstRow = context.getProperty(FIRST_ROW_NUM).asInteger();
        return getZeroBasedIndex(rawFirstRow);
    }

    static int getZeroBasedIndex(int rawFirstRow) {
        return rawFirstRow > 0 ? rawFirstRow - 1 : 0;
    }
    private String[] getRawDesiredSheets(final PropertyContext context) {
        final String desiredSheetsDelimited = context.getProperty(DESIRED_SHEETS).getValue();
        return getRawDesiredSheets(desiredSheetsDelimited, getLogger());
    }

    static String[] getRawDesiredSheets(String desiredSheetsDelimited, ComponentLog logger) {
        if (desiredSheetsDelimited != null) {
            String[] delimitedSheets = StringUtils.split(desiredSheetsDelimited, ",");
            if (delimitedSheets != null) {
                return delimitedSheets;
            } else {
                if (logger != null) {
                    logger.debug("Excel document was parsed but no sheets with the specified desired names were found.");
                }
            }
        }

        return new String[0];
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(DESIRED_SHEETS);
        properties.add(FIRST_ROW_NUM);
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);

        return properties;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String allowableValue, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        if (allowableValue.equalsIgnoreCase(HEADER_DERIVED.getValue())) {
            return new ExcelHeaderSchemaStrategy(context);
        } else if (allowableValue.equalsIgnoreCase(SchemaInferenceUtil.INFER_SCHEMA.getValue())) {
            final RecordSourceFactory<Row> sourceFactory = (variables, in) -> new ExcelRecordSource(in, context, variables);
            final SchemaInferenceEngine<Row> inference = new ExcelSchemaInference(new TimeValueInference(dateFormat, timeFormat, timestampFormat));
            return new InferSchemaAccessStrategy<>(sourceFactory, inference, getLogger());
        }

        return super.getSchemaAccessStrategy(allowableValue, schemaRegistry, context);
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(HEADER_DERIVED);
        allowableValues.add(SchemaInferenceUtil.INFER_SCHEMA);
        return allowableValues;
    }

    @Override
    public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long inputLength, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        // Use Mark/Reset of a BufferedInputStream in case we read from the Input Stream for the header.
        in.mark(1024 * 1024);
        final RecordSchema schema = getSchema(variables, new NonCloseableInputStream(in), null);
        in.reset();

        ExcelRecordReaderArgs args = new ExcelRecordReaderArgs.Builder()
                .withDateFormat(dateFormat)
                .withDesiredSheets(desiredSheets)
                .withFirstRow(firstRow)
                .withInputStream(in)
                .withLogger(logger)
                .withSchema(schema)
                .withTimeFormat(timeFormat)
                .withTimestampFormat(timestampFormat)
                .build();

        return new ExcelRecordReader(args);
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return SchemaInferenceUtil.INFER_SCHEMA;
    }
}
