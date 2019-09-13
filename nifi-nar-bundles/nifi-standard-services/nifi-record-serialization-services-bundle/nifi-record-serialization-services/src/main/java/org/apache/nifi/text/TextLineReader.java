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

package org.apache.nifi.text;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Tags({"text", "line", "record", "row", "reader", "delimited"})
@CapabilityDescription("Parses lines of textual data, returning the number of specified lines each as a separate record. This reader assumes there is a single string field and will read "
        + "the specified number of lines as the value for that field. The process is repeated as lines are present in the input, additional records are created accordingly.")
public class TextLineReader extends AbstractControllerService implements RecordReaderFactory {

    private volatile ConfigurationContext context;
    private volatile String charSet;

    public static final PropertyDescriptor LINE_SEPARATOR = new PropertyDescriptor.Builder()
            .name("linereader-line-separator")
            .displayName("Line Separator")
            .description("Specifies the character(s) to use in order to separate individual lines of text.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("\\n")
            .required(true)
            .build();

    public static final PropertyDescriptor LINES_PER_RECORD = new PropertyDescriptor.Builder()
            .name("linereader-lines-per-record")
            .displayName("Lines Per Record")
            .description("The number of lines that will be added to the field's value. This allows groups of lines to be treated as a single string for the purposes of populating the field value. "
                    + "To split each line into its own record, set this value to 1.")
            .required(true)
            .defaultValue("1")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor SKIP_LINE_COUNT = new PropertyDescriptor.Builder()
            .name("linereader-skip-count")
            .displayName("Skip Line Count")
            .description("The number of lines at the beginning of the input that should be ignored and thus not treated as records.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .build();

    protected static final PropertyDescriptor LINE_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("linereader-field-name")
            .displayName("Output Field Name")
            .description("Specifies the name of the record field in the output, whose values will be the text lines from the input as configured in the processor.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor IGNORE_EMPTY_LINES = new PropertyDescriptor.Builder()
            .name("linereader-ignore-empty-lines")
            .displayName("Ignore Empty Lines")
            .description("If this property is true, then any empty lines in the input will not be treated as part of the record field value. If this property is false, "
                    + "an empty line is considered a line. If Lines Per Record is set to 1 and this property is false, the record field will contain the empty string.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(LINE_FIELD_NAME);
        properties.add(LINES_PER_RECORD);
        properties.add(LINE_SEPARATOR);
        properties.add(SKIP_LINE_COUNT);
        properties.add(IGNORE_EMPTY_LINES);
        properties.add(CSVUtils.CHARSET);
        return properties;
    }

    @OnEnabled
    public void storeStaticProperties(final ConfigurationContext context) {
        this.context = context;
        this.charSet = context.getProperty(CSVUtils.CHARSET).getValue();
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger) throws IOException, SchemaNotFoundException {
        final String fieldName = context.getProperty(LINE_FIELD_NAME).evaluateAttributeExpressions(variables).getValue();
        final int skipLineCount = context.getProperty(SKIP_LINE_COUNT).evaluateAttributeExpressions(variables).asInteger();
        final int linesPerRecord = context.getProperty(LINES_PER_RECORD).evaluateAttributeExpressions(variables).asInteger();
        final String lineDelimiter = context.getProperty(LINE_SEPARATOR).evaluateAttributeExpressions(variables).getValue();
        final boolean ignoreEmptyLines = context.getProperty(IGNORE_EMPTY_LINES).asBoolean();

        return new TextLineRecordReader(in, logger, fieldName, skipLineCount, linesPerRecord, ignoreEmptyLines, lineDelimiter, charSet);
    }
}
