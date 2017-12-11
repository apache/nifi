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

package org.apache.nifi.grok;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import io.thekraken.grok.api.Grok;
import io.thekraken.grok.api.GrokUtils;
import io.thekraken.grok.api.exception.GrokException;

@Tags({"grok", "logs", "logfiles", "parse", "unstructured", "text", "record", "reader", "regex", "pattern", "logstash"})
@CapabilityDescription("Provides a mechanism for reading unstructured text data, such as log files, and structuring the data "
    + "so that it can be processed. The service is configured using Grok patterns. "
    + "The service reads from a stream of data and splits each message that it finds into a separate Record, each containing the fields that are configured. "
    + "If a line in the input does not match the expected message pattern, the line of text is either considered to be part of the previous "
    + "message or is skipped, depending on the configuration, with the exception of stack traces. A stack trace that is found at the end of "
    + "a log message is considered to be part of the previous message but is added to the 'stackTrace' field of the Record. If a record has "
    + "no stack trace, it will have a NULL value for the stackTrace field (assuming that the schema does in fact include a stackTrace field of type String). "
    + "Assuming that the schema includes a '_raw' field of type String, the raw message will be included in the Record.")
public class GrokReader extends SchemaRegistryService implements RecordReaderFactory {
    private volatile Grok grok;
    private volatile boolean appendUnmatchedLine;
    private volatile RecordSchema recordSchema;
    private volatile RecordSchema recordSchemaFromGrok;

    private static final String DEFAULT_PATTERN_NAME = "/default-grok-patterns.txt";

    static final AllowableValue APPEND_TO_PREVIOUS_MESSAGE = new AllowableValue("append-to-previous-message", "Append to Previous Message",
        "The line of text that does not match the Grok Expression will be appended to the last field of the prior message.");
    static final AllowableValue SKIP_LINE = new AllowableValue("skip-line", "Skip Line",
        "The line of text that does not match the Grok Expression will be skipped.");

    static final AllowableValue STRING_FIELDS_FROM_GROK_EXPRESSION = new AllowableValue("string-fields-from-grok-expression", "Use String Fields From Grok Expression",
        "The schema will be derived by using the field names present in the Grok Expression. All fields will be assumed to be of type String. Additionally, a field will be included "
            + "with a name of 'stackTrace' and a type of String.");

    static final PropertyDescriptor PATTERN_FILE = new PropertyDescriptor.Builder()
        .name("Grok Pattern File")
        .description("Path to a file that contains Grok Patterns to use for parsing logs. If not specified, a built-in default Pattern file "
            + "will be used. If specified, all patterns in the given pattern file will override the default patterns. See the Controller Service's "
            + "Additional Details for a list of pre-defined patterns.")
        .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
        .expressionLanguageSupported(true)
        .required(false)
        .build();

    static final PropertyDescriptor GROK_EXPRESSION = new PropertyDescriptor.Builder()
        .name("Grok Expression")
        .description("Specifies the format of a log line in Grok format. This allows the Record Reader to understand how to parse each log line. "
            + "If a line in the log file does not match this pattern, the line will be assumed to belong to the previous log message.")
        .addValidator(new GrokExpressionValidator())
        .required(true)
        .build();

    static final PropertyDescriptor NO_MATCH_BEHAVIOR = new PropertyDescriptor.Builder()
        .name("no-match-behavior")
        .displayName("No Match Behavior")
        .description("If a line of text is encountered and it does not match the given Grok Expression, and it is not part of a stack trace, "
            + "this property specifies how the text should be processed.")
        .allowableValues(APPEND_TO_PREVIOUS_MESSAGE, SKIP_LINE)
        .defaultValue(APPEND_TO_PREVIOUS_MESSAGE.getValue())
        .required(true)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(PATTERN_FILE);
        properties.add(GROK_EXPRESSION);
        properties.add(NO_MATCH_BEHAVIOR);
        return properties;
    }

    @OnEnabled
    public void preCompile(final ConfigurationContext context) throws GrokException, IOException {
        grok = new Grok();

        try (final InputStream in = getClass().getResourceAsStream(DEFAULT_PATTERN_NAME);
            final Reader reader = new InputStreamReader(in)) {
            grok.addPatternFromReader(reader);
        }

        if (context.getProperty(PATTERN_FILE).isSet()) {
            grok.addPatternFromFile(context.getProperty(PATTERN_FILE).evaluateAttributeExpressions().getValue());
        }

        grok.compile(context.getProperty(GROK_EXPRESSION).getValue());

        appendUnmatchedLine = context.getProperty(NO_MATCH_BEHAVIOR).getValue().equalsIgnoreCase(APPEND_TO_PREVIOUS_MESSAGE.getValue());

        this.recordSchemaFromGrok = createRecordSchema(grok);

        final String schemaAccess = context.getProperty(getSchemaAcessStrategyDescriptor()).getValue();
        if (STRING_FIELDS_FROM_GROK_EXPRESSION.getValue().equals(schemaAccess)) {
            this.recordSchema = recordSchemaFromGrok;
        } else {
            this.recordSchema = null;
        }
    }

    static RecordSchema createRecordSchema(final Grok grok) {
        final List<RecordField> fields = new ArrayList<>();

        String grokExpression = grok.getOriginalGrokPattern();
        populateSchemaFieldNames(grok, grokExpression, fields);

        fields.add(new RecordField(GrokRecordReader.STACK_TRACE_COLUMN_NAME, RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField(GrokRecordReader.RAW_MESSAGE_NAME, RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        return schema;
    }

    private static void populateSchemaFieldNames(final Grok grok, String grokExpression, final List<RecordField> fields) {
        while (grokExpression.length() > 0) {
            final Matcher matcher = GrokUtils.GROK_PATTERN.matcher(grokExpression);
            if (matcher.find()) {
                final Map<String, String> extractedGroups = GrokUtils.namedGroups(matcher, grokExpression);
                final String subName = extractedGroups.get("subname");

                if (subName == null) {
                    final String subPatternName = extractedGroups.get("pattern");
                    if (subPatternName == null) {
                        continue;
                    }

                    final String subExpression = grok.getPatterns().get(subPatternName);
                    populateSchemaFieldNames(grok, subExpression, fields);
                } else {
                    DataType dataType = RecordFieldType.STRING.getDataType();
                    final RecordField recordField = new RecordField(subName, dataType);
                    fields.add(recordField);
                }

                if (grokExpression.length() > matcher.end() + 1) {
                    grokExpression = grokExpression.substring(matcher.end());
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }


    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>();
        allowableValues.add(STRING_FIELDS_FROM_GROK_EXPRESSION);
        allowableValues.addAll(super.getSchemaAccessStrategyValues());
        return allowableValues;
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return STRING_FIELDS_FROM_GROK_EXPRESSION;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final SchemaRegistry schemaRegistry, final ConfigurationContext context) {
        if (strategy.equalsIgnoreCase(STRING_FIELDS_FROM_GROK_EXPRESSION.getValue())) {
            return createAccessStrategy();
        } else {
            return super.getSchemaAccessStrategy(strategy, schemaRegistry, context);
        }
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final SchemaRegistry schemaRegistry, final ValidationContext context) {
        if (strategy.equalsIgnoreCase(STRING_FIELDS_FROM_GROK_EXPRESSION.getValue())) {
            return createAccessStrategy();
        } else {
            return super.getSchemaAccessStrategy(strategy, schemaRegistry, context);
        }
    }

    private SchemaAccessStrategy createAccessStrategy() {
        return new SchemaAccessStrategy() {
            private final Set<SchemaField> schemaFields = EnumSet.noneOf(SchemaField.class);


            @Override
            public RecordSchema getSchema(Map<String, String> variables, InputStream contentStream, RecordSchema readSchema) throws SchemaNotFoundException {
                return recordSchema;
            }

            @Override
            public Set<SchemaField> getSuppliedSchemaFields() {
                return schemaFields;
            }
        };
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final ComponentLog logger) throws IOException, SchemaNotFoundException {
        final RecordSchema schema = getSchema(variables, in, null);
        return new GrokRecordReader(in, grok, schema, recordSchemaFromGrok, appendUnmatchedLine);
    }
}