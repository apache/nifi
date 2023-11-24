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

import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.GrokUtils;
import io.krakens.grok.api.exception.GrokException;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

@Tags({"grok", "logs", "logfiles", "parse", "unstructured", "text", "record", "reader", "regex", "pattern", "logstash"})
@CapabilityDescription("Provides a mechanism for reading unstructured text data, such as log files, and structuring the data "
    + "so that it can be processed. The service is configured using Grok patterns. "
    + "The service reads from a stream of data and splits each message that it finds into a separate Record, each containing the fields that are configured. "
    + "If a line in the input does not match the expected message pattern, the line of text is either considered to be part of the previous "
    + "message or is skipped, depending on the configuration, with the exception of stack traces. A stack trace that is found at the end of "
    + "a log message is considered to be part of the previous message but is added to the 'stackTrace' field of the Record. If a record has "
    + "no stack trace, it will have a NULL value for the stackTrace field (assuming that the schema does in fact include a stackTrace field of type String). "
    + "Assuming that the schema includes a '_raw' field of type String, the raw message will be included in the Record.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.REFERENCE_REMOTE_RESOURCES,
                        explanation = "Patterns and Expressions can reference resources over HTTP"
                )
        }
)
public class GrokReader extends SchemaRegistryService implements RecordReaderFactory {
    private volatile List<Grok> groks;
    private volatile NoMatchStrategy noMatchStrategy;
    private volatile RecordSchema recordSchema;
    private volatile RecordSchema recordSchemaFromGrok;

    static final String DEFAULT_PATTERN_NAME = "/default-grok-patterns.txt";

    static final AllowableValue APPEND_TO_PREVIOUS_MESSAGE = new AllowableValue("append-to-previous-message", "Append to Previous Message",
        "The line of text that does not match the Grok Expression will be appended to the last field of the prior message.");
    static final AllowableValue SKIP_LINE = new AllowableValue("skip-line", "Skip Line",
        "The line of text that does not match the Grok Expression will be skipped.");
    static final AllowableValue RAW_LINE = new AllowableValue("raw-line", "Raw Line",
            "The line of text that does not match the Grok Expression will only be added to the _raw field.");

    static final AllowableValue STRING_FIELDS_FROM_GROK_EXPRESSION = new AllowableValue("string-fields-from-grok-expression", "Use String Fields From Grok Expression",
            "The schema will be derived using the field names present in all configured Grok Expressions. "
            + "All schema fields will have a String type and will be marked as nullable. "
            + "The schema will also include a `stackTrace` field, and a `_raw` field containing the input line string."
    );

    static final PropertyDescriptor GROK_PATTERNS = new PropertyDescriptor.Builder()
        .name("Grok Pattern File")
        .displayName("Grok Patterns")
        .description("Grok Patterns to use for parsing logs. If not specified, a built-in default Pattern file "
            + "will be used. If specified, all patterns specified will override the default patterns. See the Controller Service's "
            + "Additional Details for a list of pre-defined patterns.")
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.URL, ResourceType.TEXT)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(false)
        .build();

    static final PropertyDescriptor GROK_EXPRESSION = new PropertyDescriptor.Builder()
        .name("Grok Expression")
        .displayName("Grok Expressions")
        .description("Specifies the format of a log line in Grok format. This allows the Record Reader to understand how to parse each log line. "
            + "The property supports one or more Grok expressions. The Reader attempts to parse input lines according to the configured order of the expressions."
            + "If a line in the log file does not match any expressions, the line will be assumed to belong to the previous log message."
            + "If other Grok patterns are referenced by this expression, they need to be supplied in the Grok Pattern File property."
        )
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.TEXT, ResourceType.URL, ResourceType.FILE)
        .required(true)
        .build();

    static final PropertyDescriptor NO_MATCH_BEHAVIOR = new PropertyDescriptor.Builder()
        .name("no-match-behavior")
        .displayName("No Match Behavior")
        .description("If a line of text is encountered and it does not match the given Grok Expression, and it is not part of a stack trace, "
            + "this property specifies how the text should be processed.")
        .allowableValues(APPEND_TO_PREVIOUS_MESSAGE, SKIP_LINE, RAW_LINE)
        .defaultValue(APPEND_TO_PREVIOUS_MESSAGE.getValue())
        .required(true)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(GROK_PATTERNS);
        properties.add(GROK_EXPRESSION);
        properties.add(NO_MATCH_BEHAVIOR);
        return properties;
    }

    @OnEnabled
    public void preCompile(final ConfigurationContext context) throws GrokException, IOException {
        GrokCompiler grokCompiler = GrokCompiler.newInstance();

        try (final Reader defaultPatterns = getDefaultPatterns()) {
            grokCompiler.register(defaultPatterns);
        }

        if (context.getProperty(GROK_PATTERNS).isSet()) {
            try (final InputStream patterns = context.getProperty(GROK_PATTERNS).evaluateAttributeExpressions().asResource().read()) {
                grokCompiler.register(patterns);
            }
        }

        groks = readGrokExpressions(context).stream()
                .map(grokCompiler::compile)
                .collect(Collectors.toList());

        if (context.getProperty(NO_MATCH_BEHAVIOR).getValue().equalsIgnoreCase(APPEND_TO_PREVIOUS_MESSAGE.getValue())) {
            noMatchStrategy = NoMatchStrategy.APPEND;
        } else if (context.getProperty(NO_MATCH_BEHAVIOR).getValue().equalsIgnoreCase(RAW_LINE.getValue())) {
            noMatchStrategy = NoMatchStrategy.RAW;
        } else {
            noMatchStrategy = NoMatchStrategy.SKIP;
        }

        this.recordSchemaFromGrok = createRecordSchema(groks);

        final String schemaAccess = context.getProperty(getSchemaAccessStrategyDescriptor()).getValue();
        if (STRING_FIELDS_FROM_GROK_EXPRESSION.getValue().equals(schemaAccess)) {
            this.recordSchema = recordSchemaFromGrok;
        } else {
            this.recordSchema = null;
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        final GrokCompiler grokCompiler = GrokCompiler.newInstance();

        final String expressionSubject = GROK_EXPRESSION.getDisplayName();

        try (final Reader defaultPatterns = getDefaultPatterns()) {
            grokCompiler.register(defaultPatterns);
        } catch (final IOException e) {
            results.add(new ValidationResult.Builder()
                    .input("Default Grok Patterns")
                    .subject(expressionSubject)
                    .valid(false)
                    .explanation("Unable to load default patterns: " + e.getMessage())
                    .build());
        }

        final ResourceReference patternsReference = validationContext.getProperty(GROK_PATTERNS).evaluateAttributeExpressions().asResource();
        final GrokExpressionValidator validator = new GrokExpressionValidator(patternsReference, grokCompiler);

        try {
            final List<String> grokExpressions = readGrokExpressions(validationContext);
            final List<ValidationResult> grokExpressionResults = grokExpressions.stream()
                    .map(grokExpression -> validator.validate(expressionSubject, grokExpression, validationContext)).collect(Collectors.toList());
            results.addAll(grokExpressionResults);
        } catch (final IOException e) {
            results.add(new ValidationResult.Builder()
                    .input("Configured Grok Expressions")
                    .subject(expressionSubject)
                    .valid(false)
                    .explanation(String.format("Read Grok Expressions failed: %s", e.getMessage()))
                    .build());
        }

        return results;
    }

    private List<String> readGrokExpressions(final PropertyContext propertyContext) throws IOException {
        final ResourceReference expressionsResource = propertyContext.getProperty(GROK_EXPRESSION).asResource();
        try (
                final InputStream expressionsStream = expressionsResource.read();
                final BufferedReader expressionsReader = new BufferedReader(new InputStreamReader(expressionsStream))
        ) {
            return expressionsReader.lines().collect(Collectors.toList());
        }
    }

    static RecordSchema createRecordSchema(final List<Grok> groks) {
        final Set<RecordField> fields = new LinkedHashSet<>();

        groks.forEach(grok -> populateSchemaFieldNames(grok, grok.getOriginalGrokPattern(), fields));

        fields.add(new RecordField(GrokRecordReader.STACK_TRACE_COLUMN_NAME, RecordFieldType.STRING.getDataType(), true));
        fields.add(new RecordField(GrokRecordReader.RAW_MESSAGE_NAME, RecordFieldType.STRING.getDataType(), true));

        return new SimpleRecordSchema(new ArrayList<>(fields));
    }

    private static void populateSchemaFieldNames(final Grok grok, String grokExpression, final Collection<RecordField> fields) {
        final Set<String> namedGroups = GrokUtils.getNameGroups(GrokUtils.GROK_PATTERN.pattern());
        while (grokExpression.length() > 0) {
            final Matcher matcher = GrokUtils.GROK_PATTERN.matcher(grokExpression);
            if (matcher.find()) {
                final Map<String, String> extractedGroups = GrokUtils.namedGroups(matcher, namedGroups);
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
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final SchemaRegistry schemaRegistry, final PropertyContext context) {
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
            public RecordSchema getSchema(Map<String, String> variables, InputStream contentStream, RecordSchema readSchema) {
                return recordSchema;
            }

            @Override
            public Set<SchemaField> getSuppliedSchemaFields() {
                return schemaFields;
            }
        };
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger) throws IOException, SchemaNotFoundException {
        final RecordSchema schema = getSchema(variables, in, null);
        return new GrokRecordReader(in, groks, schema, recordSchemaFromGrok, noMatchStrategy);
    }

    private Reader getDefaultPatterns() throws IOException {
        final InputStream inputStream = getClass().getResourceAsStream(DEFAULT_PATTERN_NAME);
        if (inputStream == null) {
            throw new IOException(String.format("Default Patterns [%s] not found", DEFAULT_PATTERN_NAME));
        }
        return new InputStreamReader(inputStream);
    }
}