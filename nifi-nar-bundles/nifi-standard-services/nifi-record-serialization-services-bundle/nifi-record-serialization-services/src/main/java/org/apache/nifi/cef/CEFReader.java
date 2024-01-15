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
package org.apache.nifi.cef;

import com.fluenda.parcefone.parser.CEFParser;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.inference.SchemaInferenceUtil.SCHEMA_CACHE;

@Tags({"cef", "record", "reader", "parser"})
@CapabilityDescription("Parses CEF (Common Event Format) events, returning each row as a record. "
    + "This reader allows for inferring a schema based on the first event in the FlowFile or providing an explicit schema for interpreting the values.")
public final class CEFReader extends SchemaRegistryService implements RecordReaderFactory {

    static final AllowableValue HEADERS_ONLY = new AllowableValue("headers-only", "Headers only", "Includes only CEF header fields into the inferred schema.");
    static final AllowableValue HEADERS_AND_EXTENSIONS = new AllowableValue("headers-and-extensions", "Headers and extensions",
            "Includes the CEF header and extension fields to the schema, but not the custom extensions.");
    static final AllowableValue CUSTOM_EXTENSIONS_AS_STRINGS = new AllowableValue("custom-extensions-as-string", "With custom extensions as strings",
            "Includes all fields into the inferred schema, involving custom extension fields as string values.");
    static final AllowableValue CUSTOM_EXTENSIONS_INFERRED = new AllowableValue("custom-extensions-inferred", "With custom extensions inferred",
            "Includes all fields into the inferred schema, involving custom extension fields with inferred data types. " +
            "The inference works based on the values in the FlowFile. In some scenarios this might result unsatisfiable behaviour. " +
            "In these cases it is suggested to use \"" + CUSTOM_EXTENSIONS_AS_STRINGS.getDisplayName() + "\" Inference Strategy or predefined schema.");

    static final PropertyDescriptor INFERENCE_STRATEGY = new PropertyDescriptor.Builder()
            .name("inference-strategy")
            .displayName("Inference Strategy")
            .description("Defines the set of fields should be included in the schema and the way the fields are being interpreted.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .allowableValues(HEADERS_ONLY, HEADERS_AND_EXTENSIONS, CUSTOM_EXTENSIONS_AS_STRINGS, CUSTOM_EXTENSIONS_INFERRED)
            .defaultValue(CUSTOM_EXTENSIONS_INFERRED.getValue())
            .dependsOn(SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA)
            .build();

    static final PropertyDescriptor RAW_FIELD = new PropertyDescriptor.Builder()
            .name("raw-message-field")
            .displayName("Raw Message Field")
            .description("If set the raw message will be added to the record using the property value as field name. This is not the same as the \"rawEvent\" extension field!")
            .addValidator(new ValidateRawField())
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor INVALID_FIELD = new PropertyDescriptor.Builder()
            .name("invalid-message-field")
            .displayName("Invalid Field")
            .description("Used when a line in the FlowFile cannot be parsed by the CEF parser. " +
                    "If set, instead of failing to process the FlowFile, a record is being added with one field. " +
                    "This record contains one field with the name specified by the property and the raw message as value.")
            .addValidator(new ValidateRawField())
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor DATETIME_REPRESENTATION = new PropertyDescriptor.Builder()
            .name("datetime-representation")
            .displayName("DateTime Locale")
            .description("The IETF BCP 47 representation of the Locale to be used when parsing date " +
                    "fields with long or short month names (e.g. may <en-US> vs. mai. <fr-FR>. The default" +
                    "value is generally safe. Only change if having issues parsing CEF messages")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(new ValidateLocale())
            .defaultValue("en-US")
            .build();

    static final PropertyDescriptor ACCEPT_EMPTY_EXTENSIONS = new PropertyDescriptor.Builder()
            .name("accept-empty-extensions")
            .displayName("Accept empty extensions")
            .description("If set to true, empty extensions will be accepted and will be associated to a null value.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    private final CEFParser parser = new CEFParser();

    private volatile String rawMessageField;
    private volatile String invalidField;
    private volatile Locale parcefoneLocale;
    private volatile boolean includeCustomExtensions;
    private volatile boolean acceptEmptyExtensions;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(RAW_FIELD);
        properties.add(INVALID_FIELD);
        properties.add(DATETIME_REPRESENTATION);
        properties.add(INFERENCE_STRATEGY);

        properties.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(SCHEMA_CACHE)
                .dependsOn(SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA)
                .build());

        properties.add(ACCEPT_EMPTY_EXTENSIONS);
        return properties;
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(SchemaInferenceUtil.INFER_SCHEMA);
        return allowableValues;
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return SchemaInferenceUtil.INFER_SCHEMA;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        if (strategy.equals(SchemaInferenceUtil.INFER_SCHEMA.getValue())) {
            final String inferenceStrategy = context.getProperty(INFERENCE_STRATEGY).getValue();
            final CEFSchemaInferenceBuilder builder = new CEFSchemaInferenceBuilder();

            if (inferenceStrategy.equals(HEADERS_AND_EXTENSIONS.getValue())) {
                builder.withExtensions();
            } else if (inferenceStrategy.equals(CUSTOM_EXTENSIONS_AS_STRINGS.getValue())) {
                builder.withCustomExtensions(CEFCustomExtensionTypeResolver.STRING_RESOLVER);
            } else if (inferenceStrategy.equals(CUSTOM_EXTENSIONS_INFERRED.getValue())) {
                builder.withCustomExtensions(CEFCustomExtensionTypeResolver.SIMPLE_RESOLVER);
            }

            if (rawMessageField != null) {
                builder.withRawMessage(rawMessageField);
            }

            if (invalidField != null) {
                builder.withInvalidField(invalidField);
            }

            final boolean failFast = invalidField == null || invalidField.isEmpty();
            final CEFSchemaInference inference = builder.build();
            return SchemaInferenceUtil.getSchemaAccessStrategy(
                strategy,
                context,
                getLogger(),
                (variables, in) -> new CEFRecordSource(in, parser, parcefoneLocale, acceptEmptyExtensions, failFast),
                () -> inference,
                () -> super.getSchemaAccessStrategy(strategy, schemaRegistry, context));
        }

        return super.getSchemaAccessStrategy(strategy, schemaRegistry, context);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        rawMessageField = context.getProperty(RAW_FIELD).evaluateAttributeExpressions().getValue();
        invalidField = context.getProperty(INVALID_FIELD).evaluateAttributeExpressions().getValue();
        parcefoneLocale = Locale.forLanguageTag(context.getProperty(DATETIME_REPRESENTATION).evaluateAttributeExpressions().getValue());

        final String inferenceStrategy = context.getProperty(INFERENCE_STRATEGY).getValue();
        final boolean inferenceNeedsCustomExtensions = !inferenceStrategy.equals(HEADERS_ONLY.getValue()) && !inferenceStrategy.equals(HEADERS_AND_EXTENSIONS.getValue());
        final boolean isInferSchema =  context.getProperty(SCHEMA_ACCESS_STRATEGY).getValue().equals(SchemaInferenceUtil.INFER_SCHEMA.getValue());

        includeCustomExtensions = !isInferSchema || inferenceNeedsCustomExtensions;
        acceptEmptyExtensions = context.getProperty(ACCEPT_EMPTY_EXTENSIONS).asBoolean();
    }

    @Override
    public RecordReader createRecordReader(
        final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger
    ) throws MalformedRecordException, IOException, SchemaNotFoundException {
        final RecordSchema schema = getSchema(variables, in, null);
        return new CEFRecordReader(in, schema, parser, logger, parcefoneLocale, rawMessageField, invalidField, includeCustomExtensions, acceptEmptyExtensions);
    }

    private static class ValidateRawField implements Validator {
        private final Set<String> headerFields = CEFSchemaUtil.getHeaderFields().stream().map(r -> r.getFieldName()).collect(Collectors.toSet());
        private final Set<String> extensionFields = CEFSchemaUtil.getExtensionTypeMapping().keySet().stream().flatMap(fields -> fields.stream()).collect(Collectors.toSet());

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (headerFields.contains(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                        .explanation(input + " is one of the CEF header fields.").build();
            }

            if (extensionFields.contains(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                        .explanation(input + " is one of the CEF extension fields.").build();
            }

            // Field names are not part of the specified CEF field names are accepted, just like null or empty value
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        }
    }
}
