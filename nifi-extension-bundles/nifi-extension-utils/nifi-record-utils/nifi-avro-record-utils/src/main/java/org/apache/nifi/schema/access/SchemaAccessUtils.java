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
package org.apache.nifi.schema.access;

import org.apache.nifi.avro.AvroSchemaValidator;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schemaregistry.services.SchemaReferenceReader;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SchemaAccessUtils {

    public static final AllowableValue SCHEMA_NAME_PROPERTY = new AllowableValue("schema-name", "Use 'Schema Name' Property",
            "The name of the Schema to use is specified by the 'Schema Name' Property. The value of this property is used to lookup the Schema in the configured Schema Registry service.");

    public static final AllowableValue SCHEMA_TEXT_PROPERTY = new AllowableValue("schema-text-property", "Use 'Schema Text' Property",
            "The text of the Schema itself is specified by the 'Schema Text' Property. The value of this property must be a valid Avro Schema. "
                    + "If Expression Language is used, the value of the 'Schema Text' property must be valid after substituting the expressions.");

    public static final AllowableValue SCHEMA_REFERENCE_READER_PROPERTY = new AllowableValue("schema-reference-reader", "Schema Reference Reader",
            "The schema reference information will be provided through a configured Schema Reference Reader service implementation.");

    public static final AllowableValue INHERIT_RECORD_SCHEMA = new AllowableValue("inherit-record-schema", "Inherit Record Schema",
        "The schema used to write records will be the same schema that was given to the Record when the Record was created.");

    public static final AllowableValue INFER_SCHEMA = new AllowableValue("infer", "Infer from Result");

    public static final String OLD_SCHEMA_ACCESS_STRATEGY_PROPERTY_NAME = "schema-access-strategy";
    public static final String OLD_SCHEMA_REGISTRY_PROPERTY_NAME = "schema-registry";
    public static final String OLD_SCHEMA_NAME_PROPERTY_NAME = "schema-name";
    public static final String OLD_SCHEMA_BRANCH_NAME_PROPERTY_NAME = "schema-branch";
    public static final String OLD_SCHEMA_VERSION_PROPERTY_NAME = "schema-version";
    public static final String OLD_SCHEMA_TEXT_PROPERTY_NAME = "schema-text";
    public static final String OLD_SCHEMA_REFERENCE_READER_PROPERTY_NAME = "schema-reference-reader";

    public static final PropertyDescriptor SCHEMA_ACCESS_STRATEGY = new PropertyDescriptor.Builder()
            .name("Schema Access Strategy")
            .description("Specifies how to obtain the schema that is to be used for interpreting the data.")
            .allowableValues(SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY, SCHEMA_REFERENCE_READER_PROPERTY)
            .defaultValue(SCHEMA_NAME_PROPERTY.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor SCHEMA_REGISTRY = new PropertyDescriptor.Builder()
            .name("Schema Registry")
            .description("Specifies the Controller Service to use for the Schema Registry")
            .identifiesControllerService(SchemaRegistry.class)
            .required(false)
            .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY, SCHEMA_REFERENCE_READER_PROPERTY)
            .build();

    public static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("Schema Name")
            .description("Specifies the name of the schema to lookup in the Schema Registry property")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${schema.name}")
            .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY)
            .required(false)
            .build();

    public static final PropertyDescriptor SCHEMA_BRANCH_NAME = new PropertyDescriptor.Builder()
            .name("Schema Branch")
            .description("Specifies the name of the branch to use when looking up the schema in the Schema Registry property. " +
                    "If the chosen Schema Registry does not support branching, this value will be ignored.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY)
            .required(false)
            .build();

    public static final PropertyDescriptor SCHEMA_VERSION = new PropertyDescriptor.Builder()
            .name("Schema Version")
            .description("Specifies the version of the schema to lookup in the Schema Registry. " +
                    "If not specified then the latest version of the schema will be retrieved.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY)
            .required(false)
            .build();

    public static final PropertyDescriptor SCHEMA_TEXT = new PropertyDescriptor.Builder()
            .name("Schema Text")
            .description("The text of an Avro-formatted Schema")
            .addValidator(new AvroSchemaValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${avro.schema}")
            .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY)
            .required(false)
            .build();

    public static final PropertyDescriptor SCHEMA_REFERENCE_READER = new PropertyDescriptor.Builder()
            .name("Schema Reference Reader")
            .description("Service implementation responsible for reading FlowFile attributes or content to determine the Schema Reference Identifier")
            .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_REFERENCE_READER_PROPERTY)
            .identifiesControllerService(SchemaReferenceReader.class)
            .required(true)
            .build();

    public static Collection<ValidationResult> validateSchemaAccessStrategy(final ValidationContext validationContext, final String schemaAccessStrategyValue,
                                                                            final List<AllowableValue> schemaAccessStrategyValues) {

        final Collection<ValidationResult> validationResults = new ArrayList<>();

        if (isSchemaRegistryRequired(schemaAccessStrategyValue)) {
            final boolean registrySet = validationContext.getProperty(SCHEMA_REGISTRY).isSet();
            if (!registrySet) {
                final String schemaAccessStrategyName = getSchemaAccessStrategyName(schemaAccessStrategyValue, schemaAccessStrategyValues);

                validationResults.add(new ValidationResult.Builder()
                        .subject("Schema Registry")
                        .explanation("The '" + schemaAccessStrategyName + "' Schema Access Strategy requires that the Schema Registry property be set.")
                        .valid(false)
                        .build());
            }
        }

        // ensure that only branch or version is specified, but not both
        if (SCHEMA_NAME_PROPERTY.getValue().equalsIgnoreCase(schemaAccessStrategyValue)) {
            final boolean branchNameSet = validationContext.getProperty(SCHEMA_BRANCH_NAME).isSet();
            final boolean versionSet = validationContext.getProperty(SCHEMA_VERSION).isSet();

            if (branchNameSet && versionSet) {
                validationResults.add(new ValidationResult.Builder()
                        .subject(SCHEMA_BRANCH_NAME.getDisplayName())
                        .explanation(SCHEMA_BRANCH_NAME.getDisplayName() + " and " + SCHEMA_VERSION.getDisplayName() + " cannot be specified together")
                        .valid(false)
                        .build());
            }
        }

        return validationResults;
    }

    public static SchemaAccessStrategy getSchemaAccessStrategy(final String allowableValue, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        if (allowableValue.equalsIgnoreCase(SCHEMA_NAME_PROPERTY.getValue())) {
            final PropertyValue schemaName = context.getProperty(SCHEMA_NAME);
            final PropertyValue schemaBranchName = context.getProperty(SCHEMA_BRANCH_NAME);
            final PropertyValue schemaVersion = context.getProperty(SCHEMA_VERSION);
            return new SchemaNamePropertyStrategy(schemaRegistry, schemaName, schemaBranchName, schemaVersion);
        } else if (allowableValue.equalsIgnoreCase(INHERIT_RECORD_SCHEMA.getValue())) {
            return new InheritSchemaFromRecord();
        } else if (allowableValue.equalsIgnoreCase(SCHEMA_TEXT_PROPERTY.getValue())) {
            return new AvroSchemaTextStrategy(context.getProperty(SCHEMA_TEXT));
        } else if (allowableValue.equalsIgnoreCase(SCHEMA_REFERENCE_READER_PROPERTY.getValue())) {
            final SchemaReferenceReader schemaReferenceReader = context.getProperty(SCHEMA_REFERENCE_READER).asControllerService(SchemaReferenceReader.class);
            return new SchemaReferenceReaderSchemaAccessStrategy(schemaReferenceReader, schemaRegistry);
        }

        return null;
    }

    private static String getSchemaAccessStrategyName(final String schemaAccessValue, final List<AllowableValue> schemaAccessStrategyValues) {
        for (final AllowableValue allowableValue : schemaAccessStrategyValues) {
            if (allowableValue.getValue().equalsIgnoreCase(schemaAccessValue)) {
                return allowableValue.getDisplayName();
            }
        }

        return null;
    }

    private static boolean isSchemaRegistryRequired(final String schemaAccessValue) {
        return SCHEMA_NAME_PROPERTY.getValue().equalsIgnoreCase(schemaAccessValue)
                || SCHEMA_REFERENCE_READER_PROPERTY.getValue().equalsIgnoreCase(schemaAccessValue);
    }
}
