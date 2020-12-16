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

package org.apache.nifi.serialization;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.ConfluentSchemaRegistryWriter;
import org.apache.nifi.schema.access.HortonworksAttributeSchemaReferenceWriter;
import org.apache.nifi.schema.access.HortonworksEncodedSchemaReferenceWriter;
import org.apache.nifi.schema.access.NopSchemaAccessWriter;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNameAsAttribute;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.access.WriteAvroSchemaAttributeStrategy;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.schema.access.SchemaAccessUtils.INHERIT_RECORD_SCHEMA;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;

public abstract class SchemaRegistryRecordSetWriter extends SchemaRegistryService {

    static final AllowableValue SCHEMA_NAME_ATTRIBUTE = new AllowableValue("schema-name", "Set 'schema.name' Attribute",
        "The FlowFile will be given an attribute named 'schema.name' and this attribute will indicate the name of the schema in the Schema Registry. Note that if"
            + "the schema for a record is not obtained from a Schema Registry, then no attribute will be added.");
    static final AllowableValue AVRO_SCHEMA_ATTRIBUTE = new AllowableValue("full-schema-attribute", "Set 'avro.schema' Attribute",
        "The FlowFile will be given an attribute named 'avro.schema' and this attribute will contain the Avro Schema that describes the records in the FlowFile. "
            + "The contents of the FlowFile need not be Avro, but the text of the schema will be used.");
    static final AllowableValue HWX_CONTENT_ENCODED_SCHEMA = new AllowableValue("hwx-content-encoded-schema", "HWX Content-Encoded Schema Reference",
        "The content of the FlowFile will contain a reference to a schema in the Schema Registry service. The reference is encoded as a single byte indicating the 'protocol version', "
            + "followed by 8 bytes indicating the schema identifier, and finally 4 bytes indicating the schema version, as per the Hortonworks Schema Registry serializers and deserializers, "
            + "as found at https://github.com/hortonworks/registry. "
            + "This will be prepended to each FlowFile. Note that "
            + "if the schema for a record does not contain the necessary identifier and version, an Exception will be thrown when attempting to write the data.");
    static final AllowableValue HWX_SCHEMA_REF_ATTRIBUTES = new AllowableValue("hwx-schema-ref-attributes", "HWX Schema Reference Attributes",
        "The FlowFile will be given a set of 3 attributes to describe the schema: 'schema.identifier', 'schema.version', and 'schema.protocol.version'. Note that if "
            + "the schema for a record does not contain the necessary identifier and version, an Exception will be thrown when attempting to write the data.");
    static final AllowableValue CONFLUENT_ENCODED_SCHEMA = new AllowableValue("confluent-encoded", "Confluent Schema Registry Reference",
        "The content of the FlowFile will contain a reference to a schema in the Schema Registry service. The reference is encoded as a single "
            + "'Magic Byte' followed by 4 bytes representing the identifier of the schema, as outlined at http://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html. "
            + "This will be prepended to each FlowFile. Note that if the schema for a record does not contain the necessary identifier and version, "
            + "an Exception will be thrown when attempting to write the data. This is based on the encoding used by version 3.2.x of the Confluent Schema Registry.");
    static final AllowableValue NO_SCHEMA = new AllowableValue("no-schema", "Do Not Write Schema", "Do not add any schema-related information to the FlowFile.");

    static final PropertyDescriptor SCHEMA_CACHE = new Builder()
        .name("schema-cache")
        .displayName("Schema Cache")
        .description("Specifies a Schema Cache to add the Record Schema to so that Record Readers can quickly lookup the schema.")
        .required(false)
        .identifiesControllerService(RecordSchemaCacheService.class)
        .build();


    /**
     * This constant is just a base spec for the actual PropertyDescriptor.
     * As it can be overridden by subclasses with different AllowableValues and default value,
     * {@link #getSchemaWriteStrategyDescriptor()} should be used to get the actual descriptor, instead of using this constant directly.
     */
    private static final PropertyDescriptor SCHEMA_WRITE_STRATEGY = new Builder()
        .name("Schema Write Strategy")
        .description("Specifies how the schema for a Record should be added to the data.")
        .required(true)
        .build();

    static final PropertyDescriptor SCHEMA_PROTOCOL_VERSION = new Builder()
        .name("schema-protocol-version")
        .displayName("Schema Protocol Version")
        .description("The protocol version to be used for Schema Write Strategies that require a protocol version, such as Hortonworks Schema Registry strategies. " +
            "Valid protocol versions for Hortonworks Schema Registry are integer values 1, 2, or 3.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .dependsOn(SCHEMA_WRITE_STRATEGY, HWX_CONTENT_ENCODED_SCHEMA, HWX_SCHEMA_REF_ATTRIBUTES)
        .defaultValue("1")
        .build();


    private volatile ConfigurationContext configurationContext;
    private volatile SchemaAccessWriter schemaAccessWriter;

    private final List<AllowableValue> schemaWriteStrategyList = Collections.unmodifiableList(Arrays.asList(
        NO_SCHEMA, SCHEMA_NAME_ATTRIBUTE, AVRO_SCHEMA_ATTRIBUTE, HWX_SCHEMA_REF_ATTRIBUTES, HWX_CONTENT_ENCODED_SCHEMA, CONFLUENT_ENCODED_SCHEMA));

    private final List<AllowableValue> schemaAccessStrategyList = Collections.unmodifiableList(Arrays.asList(
        INHERIT_RECORD_SCHEMA, SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY));

    private final Set<String> schemaWriteStrategiesRequiringProtocolVersion = new HashSet<>(Arrays.asList(
        HWX_CONTENT_ENCODED_SCHEMA.getValue(), HWX_SCHEMA_REF_ATTRIBUTES.getValue()));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();

        final AllowableValue[] strategies = getSchemaWriteStrategyValues().toArray(new AllowableValue[0]);
        properties.add(new Builder()
            .fromPropertyDescriptor(SCHEMA_WRITE_STRATEGY)
            .defaultValue(getDefaultSchemaWriteStrategy().getValue())
            .allowableValues(strategies)
            .build());
        properties.add(SCHEMA_CACHE);
        properties.add(SCHEMA_PROTOCOL_VERSION);
        properties.addAll(super.getSupportedPropertyDescriptors());

        return properties;
    }

    protected AllowableValue getDefaultSchemaWriteStrategy() {
        return NO_SCHEMA;
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return INHERIT_RECORD_SCHEMA;
    }

    protected PropertyDescriptor getSchemaWriteStrategyDescriptor() {
        return getPropertyDescriptor(SCHEMA_WRITE_STRATEGY.getName());
    }

    @OnEnabled
    public void storeSchemaWriteStrategy(final ConfigurationContext context) {
        this.configurationContext = context;

        // If Schema Protocol Version is specified without EL then we can create it up front, otherwise when
        // EL is present we will re-create it later so we can re-evaluate the EL against the incoming variables

        final String strategy = context.getProperty(getSchemaWriteStrategyDescriptor()).getValue();
        if (strategy != null) {
            final RecordSchemaCacheService recordSchemaCacheService = context.getProperty(SCHEMA_CACHE).asControllerService(RecordSchemaCacheService.class);

            final PropertyValue protocolVersionValue = getConfigurationContext().getProperty(SCHEMA_PROTOCOL_VERSION);
            if (!protocolVersionValue.isExpressionLanguagePresent()) {
                final int protocolVersion = context.getProperty(SCHEMA_PROTOCOL_VERSION).asInteger();
                this.schemaAccessWriter = createSchemaWriteStrategy(strategy, protocolVersion, recordSchemaCacheService);
            }
        }
    }

    @Override
    protected ConfigurationContext getConfigurationContext() {
        return configurationContext;
    }

    protected SchemaAccessWriter getSchemaAccessWriter(final RecordSchema schema, final Map<String,String> variables) throws SchemaNotFoundException {
        // If Schema Protocol Version is using expression language, then we reevaluate against the passed in variables
        final PropertyValue protocolVersionValue = getConfigurationContext().getProperty(SCHEMA_PROTOCOL_VERSION);
        if (protocolVersionValue.isExpressionLanguagePresent()) {
            final int protocolVersion;
            final String protocolVersionString = protocolVersionValue.evaluateAttributeExpressions(variables).getValue();
            try {
                protocolVersion = Integer.parseInt(protocolVersionString);
            } catch (NumberFormatException nfe) {
                throw new SchemaNotFoundException("Unable to create Schema Write Strategy because " + SCHEMA_PROTOCOL_VERSION.getDisplayName()
                        + " must be a positive integer, but was '" + protocolVersionString + "'", nfe);
            }

            // Now recreate the SchemaAccessWriter since we may have a new value for Schema Protocol Version
            final String strategy = getConfigurationContext().getProperty(getSchemaWriteStrategyDescriptor()).getValue();
            if (strategy != null) {
                final RecordSchemaCacheService recordSchemaCacheService = getConfigurationContext().getProperty(SCHEMA_CACHE).asControllerService(RecordSchemaCacheService.class);
                schemaAccessWriter = createSchemaWriteStrategy(strategy, protocolVersion, recordSchemaCacheService);
            }
        }

        schemaAccessWriter.validateSchema(schema);
        return schemaAccessWriter;
    }

    protected List<AllowableValue> getSchemaWriteStrategyValues() {
        return schemaWriteStrategyList;
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        return schemaAccessStrategyList;
    }

    protected SchemaAccessWriter getSchemaWriteStrategy() {
        return schemaAccessWriter;
    }

    private SchemaAccessWriter createSchemaWriteStrategy(final String strategy, final Integer protocolVersion, final RecordSchemaCacheService recordSchemaCacheService) {
        final SchemaAccessWriter writer = createRawSchemaWriteStrategy(strategy, protocolVersion);
        if (recordSchemaCacheService == null) {
            return writer;
        } else {
            return new CacheIdSchemaAccessWriter(recordSchemaCacheService, writer);
        }
    }

    private SchemaAccessWriter createRawSchemaWriteStrategy(final String strategy, final Integer protocolVersion) {
        if (strategy.equalsIgnoreCase(SCHEMA_NAME_ATTRIBUTE.getValue())) {
            return new SchemaNameAsAttribute();
        } else if (strategy.equalsIgnoreCase(AVRO_SCHEMA_ATTRIBUTE.getValue())) {
            return new WriteAvroSchemaAttributeStrategy();
        } else if (strategy.equalsIgnoreCase(HWX_CONTENT_ENCODED_SCHEMA.getValue())) {
            return new HortonworksEncodedSchemaReferenceWriter(protocolVersion);
        } else if (strategy.equalsIgnoreCase(HWX_SCHEMA_REF_ATTRIBUTES.getValue())) {
            return new HortonworksAttributeSchemaReferenceWriter(protocolVersion);
        } else if (strategy.equalsIgnoreCase(CONFLUENT_ENCODED_SCHEMA.getValue())) {
            return new ConfluentSchemaRegistryWriter();
        } else if (strategy.equalsIgnoreCase(NO_SCHEMA.getValue())) {
            return new NopSchemaAccessWriter();
        }

        return null;
    }

    protected Set<SchemaField> getRequiredSchemaFields(final ValidationContext validationContext) {
        final SchemaAccessWriter writer = getSchemaWriteStrategy();
        if (writer == null) {
            return EnumSet.noneOf(SchemaField.class);
        }

        final Set<SchemaField> requiredFields = writer.getRequiredSchemaFields();
        return requiredFields;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final Set<SchemaField> suppliedFields = getSuppliedSchemaFields(validationContext);
        final Set<SchemaField> requiredFields = getRequiredSchemaFields(validationContext);

        final Set<SchemaField> missingFields = new HashSet<>(requiredFields);
        missingFields.removeAll(suppliedFields);

        if (!missingFields.isEmpty()) {
            results.add(new ValidationResult.Builder()
                .subject("Schema Access Strategy")
                .valid(false)
                .explanation("The configured Schema Write Strategy requires the " + missingFields.iterator().next()
                    + " but the configured Schema Access Strategy does not provide this information in conjunction with the selected Schema Registry. "
                    + "This Schema Access Strategy, as configured, cannot be used in conjunction with this Schema Write Strategy.")
                .build());
        }

        final String schemaWriteStrategy = validationContext.getProperty(getSchemaWriteStrategyDescriptor()).getValue();
        final String protocolVersion = validationContext.getProperty(SCHEMA_PROTOCOL_VERSION).getValue();

        if (schemaWriteStrategy != null && schemaWriteStrategiesRequiringProtocolVersion.contains(schemaWriteStrategy) && protocolVersion == null) {
            results.add(new ValidationResult.Builder()
                    .subject(SCHEMA_PROTOCOL_VERSION.getDisplayName())
                    .valid(false)
                    .explanation("The configured Schema Write Strategy requires a Schema Protocol Version to be specified.")
                    .build());
        }

        return results;
    }
}
