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
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.schema.access.NopSchemaAccessWriter;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNameAsAttribute;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.access.SchemaReferenceWriterSchemaAccessWriter;
import org.apache.nifi.schema.access.WriteAvroSchemaAttributeStrategy;
import org.apache.nifi.schemaregistry.services.SchemaReferenceWriter;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.nifi.schema.access.SchemaAccessUtils.INHERIT_RECORD_SCHEMA;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;

public abstract class SchemaRegistryRecordSetWriter extends SchemaRegistryService {

    public static final AllowableValue SCHEMA_NAME_ATTRIBUTE = new AllowableValue("schema-name", "Set 'schema.name' Attribute",
        "The FlowFile will be given an attribute named 'schema.name' and this attribute will indicate the name of the schema in the Schema Registry. Note that if"
            + "the schema for a record is not obtained from a Schema Registry, then no attribute will be added.");

    public static final AllowableValue AVRO_SCHEMA_ATTRIBUTE = new AllowableValue("full-schema-attribute", "Set 'avro.schema' Attribute",
        "The FlowFile will be given an attribute named 'avro.schema' and this attribute will contain the Avro Schema that describes the records in the FlowFile. "
            + "The contents of the FlowFile need not be Avro, but the text of the schema will be used.");

    public static final AllowableValue SCHEMA_REFERENCE_WRITER = new AllowableValue("schema-reference-writer", "Schema Reference Writer",
            "The schema reference information will be written through a configured Schema Reference Writer service implementation.");

    public static final AllowableValue NO_SCHEMA = new AllowableValue("no-schema", "Do Not Write Schema", "Do not add any schema-related information to the FlowFile.");

    public static final PropertyDescriptor SCHEMA_CACHE = new Builder()
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

    public static PropertyDescriptor SCHEMA_REFERENCE_WRITER_SERVICE = new Builder()
            .name("Schema Reference Writer")
            .description("Service implementation responsible for writing FlowFile attributes or content header with Schema reference information")
            .dependsOn(SCHEMA_WRITE_STRATEGY, SCHEMA_REFERENCE_WRITER)
            .required(true)
            .identifiesControllerService(SchemaReferenceWriter.class)
            .build();

    private static final String OBSOLETE_CONFLUENT_ENCODED_WRITE_STRATEGY = "confluent-encoded";

    private static final String OBSOLETE_SCHEMA_PROTOCOL_VERSION = "schema-protocol-version";

    private static final String CONFLUENT_ENCODED_SCHEMA_REFERENCE_WRITER = "org.apache.nifi.confluent.schemaregistry.ConfluentEncodedSchemaReferenceWriter";

    private volatile ConfigurationContext configurationContext;

    private volatile SchemaAccessWriter schemaAccessWriter;

    private final List<AllowableValue> schemaWriteStrategies = List.of(NO_SCHEMA, SCHEMA_NAME_ATTRIBUTE, AVRO_SCHEMA_ATTRIBUTE, SCHEMA_REFERENCE_WRITER);

    private final List<AllowableValue> schemaAccessStrategies = List.of(INHERIT_RECORD_SCHEMA, SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY);

    @Override
    public void migrateProperties(final PropertyConfiguration propertyConfiguration) {
        propertyConfiguration.removeProperty(OBSOLETE_SCHEMA_PROTOCOL_VERSION);

        final Optional<String> schemaWriteStrategyFound = propertyConfiguration.getPropertyValue(SCHEMA_WRITE_STRATEGY);
        if (schemaWriteStrategyFound.isPresent()) {
            final String schemaWriteStrategy = schemaWriteStrategyFound.get();
            if (OBSOLETE_CONFLUENT_ENCODED_WRITE_STRATEGY.equals(schemaWriteStrategy)) {
                propertyConfiguration.setProperty(SCHEMA_WRITE_STRATEGY, SCHEMA_REFERENCE_WRITER.getValue());

                final String serviceId = propertyConfiguration.createControllerService(CONFLUENT_ENCODED_SCHEMA_REFERENCE_WRITER, Collections.emptyMap());
                propertyConfiguration.setProperty(SCHEMA_REFERENCE_WRITER_SERVICE, serviceId);
            }
        }
    }

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
        properties.add(SCHEMA_REFERENCE_WRITER_SERVICE);
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

    public PropertyDescriptor getSchemaWriteStrategyDescriptor() {
        return getPropertyDescriptor(SCHEMA_WRITE_STRATEGY.getName());
    }

    @OnEnabled
    public void storeSchemaWriteStrategy(final ConfigurationContext context) {
        this.configurationContext = context;

        final String strategy = context.getProperty(getSchemaWriteStrategyDescriptor()).getValue();
        final RecordSchemaCacheService recordSchemaCacheService = context.getProperty(SCHEMA_CACHE).asControllerService(RecordSchemaCacheService.class);
        final SchemaReferenceWriter schemaReferenceWriter = context.getProperty(SCHEMA_REFERENCE_WRITER_SERVICE).asControllerService(SchemaReferenceWriter.class);
        this.schemaAccessWriter = createSchemaWriteStrategy(strategy, recordSchemaCacheService, schemaReferenceWriter);
    }

    @Override
    protected ConfigurationContext getConfigurationContext() {
        return configurationContext;
    }

    protected SchemaAccessWriter getSchemaAccessWriter(final RecordSchema schema, final Map<String,String> variables) throws SchemaNotFoundException {
        schemaAccessWriter.validateSchema(schema);
        return schemaAccessWriter;
    }

    protected List<AllowableValue> getSchemaWriteStrategyValues() {
        return schemaWriteStrategies;
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        return schemaAccessStrategies;
    }

    protected SchemaAccessWriter getSchemaWriteStrategy() {
        return schemaAccessWriter;
    }

    private SchemaAccessWriter createSchemaWriteStrategy(final String strategy, final RecordSchemaCacheService recordSchemaCacheService, final SchemaReferenceWriter schemaReferenceWriter) {
        final SchemaAccessWriter writer = createRawSchemaWriteStrategy(strategy, schemaReferenceWriter);
        if (recordSchemaCacheService == null) {
            return writer;
        } else {
            return new CacheIdSchemaAccessWriter(recordSchemaCacheService, writer);
        }
    }

    private SchemaAccessWriter createRawSchemaWriteStrategy(final String strategy, final SchemaReferenceWriter schemaReferenceWriter) {
        if (strategy.equalsIgnoreCase(SCHEMA_NAME_ATTRIBUTE.getValue())) {
            return new SchemaNameAsAttribute();
        } else if (strategy.equalsIgnoreCase(AVRO_SCHEMA_ATTRIBUTE.getValue())) {
            return new WriteAvroSchemaAttributeStrategy();
        } else if (strategy.equalsIgnoreCase(SCHEMA_REFERENCE_WRITER.getValue())) {
            return new SchemaReferenceWriterSchemaAccessWriter(schemaReferenceWriter);
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

        return writer.getRequiredSchemaFields();
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

        return results;
    }
}
