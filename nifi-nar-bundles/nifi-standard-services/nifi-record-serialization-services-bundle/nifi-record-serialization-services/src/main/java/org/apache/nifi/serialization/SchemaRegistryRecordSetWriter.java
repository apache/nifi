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

import static org.apache.nifi.schema.access.SchemaAccessUtils.INHERIT_RECORD_SCHEMA;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
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

    /**
     * This constant is just a base spec for the actual PropertyDescriptor.
     * As it can be overridden by subclasses with different AllowableValues and default value,
     * {@link #getSchemaWriteStrategyDescriptor()} should be used to get the actual descriptor, instead of using this constant directly.
     */
    private static final PropertyDescriptor SCHEMA_WRITE_STRATEGY = new PropertyDescriptor.Builder()
        .name("Schema Write Strategy")
        .description("Specifies how the schema for a Record should be added to the data.")
        .required(true)
        .build();


    private volatile ConfigurationContext configurationContext;
    private volatile SchemaAccessWriter schemaAccessWriter;

    private final List<AllowableValue> schemaWriteStrategyList = Collections.unmodifiableList(Arrays.asList(
        SCHEMA_NAME_ATTRIBUTE, AVRO_SCHEMA_ATTRIBUTE, HWX_SCHEMA_REF_ATTRIBUTES, HWX_CONTENT_ENCODED_SCHEMA, CONFLUENT_ENCODED_SCHEMA, NO_SCHEMA));
    private final List<AllowableValue> schemaAccessStrategyList = Collections.unmodifiableList(Arrays.asList(
        SCHEMA_NAME_PROPERTY, INHERIT_RECORD_SCHEMA, SCHEMA_TEXT_PROPERTY));


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();

        final AllowableValue[] strategies = getSchemaWriteStrategyValues().toArray(new AllowableValue[0]);
        properties.add(new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SCHEMA_WRITE_STRATEGY)
            .defaultValue(getDefaultSchemaWriteStrategy().getValue())
            .allowableValues(strategies)
            .build());
        properties.addAll(super.getSupportedPropertyDescriptors());

        return properties;
    }

    protected AllowableValue getDefaultSchemaWriteStrategy() {
        return SCHEMA_NAME_ATTRIBUTE;
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

        final String writerValue = context.getProperty(getSchemaWriteStrategyDescriptor()).getValue();
        this.schemaAccessWriter = getSchemaWriteStrategy(writerValue);
    }

    @Override
    protected ConfigurationContext getConfigurationContext() {
        return configurationContext;
    }

    protected SchemaAccessWriter getSchemaAccessWriter(final RecordSchema schema) throws SchemaNotFoundException {
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

    protected SchemaAccessWriter getSchemaWriteStrategy(final String strategy) {
        if (strategy == null) {
            return null;
        }

        if (strategy.equalsIgnoreCase(SCHEMA_NAME_ATTRIBUTE.getValue())) {
            return new SchemaNameAsAttribute();
        } else if (strategy.equalsIgnoreCase(AVRO_SCHEMA_ATTRIBUTE.getValue())) {
            return new WriteAvroSchemaAttributeStrategy();
        } else if (strategy.equalsIgnoreCase(HWX_CONTENT_ENCODED_SCHEMA.getValue())) {
            return new HortonworksEncodedSchemaReferenceWriter();
        } else if (strategy.equalsIgnoreCase(HWX_SCHEMA_REF_ATTRIBUTES.getValue())) {
            return new HortonworksAttributeSchemaReferenceWriter();
        } else if (strategy.equalsIgnoreCase(CONFLUENT_ENCODED_SCHEMA.getValue())) {
            return new ConfluentSchemaRegistryWriter();
        } else if (strategy.equalsIgnoreCase(NO_SCHEMA.getValue())) {
            return new NopSchemaAccessWriter();
        }

        return null;
    }

    protected Set<SchemaField> getRequiredSchemaFields(final ValidationContext validationContext) {
        final String writeStrategyValue = validationContext.getProperty(getSchemaWriteStrategyDescriptor()).getValue();
        final SchemaAccessWriter writer = getSchemaWriteStrategy(writeStrategyValue);
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

        return results;
    }
}
