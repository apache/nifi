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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.avro.AvroSchemaValidator;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.AvroSchemaTextStrategy;
import org.apache.nifi.schema.access.HortonworksAttributeSchemaReferenceStrategy;
import org.apache.nifi.schema.access.HortonworksEncodedSchemaReferenceStrategy;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNamePropertyStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;

public abstract class SchemaRegistryService extends AbstractControllerService {

    static final AllowableValue SCHEMA_NAME_PROPERTY = new AllowableValue("schema-name", "Use 'Schema Name' Property",
        "The name of the Schema to use is specified by the 'Schema Name' Property. The value of this property is used to lookup the Schema in the configured Schema Registry service.");
    static final AllowableValue SCHEMA_TEXT_PROPERTY = new AllowableValue("schema-text-property", "Use 'Schema Text' Property",
        "The text of the Schema itself is specified by the 'Schema Text' Property. The value of this property must be a valid Avro Schema. "
            + "If Expression Language is used, the value of the 'Schema Text' property must be valid after substituting the expressions.");
    static final AllowableValue HWX_CONTENT_ENCODED_SCHEMA = new AllowableValue("hwx-content-encoded-schema", "HWX Content-Encoded Schema Reference",
        "The content of the FlowFile contains a reference to a schema in the Schema Registry service. The reference is encoded as a single byte indicating the 'protocol version', "
            + "followed by 8 bytes indicating the schema identifier, and finally 4 bytes indicating the schema version, as per the Hortonworks Schema Registry serializers and deserializers, "
            + "found at https://github.com/hortonworks/registry");
    static final AllowableValue HWX_SCHEMA_REF_ATTRIBUTES = new AllowableValue("hwx-schema-ref-attributes", "HWX Schema Reference Attributes",
        "The FlowFile contains 3 Attributes that will be used to lookup a Schema from the configured Schema Registry: 'schema.identifier', 'schema.version', and 'schema.protocol.version'");

    protected static final PropertyDescriptor SCHEMA_REGISTRY = new PropertyDescriptor.Builder()
        .name("Schema Registry")
        .description("Specifies the Controller Service to use for the Schema Registry")
        .identifiesControllerService(SchemaRegistry.class)
        .required(false)
        .build();

    protected static final PropertyDescriptor SCHEMA_ACCESS_STRATEGY = new PropertyDescriptor.Builder()
        .name("Schema Access Strategy")
        .description("Specifies how to obtain the schema that is to be used for interpreting the data.")
        .allowableValues(SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY, HWX_SCHEMA_REF_ATTRIBUTES, HWX_CONTENT_ENCODED_SCHEMA)
        .defaultValue(SCHEMA_NAME_PROPERTY.getValue())
        .required(true)
        .build();

    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
        .name("Schema Name")
        .description("Specifies the name of the schema to lookup in the Schema Registry property")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .defaultValue("${schema.name}")
        .required(false)
        .build();

    static final PropertyDescriptor SCHEMA_TEXT = new PropertyDescriptor.Builder()
        .name("schema-text")
        .displayName("Schema Text")
        .description("The text of an Avro-formatted Schema")
        .addValidator(new AvroSchemaValidator())
        .expressionLanguageSupported(true)
        .defaultValue("${avro.schema}")
        .required(false)
        .build();


    private volatile ConfigurationContext configurationContext;
    private volatile SchemaAccessStrategy schemaAccessStrategy;

    private final List<AllowableValue> strategyList = Collections.unmodifiableList(Arrays.asList(SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY, HWX_SCHEMA_REF_ATTRIBUTES, HWX_CONTENT_ENCODED_SCHEMA));

    private PropertyDescriptor getSchemaAcessStrategyDescriptor() {
        return getPropertyDescriptor(SCHEMA_ACCESS_STRATEGY.getName());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(2);

        final AllowableValue[] strategies = getSchemaAccessStrategyValues().toArray(new AllowableValue[0]);
        properties.add(new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SCHEMA_ACCESS_STRATEGY)
            .allowableValues(strategies)
            .defaultValue(getDefaultSchemaAccessStrategy().getValue())
            .build());

        properties.add(SCHEMA_REGISTRY);
        properties.add(SCHEMA_NAME);
        properties.add(SCHEMA_TEXT);

        return properties;
    }

    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return SCHEMA_NAME_PROPERTY;
    }

    @OnEnabled
    public void storeSchemaAccessStrategy(final ConfigurationContext context) {
        this.configurationContext = context;

        final SchemaRegistry schemaRegistry = context.getProperty(SCHEMA_REGISTRY).asControllerService(SchemaRegistry.class);

        final PropertyDescriptor descriptor = getSchemaAcessStrategyDescriptor();
        final String schemaAccess = context.getProperty(descriptor).getValue();
        this.schemaAccessStrategy = getSchemaAccessStrategy(schemaAccess, schemaRegistry);
    }

    protected ConfigurationContext getConfigurationContext() {
        return configurationContext;
    }

    protected SchemaAccessStrategy getSchemaAccessStrategy() {
        return schemaAccessStrategy;
    }

    public RecordSchema getSchema(final FlowFile flowFile, final InputStream contentStream) throws SchemaNotFoundException, IOException {
        return getSchemaAccessStrategy().getSchema(flowFile, contentStream, configurationContext);
    }

    private String getSchemaAccessStrategyName(final String schemaAccessValue) {
        for (final AllowableValue allowableValue : getSchemaAcessStrategyDescriptor().getAllowableValues()) {
            if (allowableValue.getValue().equalsIgnoreCase(schemaAccessValue)) {
                return allowableValue.getDisplayName();
            }
        }

        return null;
    }

    private boolean isSchemaRegistryRequired(final String schemaAccessValue) {
        return HWX_CONTENT_ENCODED_SCHEMA.getValue().equalsIgnoreCase(schemaAccessValue) || SCHEMA_NAME_PROPERTY.getValue().equalsIgnoreCase(schemaAccessValue)
            || HWX_SCHEMA_REF_ATTRIBUTES.getValue().equalsIgnoreCase(schemaAccessValue);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final String schemaAccessStrategy = validationContext.getProperty(getSchemaAcessStrategyDescriptor()).getValue();
        if (isSchemaRegistryRequired(schemaAccessStrategy)) {
            final boolean registrySet = validationContext.getProperty(SCHEMA_REGISTRY).isSet();
            if (!registrySet) {
                final String schemaAccessStrategyName = getSchemaAccessStrategyName(schemaAccessStrategy);

                return Collections.singleton(new ValidationResult.Builder()
                    .subject("Schema Registry")
                    .explanation("The '" + schemaAccessStrategyName + "' Schema Access Strategy requires that the Schema Registry property be set.")
                    .valid(false)
                    .build());
            }
        }

        return Collections.emptyList();
    }

    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        return strategyList;
    }

    protected Set<SchemaField> getSuppliedSchemaFields(final ValidationContext validationContext) {
        final String accessStrategyValue = validationContext.getProperty(getSchemaAcessStrategyDescriptor()).getValue();
        final SchemaRegistry schemaRegistry = validationContext.getProperty(SCHEMA_REGISTRY).asControllerService(SchemaRegistry.class);
        final SchemaAccessStrategy accessStrategy = getSchemaAccessStrategy(accessStrategyValue, schemaRegistry, validationContext);

        final Set<SchemaField> suppliedFields = accessStrategy.getSuppliedSchemaFields();
        return suppliedFields;
    }

    protected SchemaAccessStrategy getSchemaAccessStrategy(final String allowableValue, final SchemaRegistry schemaRegistry) {
        if (allowableValue.equalsIgnoreCase(SCHEMA_NAME_PROPERTY.getValue())) {
            return new SchemaNamePropertyStrategy(schemaRegistry, getConfigurationContext().getProperty(SCHEMA_NAME));
        } else if (allowableValue.equalsIgnoreCase(SCHEMA_TEXT_PROPERTY.getValue())) {
            return new AvroSchemaTextStrategy(getConfigurationContext().getProperty(SCHEMA_TEXT));
        } else if (allowableValue.equalsIgnoreCase(HWX_CONTENT_ENCODED_SCHEMA.getValue())) {
            return new HortonworksEncodedSchemaReferenceStrategy(schemaRegistry);
        } else if (allowableValue.equalsIgnoreCase(HWX_SCHEMA_REF_ATTRIBUTES.getValue())) {
            return new HortonworksAttributeSchemaReferenceStrategy(schemaRegistry);
        }

        return null;
    }

    protected SchemaAccessStrategy getSchemaAccessStrategy(final String allowableValue, final SchemaRegistry schemaRegistry, final ValidationContext context) {
        if (allowableValue.equalsIgnoreCase(SCHEMA_NAME_PROPERTY.getValue())) {
            return new SchemaNamePropertyStrategy(schemaRegistry, context.getProperty(SCHEMA_NAME));
        } else if (allowableValue.equalsIgnoreCase(SCHEMA_TEXT_PROPERTY.getValue())) {
            return new AvroSchemaTextStrategy(context.getProperty(SCHEMA_TEXT));
        } else if (allowableValue.equalsIgnoreCase(HWX_CONTENT_ENCODED_SCHEMA.getValue())) {
            return new HortonworksEncodedSchemaReferenceStrategy(schemaRegistry);
        } else if (allowableValue.equalsIgnoreCase(HWX_SCHEMA_REF_ATTRIBUTES.getValue())) {
            return new HortonworksAttributeSchemaReferenceStrategy(schemaRegistry);
        }

        return null;
    }

}
