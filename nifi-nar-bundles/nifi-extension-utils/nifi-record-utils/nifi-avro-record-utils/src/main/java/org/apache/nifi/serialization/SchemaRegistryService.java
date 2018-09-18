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
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.schema.access.SchemaAccessUtils.HWX_CONTENT_ENCODED_SCHEMA;
import static org.apache.nifi.schema.access.SchemaAccessUtils.HWX_SCHEMA_REF_ATTRIBUTES;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_BRANCH_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_VERSION;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.CONFLUENT_ENCODED_SCHEMA;

public abstract class SchemaRegistryService extends AbstractControllerService {

    private volatile ConfigurationContext configurationContext;
    protected volatile SchemaAccessStrategy schemaAccessStrategy;
    private static InputStream EMPTY_INPUT_STREAM = new ByteArrayInputStream(new byte[0]);

    private final List<AllowableValue> strategyList = Collections.unmodifiableList(Arrays.asList(
        SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY, HWX_SCHEMA_REF_ATTRIBUTES, HWX_CONTENT_ENCODED_SCHEMA, CONFLUENT_ENCODED_SCHEMA));

    protected PropertyDescriptor getSchemaAcessStrategyDescriptor() {
        return getPropertyDescriptor(SCHEMA_ACCESS_STRATEGY.getName());
    }

    protected PropertyDescriptor buildStrategyProperty(AllowableValue[] values) {
        return new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SCHEMA_ACCESS_STRATEGY)
            .allowableValues(values)
            .defaultValue(getDefaultSchemaAccessStrategy().getValue())
            .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(2);

        final AllowableValue[] strategies = getSchemaAccessStrategyValues().toArray(new AllowableValue[0]);
        properties.add(buildStrategyProperty(strategies));

        properties.add(SCHEMA_REGISTRY);
        properties.add(SCHEMA_NAME);
        properties.add(SCHEMA_VERSION);
        properties.add(SCHEMA_BRANCH_NAME);
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
        this.schemaAccessStrategy = getSchemaAccessStrategy(schemaAccess, schemaRegistry, context);
    }

    @Override
    protected ConfigurationContext getConfigurationContext() {
        return configurationContext;
    }

    protected SchemaAccessStrategy getSchemaAccessStrategy() {
        return schemaAccessStrategy;
    }

    public final RecordSchema getSchema(final Map<String, String> variables, final InputStream contentStream, final RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        final SchemaAccessStrategy accessStrategy = getSchemaAccessStrategy();
        if (accessStrategy == null) {
            throw new SchemaNotFoundException("Could not determine the Schema Access Strategy for this service");
        }

        return getSchemaAccessStrategy().getSchema(variables, contentStream, readSchema);
    }

    public RecordSchema getSchema(final Map<String, String> variables, final RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        return getSchema(variables, EMPTY_INPUT_STREAM, readSchema);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final String schemaAccessStrategy = validationContext.getProperty(getSchemaAcessStrategyDescriptor()).getValue();
        return SchemaAccessUtils.validateSchemaAccessStrategy(validationContext, schemaAccessStrategy, getSchemaAccessStrategyValues());
    }

    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        return strategyList;
    }

    protected Set<SchemaField> getSuppliedSchemaFields(final ValidationContext validationContext) {
        final String accessStrategyValue = validationContext.getProperty(getSchemaAcessStrategyDescriptor()).getValue();
        final SchemaRegistry schemaRegistry = validationContext.getProperty(SCHEMA_REGISTRY).asControllerService(SchemaRegistry.class);
        final SchemaAccessStrategy accessStrategy = getSchemaAccessStrategy(accessStrategyValue, schemaRegistry, validationContext);

        if (accessStrategy == null) {
            return EnumSet.noneOf(SchemaField.class);
        }
        final Set<SchemaField> suppliedFields = accessStrategy.getSuppliedSchemaFields();
        return suppliedFields;
    }

    protected SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final SchemaRegistry schemaRegistry, final ConfigurationContext context) {
        if (strategy == null) {
            return null;
        }

        return SchemaAccessUtils.getSchemaAccessStrategy(strategy, schemaRegistry, context);
    }

    protected SchemaAccessStrategy getSchemaAccessStrategy(final String allowableValue, final SchemaRegistry schemaRegistry, final ValidationContext context) {
        if (allowableValue == null) {
            return null;
        }
        return SchemaAccessUtils.getSchemaAccessStrategy(allowableValue, schemaRegistry, context);
    }

}
