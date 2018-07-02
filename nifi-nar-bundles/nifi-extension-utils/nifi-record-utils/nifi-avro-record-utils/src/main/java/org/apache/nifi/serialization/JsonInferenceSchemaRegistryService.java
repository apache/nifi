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
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.schema.access.AvroSchemaTextStrategy;
import org.apache.nifi.schema.access.InferenceSchemaStrategy;
import org.apache.nifi.schema.access.JsonSchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaNamePropertyStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.schema.access.SchemaAccessUtils.INFER_SCHEMA;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_BRANCH_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_VERSION;

public class JsonInferenceSchemaRegistryService extends SchemaRegistryService {
    private String schemaAccess;

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        this.storeSchemaAccessStrategy(context);
        this.schemaAccess = context.getProperty(getSchemaAcessStrategyDescriptor()).getValue();
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return INFER_SCHEMA;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final SchemaRegistry schemaRegistry, final ConfigurationContext context) {
        if (strategy == null) {
            return null;
        }

        if (strategy.equalsIgnoreCase(SCHEMA_NAME_PROPERTY.getValue())) {
            final PropertyValue schemaName = context.getProperty(SCHEMA_NAME);
            final PropertyValue schemaBranchName = context.getProperty(SCHEMA_BRANCH_NAME);
            final PropertyValue schemaVersion = context.getProperty(SCHEMA_VERSION);
            return new SchemaNamePropertyStrategy(schemaRegistry, schemaName, schemaBranchName, schemaVersion);
        } else if (strategy.equalsIgnoreCase(SCHEMA_TEXT_PROPERTY.getValue())) {
            return new AvroSchemaTextStrategy(context.getProperty(SCHEMA_TEXT));
        } else if (strategy.equalsIgnoreCase(INFER_SCHEMA.getValue())) {
            return new InferenceSchemaStrategy();
        }

        return null;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(2);

        final AllowableValue[] strategies = new AllowableValue[] {
            SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY, INFER_SCHEMA
        };

        properties.add(buildStrategyProperty(strategies));

        properties.add(SCHEMA_REGISTRY);
        properties.add(SCHEMA_NAME);
        properties.add(SCHEMA_VERSION);
        properties.add(SCHEMA_BRANCH_NAME);
        properties.add(SCHEMA_TEXT);

        return properties;
    }

    public RecordSchema getSchema(Map<String, String> variables, Map<String, Object> content, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        if (schemaAccess.equalsIgnoreCase(SCHEMA_NAME_PROPERTY.getValue()) || schemaAccess.equalsIgnoreCase(SCHEMA_TEXT_PROPERTY.getValue())) {
            return getSchema(variables, readSchema);
        } else {
            return ((JsonSchemaAccessStrategy)schemaAccessStrategy).getSchema(variables, content, readSchema);
        }
    }
}
