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
package org.apache.nifi.schemaregistry.services;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

@Tags({ "schema", "registry", "avro", "json", "csv" })
@CapabilityDescription("Provides a service for registering and accessing schemas. You can register schema "
        + "as a dynamic property where 'name' represents the schema name and 'value' represents the textual "
        + "representation of the actual schema.")
public class SimpleKeyValueSchemaRegistry extends AbstractControllerService implements SchemaRegistry {

    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        propertyDescriptors = Collections.emptyList();
    }

    private final Map<String, String> schemaNameToSchemaMap;

    public SimpleKeyValueSchemaRegistry() {
        this.schemaNameToSchemaMap = new HashMap<>();
    }

    @OnEnabled
    public void enable(ConfigurationContext configuratiponContext) throws InitializationException {
        this.schemaNameToSchemaMap.putAll(configuratiponContext.getProperties().entrySet().stream()
                .filter(propEntry -> propEntry.getKey().isDynamic())
                .collect(Collectors.toMap(propEntry -> propEntry.getKey().getName(), propEntry -> propEntry.getValue())));
    }

    /**
     *
     */
    @Override
    public String retrieveSchemaText(String schemaName) {
        if (!this.schemaNameToSchemaMap.containsKey(schemaName)) {
            throw new IllegalArgumentException("Failed to find schema; Name: '" + schemaName + ".");
        } else {
            return this.schemaNameToSchemaMap.get(schemaName);
        }
    }

    @Override
    public String retrieveSchemaText(String schemaName, Properties attributes) {
        throw new UnsupportedOperationException("This version of schema registry does not "
                + "support this operation, since schemas are only identofied by name.");
    }

    @Override
    @OnDisabled
    public void close() throws Exception {
        this.schemaNameToSchemaMap.clear();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder().required(false).name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).dynamic(true).expressionLanguageSupported(true)
                .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }
}
