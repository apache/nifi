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

package org.apache.nifi.controller.service;

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.resource.ResourceContext;
import org.apache.nifi.components.resource.StandardResourceContext;
import org.apache.nifi.components.resource.StandardResourceReferenceFactory;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.registry.VariableRegistry;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class StandardPropertyContext implements PropertyContext {
    private final Map<PropertyDescriptor, PreparedQuery> preparedQueries;
    private final Map<PropertyDescriptor, String> properties;
    private final ConfigurableComponent component;

    public StandardPropertyContext(final Map<PropertyDescriptor, String> effectivePropertyValues, final ConfigurableComponent component) {
        this.properties = effectivePropertyValues;
        this.preparedQueries = new HashMap<>();
        this.component = component;

        for (final Map.Entry<PropertyDescriptor, String> entry : properties.entrySet()) {
            final PropertyDescriptor desc = entry.getKey();
            String value = entry.getValue();
            if (value == null) {
                value = desc.getDefaultValue();
            }

            final PreparedQuery pq = Query.prepareWithParametersPreEvaluated(value);
            preparedQueries.put(desc, pq);
        }
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor property) {
        final String configuredValue = properties.get(property);

        // We need to get the 'canonical representation' of the property descriptor from the component itself,
        // since the supplied PropertyDescriptor may not have the proper default value.
        final PropertyDescriptor resolvedDescriptor = component.getPropertyDescriptor(property.getName());
        final String resolvedValue = (configuredValue == null) ? resolvedDescriptor.getDefaultValue() : configuredValue;

        final ResourceContext resourceContext = new StandardResourceContext(new StandardResourceReferenceFactory(), property);
        return new StandardPropertyValue(resourceContext, resolvedValue, null, ParameterLookup.EMPTY, preparedQueries.get(property), VariableRegistry.EMPTY_REGISTRY);
    }

    @Override
    public Map<String, String> getAllProperties() {
        final Map<String,String> propValueMap = new LinkedHashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : properties.entrySet()) {
            propValueMap.put(entry.getKey().getName(), entry.getValue());
        }
        return propValueMap;
    }

}
