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
package org.apache.nifi.util;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.registry.VariableRegistry;

public class MockConfigurationContext implements ConfigurationContext {

    private final Map<PropertyDescriptor, String> properties;
    private final ControllerServiceLookup serviceLookup;
    private final ControllerService service;
    private final VariableRegistry variableRegistry;

    public MockConfigurationContext(final Map<PropertyDescriptor, String> properties,
            final ControllerServiceLookup serviceLookup) {
        this(null, properties, serviceLookup, VariableRegistry.EMPTY_REGISTRY);
    }

    public MockConfigurationContext(final Map<PropertyDescriptor, String> properties,
            final ControllerServiceLookup serviceLookup,
            final VariableRegistry variableRegistry) {
        this(null, properties, serviceLookup, variableRegistry);
    }

    public MockConfigurationContext(final ControllerService service,
            final Map<PropertyDescriptor, String> properties,
            final ControllerServiceLookup serviceLookup,
            final VariableRegistry variableRegistry) {
        this.service = service;
        this.properties = properties;
        this.serviceLookup = serviceLookup;
        this.variableRegistry = variableRegistry;
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor property) {
        String value = properties.get(property);
        if (value == null) {
            value = getActualDescriptor(property).getDefaultValue();
        }
        return new MockPropertyValue(value, serviceLookup, variableRegistry);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return new HashMap<>(this.properties);
    }

    @Override
    public Map<String, String> getAllProperties() {
        final Map<String,String> propValueMap = new LinkedHashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : getProperties().entrySet()) {
            propValueMap.put(entry.getKey().getName(), entry.getValue());
        }
        return propValueMap;
    }

    private PropertyDescriptor getActualDescriptor(final PropertyDescriptor property) {
        if (service == null) {
            return property;
        }

        final PropertyDescriptor resolved = service.getPropertyDescriptor(property.getName());
        return resolved == null ? property : resolved;
    }

    @Override
    public String getSchedulingPeriod() {
        return "0 secs";
    }

    @Override
    public Long getSchedulingPeriod(final TimeUnit timeUnit) {
        return 0L;
    }
}
