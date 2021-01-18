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
package org.apache.nifi.stateless.core;

import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.registry.VariableRegistry;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StatelessConfigurationContext implements ConfigurationContext {

    private final Map<PropertyDescriptor, PropertyConfiguration> properties;
    private final ControllerServiceLookup serviceLookup;
    private final ControllerService service;
    private final VariableRegistry variableRegistry;
    private final ParameterContext parameterContext;

    public StatelessConfigurationContext(final ControllerService service,
                                         final Map<PropertyDescriptor, PropertyConfiguration> properties,
                                         final ControllerServiceLookup serviceLookup,
                                         final VariableRegistry variableRegistry,
                                         final ParameterContext parameterLookup) {
        this.service = service;
        this.properties = properties;
        this.serviceLookup = serviceLookup;
        this.variableRegistry = variableRegistry;
        this.parameterContext = parameterLookup;
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor property) {
        final PropertyConfiguration setPropertyValue = properties.get(property);
        final String propValue = (setPropertyValue == null) ? getActualDescriptor(property).getDefaultValue() : setPropertyValue.getEffectiveValue(parameterContext);
        return new StatelessPropertyValue(propValue, serviceLookup, parameterContext, variableRegistry);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        final List<PropertyDescriptor> supported = service.getPropertyDescriptors();

        final Map<PropertyDescriptor, String> effectiveValues = new LinkedHashMap<>();
        for (final PropertyDescriptor descriptor : supported) {
            effectiveValues.put(descriptor, null);
        }

        for (final Map.Entry<PropertyDescriptor, PropertyConfiguration> entry : properties.entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            final PropertyConfiguration configuration = entry.getValue();
            final String value = configuration.getEffectiveValue(parameterContext);

            effectiveValues.put(descriptor, value);
        }

        return effectiveValues;
    }

    @Override
    public Map<String, String> getAllProperties() {
        final Map<String, String> propValueMap = new LinkedHashMap<>();
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

    @Override
    public String getName() {
        return null;
    }
}