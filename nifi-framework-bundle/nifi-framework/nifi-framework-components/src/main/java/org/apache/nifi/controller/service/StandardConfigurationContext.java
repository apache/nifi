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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.resource.ResourceContext;
import org.apache.nifi.components.resource.StandardResourceContext;
import org.apache.nifi.components.resource.StandardResourceReferenceFactory;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.PropertyConfigurationMapper;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.util.FormatUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StandardConfigurationContext implements ConfigurationContext {

    private final ComponentNode component;
    private final ControllerServiceLookup serviceLookup;
    private final Map<PropertyDescriptor, PreparedQuery> preparedQueries;
    private final String schedulingPeriod;
    private final Long schedulingNanos;
    private final Map<PropertyDescriptor, String> properties;
    private final String annotationData;

    public StandardConfigurationContext(final ComponentNode component, final ControllerServiceLookup serviceLookup, final String schedulingPeriod) {
        this(component, serviceLookup, schedulingPeriod, component.getEffectivePropertyValues(), component.getAnnotationData());
    }

    public StandardConfigurationContext(final ComponentNode component, final Map<String, String> propertyOverrides, final String annotationDataOverride, final ParameterLookup parameterLookup,
                                        final ControllerServiceLookup serviceLookup, final String schedulingPeriod) {
        this(component, serviceLookup, schedulingPeriod, resolvePropertyValues(component, parameterLookup, propertyOverrides), annotationDataOverride);
    }

    public StandardConfigurationContext(final ComponentNode component, final ControllerServiceLookup serviceLookup, final String schedulingPeriod,
                                        final Map<PropertyDescriptor, String> propertyValues, final String annotationData) {
        this.component = component;
        this.serviceLookup = serviceLookup;
        this.schedulingPeriod = schedulingPeriod;
        this.properties = Collections.unmodifiableMap(propertyValues);
        this.annotationData = annotationData;

        if (schedulingPeriod == null) {
            schedulingNanos = null;
        } else {
            if (FormatUtils.TIME_DURATION_PATTERN.matcher(schedulingPeriod).matches()) {
                schedulingNanos = FormatUtils.getTimeDuration(schedulingPeriod, TimeUnit.NANOSECONDS);
            } else {
                schedulingNanos = null;
            }
        }

        preparedQueries = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : propertyValues.entrySet()) {
            final PropertyDescriptor desc = entry.getKey();
            String value = entry.getValue();
            if (value == null) {
                value = desc.getDefaultValue();
            }

            final PreparedQuery pq = Query.prepareWithParametersPreEvaluated(value);
            preparedQueries.put(desc, pq);
        }
    }

    private static Map<PropertyDescriptor, String> resolvePropertyValues(final ComponentNode component, final ParameterLookup parameterLookup, final Map<String, String> propertyOverrides) {
        final Map<PropertyDescriptor, String> resolvedProperties = new LinkedHashMap<>(component.getEffectivePropertyValues());
        final PropertyConfigurationMapper configurationMapper = new PropertyConfigurationMapper();

        for (final Map.Entry<String, String> entry : propertyOverrides.entrySet()) {
            final String propertyName = entry.getKey();
            final String propertyValue = entry.getValue();
            final PropertyDescriptor propertyDescriptor = component.getPropertyDescriptor(propertyName);
            if (propertyValue == null) {
                resolvedProperties.remove(propertyDescriptor);
            } else {
                final PropertyConfiguration configuration = configurationMapper.mapRawPropertyValuesToPropertyConfiguration(propertyDescriptor, propertyValue);
                final String effectiveValue = configuration.getEffectiveValue(parameterLookup);
                resolvedProperties.put(propertyDescriptor, effectiveValue);
            }
        }

        return resolvedProperties;
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor property) {
        final String configuredValue = properties.get(property);

        final String resolvedValue;
        if (configuredValue == null) {
            // We need to get the 'canonical representation' of the property descriptor from the component itself,
            // since the supplied PropertyDescriptor may not have the proper default value.
            final PropertyDescriptor resolvedDescriptor = component.getPropertyDescriptor(property.getName());
            resolvedValue = resolvedDescriptor.getDefaultValue();
        } else {
            resolvedValue = configuredValue;
        }

        final ResourceContext resourceContext = new StandardResourceContext(new StandardResourceReferenceFactory(), property);
        return new StandardPropertyValue(resourceContext, resolvedValue, serviceLookup, component.getParameterLookup(), preparedQueries.get(property));
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return properties;
    }

    @Override
    public String getAnnotationData() {
        return annotationData;
    }

    @Override
    public Map<String, String> getAllProperties() {
        final Map<String,String> propValueMap = new LinkedHashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : getProperties().entrySet()) {
            propValueMap.put(entry.getKey().getName(), entry.getValue());
        }
        return propValueMap;
    }

    @Override
    public String getSchedulingPeriod() {
        return schedulingPeriod;
    }

    @Override
    public Long getSchedulingPeriod(final TimeUnit timeUnit) {
        return schedulingNanos == null ? null : timeUnit.convert(schedulingNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public String getName() {
        return component.getName();
    }
}
