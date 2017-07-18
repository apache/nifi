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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.util.FormatUtils;

public class StandardConfigurationContext implements ConfigurationContext {

    private final ConfiguredComponent component;
    private final ControllerServiceLookup serviceLookup;
    private final Map<PropertyDescriptor, PreparedQuery> preparedQueries;
    private final VariableRegistry variableRegistry;
    private final String schedulingPeriod;
    private final Long schedulingNanos;

    public StandardConfigurationContext(final ConfiguredComponent component, final ControllerServiceLookup serviceLookup, final String schedulingPeriod,
                                        final VariableRegistry variableRegistry) {
        this.component = component;
        this.serviceLookup = serviceLookup;
        this.schedulingPeriod = schedulingPeriod;
        this.variableRegistry = variableRegistry;

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
        for (final Map.Entry<PropertyDescriptor, String> entry : component.getProperties().entrySet()) {
            final PropertyDescriptor desc = entry.getKey();
            String value = entry.getValue();
            if (value == null) {
                value = desc.getDefaultValue();
            }

            final PreparedQuery pq = Query.prepare(value);
            preparedQueries.put(desc, pq);
        }
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor property) {
        final String configuredValue = component.getProperty(property);
        return new StandardPropertyValue(configuredValue == null ? property.getDefaultValue() : configuredValue, serviceLookup, preparedQueries.get(property), variableRegistry);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return component.getProperties();
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
}
