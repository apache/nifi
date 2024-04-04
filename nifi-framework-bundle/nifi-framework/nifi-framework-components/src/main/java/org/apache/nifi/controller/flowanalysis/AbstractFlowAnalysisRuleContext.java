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

package org.apache.nifi.controller.flowanalysis;

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.resource.ResourceContext;
import org.apache.nifi.components.resource.StandardResourceContext;
import org.apache.nifi.components.resource.StandardResourceReferenceFactory;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.flowanalysis.FlowAnalysisRule;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class AbstractFlowAnalysisRuleContext implements FlowAnalysisRuleContext {
    private final FlowAnalysisRuleNode flowAnalysisRuleNode;
    private final Map<PropertyDescriptor, String> properties;
    private final ControllerServiceProvider serviceProvider;
    private final Map<PropertyDescriptor, PreparedQuery> preparedQueries;
    private final ParameterLookup parameterLookup;

    public AbstractFlowAnalysisRuleContext(
            FlowAnalysisRuleNode flowAnalysisRule,
            Map<PropertyDescriptor, String> properties,
            ControllerServiceProvider controllerServiceProvider,
            ParameterLookup parameterLookup
    ) {
        this.flowAnalysisRuleNode = flowAnalysisRule;
        this.properties = Collections.unmodifiableMap(properties);
        this.serviceProvider = controllerServiceProvider;
        this.preparedQueries = new HashMap<>();
        this.parameterLookup = parameterLookup;

        for (final Map.Entry<PropertyDescriptor, String> entry : properties.entrySet()) {
            final PropertyDescriptor desc = entry.getKey();
            String value = entry.getValue();
            if (value == null) {
                value = desc.getDefaultValue();
            }

            final PreparedQuery pq = Query.prepare(value);
            preparedQueries.put(desc, pq);
        }
    }

    protected FlowAnalysisRule getFlowAnalysisRule() {
        return flowAnalysisRuleNode.getFlowAnalysisRule();
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return properties;
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
    public PropertyValue getProperty(final PropertyDescriptor property) {
        final PropertyDescriptor descriptor = flowAnalysisRuleNode.getPropertyDescriptor(property.getName());
        if (descriptor == null) {
            return null;
        }

        final String configuredValue = properties.get(property);
        final ResourceContext resourceContext = new StandardResourceContext(new StandardResourceReferenceFactory(), descriptor);
        return new StandardPropertyValue(resourceContext,
                configuredValue == null
                ? descriptor.getDefaultValue() : configuredValue, serviceProvider, parameterLookup, preparedQueries.get(property));
    }
}
