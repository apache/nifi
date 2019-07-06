
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
package org.apache.nifi.processor;

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.Query.Range;
import org.apache.nifi.attribute.expression.language.StandardExpressionLanguageCompiler;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.expression.ExpressionLanguageCompiler;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.registry.VariableRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class StandardValidationContext implements ValidationContext {

    private final ControllerServiceProvider controllerServiceProvider;
    private final Map<PropertyDescriptor, PropertyConfiguration> properties;
    private final Map<PropertyDescriptor, PreparedQuery> preparedQueries;
    private final Map<String, Boolean> expressionLanguageSupported;
    private final String annotationData;
    private final VariableRegistry variableRegistry;
    private final String groupId;
    private final String componentId;
    private final ParameterContext parameterContext;
    private final AtomicReference<Map<PropertyDescriptor, String>> effectiveValuesRef = new AtomicReference<>();


    public StandardValidationContext(
            final ControllerServiceProvider controllerServiceProvider,
            final Map<PropertyDescriptor, PropertyConfiguration> properties,
            final String annotationData,
            final String groupId,
            final String componentId,
            final VariableRegistry variableRegistry,
            final ParameterContext parameterContext) {
        this.controllerServiceProvider = controllerServiceProvider;
        this.properties = new HashMap<>(properties);
        this.annotationData = annotationData;
        this.variableRegistry = variableRegistry;
        this.groupId = groupId;
        this.componentId = componentId;
        this.parameterContext = parameterContext;

        preparedQueries = new HashMap<>(properties.size());
        for (final Map.Entry<PropertyDescriptor, PropertyConfiguration> entry : properties.entrySet()) {
            final PropertyDescriptor desc = entry.getKey();
            final PropertyConfiguration configuration = entry.getValue();

            String value = (configuration == null) ? null : configuration.getEffectiveValue(parameterContext);
            if (value == null) {
                value = desc.getDefaultValue();
            }

            final PreparedQuery pq = Query.prepare(value);
            preparedQueries.put(desc, pq);
        }

        expressionLanguageSupported = new HashMap<>(properties.size());
        for (final PropertyDescriptor descriptor : properties.keySet()) {
            expressionLanguageSupported.put(descriptor.getName(), descriptor.isExpressionLanguageSupported());
        }
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new StandardPropertyValue(rawValue, controllerServiceProvider, parameterContext, Query.prepare(rawValue), variableRegistry);
    }

    @Override
    public ExpressionLanguageCompiler newExpressionLanguageCompiler() {
        return new StandardExpressionLanguageCompiler(variableRegistry, parameterContext);
    }

    @Override
    public ValidationContext getControllerServiceValidationContext(final ControllerService controllerService) {
        final ControllerServiceNode serviceNode = controllerServiceProvider.getControllerServiceNode(controllerService.getIdentifier());
        final ProcessGroup serviceGroup = serviceNode.getProcessGroup();
        final String serviceGroupId = serviceGroup == null ? null : serviceGroup.getIdentifier();
        return new StandardValidationContext(controllerServiceProvider, serviceNode.getProperties(), serviceNode.getAnnotationData(), serviceGroupId,
            serviceNode.getIdentifier(), variableRegistry, serviceNode.getProcessGroup().getParameterContext());
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor property) {
        final PropertyConfiguration configuredValue = properties.get(property);
        final String effectiveValue = configuredValue == null ? property.getDefaultValue() : configuredValue.getEffectiveValue(parameterContext);
        return new StandardPropertyValue(effectiveValue, controllerServiceProvider, parameterContext, preparedQueries.get(property), variableRegistry);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        final Map<PropertyDescriptor, String> effectiveValues = effectiveValuesRef.get();
        if (effectiveValues != null) {
            return effectiveValues;
        }

        final Map<PropertyDescriptor, String> valueMap = new LinkedHashMap<>();
        for (final Map.Entry<PropertyDescriptor, PropertyConfiguration> entry : this.properties.entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            final PropertyConfiguration configuration = entry.getValue();
            final String value = configuration == null ? descriptor.getDefaultValue() : configuration.getEffectiveValue(parameterContext);

            valueMap.put(entry.getKey(), value);
        }

        final Map<PropertyDescriptor, String> immutableValueMap = Collections.unmodifiableMap(valueMap);
        effectiveValuesRef.compareAndSet(null, immutableValueMap);
        return immutableValueMap;
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
    public String getAnnotationData() {
        return annotationData;
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return new ComponentSpecificControllerServiceLookup(controllerServiceProvider, componentId, groupId);
    }

    @Override
    public boolean isValidationRequired(final ControllerService service) {
        // No need to validate services that are already enabled.
        final ControllerServiceState serviceState = controllerServiceProvider.getControllerServiceNode(service.getIdentifier()).getState();
        if (serviceState == ControllerServiceState.ENABLED || serviceState == ControllerServiceState.ENABLING) {
            return false;
        }

        return true;
    }

    @Override
    public boolean isExpressionLanguagePresent(final String value) {
        if (value == null) {
            return false;
        }

        final List<Range> elRanges = Query.extractExpressionRanges(value);
        return (elRanges != null && !elRanges.isEmpty());
    }

    @Override
    public boolean isExpressionLanguageSupported(final String propertyName) {
        final Boolean supported = expressionLanguageSupported.get(propertyName);
        return Boolean.TRUE.equals(supported);
    }

    @Override
    public String getProcessGroupIdentifier() {
        return groupId;
    }

    @Override
    public Collection<String> getReferencedParameters(final String propertyName) {
        final PropertyDescriptor descriptor = new PropertyDescriptor.Builder().name(propertyName).build();
        final PropertyConfiguration configuration = this.properties.get(descriptor);
        if (configuration == null) {
            return Collections.emptyList();
        }

        final List<ParameterReference> references = configuration.getParameterReferences();
        final List<String> parameterNames = references.stream().map(ParameterReference::getParameterName).collect(Collectors.toList());
        return parameterNames;
    }

    @Override
    public boolean isParameterDefined(final String parameterName) {
        if (parameterContext == null) {
            return false;
        }

        return parameterContext.getParameter(parameterName).isPresent();
    }

    @Override
    public String toString() {
        return "StandardValidationContext[componentId=" + componentId + ", properties=" + properties + "]";
    }
}
