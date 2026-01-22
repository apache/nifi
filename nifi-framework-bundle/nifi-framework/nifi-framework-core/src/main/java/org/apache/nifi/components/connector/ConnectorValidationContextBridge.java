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

package org.apache.nifi.components.connector;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.documentation.init.EmptyControllerServiceLookup;
import org.apache.nifi.expression.ExpressionLanguageCompiler;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterLookup;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ConnectorValidationContextBridge implements ValidationContext {
    private final Map<String, String> rawValues;
    private final ParameterLookup parameterLookup;

    public ConnectorValidationContextBridge(final Map<String, String> rawValues, final ParameterLookup parameterLookup) {
        this.rawValues = rawValues;
        this.parameterLookup = parameterLookup;
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return new EmptyControllerServiceLookup();
    }

    @Override
    public ValidationContext getControllerServiceValidationContext(final ControllerService controllerService) {
        return null;
    }

    @Override
    public ExpressionLanguageCompiler newExpressionLanguageCompiler() {
        return null;
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new StandardPropertyValue(rawValue, getControllerServiceLookup(), parameterLookup);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final Map.Entry<String, String> entry : rawValues.entrySet()) {
            final PropertyDescriptor descriptor = new PropertyDescriptor.Builder()
                .name(entry.getKey())
                .build();
            properties.put(descriptor, entry.getValue());
        }

        return properties;
    }

    @Override
    public String getAnnotationData() {
        return null;
    }

    @Override
    public boolean isValidationRequired(final ControllerService controllerService) {
        return false;
    }

    @Override
    public boolean isExpressionLanguagePresent(final String value) {
        return false;
    }

    @Override
    public boolean isExpressionLanguageSupported(final String value) {
        return false;
    }

    @Override
    public String getProcessGroupIdentifier() {
        return null;
    }

    @Override
    public Collection<String> getReferencedParameters(final String propertyName) {
        return List.of();
    }

    @Override
    public boolean isParameterDefined(final String parameterName) {
        return parameterLookup.getParameter(parameterName).isPresent();
    }

    @Override
    public boolean isParameterSet(final String parameterName) {
        final Optional<Parameter> optionalParameter = parameterLookup.getParameter(parameterName);
        if (optionalParameter.isEmpty()) {
            return false;
        }

        return optionalParameter.get().getValue() != null;
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor propertyDescriptor) {
        final String rawValue = rawValues.get(propertyDescriptor.getName());
        final String effectiveValue = rawValue == null ? propertyDescriptor.getDefaultValue() : rawValue;
        return newPropertyValue(effectiveValue);
    }

    @Override
    public Map<String, String> getAllProperties() {
        return Collections.unmodifiableMap(rawValues);
    }
}
