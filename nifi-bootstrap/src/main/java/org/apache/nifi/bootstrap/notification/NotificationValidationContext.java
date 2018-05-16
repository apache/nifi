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
package org.apache.nifi.bootstrap.notification;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.Query.Range;
import org.apache.nifi.attribute.expression.language.StandardExpressionLanguageCompiler;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.expression.ExpressionLanguageCompiler;
import org.apache.nifi.registry.VariableRegistry;

public class NotificationValidationContext implements ValidationContext {
    private final NotificationContext context;
    private final Map<String, Boolean> expressionLanguageSupported;
    private final VariableRegistry variableRegistry;

    public NotificationValidationContext(final NotificationContext processContext, VariableRegistry variableRegistry) {
        this.context = processContext;

        final Map<PropertyDescriptor, String> properties = processContext.getProperties();
        expressionLanguageSupported = new HashMap<>(properties.size());
        for (final PropertyDescriptor descriptor : properties.keySet()) {
            expressionLanguageSupported.put(descriptor.getName(), descriptor.isExpressionLanguageSupported());
        }
        this.variableRegistry = variableRegistry;
    }


    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new StandardPropertyValue(rawValue, null, variableRegistry);
    }

    @Override
    public ExpressionLanguageCompiler newExpressionLanguageCompiler() {

        return new StandardExpressionLanguageCompiler(null);
    }

    @Override
    public ValidationContext getControllerServiceValidationContext(final ControllerService controllerService) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor property) {
        return context.getProperty(property);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return context.getProperties();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return null;
    }

    @Override
    public boolean isValidationRequired(final ControllerService service) {
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
        return null;
    }
}
