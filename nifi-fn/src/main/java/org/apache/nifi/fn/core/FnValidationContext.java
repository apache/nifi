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
package org.apache.nifi.fn.core;

import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.StandardExpressionLanguageCompiler;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.expression.ExpressionLanguageCompiler;
import org.apache.nifi.registry.VariableRegistry;

import java.util.*;

public class FnValidationContext implements ValidationContext {

    private final FnControllerServiceLookup lookup;
    private final Map<String, Boolean> expressionLanguageSupported;
    private final StateManager stateManager;
    private final VariableRegistry variableRegistry;
    private final FnProcessContext processContext;

    public FnValidationContext(final FnProcessContext processContext, final FnControllerServiceLookup lookup, final StateManager stateManager, final VariableRegistry variableRegistry) {
        this.processContext = processContext;
        this.lookup = lookup;
        this.stateManager = stateManager;
        this.variableRegistry = variableRegistry;

        final Map<PropertyDescriptor, String> properties = processContext.getProperties();
        expressionLanguageSupported = new HashMap<>(properties.size());
        for (final PropertyDescriptor descriptor : properties.keySet()) {
            expressionLanguageSupported.put(descriptor.getName(), descriptor.isExpressionLanguageSupported());
        }
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new FnPropertyValue(rawValue, this.lookup, variableRegistry);
    }

    @Override
    public ExpressionLanguageCompiler newExpressionLanguageCompiler() {
        return new StandardExpressionLanguageCompiler(variableRegistry);
    }

    @Override
    public ValidationContext getControllerServiceValidationContext(final ControllerService controllerService) {
        final FnProcessContext serviceProcessContext = new FnProcessContext(controllerService, lookup, null ,stateManager, variableRegistry);
        return new FnValidationContext(serviceProcessContext, lookup, stateManager, variableRegistry);
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor property) {
        return processContext.getProperty(property);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return processContext.getProperties();
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
        return processContext.getAnnotationData();
    }


    @Override
    public boolean isExpressionLanguagePresent(final String value) {
        if (value == null) {
            return false;
        }

        final List<Query.Range> elRanges = Query.extractExpressionRanges(value);
        return (elRanges != null && !elRanges.isEmpty());
    }

    @Override
    public boolean isExpressionLanguageSupported(final String propertyName) {
        final Boolean supported = expressionLanguageSupported.get(propertyName);
        return Boolean.TRUE.equals(supported);
    }

    @Override
    public String getProcessGroupIdentifier() {
        return "fn";
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return this.lookup;
    }


    @Override
    public boolean isValidationRequired(final ControllerService service) {
        return true;
    }

}
