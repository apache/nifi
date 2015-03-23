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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.Query.Range;
import org.apache.nifi.attribute.expression.language.StandardExpressionLanguageCompiler;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.expression.ExpressionLanguageCompiler;

public class StandardValidationContext implements ValidationContext {

    private final ControllerServiceProvider controllerServiceProvider;
    private final Map<PropertyDescriptor, String> properties;
    private final Map<PropertyDescriptor, PreparedQuery> preparedQueries;
    private final Map<String, Boolean> expressionLanguageSupported;
    private final String annotationData;

    public StandardValidationContext(final ControllerServiceProvider controllerServiceProvider, final Map<PropertyDescriptor, String> properties, final String annotationData) {
        this.controllerServiceProvider = controllerServiceProvider;
        this.properties = new HashMap<>(properties);
        this.annotationData = annotationData;

        preparedQueries = new HashMap<>(properties.size());
        for (final Map.Entry<PropertyDescriptor, String> entry : properties.entrySet()) {
            final PropertyDescriptor desc = entry.getKey();
            String value = entry.getValue();
            if (value == null) {
                value = desc.getDefaultValue();
            }

            final PreparedQuery pq = Query.prepare(value);
            preparedQueries.put(desc, pq);
        }

        expressionLanguageSupported = new HashMap<>(properties.size());
        for ( final PropertyDescriptor descriptor : properties.keySet() ) {
            expressionLanguageSupported.put(descriptor.getName(), descriptor.isExpressionLanguageSupported());
        }
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new StandardPropertyValue(rawValue, controllerServiceProvider, Query.prepare(rawValue));
    }

    @Override
    public ExpressionLanguageCompiler newExpressionLanguageCompiler() {
        return new StandardExpressionLanguageCompiler();
    }

    @Override
    public ValidationContext getControllerServiceValidationContext(final ControllerService controllerService) {
        final ControllerServiceNode serviceNode = controllerServiceProvider.getControllerServiceNode(controllerService.getIdentifier());
        return new StandardValidationContext(controllerServiceProvider, serviceNode.getProperties(), serviceNode.getAnnotationData());
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor property) {
        final String configuredValue = properties.get(property);
        return new StandardPropertyValue(configuredValue == null ? property.getDefaultValue() : configuredValue, controllerServiceProvider, preparedQueries.get(property));
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    @Override
    public String getAnnotationData() {
        return annotationData;
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return controllerServiceProvider;
    }
    
    @Override
    public boolean isExpressionLanguagePresent(final String value) {
        if ( value == null ) {
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
}
