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
package org.apache.nifi.script.impl;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.expression.ExpressionLanguageCompiler;

import java.util.Collection;
import java.util.Map;

public abstract class ValidationContextAdapter implements ValidationContext {

    private ValidationContext innerValidationContext;

    public ValidationContextAdapter(ValidationContext innerValidationContext) {
        assert innerValidationContext != null;
        this.innerValidationContext = innerValidationContext;
    }

    @Override
    public PropertyValue getProperty(PropertyDescriptor descriptor) {
        return innerValidationContext.getProperty(descriptor);
    }

    @Override
    public Map<String, String> getAllProperties() {
        return innerValidationContext.getAllProperties();
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return innerValidationContext.getControllerServiceLookup();
    }

    @Override
    public ValidationContext getControllerServiceValidationContext(ControllerService controllerService) {
        return innerValidationContext.getControllerServiceValidationContext(controllerService);
    }

    @Override
    public ExpressionLanguageCompiler newExpressionLanguageCompiler() {
        return innerValidationContext.newExpressionLanguageCompiler();
    }

    @Override
    public PropertyValue newPropertyValue(String value) {
        return innerValidationContext.newPropertyValue(value);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return innerValidationContext.getProperties();
    }

    @Override
    public String getAnnotationData() {
        return innerValidationContext.getAnnotationData();
    }

    @Override
    public boolean isValidationRequired(ControllerService service) {
        return innerValidationContext.isValidationRequired(service);
    }

    @Override
    public boolean isExpressionLanguagePresent(String value) {
        return innerValidationContext.isExpressionLanguagePresent(value);
    }

    @Override
    public boolean isExpressionLanguageSupported(String propertyName) {
        return innerValidationContext.isExpressionLanguageSupported(propertyName);
    }

    @Override
    public String getProcessGroupIdentifier() {
        return innerValidationContext.getProcessGroupIdentifier();
    }

    @Override
    public boolean isParameterDefined(final String parameterName) {
        return innerValidationContext.isParameterDefined(parameterName);
    }

    @Override
    public Collection<String> getReferencedParameters(final String propertyName) {
        return innerValidationContext.getReferencedParameters(propertyName);
    }
}
