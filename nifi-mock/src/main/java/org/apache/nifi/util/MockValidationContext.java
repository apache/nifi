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
package org.apache.nifi.util;

import org.apache.nifi.components.validation.AbstractValidationContext;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.Query.Range;
import org.apache.nifi.attribute.expression.language.StandardExpressionLanguageCompiler;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.expression.ExpressionLanguageCompiler;
import org.apache.nifi.parameter.ExpressionLanguageAgnosticParameterParser;
import org.apache.nifi.parameter.ExpressionLanguageAwareParameterParser;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.parameter.ParameterParser;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.parameter.ParameterTokenList;
import org.apache.nifi.parameter.StandardParameterTokenList;
import org.apache.nifi.registry.VariableRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MockValidationContext extends MockControllerServiceLookup implements ValidationContext, ControllerServiceLookup {

    private final MockProcessContext context;
    private final Map<String, Boolean> expressionLanguageSupported;
    private final StateManager stateManager;
    private final VariableRegistry variableRegistry;
    private final Map<PropertyDescriptor, PropertyConfiguration> properties;
    private volatile boolean validateExpressions = true;

    public MockValidationContext(final MockProcessContext processContext) {
        this(processContext, null, VariableRegistry.EMPTY_REGISTRY);
    }

    public MockValidationContext(final MockProcessContext processContext, final StateManager stateManager, final VariableRegistry variableRegistry) {
        this.context = processContext;
        this.stateManager = stateManager;
        this.variableRegistry = variableRegistry;

        final Map<PropertyDescriptor, String> properties = processContext.getProperties();
        expressionLanguageSupported = new HashMap<>(properties.size());
        for (final PropertyDescriptor descriptor : properties.keySet()) {
            expressionLanguageSupported.put(descriptor.getName(), descriptor.isExpressionLanguageSupported());
        }

        this.properties = createPropertyConfigurationMap(processContext);
    }

    private static Map<PropertyDescriptor, PropertyConfiguration> createPropertyConfigurationMap(final MockProcessContext processContext) {
        final Map<PropertyDescriptor, PropertyConfiguration> configurationMap = new HashMap<>();

        for (final Map.Entry<String, String> entry : processContext.getAllProperties().entrySet()) {
            final PropertyDescriptor descriptor = processContext.getPropertyDescriptor(entry.getKey());
            final ParameterTokenList tokenList = new StandardParameterTokenList(entry.getValue(), Collections.emptyList());
            final List<ParameterReference> parameterReferences = Collections.emptyList();
            final PropertyConfiguration configuration = new PropertyConfiguration(entry.getValue(), tokenList, parameterReferences);
            configurationMap.put(descriptor, configuration);
        }

        return configurationMap;
    }

    public void setValidateExpressions(final boolean validate) {
        this.validateExpressions = validate;
    }

    @Override
    public ControllerService getControllerService(final String identifier) {
        return context.getControllerService(identifier);
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new MockPropertyValue(rawValue, this, null, true, variableRegistry);
    }

    @Override
    public ExpressionLanguageCompiler newExpressionLanguageCompiler() {
        return new StandardExpressionLanguageCompiler(variableRegistry, ParameterLookup.EMPTY);
    }

    @Override
    public ValidationContext getControllerServiceValidationContext(final ControllerService controllerService) {
        final MockProcessContext serviceProcessContext = new MockProcessContext(controllerService, context, stateManager, variableRegistry);
        final MockValidationContext serviceValidationContext =  new MockValidationContext(serviceProcessContext, stateManager, variableRegistry);
        serviceValidationContext.setValidateExpressions(validateExpressions);
        return serviceValidationContext;
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor property) {
        return context.getPropertyWithoutValidatingExpressions(property);
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
        return context.getAnnotationData();
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) {
        return context.getControllerServiceIdentifiers(serviceType);
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return this;
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        return context.isControllerServiceEnabled(serviceIdentifier);
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        return context.isControllerServiceEnabled(service);
    }

    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
        final ControllerServiceConfiguration configuration = context.getConfiguration(serviceIdentifier);
        return configuration == null ? null : serviceIdentifier;
    }

    @Override
    public boolean isValidationRequired(final ControllerService service) {
        return true;
    }

    @Override
    public boolean isControllerServiceEnabling(final String serviceIdentifier) {
        return context.isControllerServiceEnabling(serviceIdentifier);
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
        return "unit test";
    }

    @Override
    public Collection<String> getReferencedParameters(final String propertyName) {
        final String rawPropertyValue = context.getProperty(propertyName).getValue();
        final boolean elSupported = isExpressionLanguageSupported(propertyName);

        final ParameterParser parser = elSupported ? new ExpressionLanguageAwareParameterParser() : new ExpressionLanguageAgnosticParameterParser();

        final List<ParameterReference> references = parser.parseTokens(rawPropertyValue).toReferenceList();
        return references.stream()
            .map(ParameterReference::getParameterName)
            .collect(Collectors.toList());
    }

    @Override
    public boolean isParameterDefined(final String parameterName) {
        return true;
    }

    @Override
    public boolean isParameterSet(final String parameterName) {
        return true;
    }

    @Override
    public boolean isDependencySatisfied(final PropertyDescriptor propertyDescriptor, final Function<String, PropertyDescriptor> propertyDescriptorLookup) {
        // The logic for isDependencySatisfied is already implemented in the AbstractValidationContext. We don't want to duplicate the logic here.
        // However, this class cannot extends AbstractValidationContext because it was already created to extend MockControllerServiceLookup. Changing that
        // hierarchy gets complicated. So, for the time being we will create a concrete implementation of the AbstractValidationContext in order to call the
        // isDependencySatisfied method.
        final ValidationContext abstractValidationContext = new AbstractValidationContext(ParameterLookup.EMPTY, properties) {
            @Override
            public ControllerServiceLookup getControllerServiceLookup() {
                return null;
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
            public PropertyValue newPropertyValue(final String value) {
                return null;
            }

            @Override
            public Map<PropertyDescriptor, String> getProperties() {
                return null;
            }

            @Override
            public String getAnnotationData() {
                return null;
            }

            @Override
            public boolean isValidationRequired(final ControllerService service) {
                return false;
            }

            @Override
            public boolean isExpressionLanguagePresent(final String value) {
                return false;
            }

            @Override
            public boolean isExpressionLanguageSupported(final String propertyName) {
                return false;
            }

            @Override
            public String getProcessGroupIdentifier() {
                return null;
            }

            @Override
            public Collection<String> getReferencedParameters(final String propertyName) {
                return null;
            }

            @Override
            public boolean isParameterDefined(final String parameterName) {
                return false;
            }

            @Override
            public boolean isParameterSet(final String parameterName) {
                return false;
            }

            @Override
            public PropertyValue getProperty(final PropertyDescriptor descriptor) {
                return null;
            }

            @Override
            public Map<String, String> getAllProperties() {
                return null;
            }
        };

        return abstractValidationContext.isDependencySatisfied(propertyDescriptor, propertyDescriptorLookup);
    }
}
