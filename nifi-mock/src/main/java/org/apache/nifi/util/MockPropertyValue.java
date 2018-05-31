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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.Query.Range;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableRegistry;

public class MockPropertyValue implements PropertyValue {
    private final String rawValue;
    private final Boolean expectExpressions;
    private final ExpressionLanguageScope expressionLanguageScope;
    private final MockControllerServiceLookup serviceLookup;
    private final PropertyDescriptor propertyDescriptor;
    private final PropertyValue stdPropValue;
    private final VariableRegistry variableRegistry;

    private boolean expressionsEvaluated = false;

    public MockPropertyValue(final String rawValue) {
        this(rawValue, null);
    }

    public MockPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup) {
        this(rawValue, serviceLookup, VariableRegistry.EMPTY_REGISTRY, null);
    }

    public MockPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup, final VariableRegistry variableRegistry) {
        this(rawValue, serviceLookup, variableRegistry, null);
    }

    public MockPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup, VariableRegistry variableRegistry, final PropertyDescriptor propertyDescriptor) {
        this(rawValue, serviceLookup, propertyDescriptor, false, variableRegistry);
    }

    private MockPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup, final PropertyDescriptor propertyDescriptor, final boolean alreadyEvaluated,
            final VariableRegistry variableRegistry) {
        this.stdPropValue = new StandardPropertyValue(rawValue, serviceLookup, variableRegistry);
        this.rawValue = rawValue;
        this.serviceLookup = (MockControllerServiceLookup) serviceLookup;
        this.expectExpressions = propertyDescriptor == null ? null : propertyDescriptor.isExpressionLanguageSupported();
        this.expressionLanguageScope = propertyDescriptor == null ? null : propertyDescriptor.getExpressionLanguageScope();
        this.propertyDescriptor = propertyDescriptor;
        this.expressionsEvaluated = alreadyEvaluated;
        this.variableRegistry = variableRegistry;
    }


    private void ensureExpressionsEvaluated() {
        if (Boolean.TRUE.equals(expectExpressions) && !expressionsEvaluated) {
            throw new IllegalStateException("Attempting to retrieve value of " + propertyDescriptor
                    + " without first evaluating Expressions, even though the PropertyDescriptor indicates "
                    + "that the Expression Language is Supported. If you realize that this is the case and do not want "
                    + "this error to occur, it can be disabled by calling TestRunner.setValidateExpressionUsage(false)");
        }
    }

    private void validateExpressionScope(boolean attributesAvailable) {
        // language scope is not null, we have attributes available but scope is not equal to FF attributes
        // it means that we're not evaluating against flow file attributes even though attributes are available
        if(expressionLanguageScope != null
                && (attributesAvailable && !ExpressionLanguageScope.FLOWFILE_ATTRIBUTES.equals(expressionLanguageScope))) {
            throw new IllegalStateException("Attempting to evaluate expression language for " + propertyDescriptor.getName()
                    + " using flow file attributes but the scope evaluation is set to " + expressionLanguageScope + ". The"
                    + " proper scope should be set in the property descriptor using"
                    + " PropertyDescriptor.Builder.expressionLanguageSupported(ExpressionLanguageScope)");
        }

        // if the service lookup is an instance of the validation context, we're in the validate() method
        // at this point we don't have any flow file available and we should not care about the scope
        // even though it is defined as FLOWFILE_ATTRIBUTES
        if(expressionLanguageScope != null
                && ExpressionLanguageScope.FLOWFILE_ATTRIBUTES.equals(expressionLanguageScope)
                && this.serviceLookup instanceof MockValidationContext) {
            return;
        }

        // we check if the input requirement is INPUT_FORBIDDEN
        // in that case, we don't care if attributes are not available even though scope is FLOWFILE_ATTRIBUTES
        // it likely means that the property has been defined in a common/abstract class used by multiple processors with
        // different input requirements.
        if(expressionLanguageScope != null
                && ExpressionLanguageScope.FLOWFILE_ATTRIBUTES.equals(expressionLanguageScope)
                && (this.serviceLookup.getInputRequirement() == null
                    || this.serviceLookup.getInputRequirement().value().equals(InputRequirement.Requirement.INPUT_FORBIDDEN))) {
            return;
        }

        // if we have a processor where input requirement is INPUT_ALLOWED, we need to check if there is an
        // incoming connection or not. If not, we don't care if attributes are not available even though scope is FLOWFILE_ATTRIBUTES
        if(expressionLanguageScope != null
                && ExpressionLanguageScope.FLOWFILE_ATTRIBUTES.equals(expressionLanguageScope)
                && !((MockProcessContext) this.serviceLookup).hasIncomingConnection()) {
            return;
        }

        // we're trying to evaluate against flow files attributes but we don't have any attributes available.
        if(expressionLanguageScope != null
                && (!attributesAvailable && ExpressionLanguageScope.FLOWFILE_ATTRIBUTES.equals(expressionLanguageScope))) {
            throw new IllegalStateException("Attempting to evaluate expression language for " + propertyDescriptor.getName()
                    + " without using flow file attributes but the scope evaluation is set to " + expressionLanguageScope + ". The"
                    + " proper scope should be set in the property descriptor using"
                    + " PropertyDescriptor.Builder.expressionLanguageSupported(ExpressionLanguageScope)");
        }
    }

    @Override
    public String getValue() {
        ensureExpressionsEvaluated();
        return stdPropValue.getValue();
    }

    @Override
    public Integer asInteger() {
        ensureExpressionsEvaluated();
        return stdPropValue.asInteger();
    }

    @Override
    public Long asLong() {
        ensureExpressionsEvaluated();
        return stdPropValue.asLong();
    }

    @Override
    public Boolean asBoolean() {
        ensureExpressionsEvaluated();
        return stdPropValue.asBoolean();
    }

    @Override
    public Float asFloat() {
        ensureExpressionsEvaluated();
        return stdPropValue.asFloat();
    }

    @Override
    public Double asDouble() {
        ensureExpressionsEvaluated();
        return stdPropValue.asDouble();
    }

    @Override
    public Long asTimePeriod(final TimeUnit timeUnit) {
        ensureExpressionsEvaluated();
        return stdPropValue.asTimePeriod(timeUnit);
    }

    @Override
    public Double asDataSize(final DataUnit dataUnit) {
        ensureExpressionsEvaluated();
        return stdPropValue.asDataSize(dataUnit);
    }

    private void markEvaluated() {
        if (Boolean.FALSE.equals(expectExpressions)) {
            throw new IllegalStateException("Attempting to Evaluate Expressions but " + propertyDescriptor
                    + " indicates that the Expression Language is not supported. If you realize that this is the case and do not want "
                    + "this error to occur, it can be disabled by calling TestRunner.setValidateExpressionUsage(false)");
        }
        expressionsEvaluated = true;
    }

    @Override
    public PropertyValue evaluateAttributeExpressions() throws ProcessException {
        return evaluateAttributeExpressions(null, null, null);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final AttributeValueDecorator decorator) throws ProcessException {
        return evaluateAttributeExpressions(null, null, decorator);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile) throws ProcessException {
        /*
         * The reason for this null check is that somewhere in the test API, it automatically assumes that a null FlowFile
         * should be treated as though it were evaluated with the VARIABLE_REGISTRY scope instead of the flowfile scope. When NiFi
         * is running, it doesn't care when it's evaluating EL against a null flowfile. However, the testing framework currently
         * raises an error which makes it not mimick real world behavior.
         */
        if (flowFile == null && expressionLanguageScope == ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) {
            return evaluateAttributeExpressions(new HashMap<>());
        } else if (flowFile == null && expressionLanguageScope == ExpressionLanguageScope.VARIABLE_REGISTRY) {
            return evaluateAttributeExpressions(); //Added this to get around a similar edge case where the a null flowfile is passed
                                                    //and the scope is to the registry
        }

        return evaluateAttributeExpressions(flowFile, null, null);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile, final AttributeValueDecorator decorator) throws ProcessException {
        return evaluateAttributeExpressions(flowFile, null, decorator);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile, final Map<String, String> additionalAttributes) throws ProcessException {
        return evaluateAttributeExpressions(flowFile, additionalAttributes, null);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final Map<String, String> attributes) throws ProcessException {
        return evaluateAttributeExpressions(null, attributes, null);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final Map<String, String> attributes, final AttributeValueDecorator decorator) throws ProcessException {
        return evaluateAttributeExpressions(null, attributes, decorator);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile, final Map<String, String> additionalAttributes, final AttributeValueDecorator decorator) throws ProcessException {
        return evaluateAttributeExpressions(flowFile, additionalAttributes, decorator, null);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(FlowFile flowFile, Map<String, String> additionalAttributes, AttributeValueDecorator decorator, Map<String, String> stateValues)
            throws ProcessException {
        markEvaluated();
        if (rawValue == null) {
            return this;
        }

        validateExpressionScope(flowFile != null || additionalAttributes != null);

        final PropertyValue newValue = stdPropValue.evaluateAttributeExpressions(flowFile, additionalAttributes, decorator, stateValues);
        return new MockPropertyValue(newValue.getValue(), serviceLookup, propertyDescriptor, true, variableRegistry);
    }

    @Override
    public ControllerService asControllerService() {
        ensureExpressionsEvaluated();
        if (rawValue == null || rawValue.equals("")) {
            return null;
        }

        return serviceLookup.getControllerService(rawValue);
    }

    @Override
    public <T extends ControllerService> T asControllerService(final Class<T> serviceType) throws IllegalArgumentException {
        ensureExpressionsEvaluated();
        if (rawValue == null || rawValue.equals("")) {
            return null;
        }

        final ControllerService service = serviceLookup.getControllerService(rawValue);
        if (serviceType.isAssignableFrom(service.getClass())) {
            return serviceType.cast(service);
        }
        throw new IllegalArgumentException("Controller Service with identifier " + rawValue + " is of type " + service.getClass() + " and cannot be cast to " + serviceType);
    }

    @Override
    public boolean isSet() {
        return rawValue != null;
    }

    @Override
    public String toString() {
        return getValue();
    }

    @Override
    public boolean isExpressionLanguagePresent() {
        if (!Boolean.TRUE.equals(expectExpressions)) {
            return false;
        }

        final List<Range> elRanges = Query.extractExpressionRanges(rawValue);
        return (elRanges != null && !elRanges.isEmpty());
    }
}
