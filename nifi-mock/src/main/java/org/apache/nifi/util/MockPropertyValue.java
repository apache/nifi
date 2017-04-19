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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.attribute.expression.language.Query.Range;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableRegistry;

public class MockPropertyValue implements PropertyValue {
    private final String rawValue;
    private final Boolean expectExpressions;
    private final ControllerServiceLookup serviceLookup;
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
        this.serviceLookup = serviceLookup;
        this.expectExpressions = propertyDescriptor == null ? null : propertyDescriptor.isExpressionLanguageSupported();
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
        if (!expectExpressions) {
            return false;
        }

        final List<Range> elRanges = Query.extractExpressionRanges(rawValue);
        return (elRanges != null && !elRanges.isEmpty());
    }
}
