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

import java.util.concurrent.TimeUnit;

import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.exception.ProcessException;

public class MockPropertyValue implements PropertyValue {

    private final String rawValue;
    private final Boolean expectExpressions;
    private final ControllerServiceLookup serviceLookup;
    private final PropertyDescriptor propertyDescriptor;
    private boolean expressionsEvaluated = false;

    public MockPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup) {
        this(rawValue, serviceLookup, null);
    }

    public MockPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup, final PropertyDescriptor propertyDescriptor) {
        this.rawValue = rawValue;
        this.serviceLookup = serviceLookup;
        this.expectExpressions = propertyDescriptor == null ? null : propertyDescriptor.isExpressionLanguageSupported();
        this.propertyDescriptor = propertyDescriptor;
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
        return rawValue;
    }

    @Override
    public Integer asInteger() {
        ensureExpressionsEvaluated();
        return (rawValue == null) ? null : Integer.parseInt(rawValue.trim());
    }

    @Override
    public Long asLong() {
        ensureExpressionsEvaluated();
        return (rawValue == null) ? null : Long.parseLong(rawValue.trim());
    }

    @Override
    public Boolean asBoolean() {
        ensureExpressionsEvaluated();
        return (rawValue == null) ? null : Boolean.parseBoolean(rawValue.trim());
    }

    @Override
    public Float asFloat() {
        ensureExpressionsEvaluated();
        return (rawValue == null) ? null : Float.parseFloat(rawValue.trim());
    }

    @Override
    public Double asDouble() {
        ensureExpressionsEvaluated();
        return (rawValue == null) ? null : Double.parseDouble(rawValue.trim());
    }

    @Override
    public Long asTimePeriod(final TimeUnit timeUnit) {
        ensureExpressionsEvaluated();
        return (rawValue == null) ? null : FormatUtils.getTimeDuration(rawValue.trim(), timeUnit);
    }

    @Override
    public Double asDataSize(final DataUnit dataUnit) {
        ensureExpressionsEvaluated();
        return rawValue == null ? null : DataUnit.parseDataSize(rawValue.trim(), dataUnit);
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
        markEvaluated();
        if (rawValue == null) {
            return this;
        }
        return evaluateAttributeExpressions(null, null);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final AttributeValueDecorator decorator) throws ProcessException {
        markEvaluated();
        if (rawValue == null) {
            return this;
        }
        return evaluateAttributeExpressions(null, decorator);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile) throws ProcessException {
        markEvaluated();
        if (rawValue == null) {
            return this;
        }
        return evaluateAttributeExpressions(flowFile, null);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile, final AttributeValueDecorator decorator) throws ProcessException {
        markEvaluated();
        if (rawValue == null) {
            return this;
        }
        return new MockPropertyValue(Query.evaluateExpressions(rawValue, flowFile, decorator), serviceLookup);
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
}
