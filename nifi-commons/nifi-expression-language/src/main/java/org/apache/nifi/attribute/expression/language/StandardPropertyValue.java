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
package org.apache.nifi.attribute.expression.language;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.util.FormatUtils;

public class StandardPropertyValue implements PropertyValue {

    private final String rawValue;
    private final ControllerServiceLookup serviceLookup;
    private final PreparedQuery preparedQuery;
    private final VariableRegistry variableRegistry;

    public StandardPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup) {
        this(rawValue, serviceLookup, Query.prepare(rawValue), VariableRegistry.EMPTY_REGISTRY);
    }

    public StandardPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup, final VariableRegistry variableRegistry) {
        this(rawValue, serviceLookup, Query.prepare(rawValue), variableRegistry);
    }

    /**
     * Constructs a new StandardPropertyValue with the given value & service
     * lookup and indicates whether or not the rawValue contains any NiFi
     * Expressions. If it is unknown whether or not the value contains any NiFi
     * Expressions, the
     * {@link #StandardPropertyValue(String, ControllerServiceLookup, VariableRegistry)}
     * constructor should be used or <code>true</code> should be passed.
     * However, if it is known that the value contains no NiFi Expression, that
     * information should be provided so that calls to
     * {@link #evaluateAttributeExpressions()} are much more efficient
     *
     * @param rawValue value
     * @param serviceLookup lookup
     * @param preparedQuery query
     * @param variableRegistry variableRegistry
     */
    public StandardPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup, final PreparedQuery preparedQuery,
            final VariableRegistry variableRegistry) {
        this.rawValue = rawValue;
        this.serviceLookup = serviceLookup;
        this.preparedQuery = preparedQuery;
        this.variableRegistry = variableRegistry;
    }

    @Override
    public String getValue() {
        return rawValue;
    }

    @Override
    public Integer asInteger() {
        return (rawValue == null) ? null : Integer.parseInt(rawValue.trim());
    }

    @Override
    public Long asLong() {
        return (rawValue == null) ? null : Long.parseLong(rawValue.trim());
    }

    @Override
    public Boolean asBoolean() {
        return (rawValue == null) ? null : Boolean.parseBoolean(rawValue.trim());
    }

    @Override
    public Float asFloat() {
        return (rawValue == null) ? null : Float.parseFloat(rawValue.trim());
    }

    @Override
    public Double asDouble() {
        return (rawValue == null) ? null : Double.parseDouble(rawValue.trim());
    }

    @Override
    public Long asTimePeriod(final TimeUnit timeUnit) {
        return (rawValue == null) ? null : FormatUtils.getTimeDuration(rawValue.trim(), timeUnit);
    }

    @Override
    public Double asDataSize(final DataUnit dataUnit) {
        return rawValue == null ? null : DataUnit.parseDataSize(rawValue.trim(), dataUnit);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions() throws ProcessException {
        return evaluateAttributeExpressions(null, null, null);
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
    public PropertyValue evaluateAttributeExpressions(final AttributeValueDecorator decorator) throws ProcessException {
        return evaluateAttributeExpressions(null, null, decorator);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile) throws ProcessException {
        return evaluateAttributeExpressions(flowFile, null, null);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile, final Map<String, String> additionalAttributes) throws ProcessException {
        return evaluateAttributeExpressions(flowFile, additionalAttributes, null);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile, final AttributeValueDecorator decorator) throws ProcessException {
        return evaluateAttributeExpressions(flowFile, null, decorator);
    }

    @Override
    public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile, final Map<String, String> additionalAttributes, final AttributeValueDecorator decorator) throws ProcessException {
        return evaluateAttributeExpressions(flowFile, additionalAttributes, decorator, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public PropertyValue evaluateAttributeExpressions(FlowFile flowFile, Map<String, String> additionalAttributes, AttributeValueDecorator decorator, Map<String, String> stateValues)
            throws ProcessException {
        if (rawValue == null || preparedQuery == null) {
            return this;
        }

        final ValueLookup lookup = new ValueLookup(variableRegistry, flowFile, additionalAttributes);
        final String evaluated = preparedQuery.evaluateExpressions(lookup, decorator, stateValues);
        return new StandardPropertyValue(evaluated, serviceLookup, new EmptyPreparedQuery(evaluated), null);
    }

    @Override
    public String toString() {
        return rawValue;
    }

    @Override
    public ControllerService asControllerService() {
        if (rawValue == null || rawValue.equals("") || serviceLookup == null) {
            return null;
        }

        return serviceLookup.getControllerService(rawValue);
    }

    @Override
    public <T extends ControllerService> T asControllerService(final Class<T> serviceType) throws IllegalArgumentException {
        if (!serviceType.isInterface()) {
            throw new IllegalArgumentException("ControllerServices may be referenced only via their interfaces; " + serviceType + " is not an interface");
        }
        if (rawValue == null || rawValue.equals("") || serviceLookup == null) {
            return null;
        }

        final ControllerService service = serviceLookup.getControllerService(rawValue);
        if (service == null) {
            return null;
        }
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
    public boolean isExpressionLanguagePresent() {
        return preparedQuery.isExpressionLanguagePresent();
    }
}
