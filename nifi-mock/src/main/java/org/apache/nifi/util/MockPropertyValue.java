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

import java.time.Duration;
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
import org.apache.nifi.components.resource.ResourceContext;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.components.resource.StandardResourceContext;
import org.apache.nifi.components.resource.StandardResourceReferenceFactory;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.exception.ProcessException;

public class MockPropertyValue implements PropertyValue {
    private final String rawValue;
    private final Boolean expectExpressions;
    private final ExpressionLanguageScope expressionLanguageScope;
    private final MockControllerServiceLookup serviceLookup;
    private final PropertyDescriptor propertyDescriptor;
    private final PropertyValue stdPropValue;

    // This is only for testing purposes as we don't want to set env/sys variables in the tests
    private Map<String, String> environmentVariables;

    private boolean expressionsEvaluated;

    public MockPropertyValue(final String rawValue) {
        this(rawValue, null, null);
    }

    public MockPropertyValue(final String rawValue, final Map<String, String> environmentVariables) {
        this(rawValue, null, environmentVariables);
    }

    public MockPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup, final Map<String, String> environmentVariables) {
        this(rawValue, serviceLookup, null, environmentVariables);
    }

    public MockPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup, final Map<String, String> environmentVariables, ParameterLookup parameterLookup) {
        this(rawValue, serviceLookup, null, false, environmentVariables, parameterLookup);
    }

    public MockPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup, final PropertyDescriptor propertyDescriptor,
            final Map<String, String> environmentVariables) {
        this(rawValue, serviceLookup, propertyDescriptor, false, environmentVariables);
    }

    protected MockPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup, final PropertyDescriptor propertyDescriptor,
                                final boolean alreadyEvaluated, final Map<String, String> environmentVariables) {
        this(rawValue, serviceLookup, propertyDescriptor, alreadyEvaluated, environmentVariables, ParameterLookup.EMPTY);
    }


    protected MockPropertyValue(final String rawValue, final ControllerServiceLookup serviceLookup, final PropertyDescriptor propertyDescriptor,
            final boolean alreadyEvaluated, final Map<String, String> environmentVariables, ParameterLookup parameterLookup) {
        final ResourceContext resourceContext = new StandardResourceContext(new StandardResourceReferenceFactory(), propertyDescriptor);
        this.stdPropValue = new StandardPropertyValue(resourceContext, rawValue, serviceLookup, parameterLookup);
        this.rawValue = rawValue;
        this.serviceLookup = (MockControllerServiceLookup) serviceLookup;
        this.expectExpressions = propertyDescriptor == null ? null : propertyDescriptor.isExpressionLanguageSupported();
        this.expressionLanguageScope = propertyDescriptor == null ? null : propertyDescriptor.getExpressionLanguageScope();
        this.propertyDescriptor = propertyDescriptor;
        this.expressionsEvaluated = alreadyEvaluated;
        this.environmentVariables = environmentVariables;
    }

    private void ensureExpressionsEvaluated() {
        if (Boolean.TRUE.equals(expectExpressions) && !expressionsEvaluated) {
            throw new IllegalStateException("Attempting to retrieve value of " + propertyDescriptor
                    + " without first evaluating Expressions, even though the PropertyDescriptor indicates "
                    + "that the Expression Language is Supported. If you realize that this is the case and do not want "
                    + "this error to occur, it can be disabled by calling TestRunner.setValidateExpressionUsage(false)");
        }
    }

    private void validateExpressionScope(boolean flowFileProvided, boolean additionalAttributesAvailable) {
        if (expressionLanguageScope == null) {
            return;
        }

        // language scope is not null, we have attributes available but scope is not equal to FF attributes
        // it means that we're not evaluating against flow file attributes even though attributes are available
        if (flowFileProvided && !ExpressionLanguageScope.FLOWFILE_ATTRIBUTES.equals(expressionLanguageScope)) {
            throw new IllegalStateException("Attempting to evaluate expression language for " + propertyDescriptor.getName()
                    + " using flow file attributes but the scope evaluation is set to " + expressionLanguageScope + ". The"
                    + " proper scope should be set in the property descriptor using"
                    + " PropertyDescriptor.Builder.expressionLanguageSupported(ExpressionLanguageScope)");
        }


        // we check if the input requirement is INPUT_FORBIDDEN
        // in that case, we don't care if attributes are not available even though scope is FLOWFILE_ATTRIBUTES
        // it likely means that the property has been defined in a common/abstract class used by multiple processors with
        // different input requirements.
        if (ExpressionLanguageScope.FLOWFILE_ATTRIBUTES.equals(expressionLanguageScope)
                && (this.serviceLookup.getInputRequirement() == null || this.serviceLookup.getInputRequirement().value().equals(InputRequirement.Requirement.INPUT_FORBIDDEN))) {
            return;
        }

        // if we have a processor where input requirement is INPUT_ALLOWED, we need to check if there is an
        // incoming connection or not. If not, we don't care if attributes are not available even though scope is FLOWFILE_ATTRIBUTES
        if (ExpressionLanguageScope.FLOWFILE_ATTRIBUTES.equals(expressionLanguageScope)
                && !((MockProcessContext) this.serviceLookup).hasIncomingConnection()) {
            return;
        }

        // we're trying to evaluate against flow files attributes but we don't have a FlowFile available.
        if (!flowFileProvided && !additionalAttributesAvailable && ExpressionLanguageScope.FLOWFILE_ATTRIBUTES.equals(expressionLanguageScope)) {
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
    public Duration asDuration() {
        return isSet() ? Duration.ofNanos(asTimePeriod(TimeUnit.NANOSECONDS)) : null;
    }

    @Override
    public Double asDataSize(final DataUnit dataUnit) {
        ensureExpressionsEvaluated();
        return stdPropValue.asDataSize(dataUnit);
    }

    private void markEvaluated() {
        if (expressionsEvaluated) {
            return;
        }

        if (Boolean.FALSE.equals(expectExpressions)) {
            throw new IllegalStateException("Attempting to Evaluate Expressions but " + propertyDescriptor
                    + " is not a supported property, or the property indicates that the Expression Language is not supported. If you realize that this is the case and do not want "
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
         * should be treated as though it were evaluated with the ENVIRONMENT scope instead of the flowfile scope. When NiFi
         * is running, it doesn't care when it's evaluating EL against a null flowfile. However, the testing framework currently
         * raises an error which makes it not mimick real world behavior.
         */
        if (flowFile == null && expressionLanguageScope == ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) {
            return evaluateAttributeExpressions(new HashMap<>());
        } else if (flowFile == null && expressionLanguageScope == ExpressionLanguageScope.ENVIRONMENT) {
            return evaluateAttributeExpressions(); //Added this to get around a similar edge case where the a null flowfile is passed
                                                    //and the scope is to the sys/env variable registry
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
        final boolean alreadyValidated = this.expressionsEvaluated;
        markEvaluated();

        if (rawValue == null) {
            return this;
        }

        if (!alreadyValidated) {
            validateExpressionScope(flowFile != null, additionalAttributes != null);
        }

        if (additionalAttributes == null ) {
            additionalAttributes = new HashMap<>();
        }
        // we need a new map here because additionalAttributes can be an unmodifiable map when it's the FlowFile attributes
        final Map<String, String> attAndEnvVarRegistry = new HashMap<>(additionalAttributes);

        if (environmentVariables != null) {
            attAndEnvVarRegistry.putAll(environmentVariables);
        }

        final PropertyValue newValue = stdPropValue.evaluateAttributeExpressions(flowFile, attAndEnvVarRegistry, decorator, stateValues);
        return new MockPropertyValue(newValue.getValue(), serviceLookup, propertyDescriptor, true, environmentVariables);
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
    public ResourceReference asResource() {
        if (propertyDescriptor == null) {
            return null;
        }

        return new StandardResourceReferenceFactory().createResourceReference(rawValue, propertyDescriptor.getResourceDefinition());
    }

    @Override
    public ResourceReferences asResources() {
        if (propertyDescriptor == null) {
            return null;
        }

        return new StandardResourceReferenceFactory().createResourceReferences(rawValue, propertyDescriptor.getResourceDefinition());
    }

    @Override
    public <E extends Enum<E>> E asAllowableValue(Class<E> enumType) throws IllegalArgumentException {
        ensureExpressionsEvaluated();
        return stdPropValue.asAllowableValue(enumType);
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
        if (rawValue == null) {
            return false;
        }

        final List<Range> elRanges = Query.extractExpressionRanges(rawValue);
        return (elRanges != null && !elRanges.isEmpty());
    }
}
