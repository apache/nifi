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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.state.MockStateManager;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestMockValidationContext {

    private static final String PARAMETER_NAME = "test-param";
    private static final String PARAMETER_VALUE = "resolved-value";
    private static final String DYNAMIC_PROPERTY = "param_based_attr";

    @Test
    void twoArgConstructorSubstitutesProcessContextParameters() {
        final Map<String, String> parameters = new HashMap<>();
        parameters.put(PARAMETER_NAME, PARAMETER_VALUE);

        final ParameterizedDynamicPropertyProcessor processor = new ParameterizedDynamicPropertyProcessor();
        final MockProcessContext processContext = new MockProcessContext(
                processor, "test", new MockStateManager(processor), new HashMap<>(), parameters);
        processContext.setProperty(DYNAMIC_PROPERTY, "#{%s}".formatted(PARAMETER_NAME));

        final MockValidationContext validationContext = new MockValidationContext(processContext, processContext.getStateManager());

        assertEquals(PARAMETER_VALUE, validationContext.evaluateParameters("#{test-param}"));
        assertEquals(PARAMETER_VALUE, getDynamicPropertyValue(validationContext));
    }

    @Test
    void twoArgConstructorLeavesMissingParameterUnset() {
        final ParameterizedDynamicPropertyProcessor processor = new ParameterizedDynamicPropertyProcessor();
        final MockProcessContext processContext = new MockProcessContext(
                processor, "test", new MockStateManager(processor), new HashMap<>(), new HashMap<>());
        processContext.setProperty(DYNAMIC_PROPERTY, "#{missing.parameter}");

        final MockValidationContext validationContext = new MockValidationContext(processContext, processContext.getStateManager());

        assertNull(getDynamicPropertyValue(validationContext));
    }

    @Test
    void testRunnerResolvesSetParameterDuringValidation() {
        final TestRunner runner = TestRunners.newTestRunner(new ParameterizedDynamicPropertyProcessor());
        runner.setParameterContextValue(PARAMETER_NAME, PARAMETER_VALUE);
        runner.setProperty(DYNAMIC_PROPERTY, "#{%s}".formatted(PARAMETER_NAME));
        runner.assertValid();
    }

    @Test
    void testRunnerStillInvalidWhenParameterMissing() {
        final TestRunner runner = TestRunners.newTestRunner(new ParameterizedDynamicPropertyProcessor());
        runner.setProperty(DYNAMIC_PROPERTY, "#{missing.parameter}");
        runner.assertNotValid();
    }

    private static String getDynamicPropertyValue(final ValidationContext validationContext) {
        for (final Map.Entry<PropertyDescriptor, String> entry : validationContext.getProperties().entrySet()) {
            if (DYNAMIC_PROPERTY.equals(entry.getKey().getName())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static class ParameterizedDynamicPropertyProcessor extends AbstractProcessor {
        @Override
        protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .required(false)
                    .addValidator(Validator.VALID)
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .dynamic(true)
                    .build();
        }

        @Override
        protected Collection<ValidationResult> customValidate(final ValidationContext context) {
            final List<ValidationResult> reasons = new ArrayList<>(super.customValidate(context));
            for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                if (entry.getKey().isDynamic() && entry.getValue() == null) {
                    reasons.add(new ValidationResult.Builder()
                            .subject(entry.getKey().getDisplayName())
                            .valid(false)
                            .explanation("Dynamic Property %s is null. Does it reference an unset parameter?".formatted(entry.getKey().getName()))
                            .build());
                }
            }
            return reasons;
        }

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        }
    }
}
