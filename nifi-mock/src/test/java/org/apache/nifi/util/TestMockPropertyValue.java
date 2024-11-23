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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestMockPropertyValue {

    private final static String FF_SCOPE_WITH_FF = "Test FlowFile scope and providing a flow file";
    private final static String ENV_SCOPE_WITH_FF = "Test Env scope and providing a flow file";
    private final static String NONE_SCOPE_WITH_FF = "Test None scope and providing a flow file";
    private final static String FF_SCOPE_NO_FF = "Test FlowFile scope and no flow file";
    private final static String ENV_SCOPE_NO_FF = "Test Env scope and no flow file";
    private final static String NONE_SCOPE_NO_FF = "Test None scope and no flow file";
    private final static String FF_SCOPE_WITH_MAP = "Test FlowFile scope and providing a Map";
    private final static String ENV_SCOPE_WITH_MAP = "Test Env scope and providing a Map";
    private final static String NONE_SCOPE_WITH_MAP = "Test None scope and providing a Map";

    @Test
    public void testELScopeValidationProcessorWithInput() {
        final DummyProcessorWithInput processor = new DummyProcessorWithInput();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(DummyProcessorWithInput.PROP_FF_SCOPE, "${test}");
        runner.setProperty(DummyProcessorWithInput.PROP_ENV_SCOPE, "${test}");
        runner.setProperty(DummyProcessorWithInput.PROP_NONE_SCOPE, "${test}");

        // This case is expected to work: we evaluate against a FlowFile and the
        // property has the expected FlowFile scope
        processor.setTestCase(FF_SCOPE_WITH_FF);
        runner.enqueue("");
        runner.run();

        // This case is not expected to work: we evaluate against a FlowFile but the
        // property is not supposed to support evaluation against a FlowFile
        processor.setTestCase(ENV_SCOPE_WITH_FF);
        runner.enqueue("");
        assertThrows(AssertionError.class, runner::run);

        // This case is not expected to work: no EL support on the property
        processor.setTestCase(NONE_SCOPE_WITH_FF);
        runner.enqueue("");
        assertThrows(AssertionError.class, runner::run);

        // This case is supposed to fail: there is an incoming connection, there is a
        // FlowFile available, we have EL scope but we don't evaluate against the FF
        processor.setTestCase(FF_SCOPE_NO_FF);
        runner.enqueue("");
        assertThrows(AssertionError.class, runner::run);

        processor.setTestCase(ENV_SCOPE_NO_FF);
        runner.enqueue("");
        runner.run();

        // This case is not expected to work: no EL support on the property
        processor.setTestCase(NONE_SCOPE_NO_FF);
        runner.enqueue("");
        assertThrows(AssertionError.class, runner::run);

        // This case is accepted as we have an incoming connection, we may be evaluating
        // against a map made of the flow file attributes + additional key/value pairs.
        processor.setTestCase(FF_SCOPE_WITH_MAP);
        runner.enqueue("");
        runner.run();

        processor.setTestCase(ENV_SCOPE_WITH_MAP);
        runner.enqueue("");
        runner.run();

        // This case is not expected to work: no EL support on the property
        processor.setTestCase(NONE_SCOPE_WITH_MAP);
        runner.enqueue("");
        assertThrows(AssertionError.class, runner::run);
    }

    @Test
    public void testELScopeValidationProcessorNoInput() {
        final DummyProcessorNoInput processor = new DummyProcessorNoInput();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        runner.setProperty(DummyProcessorNoInput.PROP_FF_SCOPE, "${test}");
        runner.setProperty(DummyProcessorNoInput.PROP_ENV_SCOPE, "${test}");
        runner.setProperty(DummyProcessorNoInput.PROP_NONE_SCOPE, "${test}");

        // This case is supposed to be OK: in that case, we don't care if attributes are
        // not available even though scope is FLOWFILE_ATTRIBUTES it likely means that
        // the property has been defined in a common/abstract class used by multiple
        // processors with different input requirements.
        processor.setTestCase(FF_SCOPE_NO_FF);
        runner.run();

        processor.setTestCase(ENV_SCOPE_NO_FF);
        runner.run();

        // This case is not expected to work: no EL support on the property
        processor.setTestCase(NONE_SCOPE_NO_FF);
        assertThrows(AssertionError.class, runner::run);

        // This case is supposed to be OK: in that case, we don't care if attributes are
        // not available even though scope is FLOWFILE_ATTRIBUTES it likely means that
        // the property has been defined in a common/abstract class used by multiple
        // processors with different input requirements.
        processor.setTestCase(FF_SCOPE_WITH_MAP);
        runner.run();

        processor.setTestCase(ENV_SCOPE_WITH_MAP);
        runner.run();

        // This case is not expected to work: no EL support on the property
        processor.setTestCase(NONE_SCOPE_WITH_MAP);
        assertThrows(AssertionError.class, runner::run);
    }

    @InputRequirement(Requirement.INPUT_ALLOWED)
    private static class DummyProcessorWithInput extends AbstractProcessor {
        static final PropertyDescriptor PROP_FF_SCOPE = new PropertyDescriptor.Builder()
                .name("Property with FF scope")
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
        static final PropertyDescriptor PROP_ENV_SCOPE = new PropertyDescriptor.Builder()
                .name("Property with Env scope")
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
        static final PropertyDescriptor PROP_NONE_SCOPE = new PropertyDescriptor.Builder()
                .name("Property with None scope")
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        static final Relationship SUCCESS = new Relationship.Builder()
                .name("success")
                .build();

        private String testCase;

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return List.of(PROP_ENV_SCOPE, PROP_FF_SCOPE, PROP_NONE_SCOPE);
        }

        @Override
        public Set<Relationship> getRelationships() {
            return Set.of(SUCCESS);
        }

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            FlowFile ff = session.get();

            final Map<String, String> map = Map.of("test", "test");

            switch (getTestCase()) {
            case FF_SCOPE_WITH_FF -> context.getProperty(PROP_FF_SCOPE).evaluateAttributeExpressions(ff);
            case ENV_SCOPE_WITH_FF -> context.getProperty(PROP_ENV_SCOPE).evaluateAttributeExpressions(ff);
            case NONE_SCOPE_WITH_FF -> context.getProperty(PROP_NONE_SCOPE).evaluateAttributeExpressions(ff);
            case FF_SCOPE_NO_FF -> context.getProperty(PROP_FF_SCOPE).evaluateAttributeExpressions();
            case ENV_SCOPE_NO_FF -> context.getProperty(PROP_ENV_SCOPE).evaluateAttributeExpressions();
            case NONE_SCOPE_NO_FF -> context.getProperty(PROP_NONE_SCOPE).evaluateAttributeExpressions();
            case FF_SCOPE_WITH_MAP -> context.getProperty(PROP_FF_SCOPE).evaluateAttributeExpressions(map);
            case ENV_SCOPE_WITH_MAP -> context.getProperty(PROP_ENV_SCOPE).evaluateAttributeExpressions(map);
            case NONE_SCOPE_WITH_MAP -> context.getProperty(PROP_NONE_SCOPE).evaluateAttributeExpressions(map);
            }

            if (ff != null) {
                session.transfer(ff, SUCCESS);
            }
        }

        public String getTestCase() {
            return testCase;
        }

        public void setTestCase(String testCase) {
            this.testCase = testCase;
        }
    }

    @InputRequirement(Requirement.INPUT_FORBIDDEN)
    private static class DummyProcessorNoInput extends AbstractProcessor {
        static final PropertyDescriptor PROP_FF_SCOPE = new PropertyDescriptor.Builder()
                .name("Property with FF scope")
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
        static final PropertyDescriptor PROP_ENV_SCOPE = new PropertyDescriptor.Builder()
                .name("Property with Env scope")
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
        static final PropertyDescriptor PROP_NONE_SCOPE = new PropertyDescriptor.Builder()
                .name("Property with None scope")
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        static final Relationship SUCCESS = new Relationship.Builder()
                .name("success")
                .build();

        private String testCase;

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return List.of(PROP_ENV_SCOPE, PROP_FF_SCOPE, PROP_NONE_SCOPE);
        }

        @Override
        public Set<Relationship> getRelationships() {
            return Set.of(SUCCESS);
        }

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            final Map<String, String> map = Map.of("test", "test");

            switch (getTestCase()) {
            case FF_SCOPE_NO_FF -> context.getProperty(PROP_FF_SCOPE).evaluateAttributeExpressions();
            case ENV_SCOPE_NO_FF -> context.getProperty(PROP_ENV_SCOPE).evaluateAttributeExpressions();
            case NONE_SCOPE_NO_FF -> context.getProperty(PROP_NONE_SCOPE).evaluateAttributeExpressions();
            case FF_SCOPE_WITH_MAP -> context.getProperty(PROP_FF_SCOPE).evaluateAttributeExpressions(map);
            case ENV_SCOPE_WITH_MAP -> context.getProperty(PROP_ENV_SCOPE).evaluateAttributeExpressions(map);
            case NONE_SCOPE_WITH_MAP -> context.getProperty(PROP_NONE_SCOPE).evaluateAttributeExpressions(map);
            }
        }

        public String getTestCase() {
            return testCase;
        }

        public void setTestCase(String testCase) {
            this.testCase = testCase;
        }
    }
}