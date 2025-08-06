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

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestStandardProcessorTestRunner {

    @Test
    public void testProcessContextPassedToOnStoppedMethods() {
        final ProcessorWithOnStop proc = new ProcessorWithOnStop();
        final TestRunner runner = TestRunners.newTestRunner(proc);

        assertEquals(0, proc.getOnStoppedCallsWithContext());
        assertEquals(0, proc.getOnStoppedCallsWithoutContext());

        runner.run(1, false);

        assertEquals(0, proc.getOnStoppedCallsWithContext());
        assertEquals(0, proc.getOnStoppedCallsWithoutContext());

        runner.run(1, true);

        assertEquals(1, proc.getOnStoppedCallsWithContext());
        assertEquals(1, proc.getOnStoppedCallsWithoutContext());
    }

    @Test
    public void testAllConditionsMet() {
        TestRunner runner = new StandardProcessorTestRunner(new GoodProcessor());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("GROUP_ATTRIBUTE_KEY", "1");
        attributes.put("KeyB", "hihii");
        runner.enqueue("1,hello\n1,good-bye".getBytes(), attributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(GoodProcessor.REL_SUCCESS, 1);

        runner.assertAllConditionsMet("success",
                mff -> mff.isAttributeEqual("GROUP_ATTRIBUTE_KEY", "1") && mff.isContentEqual("1,hello\n1,good-bye")
        );
    }

    @Test
    public void testAllConditionsMetComplex() {
        TestRunner runner = new StandardProcessorTestRunner(new GoodProcessor());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("GROUP_ATTRIBUTE_KEY", "1");
        attributes.put("KeyB", "hihii");
        runner.enqueue("1,hello\n1,good-bye".getBytes(), attributes);

        attributes.clear();
        attributes.put("age", "34");
        runner.enqueue("May Andersson".getBytes(), attributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(GoodProcessor.REL_SUCCESS, 2);

        Predicate<MockFlowFile> firstPredicate = mff -> mff.isAttributeEqual("GROUP_ATTRIBUTE_KEY", "1");
        Predicate<MockFlowFile> either = firstPredicate.or(mff -> mff.isAttributeEqual("age", "34"));

        runner.assertAllConditionsMet("success", either);
    }

    @Test
    public void testNumThreads() {
        final ProcessorWithOnStop proc = new ProcessorWithOnStop();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setThreadCount(5);
        runner.run(1, true);
        assertEquals(5, runner.getProcessContext().getMaxConcurrentTasks());
    }

    @Test
    public void testFlowFileValidator() {
        final AddAttributeProcessor proc = new AddAttributeProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);

        runner.run(5, true);
        runner.assertTransferCount(AddAttributeProcessor.REL_SUCCESS, 3);
        runner.assertTransferCount(AddAttributeProcessor.REL_FAILURE, 2);
        runner.assertAllFlowFilesContainAttribute(AddAttributeProcessor.REL_SUCCESS, AddAttributeProcessor.KEY);
        runner.assertAllFlowFiles(AddAttributeProcessor.REL_SUCCESS, f -> assertEquals("value", f.getAttribute(AddAttributeProcessor.KEY)));
    }

    @Test
    public void testFailFlowFileValidator() {
        final AddAttributeProcessor proc = new AddAttributeProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);

        runner.run(5, true);
        assertThrows(AssertionError.class, () -> runner.assertAllFlowFiles(f -> assertEquals("value", f.getAttribute(AddAttributeProcessor.KEY))));
    }

    @Test
    public void testFailAllFlowFilesContainAttribute() {
        final AddAttributeProcessor proc = new AddAttributeProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);

        runner.run(5, true);
        assertThrows(AssertionError.class, () -> runner.assertAllFlowFilesContainAttribute(AddAttributeProcessor.KEY));
    }

    @Test
    public void testAllFlowFilesContainAttribute() {
        final AddAttributeProcessor proc = new AddAttributeProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);

        runner.run(1, true);
        runner.assertAllFlowFilesContainAttribute(AddAttributeProcessor.KEY);
    }

    @Test
    public void testControllerServiceUpdateShouldCallOnSetProperty() {
        // Arrange
        final ControllerService testService = new SimpleTestService();
        final AddAttributeProcessor proc = new AddAttributeProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        final String serviceIdentifier = "test";
        final String pdName = "name";
        final String pdValue = "exampleName";
        try {
            runner.addControllerService(serviceIdentifier, testService);
        } catch (InitializationException e) {
            fail(e.getMessage());
        }

        assertFalse(((SimpleTestService) testService).isOpmCalled(), "onPropertyModified has been called");

        // Act
        ValidationResult vr = runner.setProperty(testService, pdName, pdValue);

        // Assert
        assertTrue(vr.isValid());

        ControllerServiceConfiguration csConf = ((MockProcessContext) runner.getProcessContext()).getConfiguration(serviceIdentifier);
        PropertyDescriptor propertyDescriptor = testService.getPropertyDescriptor(pdName);
        String retrievedPDValue = csConf.getProperties().get(propertyDescriptor);

        assertEquals(pdValue, retrievedPDValue);
        assertTrue(((SimpleTestService) testService).isOpmCalled(), "onPropertyModified has not been called");
    }

    @Test
    public void testProcessorNameShouldBeSet() {
        final AddAttributeProcessor proc = new AddAttributeProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc, "TestName");
        assertEquals("TestName", runner.getProcessContext().getName());
    }

    @Test
    public void testProcessorInvalidWhenControllerServiceDisabled() {
        final ControllerService testService = new RequiredPropertyTestService();
        final AddAttributeProcessor proc = new AddAttributeProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        final String serviceIdentifier = "test";
        final String pdValue = "exampleName";
        try {
            runner.addControllerService(serviceIdentifier, testService);
        } catch (InitializationException e) {
            fail(e.getMessage());
        }

        // controller service invalid due to no value on required property; processor must also be invalid
        runner.assertNotValid(testService);
        runner.assertNotValid();

        // add required property; controller service valid but not enabled; processor must be invalid
        runner.setProperty(testService, RequiredPropertyTestService.namePropertyDescriptor, pdValue);
        runner.assertValid(testService);
        runner.assertNotValid();

        // enable controller service; processor now valid
        runner.enableControllerService(testService);
        runner.assertValid(testService);
        runner.assertValid();
    }

    private static class ProcessorWithOnStop extends AbstractProcessor {

        private int callsWithContext = 0;
        private int callsWithoutContext = 0;

        @OnStopped
        public void onStoppedWithContext(final ProcessContext procContext) {
            callsWithContext++;
        }

        @OnStopped
        public void onStoppedWithoutContext() {
            callsWithoutContext++;
        }

        public int getOnStoppedCallsWithContext() {
            return callsWithContext;
        }

        public int getOnStoppedCallsWithoutContext() {
            return callsWithoutContext;
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }

    }

    private static class AddAttributeProcessor extends AbstractProcessor {
        public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("success").build();
        public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("failure").build();
        public static final String KEY = "KEY";

        private Set<Relationship> relationships;
        private int counter = 0;

        @Override
        protected void init(final ProcessorInitializationContext context) {
            final Set<Relationship> relationships = new HashSet<>();
            relationships.add(REL_SUCCESS);
            relationships.add(REL_FAILURE);
            this.relationships = Collections.unmodifiableSet(relationships);
        }

        @Override
        public Set<Relationship> getRelationships() {
            return relationships;
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
            FlowFile ff = session.create();
            if (counter % 2 == 0) {
                ff = session.putAttribute(ff, KEY, "value");
                session.transfer(ff, REL_SUCCESS);
            } else {
                session.transfer(ff, REL_FAILURE);
            }
            counter++;
        }
    }

    private static class GoodProcessor extends AbstractProcessor {

        public static final Relationship REL_SUCCESS = new Relationship.Builder()
                .name("success")
                .description("Successfully created FlowFile from ...")
                .build();

        public static final Relationship REL_FAILURE = new Relationship.Builder()
                .name("failure")
                .description("... execution failed. Incoming FlowFile will be penalized and routed to this relationship")
                .build();

        private final Set<Relationship> relationships;

        public GoodProcessor() {
            final Set<Relationship> r = new HashSet<>();
            r.add(REL_SUCCESS);
            r.add(REL_FAILURE);
            relationships = Collections.unmodifiableSet(r);
        }

        @Override
        public Set<Relationship> getRelationships() {
            return relationships;
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

            for (FlowFile incoming : session.get(20)) {
                session.transfer(incoming, REL_SUCCESS);
            }
        }
    }

    private static class SimpleTestService extends AbstractControllerService {
        private final String PD_NAME = "name";
        private PropertyDescriptor namePropertyDescriptor = new PropertyDescriptor.Builder()
                .name(PD_NAME)
                .displayName("Controller Service Name")
                .required(false)
                .sensitive(false)
                .allowableValues("exampleName", "anotherExampleName")
                .build();

        private boolean opmCalled = false;

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return List.of(namePropertyDescriptor);
        }

        @Override
        public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
            getLogger().info("onPropertyModified called for PD {} with old value {} and new value {}", descriptor.getName(), oldValue, newValue);
            opmCalled = true;
        }

        public boolean isOpmCalled() {
            return opmCalled;
        }
    }

    private static class RequiredPropertyTestService extends AbstractControllerService {
        private static final String PD_NAME = "name";
        protected static final  PropertyDescriptor namePropertyDescriptor = new PropertyDescriptor.Builder()
                .name(PD_NAME)
                .displayName("Controller Service Name")
                .required(true)
                .sensitive(false)
                .allowableValues("exampleName", "anotherExampleName")
                .build();

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return List.of(namePropertyDescriptor);
        }
    }

    @Test
    public void testErrorLogMessageArguments() {
        final String compName = "name of component";
        final MockComponentLog logger = new MockComponentLog("first id", compName);

        final Throwable t = new RuntimeException("Intentional Exception for testing purposes");
        logger.error("expected test error", t);

        final List<LogMessage>  log = logger.getErrorMessages();
        final LogMessage msg = log.getFirst();

        assertTrue(msg.getMsg().contains("expected test error"));
        assertNotNull(msg.getThrowable());
    }
}
