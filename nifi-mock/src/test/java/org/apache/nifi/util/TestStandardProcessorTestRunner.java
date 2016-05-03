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

import static org.junit.Assert.assertEquals;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.junit.Ignore;
import org.junit.Test;

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
    public void testNumThreads() {
        final ProcessorWithOnStop proc = new ProcessorWithOnStop();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setThreadCount(5);
        runner.run(1, true);
        assertEquals(5, runner.getProcessContext().getMaxConcurrentTasks());
    }

    @Test(expected = AssertionError.class)
    @Ignore("This should not be enabled until we actually fail processor unit tests for using deprecated methods")
    public void testFailOnDeprecatedTypeAnnotation() {
        new StandardProcessorTestRunner(new DeprecatedAnnotation());
    }

    @Test
    @Ignore("This should not be enabled until we actually fail processor unit tests for using deprecated methods")
    public void testDoesNotFailOnNonDeprecatedTypeAnnotation() {
        new StandardProcessorTestRunner(new NewAnnotation());
    }

    @Test(expected = AssertionError.class)
    @Ignore("This should not be enabled until we actually fail processor unit tests for using deprecated methods")
    public void testFailOnDeprecatedMethodAnnotation() {
        new StandardProcessorTestRunner(new DeprecatedMethodAnnotation());
    }

    @Test
    @Ignore("This should not be enabled until we actually fail processor unit tests for using deprecated methods")
    public void testDoesNotFailOnNonDeprecatedMethodAnnotation() {
        new StandardProcessorTestRunner(new NewMethodAnnotation());
    }

    @SuppressWarnings("deprecation")
    @org.apache.nifi.processor.annotation.Tags({"deprecated"})
    private static class DeprecatedAnnotation extends AbstractProcessor {

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }
    }

    @org.apache.nifi.annotation.documentation.Tags({"deprecated"})
    private static class NewAnnotation extends AbstractProcessor {

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }
    }

    private static class NewMethodAnnotation extends AbstractProcessor {

        @org.apache.nifi.annotation.lifecycle.OnScheduled
        public void dummy() {

        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }
    }

    private static class DeprecatedMethodAnnotation extends AbstractProcessor {

        @SuppressWarnings("deprecation")
        @org.apache.nifi.processor.annotation.OnScheduled
        public void dummy() {

        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }
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
}
