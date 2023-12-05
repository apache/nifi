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
package org.apache.nifi.retry;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

public class SynchronousRetryTemplateTest {
    public static final int NUMBER_OF_RETRIES = 2;
    public static final Callable<String> FALLBACK_ACTION = () -> "fallback";
    public static final Callable<String> FALLBACK_ACTION_FAILING = () -> {
        throw new RuntimeException("fallback");
    };

    @Test
    public void testHappyPath() {
        final TestAction action = new TestAction(0);
        final List<Exception> errors = new LinkedList<>();
        final RetryTemplate testSubject = RetryTemplate.builder()
                .times(NUMBER_OF_RETRIES)
                .errorAction(getErrorAction(errors))
                .buildSynchronousRetryTemplate();

        final String result = testSubject.execute(() -> action.getValue("okay"), FALLBACK_ACTION);

        Assertions.assertEquals("okay", result);
        Assertions.assertEquals(1, action.getNumberOfCalls());
        Assertions.assertEquals(0, errors.size());
    }

    @Test
    public void testConstantFailure() {
        final TestAction action = new TestAction(3);
        final List<Exception> errors = new LinkedList<>();
        final RetryTemplate testSubject = RetryTemplate.builder()
                .times(NUMBER_OF_RETRIES)
                .errorAction(getErrorAction(errors))
                .buildSynchronousRetryTemplate();

        final String result = testSubject.execute(() -> action.getValue("okay"), FALLBACK_ACTION);

        Assertions.assertEquals("fallback", result);
        Assertions.assertEquals(3, action.getNumberOfCalls());
        Assertions.assertEquals(3, errors.size());
    }

    @Test
    public void testFixedWhileRetrying() {
        final TestAction action = new TestAction(2);
        final List<Exception> errors = new LinkedList<>();
        final RetryTemplate testSubject = RetryTemplate.builder()
                .times(NUMBER_OF_RETRIES)
                .errorAction(getErrorAction(errors))
                .buildSynchronousRetryTemplate();

        final String result = testSubject.execute(() -> action.getValue("okay"), FALLBACK_ACTION);

        Assertions.assertEquals("okay", result);
        Assertions.assertEquals(3, action.getNumberOfCalls());
        Assertions.assertEquals(2, errors.size());
    }

    @Test
    public void testFallbackActionFails() {
        final TestAction action = new TestAction(3);
        final List<Exception> errors = new LinkedList<>();
        final RetryTemplate testSubject = RetryTemplate.builder()
                .times(NUMBER_OF_RETRIES)
                .errorAction(getErrorAction(errors))
                .buildSynchronousRetryTemplate();

        Assertions.assertThrows(RuntimeException.class, () -> testSubject.execute(() -> action.getValue("okay"), FALLBACK_ACTION_FAILING));
        Assertions.assertEquals(3, action.getNumberOfCalls());
    }

    @Test
    public void testErrorHandlingFailureShouldNotDisruptRetries() {
        final TestAction action = new TestAction(3);
        final RetryTemplate testSubject = RetryTemplate.builder()
                .times(NUMBER_OF_RETRIES)
                .errorAction((i, e) -> {
                    throw new RuntimeException("error handling failed");
                })
                .buildSynchronousRetryTemplate();

        final String result = testSubject.execute(() -> action.getValue("okay"), FALLBACK_ACTION);

        Assertions.assertEquals("fallback", result);
        Assertions.assertEquals(3, action.getNumberOfCalls());
    }

    @Test
    public void testHappyPathWithNoReturnValue() {
        final TestActionWithoutReturnValue action = new TestActionWithoutReturnValue(0);
        final List<Exception> errors = new LinkedList<>();
        final RetryTemplate testSubject = RetryTemplate
                .builder()
                .times(NUMBER_OF_RETRIES)
                .errorAction(getErrorAction(errors))
                .buildSynchronousRetryTemplate();

        testSubject.executeWithoutValue(() -> action.consumeValue("okay"));

        Assertions.assertEquals(1, action.getNumberOfCalls());
        Assertions.assertEquals(0, errors.size());
    }

    @Test
    public void testConstantFailureWithNoReturnValue() {
        final TestActionWithoutReturnValue action = new TestActionWithoutReturnValue(3);
        final List<Exception> errors = new LinkedList<>();
        final RetryTemplate testSubject = RetryTemplate
                .builder()
                .times(NUMBER_OF_RETRIES)
                .errorAction(getErrorAction(errors))
                .buildSynchronousRetryTemplate();

        testSubject.executeWithoutValue(() -> action.consumeValue("okay"));

        Assertions.assertEquals(3, action.getNumberOfCalls());
        Assertions.assertEquals(3, errors.size());
    }

    @Test
    public void testFixedWhileRetryingWithNoReturnValue() {
        final TestActionWithoutReturnValue action = new TestActionWithoutReturnValue(2);
        final List<Exception> errors = new LinkedList<>();
        final RetryTemplate testSubject = RetryTemplate
                .builder()
                .times(NUMBER_OF_RETRIES)
                .errorAction(getErrorAction(errors))
                .buildSynchronousRetryTemplate();

        testSubject.executeWithoutValue(() -> action.consumeValue("okay"));

        Assertions.assertEquals(3, action.getNumberOfCalls());
        Assertions.assertEquals(2, errors.size());
    }

    @Test
    public void testRetryConditionCanPreventFurtherTries() {
        final TestAction action = new TestAction(1);
        final List<Exception> errors = new LinkedList<>();
        final RetryTemplate testSubject = RetryTemplate
                .builder()
                .times(NUMBER_OF_RETRIES)
                .retryCondition(c -> false)
                .errorAction(getErrorAction(errors))
                .buildSynchronousRetryTemplate();

        final String result = testSubject.execute(() -> action.getValue("okay"), FALLBACK_ACTION);

        Assertions.assertEquals(1, action.getNumberOfCalls());
        Assertions.assertEquals(1, errors.size());
        Assertions.assertEquals("fallback", result);
    }

    private BiConsumer<Integer, Exception> getErrorAction(final List<Exception> aggregator) {
        return (integer, e) -> aggregator.add(e);
    }

    private static class TestAction {
        private final int numberOfInitialErrors;
        private int numberOfCalls = 0;

        private TestAction(final int numberOfInitialErrors) {
            this.numberOfInitialErrors = numberOfInitialErrors;
        }

        String getValue(final String value) throws Exception{
            numberOfCalls++;

            if (numberOfCalls <= numberOfInitialErrors) {
                throw new Exception("Test error");
            }

            return value;
        }

        public int getNumberOfCalls() {
            return numberOfCalls;
        }
    }

    private static class TestActionWithoutReturnValue {
        private final int numberOfInitialErrors;
        private int numberOfCalls = 0;

        private TestActionWithoutReturnValue(final int numberOfInitialErrors) {
            this.numberOfInitialErrors = numberOfInitialErrors;
        }

        void consumeValue(final String value) throws Exception {
            numberOfCalls++;

            if (numberOfCalls <= numberOfInitialErrors) {
                throw new Exception("Test error");
            }
        }

        public int getNumberOfCalls() {
            return numberOfCalls;
        }
    }
}
