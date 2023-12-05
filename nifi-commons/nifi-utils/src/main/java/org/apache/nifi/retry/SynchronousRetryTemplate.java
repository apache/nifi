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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * Implementation of the retry template which attempts to execute the given action using the caller thread.
 */
final class SynchronousRetryTemplate implements RetryTemplate {
    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronousRetryTemplate.class);

    final RetryCondition retryCondition;
    final BiConsumer<Integer, Exception> failureHandler;

    SynchronousRetryTemplate(final RetryCondition retryCondition, final BiConsumer<Integer, Exception> failureHandler) {
        this.retryCondition = retryCondition;
        this.failureHandler = failureHandler;
    }

    @Override
    public <T> T execute(final Callable<T> action, final Callable<T> fallbackAction) {
        final MutableRetryExecutionContext executionContext = new MutableRetryExecutionContext();

        do {
            executionContext.increaseNumberOfAttempts();

            try {
                LOGGER.debug("Trying action {}. Sequence of attempt: {} ", action.toString(), executionContext.getNumberOfAttempts());
                return action.call();
            } catch (final Exception e) {
                executionContext.recordFailureReason(e);
                LOGGER.warn("Trying action {} failed at attempt {}. Reason: {}", action.toString(), executionContext.getNumberOfAttempts(), e);

                try {
                    LOGGER.debug("Failure handler is triggered");
                    failureHandler.accept(executionContext.getNumberOfAttempts(), e);
                } catch (final Exception e2) {
                    LOGGER.error("Unexpected result during error handling", e2);
                }
            }
        } while (retryCondition.allowNextAttempt(executionContext));

        try {
            LOGGER.debug("Calling fallback action for action {}", action);
            return fallbackAction.call();
        } catch (final Exception e) {
            throw new IllegalStateException("Calling the fallback action was unsuccessful", e);
        }
    }
}
