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

import java.util.concurrent.Callable;

/**
 * Wrapper around a code segment which in case of error should be retried. The details of the retry strategy (number of
 * retry attempts, possible waiting time, asynchronous behaviour) depends on the implementation.
 *
 * @param <T> Type of the return value.
 */
public interface RetryTemplate {
    /**
     * Attempts to execute the action. This might result multiple calls on the action and possible execution of error handling
     * code pieces. Recall on the action indicated by exceptions. In case of unsuccessful calls a fallback action might be called.
     *
     * @param action The action to execute.
     * @param fallbackAction In case the intended action cannot be executed successfully after retries, the fallback action is called one time.
     *                       The fallback action might contain static response or error handling code, but it is eminent that it should not throw an exception.
     *
     * @return The return value of the action if any, otherwise the return value of the fallback action.
     */
    <T> T execute(final Callable<T> action, final Callable<T> fallbackAction);

    /**
     * For cases where the return value is not taken into account (or the action has no meaningful return value) this is an
     * alternative to {@code #execute}. The same retry strategy is applied but without returning results.
     *
     * @param action Te action to execute.
     */
    default void executeWithoutValue(final NoReturnCallable action) {
        executeWithoutValue(action, () -> {});
    }
    /**
     * For cases where the return value is not taken into account (or the action has no meaningful return value) this is an
     * alternative to {@code #execute}. The same retry strategy is applied but without returning results.
     *
     * @param action Te action to execute.
     * @param fallbackAction In case the intended action cannot be executed successfully after retries, the fallback action is called one time.
     *                       The fallback action might contain static response or error handling code, but it is eminent that it should not throw an exception.
     */
    default void executeWithoutValue(final NoReturnCallable action, final NoReturnCallable fallbackAction) {
        this.execute(
            () -> {
                action.call();
                return null;
            },
            () -> {
                fallbackAction.call();
                return null;
            }
        );
    }
    /**
     *
     * @return Returns a builder for {@code RetriableAction} with no expected return type from the retried action.
     */
    static RetryTemplateBuilder builder() {
        return new RetryTemplateBuilder();
    }
}
