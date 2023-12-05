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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

public final class RetryTemplateBuilder {
    private Set<RetryCondition> retryConditions = new HashSet<>();
    private BiConsumer<Integer, Exception> errorAction = (i, e) -> {};
    /**
     * @param numberOfRetries The number of maximum attempted retries. This counts above the original try.
     */
    public RetryTemplateBuilder times(final int numberOfRetries) {
        retryConditions.add(new LimitedAttemptRetryCondition(numberOfRetries));
        return this;
    }

    /**
     * @param abortingExceptions Exceptions might prevent the {@code RetryTemplate} from further attempts
     */
    @SafeVarargs
    public final RetryTemplateBuilder abortingExceptions(final Class<? extends Exception>... abortingExceptions) {
        retryConditions.add(new ExceptionExcludingRetryCondition(Arrays.asList(abortingExceptions)));
        return this;
    }

    /**
     * @param retryCondition Retry might be abandoned prematurely based on the exception thrown. If the condition returns false, the retry template
     *                       will not make further attempts even if the number of retries would make it possible but instantly switches to the fallback
     *                       action.
     */
    public RetryTemplateBuilder retryCondition(final RetryCondition retryCondition) {
        this.retryConditions.add(retryCondition);
        return this;
    }

    /**
     * @param errorAction The code block is expected to be run after an unsuccessful attempt of executing the action. The first argument
     *                    of the consumer if the sequence number of the failed attempt and the second is the exception caused the failure.
     */
    public RetryTemplateBuilder errorAction(final BiConsumer<Integer, Exception> errorAction) {
        this.errorAction = errorAction;
        return this;
    }

    /**
     * @return An instance of {@code RetryTemplate} which runs on the same thread as the caller.
     */
    public RetryTemplate buildSynchronousRetryTemplate() {
        if (retryConditions.size() == 0) {
            throw new IllegalArgumentException("No retry condition is set");
        }

        return new SynchronousRetryTemplate(new CompositeRetryCondition(retryConditions), errorAction);
    }
}
