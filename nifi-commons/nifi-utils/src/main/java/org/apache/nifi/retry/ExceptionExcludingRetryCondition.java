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

import java.util.LinkedList;
import java.util.List;

final class ExceptionExcludingRetryCondition implements RetryCondition {
    private final List<Class<? extends Exception>> excludedExceptionTypes = new LinkedList<>();

    ExceptionExcludingRetryCondition(List<Class<? extends Exception>> excludedExceptionTypes) {
        this.excludedExceptionTypes.addAll(excludedExceptionTypes);
    }

    @Override
    public boolean allowNextAttempt(final RetryExecutionContext context) {
        for (final Class<? extends Exception> excludedExceptionType : excludedExceptionTypes) {
            if (excludedExceptionType.isInstance(context.getLastFailureReason())) {
                return false;
            }
        }

        return true;
    }
}
