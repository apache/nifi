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
package org.apache.nifi.jms.processors.helpers;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;

public class AssertionUtils {

    public static <T extends Throwable> void assertCausedBy(final Class<T> expectedType, final Runnable runnable) {
        assertCausedBy(expectedType, null, runnable);
    }

    public static <T extends Throwable> void assertCausedBy(final Class<T> expectedType, final String expectedMessage, final Runnable runnable) {
        try {
            runnable.run();
            fail(String.format("Expected an exception to be thrown with a cause of %s, but nothing was thrown.", expectedType.getCanonicalName()));
        } catch (final Throwable throwable) {
            final List<Throwable> causes = ExceptionUtils.getThrowableList(throwable);
            for (final Throwable cause : causes) {
                if (expectedType.isInstance(cause)) {
                    if (expectedMessage != null) {
                        if (cause.getMessage() != null && cause.getMessage().startsWith(expectedMessage)) {
                            return;
                        }
                    } else {
                        return;
                    }
                }
            }
            fail(String.format("Exception is thrown but not found %s as a cause. Received exception is: %s", expectedType.getCanonicalName(), throwable), throwable);
        }
    }

    public static void assertCausedBy(final Throwable expectedException, final Runnable runnable) {
        try {
            runnable.run();
            fail(String.format("Expected an exception to be thrown with a cause of %s, but nothing was thrown.", expectedException));
        } catch (final Throwable throwable) {
            final List<Throwable> causes = ExceptionUtils.getThrowableList(throwable);
            for (final Throwable cause : causes) {
                if (cause.equals(expectedException)) {
                    return;
                }
            }
            fail(String.format("Exception is thrown but not found %s as a cause. Received exception is: %s", expectedException, throwable), throwable);
        }
    }
}
