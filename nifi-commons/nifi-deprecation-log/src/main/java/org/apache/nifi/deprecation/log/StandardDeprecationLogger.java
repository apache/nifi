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
package org.apache.nifi.deprecation.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;

/**
 * Standard implementation of Deprecation Logger based on SLF4J
 */
class StandardDeprecationLogger implements DeprecationLogger {
    private static final String LOGGER_NAME_FORMAT = "deprecation.%s";

    private final Class<?> referenceClass;

    private final Logger logger;

    /**
     * Standard Deprecation Logger constructor with reference class for deriving logger
     *
     * @param referenceClass Reference Class
     */
    StandardDeprecationLogger(final Class<?> referenceClass) {
        this.referenceClass = Objects.requireNonNull(referenceClass, "Reference Class required");
        this.logger = LoggerFactory.getLogger(String.format(LOGGER_NAME_FORMAT, referenceClass.getName()));
    }

    /**
     * Log deprecation warning with optional arguments for message placeholders and Deprecation Exception for tracking
     *
     * @param message Message required
     * @param arguments Variable array of arguments to populate message placeholders
     */
    @Override
    public void warn(final String message, final Object... arguments) {
        Objects.requireNonNull(message, "Message required");
        final Object[] extendedArguments = getExtendedArguments(arguments);
        logger.warn(message, extendedArguments);
    }

    private Object[] getExtendedArguments(final Object... arguments) {
        final Object[] messageArguments = arguments == null ? new Object[0] : arguments;
        final int messageArgumentsLength = messageArguments.length;

        final Object[] extendedArguments = Arrays.copyOf(messageArguments, messageArgumentsLength + 1);
        extendedArguments[messageArgumentsLength] = new DeprecationException(referenceClass);

        return extendedArguments;
    }
}
