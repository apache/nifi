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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardDeprecationLoggerTest {
    private static final Class<?> REFERENCE_CLASS = StandardDeprecationLoggerTest.class;

    private static final String LOGGER_NAME = String.format("deprecation.%s", REFERENCE_CLASS.getName());

    private static final String MESSAGE = "Feature not used";

    private static final String MESSAGE_PLACEHOLDER = "Feature not used [{}]";

    private static final String PLACEHOLDER = "PLACEHOLDER";

    @Mock
    Logger logger;

    @Test
    void testWarn() {
        try (final MockedStatic<LoggerFactory> ignored = mockStatic(LoggerFactory.class)) {
            when(LoggerFactory.getLogger(eq(LOGGER_NAME))).thenReturn(logger);

            final DeprecationLogger deprecationLogger = new StandardDeprecationLogger(REFERENCE_CLASS);

            deprecationLogger.warn(MESSAGE);

            verify(logger).warn(eq(MESSAGE), ArgumentMatchers.<Object[]>any());
        }
    }

    @Test
    void testWarnArguments() {
        try (final MockedStatic<LoggerFactory> ignored = mockStatic(LoggerFactory.class)) {
            when(LoggerFactory.getLogger(eq(LOGGER_NAME))).thenReturn(logger);

            final DeprecationLogger deprecationLogger = new StandardDeprecationLogger(REFERENCE_CLASS);

            deprecationLogger.warn(MESSAGE_PLACEHOLDER, PLACEHOLDER);

            verify(logger).warn(eq(MESSAGE_PLACEHOLDER), ArgumentMatchers.<Object[]>any());
        }
    }
}
