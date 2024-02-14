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
package org.apache.nifi.py4j;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.StringReader;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class PythonProcessLogReaderTest {

    private static final String PROCESS_LOGGER = "org.apache.nifi.py4j.ProcessLog";

    private static final String CONTROLLER_LOGGER = "org.apache.nifi.py4j.Controller";

    private static final String LOG_MESSAGE = "Testing Python Processing";

    private static final String LINE_FORMAT = "%s %s:%s";

    private static final String LINE_UNFORMATTED = "Testing message without level or logger";

    private static final String LINE_MISSING_SEPARATOR = "%s %s".formatted(PythonProcessLogReader.LogLevel.INFO.getLevel(), LOG_MESSAGE);

    @Mock
    private Logger logger;

    @Test
    void testDebug() {
        runCommand(PythonProcessLogReader.LogLevel.DEBUG);

        verify(logger).debug(eq(LOG_MESSAGE));
    }

    @Test
    void testInfo() {
        runCommand(PythonProcessLogReader.LogLevel.INFO);

        verify(logger).info(eq(LOG_MESSAGE));
    }

    @Test
    void testWarning() {
        runCommand(PythonProcessLogReader.LogLevel.WARNING);

        verify(logger).warn(eq(LOG_MESSAGE));
    }

    @Test
    void testError() {
        runCommand(PythonProcessLogReader.LogLevel.ERROR);

        verify(logger).error(eq(LOG_MESSAGE));
    }

    @Test
    void testCritical() {
        runCommand(PythonProcessLogReader.LogLevel.CRITICAL);

        verify(logger).error(eq(LOG_MESSAGE));
    }

    @Test
    void testUnformatted() {
        try (MockedStatic<LoggerFactory> loggerFactory = mockStatic(LoggerFactory.class)) {
            loggerFactory.when(() -> LoggerFactory.getLogger(eq(PROCESS_LOGGER))).thenReturn(logger);

            final BufferedReader reader = new BufferedReader(new StringReader(LINE_UNFORMATTED));
            final Runnable command = new PythonProcessLogReader(reader);
            command.run();
        }

        verify(logger).info(eq(LINE_UNFORMATTED));
    }

    @Test
    void testMissingSeparator() {
        try (MockedStatic<LoggerFactory> loggerFactory = mockStatic(LoggerFactory.class)) {
            loggerFactory.when(() -> LoggerFactory.getLogger(eq(PROCESS_LOGGER))).thenReturn(logger);

            final BufferedReader reader = new BufferedReader(new StringReader(LINE_MISSING_SEPARATOR));
            final Runnable command = new PythonProcessLogReader(reader);
            command.run();
        }

        verify(logger).info(eq(LOG_MESSAGE));
    }

    private void runCommand(final PythonProcessLogReader.LogLevel logLevel) {
        try (MockedStatic<LoggerFactory> loggerFactory = mockStatic(LoggerFactory.class)) {
            loggerFactory.when(() -> LoggerFactory.getLogger(eq(CONTROLLER_LOGGER))).thenReturn(logger);

            final String line = getLine(logLevel, CONTROLLER_LOGGER);
            final BufferedReader reader = new BufferedReader(new StringReader(line));
            final Runnable command = new PythonProcessLogReader(reader);
            command.run();
        }
    }

    private String getLine(final PythonProcessLogReader.LogLevel logLevel, final String loggerName) {
        return LINE_FORMAT.formatted(logLevel.getLevel(), loggerName, LOG_MESSAGE);
    }
}
