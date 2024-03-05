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

import org.apache.nifi.py4j.logging.PythonLogLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PythonProcessLogReaderTest {

    private static final String PROCESS_LOGGER = "org.apache.nifi.py4j.ProcessLog";

    private static final String CONTROLLER_LOGGER = "org.apache.nifi.py4j.Controller";

    private static final String LOG_MESSAGE = "Testing Python Processing";

    private static final String LINE_FORMAT = "PY4JLOG %s %s:%s %d%n";

    private static final String LINE_UNFORMATTED = "Testing message without level or logger";

    private static final String LINE_MISSING_SEPARATOR = "%s %s".formatted(PythonLogLevel.INFO.getLevel(), LOG_MESSAGE);

    private static final String LINE_EMPTY = "";

    private static final String MESSAGE_TRACEBACK = String.format("Command Failed%nTrackback (most recent call last):%n  File: command.py, line 1%nError: name is not defined");

    private static final int FIRST_LOG = 1;

    private static final String FIRST_LOG_MESSAGE = String.format("%s %d", LOG_MESSAGE, FIRST_LOG);

    private static final String NUMBERED_LOG_FORMAT = "%s %d";

    @Mock
    private Logger processLogger;

    @Mock
    private Logger controllerLogger;

    @Test
    void testDebug() {
        runCommand(PythonLogLevel.DEBUG, controllerLogger);

        verify(controllerLogger).debug(eq(FIRST_LOG_MESSAGE));
    }

    @Test
    void testInfo() {
        runCommand(PythonLogLevel.INFO, controllerLogger);

        verify(controllerLogger).info(eq(FIRST_LOG_MESSAGE));
    }

    @Test
    void testWarning() {
        runCommand(PythonLogLevel.WARNING, controllerLogger);

        verify(controllerLogger).warn(eq(FIRST_LOG_MESSAGE));
    }

    @Test
    void testError() {
        runCommand(PythonLogLevel.ERROR, controllerLogger);

        verify(controllerLogger).error(eq(FIRST_LOG_MESSAGE));
    }

    @Test
    void testCritical() {
        runCommand(PythonLogLevel.CRITICAL, controllerLogger);

        verify(controllerLogger).error(eq(FIRST_LOG_MESSAGE));
    }

    @Test
    void testUnformatted() {
        try (MockedStatic<LoggerFactory> loggerFactory = mockStatic(LoggerFactory.class)) {
            setupLogger(loggerFactory, processLogger, PROCESS_LOGGER);

            final BufferedReader reader = new BufferedReader(new StringReader(LINE_UNFORMATTED));
            final Runnable command = new PythonProcessLogReader(reader);
            command.run();

            verify(processLogger).info(eq(LINE_UNFORMATTED));
        }
    }

    @Test
    void testMissingSeparator() {
        try (MockedStatic<LoggerFactory> loggerFactory = mockStatic(LoggerFactory.class)) {
            setupLogger(loggerFactory, processLogger, PROCESS_LOGGER);

            final BufferedReader reader = new BufferedReader(new StringReader(LINE_MISSING_SEPARATOR));
            final Runnable command = new PythonProcessLogReader(reader);
            command.run();

            verify(processLogger).info(eq(LINE_MISSING_SEPARATOR));
        }
    }

    @Test
    void testEmpty() {
        try (MockedStatic<LoggerFactory> loggerFactory = mockStatic(LoggerFactory.class)) {
            setupLogger(loggerFactory, processLogger, PROCESS_LOGGER);

            final BufferedReader reader = new BufferedReader(new StringReader(LINE_EMPTY));
            final Runnable command = new PythonProcessLogReader(reader);
            command.run();

            verifyNoInteractions(processLogger);
        }
    }

    @Test
    void testInfoMultipleMessages() {
        final int messages = 2;

        try (MockedStatic<LoggerFactory> loggerFactory = mockStatic(LoggerFactory.class)) {
            setupLogger(loggerFactory, controllerLogger, CONTROLLER_LOGGER);

            final StringBuilder builder = new StringBuilder();
            for (int i = 0; i < messages; i++) {
                builder.append(getLine(PythonLogLevel.INFO, i));
                builder.append(System.lineSeparator());
            }

            final String lines = builder.toString();
            final BufferedReader reader = new BufferedReader(new StringReader(lines));
            final Runnable command = new PythonProcessLogReader(reader);
            command.run();
        }

        for (int i = 0; i < messages; i++) {
            final String expected = NUMBERED_LOG_FORMAT.formatted(LOG_MESSAGE, i);
            verify(controllerLogger).info(eq(expected));
        }
    }

    @Test
    void testErrorMultipleLines() {
        try (MockedStatic<LoggerFactory> loggerFactory = mockStatic(LoggerFactory.class)) {
            setupLogger(loggerFactory, controllerLogger, CONTROLLER_LOGGER);

            final String lines = LINE_FORMAT.formatted(PythonLogLevel.ERROR.getLevel(), CONTROLLER_LOGGER, MESSAGE_TRACEBACK, FIRST_LOG);

            final BufferedReader reader = new BufferedReader(new StringReader(lines));
            final Runnable command = new PythonProcessLogReader(reader);
            command.run();
        }

        final String expected = NUMBERED_LOG_FORMAT.formatted(MESSAGE_TRACEBACK, FIRST_LOG);
        verify(controllerLogger).error(eq(expected));
    }

    @Test
    void testReaderException() throws IOException {
        try (MockedStatic<LoggerFactory> loggerFactory = mockStatic(LoggerFactory.class)) {
            setupLogger(loggerFactory, processLogger, PROCESS_LOGGER);

            final BufferedReader reader = mock(BufferedReader.class);
            when(reader.readLine()).thenThrow(new IOException());

            final Runnable command = new PythonProcessLogReader(reader);
            command.run();

            verify(processLogger).error(anyString(), isA(IOException.class));
        }
    }

    private void runCommand(final PythonLogLevel logLevel, final Logger logger) {
        try (MockedStatic<LoggerFactory> loggerFactory = mockStatic(LoggerFactory.class)) {
            setupLogger(loggerFactory, logger, CONTROLLER_LOGGER);

            final String line = getLine(logLevel, FIRST_LOG);
            final BufferedReader reader = new BufferedReader(new StringReader(line));
            final Runnable command = new PythonProcessLogReader(reader);
            command.run();
        }
    }

    private void setupLogger(final MockedStatic<LoggerFactory> loggerFactory, final Logger logger, final String loggerName) {
        loggerFactory.when(() -> LoggerFactory.getLogger(eq(loggerName))).thenReturn(logger);
    }

    private String getLine(final PythonLogLevel logLevel, final int number) {
        return LINE_FORMAT.formatted(logLevel.getLevel(), CONTROLLER_LOGGER, LOG_MESSAGE, number);
    }
}
