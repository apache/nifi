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
package org.apache.nifi.processor;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestSimpleProcessLogger {

    private static final String FIRST_MESSAGE = "FIRST";
    private static final String SECOND_MESSAGE = "SECOND";
    private static final String THIRD_MESSAGE = "THIRD";

    private static final String EXPECTED_CAUSES = String.join(System.lineSeparator(),
            String.format("%s: %s", IllegalArgumentException.class.getName(), FIRST_MESSAGE),
            String.format("- Caused by: %s: %s", RuntimeException.class.getName(), SECOND_MESSAGE),
            String.format("- Caused by: %s: %s", SecurityException.class.getName(), THIRD_MESSAGE)
    );

    private static final Exception EXCEPTION = new IllegalArgumentException(FIRST_MESSAGE, new RuntimeException(SECOND_MESSAGE, new SecurityException(THIRD_MESSAGE)));

    private static final Object[] EXCEPTION_ARGUMENTS = new Object[]{EXCEPTION};

    private static final Throwable NULL_THROWABLE = null;

    private static final Object[] EMPTY_ARGUMENTS = new Object[]{};

    private static final String FIRST = "FIRST";

    private static final int SECOND = 2;

    private static final Object[] VALUE_ARGUMENTS = new Object[]{FIRST, SECOND};

    private static final String LOG_MESSAGE = "Processed";

    @Mock
    private ConfigurableComponent component;

    @Mock
    private LogRepository logRepository;

    @Mock
    private Logger logger;

    private Object[] componentArray;

    private Object[] componentValueArray;

    private Object[] componentCausesArray;

    private SimpleProcessLogger componentLog;

    @BeforeEach
    public void setLogger() throws IllegalAccessException {
        componentLog = new SimpleProcessLogger(component, logRepository);
        FieldUtils.writeDeclaredField(componentLog, "logger", logger, true);

        componentArray = new Object[]{component};
        componentValueArray = new Object[]{component, FIRST, SECOND};
        componentCausesArray = new Object[]{component, EXPECTED_CAUSES};

        when(logger.isTraceEnabled()).thenReturn(true);
        when(logger.isDebugEnabled()).thenReturn(true);
        when(logger.isInfoEnabled()).thenReturn(true);
        when(logger.isWarnEnabled()).thenReturn(true);
        when(logger.isErrorEnabled()).thenReturn(true);
    }

    @Test
    public void testLogLevelMessage() {
        for (final LogLevel logLevel : LogLevel.values()) {
            componentLog.log(logLevel, LOG_MESSAGE);

            switch (logLevel) {
                case TRACE:
                    verify(logger).trace(anyString(), eq(component));
                    break;
                case DEBUG:
                    verify(logger).debug(anyString(), eq(component));
                    break;
                case INFO:
                    verify(logger).info(anyString(), eq(component));
                    break;
                case WARN:
                    verify(logger).warn(anyString(), eq(component));
                    break;
                case ERROR:
                    verify(logger).error(anyString(), eq(component));
                    break;
                default:
                    continue;
            }

            verify(logRepository).addLogMessage(eq(logLevel), anyString(), eq(componentArray));
        }
    }

    @Test
    public void testLogLevelMessageThrowable() {
        for (final LogLevel logLevel : LogLevel.values()) {
            componentLog.log(logLevel, LOG_MESSAGE, EXCEPTION);

            switch (logLevel) {
                case TRACE:
                    verify(logger).trace(anyString(), eq(component), eq(EXCEPTION));
                    break;
                case DEBUG:
                    verify(logger).debug(anyString(), eq(component), eq(EXCEPTION));
                    break;
                case INFO:
                    verify(logger).info(anyString(), eq(component), eq(EXCEPTION));
                    break;
                case WARN:
                    verify(logger).warn(anyString(), eq(component), eq(EXCEPTION));
                    break;
                case ERROR:
                    verify(logger).error(anyString(), eq(component), eq(EXCEPTION));
                    break;
                default:
                    continue;
            }

            verify(logRepository).addLogMessage(eq(logLevel), anyString(), eq(componentCausesArray), eq(EXCEPTION));
        }
    }

    @Test
    public void testLogLevelMessageArgumentsLastArgumentThrowable() {
        for (final LogLevel logLevel : LogLevel.values()) {
            componentLog.log(logLevel, LOG_MESSAGE, EXCEPTION_ARGUMENTS);

            switch (logLevel) {
                case TRACE:
                    verify(logger).trace(anyString(), eq(componentArray), eq(EXCEPTION));
                    break;
                case DEBUG:
                    verify(logger).debug(anyString(), eq(componentArray), eq(EXCEPTION));
                    break;
                case INFO:
                    verify(logger).info(anyString(), eq(componentArray), eq(EXCEPTION));
                    break;
                case WARN:
                    verify(logger).warn(anyString(), eq(componentArray), eq(EXCEPTION));
                    break;
                case ERROR:
                    verify(logger).error(anyString(), eq(componentArray), eq(EXCEPTION));
                    break;
                default:
                    continue;
            }

            verify(logRepository).addLogMessage(eq(logLevel), anyString(), eq(componentCausesArray), eq(EXCEPTION));
        }
    }

    @Test
    public void testLogLevelMessageArgumentsThrowable() {
        for (final LogLevel logLevel : LogLevel.values()) {
            componentLog.log(logLevel, LOG_MESSAGE, EMPTY_ARGUMENTS, EXCEPTION);

            switch (logLevel) {
                case TRACE:
                    verify(logger).trace(anyString(), eq(componentArray), eq(EXCEPTION));
                    break;
                case DEBUG:
                    verify(logger).debug(anyString(), eq(componentArray), eq(EXCEPTION));
                    break;
                case INFO:
                    verify(logger).info(anyString(), eq(componentArray), eq(EXCEPTION));
                    break;
                case WARN:
                    verify(logger).warn(anyString(), eq(componentArray), eq(EXCEPTION));
                    break;
                case ERROR:
                    verify(logger).error(anyString(), eq(componentArray), eq(EXCEPTION));
                    break;
                default:
                    continue;
            }

            verify(logRepository).addLogMessage(eq(logLevel), anyString(), eq(componentCausesArray), eq(EXCEPTION));
        }
    }

    @Test
    public void testLogLevelMessageArgumentsThrowableNull() {
        for (final LogLevel logLevel : LogLevel.values()) {
            componentLog.log(logLevel, LOG_MESSAGE, VALUE_ARGUMENTS, NULL_THROWABLE);

            switch (logLevel) {
                case TRACE:
                    verify(logger).trace(anyString(), eq(component), eq(FIRST), eq(SECOND));
                    break;
                case DEBUG:
                    verify(logger).debug(anyString(), eq(component), eq(FIRST), eq(SECOND));
                    break;
                case INFO:
                    verify(logger).info(anyString(), eq(component), eq(FIRST), eq(SECOND));
                    break;
                case WARN:
                    verify(logger).warn(anyString(), eq(component), eq(FIRST), eq(SECOND));
                    break;
                case ERROR:
                    verify(logger).error(anyString(), eq(component), eq(FIRST), eq(SECOND));
                    break;
                default:
                    continue;
            }

            verify(logRepository).addLogMessage(eq(logLevel), anyString(), eq(componentValueArray));
        }
    }
}
