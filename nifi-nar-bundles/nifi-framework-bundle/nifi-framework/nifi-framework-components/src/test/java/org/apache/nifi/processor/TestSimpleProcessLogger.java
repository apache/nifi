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
import org.apache.nifi.logging.StandardLoggingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.event.Level;
import org.slf4j.spi.LoggingEventBuilder;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
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

    private static final String EXCEPTION_STRING = EXCEPTION.toString();

    private static final Throwable NULL_THROWABLE = null;

    private static final String FIRST = "FIRST";

    private static final int SECOND = 2;

    private static final Object[] VALUE_ARGUMENTS = new Object[]{FIRST, SECOND};

    private static final Object[] VALUE_EXCEPTION_ARGUMENTS = new Object[]{FIRST, SECOND, EXCEPTION};

    private static final String LOG_MESSAGE = "Processed";

    private static final String LOG_MESSAGE_WITH_COMPONENT = String.format("{} %s", LOG_MESSAGE);

    private static final String LOG_MESSAGE_WITH_COMPONENT_AND_CAUSES = String.format("{} %s: {}", LOG_MESSAGE);

    private static final String LOG_ARGUMENTS_MESSAGE = "Processed {} {}";

    private static final String LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT = String.format("{} %s", LOG_ARGUMENTS_MESSAGE);

    private static final String LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT_AND_CAUSES = String.format("{} %s: {}", LOG_ARGUMENTS_MESSAGE);

    private static final String DISCRIMINATOR_KEY = "logFileSuffix";

    private static final String LOG_FILE_SUFFIX = "myGroup";

    @Mock
    private ConfigurableComponent component;

    @Mock
    private LogRepository logRepository;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Logger logger;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    LoggingEventBuilder loggingEventBuilder;

    @Mock
    StandardLoggingContext loggingContext;

    private Object[] componentArguments;

    private Object[] componentValueArguments;

    private Object[] componentValueCausesArguments;

    private Object[] componentCausesArguments;

    private SimpleProcessLogger componentLog;

    private final ArgumentCaptor<Object[]> argumentCaptor = ArgumentCaptor.forClass(Object[].class);

    @BeforeEach
    public void setLogger() throws IllegalAccessException {
        componentLog = new SimpleProcessLogger(component, logRepository, loggingContext);
        FieldUtils.writeDeclaredField(componentLog, "logger", logger, true);

        componentArguments = new Object[]{component};
        componentValueArguments = new Object[]{component, FIRST, SECOND};
        componentValueCausesArguments = new Object[]{component, FIRST, SECOND, EXPECTED_CAUSES};
        componentCausesArguments = new Object[]{component, EXPECTED_CAUSES};

        when(logger.isTraceEnabled()).thenReturn(true);
        when(logger.isDebugEnabled()).thenReturn(true);
        when(logger.isInfoEnabled()).thenReturn(true);
        when(logger.isWarnEnabled()).thenReturn(true);
        when(logger.isErrorEnabled()).thenReturn(true);
        when(logger.makeLoggingEventBuilder(any(Level.class))).thenReturn(loggingEventBuilder);
        when(loggingContext.getDiscriminatorKey()).thenReturn(DISCRIMINATOR_KEY);
        when(loggingContext.getLogFileSuffix()).thenReturn(Optional.of(LOG_FILE_SUFFIX));
        when(loggingEventBuilder.addKeyValue(any(String.class), any(String.class))).thenReturn(loggingEventBuilder);
    }

    @Test
    public void testLogLevelMessage() {
        for (final LogLevel logLevel : LogLevel.values()) {
            componentLog.log(logLevel, LOG_MESSAGE);

            switch (logLevel) {
                case TRACE:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.TRACE));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(1, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);

                    reset(loggingEventBuilder);
                    break;
                case DEBUG:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.DEBUG));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(1, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);

                    reset(loggingEventBuilder);
                    break;
                case INFO:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.INFO));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(1, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);

                    reset(loggingEventBuilder);
                    break;
                case WARN:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.WARN));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(1, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);

                    reset(loggingEventBuilder);
                    break;
                case ERROR:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.ERROR));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(1, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);

                    reset(loggingEventBuilder);
                    break;
                default:
                    continue;
            }

            verify(logRepository).addLogMessage(eq(logLevel), eq(LOG_MESSAGE_WITH_COMPONENT), eq(componentArguments));
        }
    }

    @Test
    public void testLogLevelMessageArguments() {
        for (final LogLevel logLevel : LogLevel.values()) {
            componentLog.log(logLevel, LOG_ARGUMENTS_MESSAGE, VALUE_ARGUMENTS);

            switch (logLevel) {
                case TRACE:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.TRACE));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(3, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);
                    assertEquals(FIRST, argumentCaptor.getValue()[1]);
                    assertEquals(SECOND, argumentCaptor.getValue()[2]);

                    reset(loggingEventBuilder);
                    break;
                case DEBUG:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.DEBUG));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(3, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);
                    assertEquals(FIRST, argumentCaptor.getValue()[1]);
                    assertEquals(SECOND, argumentCaptor.getValue()[2]);

                    reset(loggingEventBuilder);
                    break;
                case INFO:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.INFO));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(3, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);
                    assertEquals(FIRST, argumentCaptor.getValue()[1]);
                    assertEquals(SECOND, argumentCaptor.getValue()[2]);

                    reset(loggingEventBuilder);
                    break;
                case WARN:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.WARN));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(3, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);
                    assertEquals(FIRST, argumentCaptor.getValue()[1]);
                    assertEquals(SECOND, argumentCaptor.getValue()[2]);

                    reset(loggingEventBuilder);
                    break;
                case ERROR:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.ERROR));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(3, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);
                    assertEquals(FIRST, argumentCaptor.getValue()[1]);
                    assertEquals(SECOND, argumentCaptor.getValue()[2]);

                    reset(loggingEventBuilder);
                    break;
                default:
                    continue;
            }

            verify(logRepository).addLogMessage(eq(logLevel), eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), eq(componentValueArguments));
        }
    }

    @Test
    public void testLogLevelMessageThrowable() {
        for (final LogLevel logLevel : LogLevel.values()) {
            componentLog.log(logLevel, LOG_MESSAGE, EXCEPTION);

            switch (logLevel) {
                case TRACE:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.TRACE));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(2, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);
                    assertEquals(EXCEPTION, argumentCaptor.getValue()[1]);

                    reset(loggingEventBuilder);
                    break;
                case DEBUG:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.DEBUG));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(2, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);
                    assertEquals(EXCEPTION, argumentCaptor.getValue()[1]);

                    reset(loggingEventBuilder);
                    break;
                case INFO:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.INFO));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(2, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);
                    assertEquals(EXCEPTION, argumentCaptor.getValue()[1]);

                    reset(loggingEventBuilder);
                    break;
                case WARN:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.WARN));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(2, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);
                    assertEquals(EXCEPTION, argumentCaptor.getValue()[1]);

                    reset(loggingEventBuilder);
                    break;
                case ERROR:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.ERROR));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(2, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);
                    assertEquals(EXCEPTION, argumentCaptor.getValue()[1]);

                    reset(loggingEventBuilder);
                    break;
                default:
                    continue;
            }

            verify(logRepository).addLogMessage(eq(logLevel), eq(LOG_MESSAGE_WITH_COMPONENT_AND_CAUSES), eq(componentCausesArguments), eq(EXCEPTION));
        }
    }

    @Test
    public void testLogLevelMessageArgumentsThrowable() {
        for (final LogLevel logLevel : LogLevel.values()) {
            componentLog.log(logLevel, LOG_ARGUMENTS_MESSAGE, VALUE_EXCEPTION_ARGUMENTS);

            switch (logLevel) {
                case TRACE:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.TRACE));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(5, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);
                    assertEquals(FIRST, argumentCaptor.getValue()[1]);
                    assertEquals(SECOND, argumentCaptor.getValue()[2]);
                    assertEquals(EXCEPTION_STRING, argumentCaptor.getValue()[3]);
                    assertEquals(EXCEPTION, argumentCaptor.getValue()[4]);

                    reset(loggingEventBuilder);
                    break;
                case DEBUG:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.DEBUG));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(5, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);
                    assertEquals(FIRST, argumentCaptor.getValue()[1]);
                    assertEquals(SECOND, argumentCaptor.getValue()[2]);
                    assertEquals(EXCEPTION_STRING, argumentCaptor.getValue()[3]);
                    assertEquals(EXCEPTION, argumentCaptor.getValue()[4]);

                    reset(loggingEventBuilder);
                    break;
                case INFO:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.INFO));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(5, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);
                    assertEquals(FIRST, argumentCaptor.getValue()[1]);
                    assertEquals(SECOND, argumentCaptor.getValue()[2]);
                    assertEquals(EXCEPTION_STRING, argumentCaptor.getValue()[3]);
                    assertEquals(EXCEPTION, argumentCaptor.getValue()[4]);

                    reset(loggingEventBuilder);
                    break;
                case WARN:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.WARN));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(5, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);
                    assertEquals(FIRST, argumentCaptor.getValue()[1]);
                    assertEquals(SECOND, argumentCaptor.getValue()[2]);
                    assertEquals(EXCEPTION_STRING, argumentCaptor.getValue()[3]);
                    assertEquals(EXCEPTION, argumentCaptor.getValue()[4]);

                    reset(loggingEventBuilder);
                    break;
                case ERROR:
                    verify(logger, times(1)).makeLoggingEventBuilder(eq(Level.ERROR));
                    verify(loggingEventBuilder, times(1))
                            .addKeyValue(eq(DISCRIMINATOR_KEY), eq(LOG_FILE_SUFFIX));
                    verify(loggingEventBuilder
                            .addKeyValue(DISCRIMINATOR_KEY, LOG_FILE_SUFFIX), times(1))
                            .log(eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT), argumentCaptor.capture());

                    assertEquals(5, argumentCaptor.getValue().length);
                    assertEquals(component, argumentCaptor.getValue()[0]);
                    assertEquals(FIRST, argumentCaptor.getValue()[1]);
                    assertEquals(SECOND, argumentCaptor.getValue()[2]);
                    assertEquals(EXCEPTION_STRING, argumentCaptor.getValue()[3]);
                    assertEquals(EXCEPTION, argumentCaptor.getValue()[4]);

                    reset(loggingEventBuilder);
                    break;
                default:
                    continue;
            }

            verify(logRepository).addLogMessage(eq(logLevel), eq(LOG_ARGUMENTS_MESSAGE_WITH_COMPONENT_AND_CAUSES), eq(componentValueCausesArguments), eq(EXCEPTION));
        }
    }
}
