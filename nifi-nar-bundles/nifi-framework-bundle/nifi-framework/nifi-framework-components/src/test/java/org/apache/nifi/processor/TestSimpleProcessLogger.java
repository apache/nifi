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

import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.reporting.ReportingTask;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.lang.reflect.Field;

import static org.apache.nifi.processor.SimpleProcessLogger.NEW_LINE_ARROW;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestSimpleProcessLogger {

    private static final String EXPECTED_CAUSES = "java.lang.RuntimeException: third" + System.lineSeparator() +
            NEW_LINE_ARROW + " causes: java.lang.RuntimeException: second" + System.lineSeparator() +
            NEW_LINE_ARROW + " causes: java.lang.RuntimeException: first";

    private final Exception e = new RuntimeException("first", new RuntimeException("second", new RuntimeException("third")));

    private ReportingTask task;

    private SimpleProcessLogger componentLog;

    private Logger logger;

    @Before
    public void before() {
        task = mock(ReportingTask.class);
        when(task.getIdentifier()).thenReturn("foo");
        when(task.toString()).thenReturn("MyTask");
        componentLog = new SimpleProcessLogger(task.getIdentifier(), task);
        try {
            Field loggerField = componentLog.getClass().getDeclaredField("logger");
            loggerField.setAccessible(true);
            logger = mock(Logger.class);

            when(logger.isDebugEnabled()).thenReturn(true);
            when(logger.isInfoEnabled()).thenReturn(true);
            when(logger.isWarnEnabled()).thenReturn(true);
            when(logger.isErrorEnabled()).thenReturn(true);
            when(logger.isTraceEnabled()).thenReturn(true);

            loggerField.set(componentLog, logger);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void validateDelegateLoggerReceivesThrowableToStringOnError() {
        componentLog.error("Hello {}", e);
        verify(logger, times(1)).error(anyString(), eq(task), eq(EXPECTED_CAUSES), eq(e));
    }

    @Test
    public void validateDelegateLoggerReceivesThrowableToStringOnInfo() {
        componentLog.info("Hello {}", e);
        verify(logger, times(1)).info(anyString(), eq(e));
    }

    @Test
    public void validateDelegateLoggerReceivesThrowableToStringOnTrace() {
        componentLog.trace("Hello {}", e);
        verify(logger, times(1)).trace(anyString(), eq(task), eq(EXPECTED_CAUSES), eq(e));
    }

    @Test
    public void validateDelegateLoggerReceivesThrowableToStringOnWarn() {
        componentLog.warn("Hello {}", e);
        verify(logger, times(1)).warn(anyString(), eq(task), eq(EXPECTED_CAUSES), eq(e));
    }

    @Test
    public void validateDelegateLoggerReceivesThrowableToStringOnLogWithLevel() {
        componentLog.log(LogLevel.WARN, "Hello {}", e);
        verify(logger, times(1)).warn(anyString(), eq(task), eq(EXPECTED_CAUSES), eq(e));
        componentLog.log(LogLevel.ERROR, "Hello {}", e);
        verify(logger, times(1)).error(anyString(), eq(task), eq(EXPECTED_CAUSES), eq(e));
        componentLog.log(LogLevel.INFO, "Hello {}", e);
        verify(logger, times(1)).info(anyString(), eq(e));
        componentLog.log(LogLevel.TRACE, "Hello {}", e);
        verify(logger, times(1)).trace(anyString(), eq(task), eq(EXPECTED_CAUSES), eq(e));
    }
}
