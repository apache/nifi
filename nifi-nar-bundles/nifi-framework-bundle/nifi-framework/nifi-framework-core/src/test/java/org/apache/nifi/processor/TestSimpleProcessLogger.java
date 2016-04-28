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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.reporting.ReportingTask;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.internal.matchers.VarargMatcher;
import org.slf4j.Logger;

public class TestSimpleProcessLogger {
    private final Exception e = new RuntimeException("intentional");

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
        verify(logger, times(1)).error(anyString(), argThat(new MyVarargMatcher()));
    }

    @Test
    public void validateDelegateLoggerReceivesThrowableToStringOnInfo() {
        componentLog.info("Hello {}", e);
        verify(logger, times(1)).info(anyString(), argThat(new MyVarargMatcher()));
    }

    @Test
    public void validateDelegateLoggerReceivesThrowableToStringOnTrace() {
        componentLog.trace("Hello {}", e);
        verify(logger, times(1)).trace(anyString(), argThat(new MyVarargMatcher()));
    }

    @Test
    public void validateDelegateLoggerReceivesThrowableToStringOnWarn() {
        componentLog.warn("Hello {}", e);
        verify(logger, times(1)).warn(anyString(), argThat(new MyVarargMatcher()));
    }

    @Test
    public void validateDelegateLoggerReceivesThrowableToStringOnLogWithLevel() {
        componentLog.log(LogLevel.WARN, "Hello {}", e);
        verify(logger, times(1)).warn(anyString(), argThat(new MyVarargMatcher()));
        componentLog.log(LogLevel.ERROR, "Hello {}", e);
        verify(logger, times(1)).error(anyString(), argThat(new MyVarargMatcher()));
        componentLog.log(LogLevel.INFO, "Hello {}", e);
        verify(logger, times(1)).info(anyString(), argThat(new MyVarargMatcher()));
        componentLog.log(LogLevel.TRACE, "Hello {}", e);
        verify(logger, times(1)).trace(anyString(), argThat(new MyVarargMatcher()));
    }

    /**
     *
     */
    private class MyVarargMatcher extends ArgumentMatcher<Object[]>implements VarargMatcher {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean matches(Object argument) {
            Object[] args = (Object[]) argument;
            assertEquals(task, args[0]);
            assertEquals(e.toString(), args[1]);
            return true;
        }
    }
}
