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

package org.apache.nifi.minifi.bootstrap.status.reporters;

import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.minifi.bootstrap.QueryableStatusAggregator;
import org.apache.nifi.minifi.commons.status.FlowStatusReport;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.minifi.bootstrap.status.reporters.StatusLogger.ENCOUNTERED_IO_EXCEPTION;
import static org.apache.nifi.minifi.bootstrap.status.reporters.StatusLogger.LOGGING_LEVEL_KEY;
import static org.apache.nifi.minifi.bootstrap.status.reporters.StatusLogger.QUERY_KEY;
import static org.apache.nifi.minifi.bootstrap.status.reporters.StatusLogger.REPORT_PERIOD_KEY;
import static org.mockito.Mockito.verify;

public class StatusLoggerTest {

    private static final String MOCK_STATUS = "FlowStatusReport{controllerServiceStatusList=null, processorStatusList=[{name='TailFile', processorHealth={runStatus='Running', hasBulletins=false, " +
            "validationErrorList=[]}, processorStats=null, bulletinList=null}], connectionStatusList=null, remoteProcessingGroupStatusList=null, instanceStatus=null, systemDiagnosticsStatus=null," +
            " reportingTaskStatusList=null, errorsGeneratingReport=[]}";

    private static final String MOCK_QUERY = "processor:all:health";

    private StatusLogger statusLogger;
    private Logger logger;
    private QueryableStatusAggregator queryableStatusAggregator;
    private FlowStatusReport flowStatusReport;

    @Before
    public void init() throws IOException, NoSuchFieldException, IllegalAccessException {
        statusLogger = Mockito.spy(new StatusLogger());

        logger = Mockito.mock(Logger.class);
        queryableStatusAggregator = Mockito.mock(QueryableStatusAggregator.class);
        flowStatusReport = Mockito.mock(FlowStatusReport.class);

        Mockito.when(flowStatusReport.toString()).thenReturn(MOCK_STATUS);

        Field field = StatusLogger.class.getDeclaredField("logger");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, logger);


        Mockito.when(queryableStatusAggregator.statusReport(MOCK_QUERY)).thenReturn(flowStatusReport);
    }

    @Test(expected = IllegalStateException.class)
    public void testFailedInitDueToFatalLogLevel() {
        Properties properties = new Properties();
        properties.setProperty(REPORT_PERIOD_KEY, "1");
        properties.setProperty(LOGGING_LEVEL_KEY, LogLevel.FATAL.name());
        properties.setProperty(QUERY_KEY, MOCK_QUERY);

        statusLogger.initialize(properties, queryableStatusAggregator);
    }

    @Test(expected = IllegalStateException.class)
    public void testFailedInitDueToNoPeriod() {
        Properties properties = new Properties();
        properties.setProperty(LOGGING_LEVEL_KEY, LogLevel.INFO.name());
        properties.setProperty(QUERY_KEY, MOCK_QUERY);

        statusLogger.initialize(properties, queryableStatusAggregator);
    }

    @Test(expected = IllegalStateException.class)
    public void testFailedInitDueToNoQuery() {
        Properties properties = new Properties();
        properties.setProperty(REPORT_PERIOD_KEY, "1");
        properties.setProperty(LOGGING_LEVEL_KEY, LogLevel.INFO.name());

        statusLogger.initialize(properties, queryableStatusAggregator);
    }

    @Test
    public void TestTrace() {
        statusLogger.initialize(getProperties(LogLevel.TRACE), queryableStatusAggregator);
        statusLogger.setScheduledExecutorService(new RunOnceScheduledExecutorService(1));
        statusLogger.start();

        verify(logger, Mockito.atLeastOnce()).trace(MOCK_STATUS, (Throwable) null);
    }

    @Test
    public void TestDebug() {
        statusLogger.initialize(getProperties(LogLevel.DEBUG), queryableStatusAggregator);
        statusLogger.setScheduledExecutorService(new RunOnceScheduledExecutorService(1));
        statusLogger.start();

        verify(logger, Mockito.atLeastOnce()).debug(MOCK_STATUS, (Throwable) null);
    }

    @Test
    public void TestInfo() {
        statusLogger.initialize(getProperties(LogLevel.INFO), queryableStatusAggregator);
        statusLogger.setScheduledExecutorService(new RunOnceScheduledExecutorService(1));
        statusLogger.start();

        verify(logger, Mockito.atLeastOnce()).info(MOCK_STATUS, (Throwable) null);
    }

    @Test
    public void TestWarn() {
        statusLogger.initialize(getProperties(LogLevel.WARN), queryableStatusAggregator);
        statusLogger.setScheduledExecutorService(new RunOnceScheduledExecutorService(1));
        statusLogger.start();

        verify(logger, Mockito.atLeastOnce()).warn(MOCK_STATUS, (Throwable) null);
    }

    @Test
    public void TestError() {
        statusLogger.initialize(getProperties(LogLevel.ERROR), queryableStatusAggregator);
        statusLogger.setScheduledExecutorService(new RunOnceScheduledExecutorService(1));
        statusLogger.start();

        verify(logger, Mockito.atLeastOnce()).error(MOCK_STATUS, (Throwable) null);
    }

    // Exception testing
    @Test
    public void TestTraceException() throws IOException {
        Properties properties = new Properties();
        properties.setProperty(REPORT_PERIOD_KEY, "1");
        properties.setProperty(LOGGING_LEVEL_KEY, LogLevel.TRACE.name());
        properties.setProperty(QUERY_KEY, MOCK_QUERY);

        IOException ioException = new IOException("This is an expected test exception");
        Mockito.when(queryableStatusAggregator.statusReport(MOCK_QUERY)).thenThrow(ioException);

        statusLogger.initialize(properties, queryableStatusAggregator);
        statusLogger.setScheduledExecutorService(new RunOnceScheduledExecutorService(1));
        statusLogger.start();

        verify(logger, Mockito.atLeastOnce()).trace(ENCOUNTERED_IO_EXCEPTION, ioException);
    }

    @Test
    public void TestDebugException() throws IOException {
        IOException ioException = new IOException("This is an expected test exception");
        Mockito.when(queryableStatusAggregator.statusReport(MOCK_QUERY)).thenThrow(ioException);

        statusLogger.initialize(getProperties(LogLevel.DEBUG), queryableStatusAggregator);
        statusLogger.setScheduledExecutorService(new RunOnceScheduledExecutorService(1));
        statusLogger.start();

        verify(logger, Mockito.atLeastOnce()).debug(ENCOUNTERED_IO_EXCEPTION, ioException);
    }

    @Test
    public void TestInfoException() throws IOException {
        IOException ioException = new IOException("This is an expected test exception");
        Mockito.when(queryableStatusAggregator.statusReport(MOCK_QUERY)).thenThrow(ioException);

        statusLogger.initialize(getProperties(LogLevel.INFO), queryableStatusAggregator);
        statusLogger.setScheduledExecutorService(new RunOnceScheduledExecutorService(1));
        statusLogger.start();

        verify(logger, Mockito.atLeastOnce()).info(ENCOUNTERED_IO_EXCEPTION, ioException);
    }

    @Test
    public void TestWarnException() throws IOException {
        IOException ioException = new IOException("This is an expected test exception");
        Mockito.when(queryableStatusAggregator.statusReport(MOCK_QUERY)).thenThrow(ioException);

        statusLogger.initialize(getProperties(LogLevel.WARN), queryableStatusAggregator);
        statusLogger.setScheduledExecutorService(new RunOnceScheduledExecutorService(1));
        statusLogger.start();

        verify(logger, Mockito.atLeastOnce()).warn(ENCOUNTERED_IO_EXCEPTION, ioException);
    }

    @Test
    public void TestErrorException() throws IOException {
        IOException ioException = new IOException("This is an expected test exception");
        Mockito.when(queryableStatusAggregator.statusReport(MOCK_QUERY)).thenThrow(ioException);

        statusLogger.initialize(getProperties(LogLevel.ERROR), queryableStatusAggregator);
        statusLogger.setScheduledExecutorService(new RunOnceScheduledExecutorService(1));
        statusLogger.start();

        verify(logger, Mockito.atLeastOnce()).error(ENCOUNTERED_IO_EXCEPTION, ioException);
    }

    private static Properties getProperties(LogLevel logLevel) {
        Properties properties = new Properties();
        properties.setProperty(REPORT_PERIOD_KEY, "1");
        properties.setProperty(LOGGING_LEVEL_KEY, logLevel.name());
        properties.setProperty(QUERY_KEY, MOCK_QUERY);
        return properties;
    }

    private static class RunOnceScheduledExecutorService extends ScheduledThreadPoolExecutor {

        public RunOnceScheduledExecutorService(int corePoolSize) {
            super(corePoolSize);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            command.run();
            // Return value is not used
            return null;
        }
    }
}
