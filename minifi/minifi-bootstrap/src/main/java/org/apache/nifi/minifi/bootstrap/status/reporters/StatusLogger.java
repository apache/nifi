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
import org.apache.nifi.minifi.bootstrap.status.PeriodicStatusReporter;
import org.apache.nifi.minifi.commons.status.FlowStatusReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import static org.apache.nifi.minifi.commons.schema.common.BootstrapPropertyKeys.STATUS_REPORTER_PROPERTY_PREFIX;

public class StatusLogger extends PeriodicStatusReporter {


    private volatile QueryableStatusAggregator queryableStatusAggregator;
    private volatile LogLevel logLevel;
    private volatile String statusQuery;

    private static final Logger logger = LoggerFactory.getLogger(StatusLogger.class);


    public static final String LOGGER_STATUS_REPORTER_PROPERTY_PREFIX = STATUS_REPORTER_PROPERTY_PREFIX + ".log";
    public static final String REPORT_PERIOD_KEY = LOGGER_STATUS_REPORTER_PROPERTY_PREFIX + ".period";
    public static final String LOGGING_LEVEL_KEY = LOGGER_STATUS_REPORTER_PROPERTY_PREFIX + ".level";
    public static final String QUERY_KEY = LOGGER_STATUS_REPORTER_PROPERTY_PREFIX + ".query";

    static final String ENCOUNTERED_IO_EXCEPTION = "Encountered an IO Exception while attempting to query the flow status.";

    @Override
    public void initialize(Properties properties,  QueryableStatusAggregator queryableStatusAggregator) {
        this.queryableStatusAggregator = queryableStatusAggregator;

        String periodString = properties.getProperty(REPORT_PERIOD_KEY);
        if (periodString == null) {
            throw new IllegalStateException(REPORT_PERIOD_KEY + " is null but it is required. Please configure it.");
        }
        try {
            setPeriod(Integer.parseInt(periodString));
        } catch (NumberFormatException e) {
            throw new IllegalStateException(REPORT_PERIOD_KEY + " is not a valid number.", e);
        }


        String loglevelString = properties.getProperty(LOGGING_LEVEL_KEY);
        if (loglevelString == null) {
            throw new IllegalStateException(LOGGING_LEVEL_KEY + " is null but it is required. Please configure it.");
        }

        try {
            logLevel = LogLevel.valueOf(loglevelString.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Value set for " + LOGGING_LEVEL_KEY + " is not a valid log level.");
        }

        if (LogLevel.FATAL.equals(logLevel)){
            throw new IllegalStateException("Cannot log status at the FATAL level. Please configure " + LOGGING_LEVEL_KEY + " to another value.");
        }

        statusQuery = properties.getProperty(QUERY_KEY);
        if (statusQuery == null) {
            throw new IllegalStateException(QUERY_KEY + " is null but it is required. Please configure it.");
        }

        reportRunner = new ReportStatusRunner();
    }


    private class ReportStatusRunner implements Runnable {

        @Override
        public void run() {
            try {
                String toLog;
                Exception exception = null;
                try {
                    FlowStatusReport flowStatusReport = queryableStatusAggregator.statusReport(statusQuery);
                    toLog = flowStatusReport.toString();
                } catch (IOException e) {
                    toLog = ENCOUNTERED_IO_EXCEPTION;
                    exception = e;
                }

                // If exception is null the logger will ignore it.
                switch (logLevel) {
                    case TRACE:
                        logger.trace(toLog, exception);
                        break;
                    case DEBUG:
                        logger.debug(toLog, exception);
                        break;
                    case INFO:
                        logger.info(toLog, exception);
                        break;
                    case WARN:
                        logger.warn(toLog, exception);
                        break;
                    case ERROR:
                        logger.error(toLog, exception);
                        break;
                    default:
                        throw new IllegalStateException("Cannot log status at level " + logLevel + ". Please configure another.");
                }

            } catch (Exception e) {
                switch (logLevel) {
                    case ERROR:
                        logger.error("Unexpected exception when attempting to report the status", e);
                        break;
                    default:
                        logger.warn("Unexpected exception when attempting to report the status", e);
                }
            }
        }
    }
}
