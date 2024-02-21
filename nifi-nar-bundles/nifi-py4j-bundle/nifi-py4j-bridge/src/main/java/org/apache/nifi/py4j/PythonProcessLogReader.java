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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Objects;

/**
 * Runnable Command for reading a line from Process Output Stream and writing to a Logger
 */
class PythonProcessLogReader implements Runnable {
    private static final int LOG_LEVEL_BEGIN_INDEX = 0;

    private static final int LOG_LEVEL_END_INDEX = 2;

    private static final int MESSAGE_BEGIN_INDEX = 3;

    private static final int MINIMUM_LINE_LENGTH = 4;

    private static final char NAME_MESSAGE_SEPARATOR = ':';

    private static final int INDEX_NOT_FOUND = -1;

    private final Logger processLogger = LoggerFactory.getLogger("org.apache.nifi.py4j.ProcessLog");

    private final BufferedReader processReader;

    PythonProcessLogReader(final BufferedReader processReader) {
        this.processReader = Objects.requireNonNull(processReader, "Reader required");
    }

    @Override
    public void run() {
        try {
            String line = processReader.readLine();
            while (line != null) {
                if (line.length() > MINIMUM_LINE_LENGTH) {
                    processLine(line);
                }
                line = processReader.readLine();
            }
        } catch (final IOException e) {
            processLogger.error("Failed to read output of Python Process", e);
        }
    }

    private void processLine(final String line) {
        final String levelNumber = line.substring(LOG_LEVEL_BEGIN_INDEX, LOG_LEVEL_END_INDEX);
        final LogLevel logLevel = getLogLevel(levelNumber);
        if (LogLevel.NOTSET == logLevel) {
            processLogger.info(line);
        } else {
            final String message = line.substring(MESSAGE_BEGIN_INDEX);
            processMessage(logLevel, message);
        }
    }

    private void processMessage(final LogLevel logLevel, final String message) {
        final int nameSeparatorIndex = message.indexOf(NAME_MESSAGE_SEPARATOR);
        if (INDEX_NOT_FOUND == nameSeparatorIndex) {
            log(processLogger, logLevel, message);
        } else {
            final String loggerName = message.substring(0, nameSeparatorIndex);
            final Logger messageLogger = LoggerFactory.getLogger(loggerName);

            final int messageBeginIndex = nameSeparatorIndex + 1;
            final String logMessage = message.substring(messageBeginIndex);
            log(messageLogger, logLevel, logMessage);
        }
    }

    private void log(final Logger logger, final LogLevel logLevel, final String message) {
        if (LogLevel.DEBUG == logLevel) {
            logger.debug(message);
        } else if (LogLevel.INFO == logLevel) {
            logger.info(message);
        } else if (LogLevel.WARNING == logLevel) {
            logger.warn(message);
        } else if (LogLevel.ERROR == logLevel) {
            logger.error(message);
        } else {
            logger.error(message);
        }
    }

    private LogLevel getLogLevel(final String levelNumber) {
        LogLevel logLevel = LogLevel.NOTSET;

        for (final LogLevel currentLogLevel : LogLevel.values()) {
            if (currentLogLevel.level.equals(levelNumber)) {
                logLevel = currentLogLevel;
                break;
            }
        }

        return logLevel;
    }

    enum LogLevel {
        NOTSET("0"),

        DEBUG("10"),

        INFO("20"),

        WARNING("30"),

        ERROR("40"),

        CRITICAL("50");

        private final String level;

        LogLevel(final String level) {
            this.level = level;
        }

        String getLevel() {
            return level;
        }
    }
}
