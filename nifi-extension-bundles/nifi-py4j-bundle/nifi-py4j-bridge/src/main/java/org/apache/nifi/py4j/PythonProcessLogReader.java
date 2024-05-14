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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * Runnable Command for reading a line from Process Output Stream and writing to a Logger
 */
class PythonProcessLogReader implements Runnable {
    private static final int LOG_LEVEL_BEGIN_INDEX = 0;

    private static final int LOG_LEVEL_END_INDEX = 2;

    private static final int MESSAGE_BEGIN_INDEX = 3;

    private static final char NAME_MESSAGE_SEPARATOR = ':';

    private static final int MINIMUM_LOGGER_NAME_INDEX = 3;

    private static final String LOG_PREFIX = "PY4JLOG";

    private static final int PREFIXED_LOG_LEVEL_BEGIN_INDEX = 8;

    private static final String LINE_SEPARATOR = System.lineSeparator();

    private static final Map<String, PythonLogLevel> PYTHON_LOG_LEVELS = Arrays.stream(PythonLogLevel.values()).collect(
            Collectors.toUnmodifiableMap(
                    pythonLogLevel -> Integer.toString(pythonLogLevel.getLevel()),
                    pythonLogLevel -> pythonLogLevel
            )
    );

    private final Logger processLogger = LoggerFactory.getLogger("org.apache.nifi.py4j.ProcessLog");

    private final BufferedReader processReader;

    /**
     * Standard constructor with Buffered Reader connected to Python Process Output Stream
     *
     * @param processReader Reader from Process Output Stream
     */
    PythonProcessLogReader(final BufferedReader processReader) {
        this.processReader = Objects.requireNonNull(processReader, "Reader required");
    }

    /**
     * Read lines from Process Reader and write log messages based on parsed level and named logger
     */
    @Override
    public void run() {
        final Queue<ParsedRecord> parsedRecords = new ArrayDeque<>();

        try {
            String line = processReader.readLine();
            while (line != null) {
                try {
                    processLine(line, parsedRecords);

                    if (parsedRecords.size() == 2) {
                        // Log previous record after creating a new record
                        final ParsedRecord parsedRecord = parsedRecords.remove();
                        log(parsedRecord);
                    }

                    if (!processReader.ready()) {
                        // Log queued records when Process Reader is not ready
                        while (!parsedRecords.isEmpty()) {
                            final ParsedRecord parsedRecord = parsedRecords.poll();
                            log(parsedRecord);
                        }
                    }
                } catch (final Exception e) {
                    processLogger.error("Failed to handle log from Python Process", e);
                }

                // Read and block for subsequent lines
                line = processReader.readLine();
            }
        } catch (final IOException e) {
            processLogger.error("Failed to read output of Python Process", e);
        }

        // Handle last buffered message following closure of process stream
        for (final ParsedRecord parsedRecord : parsedRecords) {
            log(parsedRecord);
        }
    }

    private void processLine(final String line, final Queue<ParsedRecord> parsedRecords) {
        final int logPrefixIndex = line.indexOf(LOG_PREFIX);
        if (logPrefixIndex == 0) {
            final String levelLogLine = line.substring(PREFIXED_LOG_LEVEL_BEGIN_INDEX);
            final String levelNumber = levelLogLine.substring(LOG_LEVEL_BEGIN_INDEX, LOG_LEVEL_END_INDEX);
            final PythonLogLevel logLevel = PYTHON_LOG_LEVELS.getOrDefault(levelNumber, PythonLogLevel.NOTSET);
            final String loggerMessage = levelLogLine.substring(MESSAGE_BEGIN_INDEX);

            final int nameSeparatorIndex = loggerMessage.indexOf(NAME_MESSAGE_SEPARATOR);
            final String message;
            final Logger logger;
            if (nameSeparatorIndex < MINIMUM_LOGGER_NAME_INDEX) {
                // Set ProcessLog when named logger not found
                logger = processLogger;
                message = loggerMessage;
            } else {
                final String loggerName = loggerMessage.substring(0, nameSeparatorIndex);
                logger = LoggerFactory.getLogger(loggerName);

                final int messageBeginIndex = nameSeparatorIndex + 1;
                message = loggerMessage.substring(messageBeginIndex);
            }

            final StringBuilder buffer = new StringBuilder(message);
            final ParsedRecord parsedRecord = new ParsedRecord(logLevel, logger, buffer);
            parsedRecords.add(parsedRecord);
        } else {
            final ParsedRecord lastRecord = parsedRecords.peek();
            if (lastRecord == null) {
                final StringBuilder buffer = new StringBuilder(line);
                final ParsedRecord parsedRecord = new ParsedRecord(PythonLogLevel.INFO, processLogger, buffer);
                parsedRecords.add(parsedRecord);
            } else if (!line.isEmpty()) {
                // Add line separator for buffering multiple lines in the same record
                lastRecord.buffer.append(LINE_SEPARATOR);
                lastRecord.buffer.append(line);
            }
        }
    }

    private void log(final ParsedRecord parsedRecord) {
        final PythonLogLevel logLevel = parsedRecord.level;
        final Logger logger = parsedRecord.logger;
        final String message = parsedRecord.buffer.toString();

        if (PythonLogLevel.DEBUG == logLevel) {
            logger.debug(message);
        } else if (PythonLogLevel.INFO == logLevel) {
            logger.info(message);
        } else if (PythonLogLevel.WARNING == logLevel) {
            logger.warn(message);
        } else if (PythonLogLevel.ERROR == logLevel) {
            logger.error(message);
        } else if (PythonLogLevel.CRITICAL == logLevel) {
            logger.error(message);
        } else {
            logger.warn(message);
        }
    }

    private record ParsedRecord(PythonLogLevel level, Logger logger, StringBuilder buffer) { }
}
