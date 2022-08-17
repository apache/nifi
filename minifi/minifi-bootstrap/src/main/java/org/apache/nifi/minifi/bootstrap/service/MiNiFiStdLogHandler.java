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
package org.apache.nifi.minifi.bootstrap.service;

import static org.apache.nifi.minifi.bootstrap.service.MiNiFiStdLogHandler.LoggerType.ERROR;
import static org.apache.nifi.minifi.bootstrap.service.MiNiFiStdLogHandler.LoggerType.STDOUT;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiNiFiStdLogHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MiNiFiStdLogHandler.class);
    private static final String READ_FAILURE_MESSAGE = "Failed to read from MiNiFi's Standard {} stream";
    private static final String EXCEPTION_MESSAGE = "Exception: ";

    private final ExecutorService loggingExecutor;
    private Set<Future<?>> loggingFutures;

    public MiNiFiStdLogHandler() {
        loggingExecutor = Executors.newFixedThreadPool(2, runnable -> {
            Thread t = Executors.defaultThreadFactory().newThread(runnable);
            t.setDaemon(true);
            t.setName("MiNiFi logging handler");
            return t;
        });
    }

    public void initLogging(Process process) {
        LOGGER.debug("Initializing MiNiFi's standard output/error loggers...");
        Optional.ofNullable(loggingFutures)
            .map(Set::stream)
            .orElse(Stream.empty())
            .forEach(future -> future.cancel(false));

        Set<Future<?>> futures = new HashSet<>();
        futures.add(getFuture(process.getInputStream(), STDOUT));
        futures.add(getFuture(process.getErrorStream(), ERROR));
        loggingFutures = futures;
    }

    @NotNull
    private Future<?> getFuture(InputStream in, LoggerType loggerType) {
        return loggingExecutor.submit(() -> {
            Logger logger = LoggerFactory.getLogger(loggerType.getLoggerName());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (loggerType == ERROR) {
                        logger.error(line);
                    } else {
                        logger.info(line);
                    }
                }
            } catch (IOException e) {
                LOGGER.warn(READ_FAILURE_MESSAGE, loggerType.getDisplayName());
                LOGGER.warn(EXCEPTION_MESSAGE, e);
            }
        });
    }

    public void shutdown() {
        LOGGER.debug("Shutting down MiNiFi's standard output/error loggers...");
        loggingExecutor.shutdown();
    }

    enum LoggerType {
        STDOUT("Output", "org.apache.nifi.minifi.StdOut"),
        ERROR("Error", "org.apache.nifi.minifi.StdErr");

        final String displayName;
        final String loggerName;

        LoggerType(String displayName, String loggerName) {
            this.displayName = displayName;
            this.loggerName = loggerName;
        }

        public String getDisplayName() {
            return displayName;
        }

        public String getLoggerName() {
            return loggerName;
        }
    }
}
