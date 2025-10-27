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
package org.apache.nifi.bootstrap.command;

import org.apache.nifi.bootstrap.command.io.FileResponseStreamHandler;
import org.apache.nifi.bootstrap.command.io.LoggerResponseStreamHandler;
import org.apache.nifi.bootstrap.command.io.ResponseStreamHandler;
import org.apache.nifi.bootstrap.command.process.ProcessHandleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.nifi.bootstrap.command.io.HttpRequestMethod.GET;
import static org.apache.nifi.bootstrap.configuration.ManagementServerPath.HEALTH_DIAGNOSTICS;

/**
 * Diagnostics implementation of Bootstrap Command Provider supporting optional verbose and optional path arguments
 */
class DiagnosticsBootstrapCommandProvider implements BootstrapCommandProvider {
    private static final String VERBOSE_REQUESTED = "--verbose";

    private static final String VERBOSE_QUERY = "verbose=true";

    private static final int FIRST_ARGUMENT = 1;

    private final ProcessHandleProvider processHandleProvider;

    DiagnosticsBootstrapCommandProvider(final ProcessHandleProvider processHandleProvider) {
        this.processHandleProvider = Objects.requireNonNull(processHandleProvider, "Process Handle Provider required");
    }

    @Override
    public BootstrapCommand getBootstrapCommand(final String[] arguments) {
        final String verboseQuery = getVerboseQuery(arguments);
        final ResponseStreamHandler responseStreamHandler = getDiagnosticsResponseStreamHandler(arguments);
        return new ManagementServerBootstrapCommand(processHandleProvider, GET, HEALTH_DIAGNOSTICS, verboseQuery, HTTP_OK, responseStreamHandler);
    }

    private String getVerboseQuery(final String[] arguments) {
        String query = null;

        for (final String argument : arguments) {
            if (VERBOSE_REQUESTED.contentEquals(argument)) {
                query = VERBOSE_QUERY;
                break;
            }
        }

        return query;
    }

    private ResponseStreamHandler getDiagnosticsResponseStreamHandler(final String[] arguments) {
        final ResponseStreamHandler responseStreamHandler;

        final Optional<Path> outputPathFound = Arrays.stream(arguments)
                .skip(FIRST_ARGUMENT)
                .filter(Predicate.not(VERBOSE_REQUESTED::contentEquals))
                .findFirst()
                .map(Paths::get);

        if (outputPathFound.isPresent()) {
            final Path outputPath = outputPathFound.get();
            responseStreamHandler = new FileResponseStreamHandler(outputPath);
        } else {
            final Logger logger = LoggerFactory.getLogger(StandardBootstrapCommandProvider.class);
            responseStreamHandler = new LoggerResponseStreamHandler(logger);
        }

        return responseStreamHandler;
    }
}
