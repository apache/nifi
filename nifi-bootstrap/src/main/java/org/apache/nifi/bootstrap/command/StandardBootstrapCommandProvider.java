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

import org.apache.nifi.bootstrap.command.io.BootstrapArgument;
import org.apache.nifi.bootstrap.command.io.BootstrapArgumentParser;
import org.apache.nifi.bootstrap.command.io.FileResponseStreamHandler;
import org.apache.nifi.bootstrap.command.io.LoggerResponseStreamHandler;
import org.apache.nifi.bootstrap.command.io.ResponseStreamHandler;
import org.apache.nifi.bootstrap.command.io.StandardBootstrapArgumentParser;
import org.apache.nifi.bootstrap.command.process.StandardProcessHandleProvider;
import org.apache.nifi.bootstrap.command.process.ProcessHandleProvider;
import org.apache.nifi.bootstrap.command.process.VirtualMachineProcessHandleProvider;
import org.apache.nifi.bootstrap.configuration.ApplicationClassName;
import org.apache.nifi.bootstrap.configuration.ConfigurationProvider;
import org.apache.nifi.bootstrap.configuration.StandardConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.nifi.bootstrap.command.io.HttpRequestMethod.DELETE;
import static org.apache.nifi.bootstrap.command.io.HttpRequestMethod.GET;
import static org.apache.nifi.bootstrap.configuration.ManagementServerPath.HEALTH;
import static org.apache.nifi.bootstrap.configuration.ManagementServerPath.HEALTH_CLUSTER;
import static org.apache.nifi.bootstrap.configuration.ManagementServerPath.HEALTH_DIAGNOSTICS;
import static org.apache.nifi.bootstrap.configuration.ManagementServerPath.HEALTH_STATUS_HISTORY;

/**
 * Standard implementation of Bootstrap Command Provider with parsing of supported commands
 */
public class StandardBootstrapCommandProvider implements BootstrapCommandProvider {
    private static final String SHUTDOWN_REQUESTED = "--shutdown=true";

    private static final String VERBOSE_REQUESTED = "--verbose";

    private static final String VERBOSE_QUERY = "verbose=true";

    private static final String DAYS_QUERY = "days=%d";

    private static final String EMPTY_QUERY = null;

    private static final int FIRST_ARGUMENT = 1;

    private static final int SECOND_ARGUMENT = 2;

    private static final int PATH_ARGUMENTS = 2;

    private static final int DAYS_PATH_ARGUMENTS = 3;

    private static final int DAYS_REQUESTED_DEFAULT = 1;

    private static final Duration START_WATCH_DELAY = Duration.ofSeconds(60);

    private static final BootstrapArgumentParser bootstrapArgumentParser = new StandardBootstrapArgumentParser();

    private static final Logger commandLogger = LoggerFactory.getLogger(ApplicationClassName.BOOTSTRAP_COMMAND.getName());

    /**
     * Get Bootstrap Command
     *
     * @param arguments Application arguments
     * @return Bootstrap Command to run
     */
    @Override
    public BootstrapCommand getBootstrapCommand(final String[] arguments) {
        final BootstrapCommand bootstrapCommand;

        final Optional<BootstrapArgument> bootstrapArgumentFound = bootstrapArgumentParser.getBootstrapArgument(arguments);
        if (bootstrapArgumentFound.isPresent()) {
            final BootstrapArgument bootstrapArgument = bootstrapArgumentFound.get();
            bootstrapCommand = getBootstrapCommand(bootstrapArgument, arguments);
        } else {
            bootstrapCommand = new UnknownBootstrapCommand();
        }

        return bootstrapCommand;
    }

    private BootstrapCommand getBootstrapCommand(final BootstrapArgument bootstrapArgument, final String[] arguments) {
        final ConfigurationProvider configurationProvider = new StandardConfigurationProvider(System.getenv(), System.getProperties());
        final ProcessHandleProvider processHandleProvider = getProcessHandleProvider(configurationProvider);
        final ResponseStreamHandler commandLoggerStreamHandler = new LoggerResponseStreamHandler(commandLogger);
        final BootstrapCommand stopBootstrapCommand = new StopBootstrapCommand(processHandleProvider, configurationProvider);

        final BootstrapCommand bootstrapCommand;

        if (BootstrapArgument.CLUSTER_STATUS == bootstrapArgument) {
            bootstrapCommand = new ManagementServerBootstrapCommand(processHandleProvider, HEALTH_CLUSTER, commandLoggerStreamHandler);
        } else if (BootstrapArgument.DECOMMISSION == bootstrapArgument) {
            bootstrapCommand = getDecommissionCommand(processHandleProvider, stopBootstrapCommand, arguments);
        } else if (BootstrapArgument.DIAGNOSTICS == bootstrapArgument) {
            bootstrapCommand = getDiagnosticsCommand(processHandleProvider, arguments);
        } else if (BootstrapArgument.GET_RUN_COMMAND == bootstrapArgument) {
            bootstrapCommand = new GetRunCommandBootstrapCommand(configurationProvider, processHandleProvider, System.out);
        } else if (BootstrapArgument.START == bootstrapArgument) {
            final BootstrapCommand runBootstrapCommand = new RunBootstrapCommand(configurationProvider, processHandleProvider);
            final ProcessHandle currentProcessHandle = ProcessHandle.current();
            final BootstrapCommand statusBootstrapCommand = new ApplicationProcessStatusBootstrapCommand(currentProcessHandle);
            bootstrapCommand = new StartBootstrapCommand(runBootstrapCommand, statusBootstrapCommand, START_WATCH_DELAY);
        } else if (BootstrapArgument.STATUS == bootstrapArgument) {
            bootstrapCommand = new ManagementServerBootstrapCommand(processHandleProvider, HEALTH, commandLoggerStreamHandler);
        } else if (BootstrapArgument.STATUS_HISTORY == bootstrapArgument) {
            bootstrapCommand = getStatusHistoryCommand(processHandleProvider, arguments);
        } else if (BootstrapArgument.STOP == bootstrapArgument) {
            bootstrapCommand = stopBootstrapCommand;
        } else {
            bootstrapCommand = new UnknownBootstrapCommand();
        }

        return bootstrapCommand;
    }

    private BootstrapCommand getDecommissionCommand(final ProcessHandleProvider processHandleProvider, final BootstrapCommand stopBootstrapCommand, final String[] arguments) {
        final ResponseStreamHandler responseStreamHandler = new LoggerResponseStreamHandler(commandLogger);
        final List<BootstrapCommand> bootstrapCommands = new ArrayList<>();
        final BootstrapCommand decommissionCommand = new ManagementServerBootstrapCommand(processHandleProvider, DELETE, HEALTH_CLUSTER, EMPTY_QUERY, HTTP_ACCEPTED, responseStreamHandler);
        bootstrapCommands.add(decommissionCommand);
        if (isShutdownRequested(arguments)) {
            bootstrapCommands.add(stopBootstrapCommand);
        }
        return new SequenceBootstrapCommand(bootstrapCommands);
    }

    private BootstrapCommand getDiagnosticsCommand(final ProcessHandleProvider processHandleProvider, final String[] arguments) {
        final String verboseQuery = getVerboseQuery(arguments);
        final ResponseStreamHandler responseStreamHandler = getDiagnosticsResponseStreamHandler(arguments);
        return new ManagementServerBootstrapCommand(processHandleProvider, GET, HEALTH_DIAGNOSTICS, verboseQuery, HTTP_OK, responseStreamHandler);
    }

    private ResponseStreamHandler getDiagnosticsResponseStreamHandler(final String[] arguments) {
        final ResponseStreamHandler responseStreamHandler;

        if (arguments.length == PATH_ARGUMENTS) {
            final String outputPathArgument = arguments[FIRST_ARGUMENT];
            final Path outputPath = Paths.get(outputPathArgument);
            responseStreamHandler = new FileResponseStreamHandler(outputPath);
        } else {
            final Logger logger = LoggerFactory.getLogger(StandardBootstrapCommandProvider.class);
            responseStreamHandler = new LoggerResponseStreamHandler(logger);
        }

        return responseStreamHandler;
    }

    private BootstrapCommand getStatusHistoryCommand(final ProcessHandleProvider processHandleProvider, final String[] arguments) {
        final String daysQuery = getStatusHistoryDaysQuery(arguments);
        final ResponseStreamHandler responseStreamHandler = getStatusHistoryResponseStreamHandler(arguments);
        return new ManagementServerBootstrapCommand(processHandleProvider, GET, HEALTH_STATUS_HISTORY, daysQuery, HTTP_OK, responseStreamHandler);
    }

    private boolean isShutdownRequested(final String[] arguments) {
        boolean shutdownRequested = false;

        for (final String argument : arguments) {
            if (SHUTDOWN_REQUESTED.contentEquals(argument)) {
                shutdownRequested = true;
                break;
            }
        }

        return shutdownRequested;
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

    private String getStatusHistoryDaysQuery(final String[] arguments) {
        final int daysRequested;

        if (arguments.length == DAYS_PATH_ARGUMENTS) {
            final String daysRequestArgument = arguments[FIRST_ARGUMENT];
            daysRequested = getStatusHistoryDaysRequested(daysRequestArgument);
        } else {
            daysRequested = DAYS_REQUESTED_DEFAULT;
        }

        return DAYS_QUERY.formatted(daysRequested);
    }

    private int getStatusHistoryDaysRequested(final String daysRequestArgument) {
        int daysRequested;

        try {
            daysRequested = Integer.parseInt(daysRequestArgument);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("Status History Days requested not valid");
        }

        return daysRequested;
    }

    private ResponseStreamHandler getStatusHistoryResponseStreamHandler(final String[] arguments) {
        final ResponseStreamHandler responseStreamHandler;

        if (arguments.length == PATH_ARGUMENTS) {
            final String outputPathArgument = arguments[FIRST_ARGUMENT];
            final Path outputPath = Paths.get(outputPathArgument);
            responseStreamHandler = new FileResponseStreamHandler(outputPath);
        } else if (arguments.length == DAYS_PATH_ARGUMENTS) {
            final String outputPathArgument = arguments[SECOND_ARGUMENT];
            final Path outputPath = Paths.get(outputPathArgument);
            responseStreamHandler = new FileResponseStreamHandler(outputPath);
        } else {
            final Logger logger = LoggerFactory.getLogger(StandardBootstrapCommandProvider.class);
            responseStreamHandler = new LoggerResponseStreamHandler(logger);
        }

        return responseStreamHandler;
    }

    private ProcessHandleProvider getProcessHandleProvider(final ConfigurationProvider configurationProvider) {
        final ProcessHandleProvider processHandleProvider;

        final ProcessHandle currentProcessHandle = ProcessHandle.current();
        final ProcessHandle.Info currentProcessHandleInfo = currentProcessHandle.info();
        final Optional<String[]> currentProcessArguments = currentProcessHandleInfo.arguments();

        if (currentProcessArguments.isPresent()) {
            processHandleProvider = new StandardProcessHandleProvider(configurationProvider);
        } else {
            // Use Virtual Machine Attach API when ProcessHandle does not support arguments as described in JDK-8176725
            processHandleProvider = new VirtualMachineProcessHandleProvider(configurationProvider);
        }

        return processHandleProvider;
    }
}
