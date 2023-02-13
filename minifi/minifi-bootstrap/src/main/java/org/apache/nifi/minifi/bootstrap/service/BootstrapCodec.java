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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Optional;
import org.apache.nifi.minifi.bootstrap.RunMiNiFi;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeListener;
import org.apache.nifi.minifi.bootstrap.exception.InvalidCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootstrapCodec {

    private static final String TRUE = Boolean.TRUE.toString();
    private static final String FALSE = Boolean.FALSE.toString();

    private final RunMiNiFi runner;
    private final Logger logger = LoggerFactory.getLogger(BootstrapCodec.class);
    private final UpdatePropertiesService updatePropertiesService;
    private final UpdateConfigurationService updateConfigurationService;

    public BootstrapCodec(RunMiNiFi runner, BootstrapFileProvider bootstrapFileProvider, ConfigurationChangeListener configurationChangeListener) {
        this.runner = runner;
        this.updatePropertiesService = new UpdatePropertiesService(runner, logger, bootstrapFileProvider);
        this.updateConfigurationService = new UpdateConfigurationService(runner, configurationChangeListener, bootstrapFileProvider);
    }

    public void communicate(InputStream in, OutputStream out) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));

        String line = reader.readLine();
        String[] splits = Optional.ofNullable(line).map(l -> l.split(" ")).orElse(new String[0]);
        if (splits.length == 0) {
            throw new IOException("Received invalid command from MiNiFi: " + line);
        }

        BootstrapCommand cmd = BootstrapCommand.safeValueOf(splits[0]);
        String[] args;
        if (splits.length == 1) {
            args = new String[0];
        } else {
            args = Arrays.copyOfRange(splits, 1, splits.length);
        }

        try {
            processRequest(cmd, args, writer);
        } catch (InvalidCommandException exception) {
            throw new IOException("Received invalid command from MiNiFi: " + line, exception);
        }
    }

    private void processRequest(BootstrapCommand cmd, String[] args, BufferedWriter writer) throws InvalidCommandException, IOException {
        switch (cmd) {
            case PORT:
                handlePortCommand(args, writer);
                break;
            case STARTED:
                handleStartedCommand(args, writer);
                break;
            case SHUTDOWN:
                handleShutDownCommand(writer);
                break;
            case RELOAD:
                handleReloadCommand(writer);
                break;
            case UPDATE_PROPERTIES:
                handlePropertiesUpdateCommand(writer);
                break;
            case UPDATE_CONFIGURATION:
                handleUpdateConfigurationCommand(writer);
                break;
            default:
                throw new InvalidCommandException("Unknown command: " + cmd);
        }
    }

    private void handleUpdateConfigurationCommand(BufferedWriter writer) throws IOException {
        logger.debug("Received 'UPDATE_CONFIGURATION' command from MINIFI");
        writeOk(writer);
        runner.setCommandInProgress(true);
        updateConfigurationService.handleUpdate().ifPresent(runner::sendAcknowledgeToMiNiFi);
    }

    private void handlePropertiesUpdateCommand(BufferedWriter writer) throws IOException {
        logger.debug("Received 'UPDATE_PROPERTIES' command from MINIFI");
        writeOk(writer);
        runner.setCommandInProgress(true);
        updatePropertiesService.handleUpdate().ifPresent(runner::sendAcknowledgeToMiNiFi);
    }

    private void handleReloadCommand(BufferedWriter writer) throws IOException {
        logger.debug("Received 'RELOAD' command from MINIFI");
        writeOk(writer);
    }

    private void handleShutDownCommand(BufferedWriter writer) throws IOException {
        logger.debug("Received 'SHUTDOWN' command from MINIFI");
        writeOk(writer);
        runner.shutdownChangeNotifier();
        runner.getPeriodicStatusReporterManager().shutdownPeriodicStatusReporters();
    }

    private void handleStartedCommand(String[] args, BufferedWriter writer) throws InvalidCommandException, IOException {
        logger.info("Received 'STARTED' command from MINIFI");
        if (args.length != 1) {
            throw new InvalidCommandException("STARTED command must contain a status argument");
        }

        if (!TRUE.equalsIgnoreCase(args[0]) && !FALSE.equalsIgnoreCase(args[0])) {
            throw new InvalidCommandException("Invalid status for STARTED command; should be true or false, but was '" + args[0] + "'");
        }

        writeOk(writer);
        runner.getPeriodicStatusReporterManager().shutdownPeriodicStatusReporters();
        runner.getPeriodicStatusReporterManager().startPeriodicNotifiers();
        runner.getConfigurationChangeCoordinator().start();

        runner.setNiFiStarted(Boolean.parseBoolean(args[0]));
    }

    private void handlePortCommand(String[] args, BufferedWriter writer) throws InvalidCommandException, IOException {
        logger.debug("Received 'PORT' command from MINIFI");
        if (args.length != 2) {
            throw new InvalidCommandException("PORT command must contain the port and secretKey arguments");
        }

        int port;
        try {
            port = Integer.parseInt(args[0]);
        } catch (NumberFormatException nfe) {
            throw new InvalidCommandException("Invalid Port number; should be integer between 1 and 65535");
        }

        if (port < 1 || port > 65535) {
            throw new InvalidCommandException("Invalid Port number; should be integer between 1 and 65535");
        }

        writeOk(writer);
        runner.setMiNiFiParameters(port, args[1]);
    }

    private void writeOk(BufferedWriter writer) throws IOException {
        writer.write("OK");
        writer.newLine();
        writer.flush();
    }

    private enum BootstrapCommand {
        PORT, STARTED, SHUTDOWN, RELOAD, UPDATE_PROPERTIES, UPDATE_CONFIGURATION, UNKNOWN;

        public static BootstrapCommand safeValueOf(String value) {
            try {
                return BootstrapCommand.valueOf(value);
            } catch (IllegalArgumentException e) {
                return UNKNOWN;
            }
        }
    }
}
