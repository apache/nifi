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

import org.apache.nifi.bootstrap.command.process.ManagementServerAddressProvider;
import org.apache.nifi.bootstrap.command.process.ProcessBuilderProvider;
import org.apache.nifi.bootstrap.command.process.ProcessHandleProvider;
import org.apache.nifi.bootstrap.command.process.StandardManagementServerAddressProvider;
import org.apache.nifi.bootstrap.command.process.StandardProcessBuilderProvider;
import org.apache.nifi.bootstrap.configuration.ConfigurationProvider;
import org.apache.nifi.bootstrap.property.ApplicationPropertyHandler;
import org.apache.nifi.bootstrap.property.SecurityApplicationPropertyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Bootstrap Command to get the command arguments for running the application
 */
class GetRunCommandBootstrapCommand implements BootstrapCommand {

    private static final String SPACE_SEPARATOR = " ";

    private static final Logger logger = LoggerFactory.getLogger(GetRunCommandBootstrapCommand.class);

    private final ConfigurationProvider configurationProvider;

    private final ProcessHandleProvider processHandleProvider;

    private final ManagementServerAddressProvider managementServerAddressProvider;

    private final PrintStream outputStream;

    private CommandStatus commandStatus = CommandStatus.ERROR;

    public GetRunCommandBootstrapCommand(final ConfigurationProvider configurationProvider, final ProcessHandleProvider processHandleProvider, final PrintStream outputStream) {
        this.configurationProvider = Objects.requireNonNull(configurationProvider);
        this.processHandleProvider = Objects.requireNonNull(processHandleProvider);
        this.outputStream = Objects.requireNonNull(outputStream);
        this.managementServerAddressProvider = new StandardManagementServerAddressProvider(configurationProvider);
    }

    @Override
    public CommandStatus getCommandStatus() {
        return commandStatus;
    }

    @Override
    public void run() {
        try {
            final Optional<ProcessHandle> applicationProcessHandle = processHandleProvider.findApplicationProcessHandle();

            if (applicationProcessHandle.isEmpty()) {
                final ApplicationPropertyHandler securityApplicationPropertyHandler = new SecurityApplicationPropertyHandler(logger);
                securityApplicationPropertyHandler.handleProperties(configurationProvider.getApplicationProperties());

                final ProcessBuilderProvider processBuilderProvider = new StandardProcessBuilderProvider(configurationProvider, managementServerAddressProvider);
                final ProcessBuilder processBuilder = processBuilderProvider.getApplicationProcessBuilder();
                final List<String> command = processBuilder.command();
                final String processCommand = String.join(SPACE_SEPARATOR, command);
                outputStream.println(processCommand);

                commandStatus = CommandStatus.SUCCESS;
            } else {
                logger.info("Application Process [{}] running", applicationProcessHandle.get().pid());
                commandStatus = CommandStatus.ERROR;
            }
        } catch (final Throwable e) {
            logger.warn("Application Process command building failed", e);
            commandStatus = CommandStatus.FAILED;
        }
    }
}
