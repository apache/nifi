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

import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.nifi.bootstrap.command.process.ManagementServerAddressProvider;
import org.apache.nifi.bootstrap.command.process.ProcessBuilderProvider;
import org.apache.nifi.bootstrap.command.process.ProcessHandleProvider;
import org.apache.nifi.bootstrap.command.process.StandardManagementServerAddressProvider;
import org.apache.nifi.bootstrap.command.process.StandardProcessBuilderProvider;
import org.apache.nifi.bootstrap.configuration.ApplicationClassName;
import org.apache.nifi.bootstrap.configuration.ConfigurationProvider;
import org.apache.nifi.bootstrap.process.RuntimeValidatorExecutor;
import org.apache.nifi.bootstrap.property.ApplicationPropertyHandler;
import org.apache.nifi.bootstrap.property.SecurityApplicationPropertyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Objects;
import java.util.Optional;

/**
 * Bootstrap Command to run the application
 */
class RunBootstrapCommand implements BootstrapCommand {

    private static final String SPACE_SEPARATOR = " ";

    private static final Logger commandLogger = LoggerFactory.getLogger(ApplicationClassName.BOOTSTRAP_COMMAND.getName());

    private static final Logger logger = LoggerFactory.getLogger(RunBootstrapCommand.class);

    private static final RuntimeValidatorExecutor runtimeValidatorExecutor = new RuntimeValidatorExecutor();

    private final ConfigurationProvider configurationProvider;

    private final ProcessHandleProvider processHandleProvider;

    private final ManagementServerAddressProvider managementServerAddressProvider;

    private CommandStatus commandStatus = CommandStatus.ERROR;

    public RunBootstrapCommand(final ConfigurationProvider configurationProvider, final ProcessHandleProvider processHandleProvider) {
        this.configurationProvider = Objects.requireNonNull(configurationProvider);
        this.processHandleProvider = Objects.requireNonNull(processHandleProvider);
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
                writePlatformProperties();

                runtimeValidatorExecutor.execute();

                final ApplicationPropertyHandler securityApplicationPropertyHandler = new SecurityApplicationPropertyHandler(logger);
                securityApplicationPropertyHandler.handleProperties(configurationProvider.getApplicationProperties());

                final ProcessBuilderProvider processBuilderProvider = new StandardProcessBuilderProvider(configurationProvider, managementServerAddressProvider);

                final ProcessBuilder processBuilder = processBuilderProvider.getApplicationProcessBuilder();
                processBuilder.inheritIO();

                final String command = String.join(SPACE_SEPARATOR, processBuilder.command());
                logger.info(command);

                final Process process = processBuilder.start();
                if (process.isAlive()) {
                    commandStatus = CommandStatus.SUCCESS;
                    commandLogger.info("Application Process [{}] started", process.pid());
                } else {
                    commandStatus = CommandStatus.STOPPED;
                    commandLogger.error("Application Process [{}] start failed", process.pid());
                }
            } else {
                commandLogger.info("Application Process [{}] running", applicationProcessHandle.get().pid());
                commandStatus = CommandStatus.ERROR;
            }
        } catch (final Throwable e) {
            commandLogger.warn("Application Process run failed", e);
            commandStatus = CommandStatus.FAILED;
        }
    }

    private void writePlatformProperties() {
        final Runtime.Version version = Runtime.version();
        logger.info("Java Version: {}", version);

        final Runtime runtime = Runtime.getRuntime();
        logger.info("Available Processors: {}", runtime.availableProcessors());

        final OperatingSystemMXBean operatingSystem = ManagementFactory.getOperatingSystemMXBean();
        if (operatingSystem instanceof UnixOperatingSystemMXBean unixOperatingSystem) {
            logger.info("Total Memory: {}", unixOperatingSystem.getTotalMemorySize());
            logger.info("Maximum File Descriptors: {}", unixOperatingSystem.getMaxFileDescriptorCount());
        }
    }
}
