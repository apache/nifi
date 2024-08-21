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

import org.apache.nifi.bootstrap.command.process.ProcessHandleProvider;
import org.apache.nifi.bootstrap.configuration.ApplicationClassName;
import org.apache.nifi.bootstrap.configuration.ConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Bootstrap Command to run for stopping application
 */
class StopBootstrapCommand implements BootstrapCommand {

    private static final Duration FORCE_TERMINATION_TIMEOUT = Duration.ofSeconds(5);

    private static final Logger logger = LoggerFactory.getLogger(ApplicationClassName.BOOTSTRAP_COMMAND.getName());

    private final ProcessHandleProvider processHandleProvider;

    private final ConfigurationProvider configurationProvider;

    private CommandStatus commandStatus = CommandStatus.ERROR;

    StopBootstrapCommand(final ProcessHandleProvider processHandleProvider, final ConfigurationProvider configurationProvider) {
        this.processHandleProvider = Objects.requireNonNull(processHandleProvider);
        this.configurationProvider = Objects.requireNonNull(configurationProvider);
    }

    @Override
    public CommandStatus getCommandStatus() {
        return commandStatus;
    }

    @Override
    public void run() {
        final Optional<ProcessHandle> processHandle = processHandleProvider.findApplicationProcessHandle();

        if (processHandle.isEmpty()) {
            commandStatus = CommandStatus.SUCCESS;
            logger.info("Application Process not running");
        } else {
            stopBootstrapProcess();
            destroy(processHandle.get());
        }
    }

    private void stopBootstrapProcess() {
        final Optional<ProcessHandle> bootstrapProcessHandleFound = processHandleProvider.findBootstrapProcessHandle();
        if (bootstrapProcessHandleFound.isPresent()) {
            final ProcessHandle bootstrapProcessHandle = bootstrapProcessHandleFound.get();

            final boolean destroyRequested = bootstrapProcessHandle.destroy();
            final long pid = bootstrapProcessHandle.pid();
            if (destroyRequested) {
                logger.info("Bootstrap Process [{}] termination requested", pid);
                onBootstrapDestroyCompleted(bootstrapProcessHandle);
            } else {
                logger.warn("Bootstrap Process [{}] termination request failed", pid);
            }
        }
    }

    private void onBootstrapDestroyCompleted(final ProcessHandle bootstrapProcessHandle) {
        final long pid = bootstrapProcessHandle.pid();
        final CompletableFuture<ProcessHandle> onExitHandle = bootstrapProcessHandle.onExit();
        try {
            final ProcessHandle completedProcessHandle = onExitHandle.get(FORCE_TERMINATION_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            logger.info("Bootstrap Process [{}] termination completed", completedProcessHandle.pid());
        } catch (final Exception e) {
            logger.warn("Bootstrap Process [{}] termination failed", pid);
        }
    }

    private void destroy(final ProcessHandle applicationProcessHandle) {
        final boolean destroyRequested = applicationProcessHandle.destroy();
        logger.info("Application Process [{}] termination requested", applicationProcessHandle.pid());
        if (destroyRequested) {
            onDestroyCompleted(applicationProcessHandle);
        } else {
            logger.warn("Application Process [{}] termination request failed", applicationProcessHandle.pid());
            destroyForcibly(applicationProcessHandle);
        }
    }

    private void destroyForcibly(final ProcessHandle applicationProcessHandle) {
        final boolean destroyForciblyRequested = applicationProcessHandle.destroyForcibly();
        if (destroyForciblyRequested) {
            logger.warn("Application Process [{}] force termination failed", applicationProcessHandle.pid());
        } else {
            onDestroyForciblyCompleted(applicationProcessHandle);
        }
    }

    private void onDestroyCompleted(final ProcessHandle applicationProcessHandle) {
        final long pid = applicationProcessHandle.pid();
        final CompletableFuture<ProcessHandle> onExitHandle = applicationProcessHandle.onExit();
        final Duration gracefulShutdownTimeout = configurationProvider.getGracefulShutdownTimeout();
        try {
            final ProcessHandle completedProcessHandle = onExitHandle.get(gracefulShutdownTimeout.toSeconds(), TimeUnit.SECONDS);
            logger.info("Application Process [{}] termination completed", completedProcessHandle.pid());
            commandStatus = CommandStatus.SUCCESS;
        } catch (final Exception e) {
            logger.warn("Application Process [{}] termination failed", pid);
            destroyForcibly(applicationProcessHandle);
        }
    }

    private void onDestroyForciblyCompleted(final ProcessHandle applicationProcessHandle) {
        final long pid = applicationProcessHandle.pid();
        final CompletableFuture<ProcessHandle> onExitHandle = applicationProcessHandle.onExit();
        try {
            final ProcessHandle completedProcessHandle = onExitHandle.get(FORCE_TERMINATION_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            logger.warn("Application Process [{}] force termination completed", completedProcessHandle.pid());
            commandStatus = CommandStatus.SUCCESS;
        } catch (final Exception e) {
            logger.warn("Application Process [{}] force termination request failed", pid);
            commandStatus = CommandStatus.ERROR;
        }
    }
}
