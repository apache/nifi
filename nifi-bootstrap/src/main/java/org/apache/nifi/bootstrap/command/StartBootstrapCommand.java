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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Start Bootstrap Command executes the Run Command and monitors status
 */
class StartBootstrapCommand implements BootstrapCommand {

    private static final long MONITOR_INTERVAL = 5;

    private static final Logger logger = LoggerFactory.getLogger(StartBootstrapCommand.class);

    private final BootstrapCommand runCommand;

    private final BootstrapCommand statusCommand;

    private final Duration startWatchDelay;

    private final ScheduledExecutorService scheduledExecutorService;

    private CommandStatus commandStatus = CommandStatus.ERROR;

    StartBootstrapCommand(final BootstrapCommand runCommand, final BootstrapCommand statusCommand, final Duration startWatchDelay) {
        this.runCommand = Objects.requireNonNull(runCommand);
        this.statusCommand = Objects.requireNonNull(statusCommand);
        this.startWatchDelay = Objects.requireNonNull(startWatchDelay);

        this.scheduledExecutorService = Executors.newScheduledThreadPool(1, command -> {
            final Thread thread = new Thread(command);
            thread.setName(StartBootstrapCommand.class.getSimpleName());
            return thread;
        });
    }

    @Override
    public CommandStatus getCommandStatus() {
        return commandStatus;
    }

    @Override
    public void run() {
        runCommand.run();
        commandStatus = runCommand.getCommandStatus();

        if (CommandStatus.SUCCESS == commandStatus) {
            final StartWatchCommand startWatchCommand = new StartWatchCommand();
            scheduledExecutorService.schedule(startWatchCommand, startWatchDelay.toMillis(), TimeUnit.MILLISECONDS);
            commandStatus = CommandStatus.RUNNING;
        } else {
            scheduledExecutorService.shutdown();
        }
    }

    private class StartWatchCommand implements Runnable {

        @Override
        public void run() {
            statusCommand.run();
            final CommandStatus status = statusCommand.getCommandStatus();
            if (CommandStatus.SUCCESS == status) {
                final WatchCommand watchCommand = new WatchCommand();
                scheduledExecutorService.scheduleAtFixedRate(watchCommand, MONITOR_INTERVAL, MONITOR_INTERVAL, TimeUnit.SECONDS);
                logger.info("Application monitoring started");
            } else {
                logger.warn("Application monitoring failed with status [{}]", status);
                scheduledExecutorService.shutdown();
                commandStatus = status;
            }
        }
    }

    private class WatchCommand implements Runnable {

        @Override
        public void run() {
            statusCommand.run();
            final CommandStatus status = statusCommand.getCommandStatus();
            if (CommandStatus.SUCCESS == status) {
                logger.debug("Application running");
            } else {
                logger.warn("Application not running [{}]", status);
                runCommand.run();
            }
        }
    }
}
