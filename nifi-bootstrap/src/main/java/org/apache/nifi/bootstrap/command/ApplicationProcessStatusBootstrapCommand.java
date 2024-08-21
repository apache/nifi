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

import java.util.Objects;
import java.util.Optional;

/**
 * Bootstrap Command to return the status of the application process as a child of the bootstrap process
 */
class ApplicationProcessStatusBootstrapCommand implements BootstrapCommand {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationProcessStatusBootstrapCommand.class);

    private final ProcessHandle processHandle;

    private CommandStatus commandStatus = CommandStatus.ERROR;

    ApplicationProcessStatusBootstrapCommand(final ProcessHandle processHandle) {
        this.processHandle = Objects.requireNonNull(processHandle);
    }

    @Override
    public CommandStatus getCommandStatus() {
        return commandStatus;
    }

    @Override
    public void run() {
        final Optional<ProcessHandle> childProcessHandleFound = processHandle.children().findFirst();

        if (childProcessHandleFound.isEmpty()) {
            logger.info("Application Process not found");
            commandStatus = CommandStatus.STOPPED;
        } else {
            final ProcessHandle childProcessHandle = childProcessHandleFound.get();
            if (childProcessHandle.isAlive()) {
                logger.info("Application Process [{}] running", childProcessHandle.pid());
                commandStatus = CommandStatus.SUCCESS;
            } else {
                logger.info("Application Process [{}] stopped", childProcessHandle.pid());
                commandStatus = CommandStatus.COMMUNICATION_FAILED;
            }
        }
    }
}
