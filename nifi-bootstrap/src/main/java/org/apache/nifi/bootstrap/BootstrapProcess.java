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
package org.apache.nifi.bootstrap;

import org.apache.nifi.bootstrap.command.BootstrapCommand;
import org.apache.nifi.bootstrap.command.BootstrapCommandProvider;
import org.apache.nifi.bootstrap.command.CommandStatus;
import org.apache.nifi.bootstrap.command.StandardBootstrapCommandProvider;
import org.apache.nifi.bootstrap.configuration.ApplicationClassName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bootstrap Process responsible for reading configuration and maintaining application status
 */
public class BootstrapProcess {
    /**
     * Start Application
     *
     * @param arguments Array of arguments
     */
    public static void main(final String[] arguments) {
        final BootstrapCommandProvider bootstrapCommandProvider = new StandardBootstrapCommandProvider();
        final BootstrapCommand bootstrapCommand = bootstrapCommandProvider.getBootstrapCommand(arguments);
        run(bootstrapCommand);
    }

    private static void run(final BootstrapCommand bootstrapCommand) {
        bootstrapCommand.run();
        final CommandStatus commandStatus = bootstrapCommand.getCommandStatus();
        if (CommandStatus.RUNNING == commandStatus) {
            final Logger logger = LoggerFactory.getLogger(ApplicationClassName.BOOTSTRAP_COMMAND.getName());
            logger.info("Bootstrap Process Running");
        } else {
            final int status = commandStatus.getStatus();
            System.exit(status);
        }
    }
}
