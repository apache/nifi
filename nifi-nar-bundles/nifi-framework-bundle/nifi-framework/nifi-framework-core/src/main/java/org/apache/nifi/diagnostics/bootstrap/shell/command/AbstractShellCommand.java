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
package org.apache.nifi.diagnostics.bootstrap.shell.command;

import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.diagnostics.bootstrap.shell.result.ShellCommandResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

public abstract class AbstractShellCommand implements PlatformShellCommand {
    private static final Logger logger = LoggerFactory.getLogger(AbstractShellCommand.class);
    private final String name;
    private final String[] windowsCommand;
    private final String[] linuxCommand;
    private final String[] macCommand;
    private final ShellCommandResult result;

    public AbstractShellCommand(final String name, final String[] windowsCommand, final String[] linuxCommand, final String[] macCommand, final ShellCommandResult result) {
        this.name = name;
        this.windowsCommand = windowsCommand;
        this.linuxCommand = linuxCommand;
        this.macCommand = macCommand;
        this.result = result;
    }

    public String getName() {
        return name;
    }

    public String[] getCommand() {
        if (SystemUtils.IS_OS_MAC) {
            return macCommand;
        } else if (SystemUtils.IS_OS_UNIX || SystemUtils.IS_OS_LINUX) {
            return linuxCommand;
        } else if (SystemUtils.IS_OS_WINDOWS) {
            return windowsCommand;
        } else {
            throw new UnsupportedOperationException("Operating system not supported.");
        }
    }

    @Override
    public Collection<String> execute() {
        final ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(getCommand());
        try {
            final Process process = processBuilder.start();
            return result.createResult(process.getInputStream());
        } catch (UnsupportedOperationException e) {
            logger.warn(String.format("Operating system is not supported, failed to execute command: %s, ", name));
            return Collections.EMPTY_LIST;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to execute command: %s", name), e);
        }
    }
}
