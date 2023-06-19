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
package org.apache.nifi.diagnostics.bootstrap.shell;

import org.apache.nifi.diagnostics.bootstrap.shell.command.AbstractShellCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class ShellCommandExecutor {
    private static final Logger logger = LoggerFactory.getLogger(ShellCommandExecutor.class);

    public static Collection<String> execute(final AbstractShellCommand command) {
        final ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(command.getCommand());
        try {
            final Process process = processBuilder.start();
            return command.getParser().createResult(process);
        } catch (IndexOutOfBoundsException e) {
            logger.warn(String.format("Operating system is not supported, failed to execute command: %s, ", command.getName()));
            return Collections.EMPTY_LIST;
        } catch (IOException | NullPointerException e) {
            throw new RuntimeException(String.format("Failed to execute command: %s", command.getName()), e);
        }
    }
}
