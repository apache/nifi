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
package org.apache.nifi.toolkit.config;

import org.apache.nifi.toolkit.config.command.RegistryEncryptConfig;
import org.apache.nifi.toolkit.config.command.StandardEncryptConfig;
import picocli.CommandLine;

import java.util.Arrays;

/**
 * Encrypt Config Command launcher for Command Line implementation
 */
public class EncryptConfigCommand {
    private static final String NIFI_REGISTRY_ARGUMENT = "--nifiRegistry";

    /**
     * Main command method launches Picocli Command Line implementation of Encrypt Config
     *
     * @param arguments Command line arguments
     */
    public static void main(final String[] arguments) {
        final Object command;

        final String[] filteredArguments;
        if (isRegistryModeRequested(arguments)) {
            command = new RegistryEncryptConfig();
            filteredArguments = getFilteredArguments(arguments);
        } else {
            command = new StandardEncryptConfig();
            filteredArguments = arguments;
        }

        final CommandLine commandLine = new CommandLine(command);
        if (filteredArguments.length == 0) {
            commandLine.usage(System.out);
        } else {
            final int status = commandLine.execute(filteredArguments);
            System.exit(status);
        }
    }

    private static boolean isRegistryModeRequested(final String[] arguments) {
        boolean registryModeRequested = false;

        for (final String argument : arguments) {
            if (NIFI_REGISTRY_ARGUMENT.equals(argument)) {
                registryModeRequested = true;
                break;
            }
        }

        return registryModeRequested;
    }

    private static String[] getFilteredArguments(final String[] arguments) {
        return Arrays.stream(arguments)
                .filter(argument -> !NIFI_REGISTRY_ARGUMENT.equals(argument))
                .toList()
                .toArray(new String[]{});
    }
}
