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
package org.apache.nifi.bootstrap.command.process;

import org.apache.nifi.bootstrap.configuration.SystemProperty;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provider implementation resolves the Management Server Address from command arguments of the application Process Handle
 */
public class ProcessHandleManagementServerAddressProvider implements ManagementServerAddressProvider {
    private static final Pattern ADDRESS_ARGUMENT_PATTERN = Pattern.compile("^-D%s=(.+?)$".formatted(SystemProperty.MANAGEMENT_SERVER_ADDRESS.getProperty()));

    private static final int ADDRESS_GROUP = 1;

    private final ProcessHandle processHandle;

    public ProcessHandleManagementServerAddressProvider(final ProcessHandle processHandle) {
        this.processHandle = Objects.requireNonNull(processHandle);
    }

    /**
     * Get Management Server Address with port number from command argument in Process Handle
     *
     * @return Management Server Address or null when not found
     */
    @Override
    public Optional<String> getAddress() {
        final ProcessHandle.Info info = processHandle.info();

        final String managementServerAddress;

        final Optional<String[]> argumentsFound = info.arguments();
        if (argumentsFound.isPresent()) {
            final String[] arguments = argumentsFound.get();
            managementServerAddress = findManagementServerAddress(arguments);
        } else {
            managementServerAddress = null;
        }

        return Optional.ofNullable(managementServerAddress);
    }

    private String findManagementServerAddress(final String[] arguments) {
        String managementServerAddress = null;

        for (final String argument : arguments) {
            final Matcher matcher = ADDRESS_ARGUMENT_PATTERN.matcher(argument);
            if (matcher.matches()) {
                managementServerAddress = matcher.group(ADDRESS_GROUP);
                break;
            }
        }

        return managementServerAddress;
    }
}
