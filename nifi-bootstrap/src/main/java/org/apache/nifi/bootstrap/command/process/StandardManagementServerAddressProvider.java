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

import org.apache.nifi.bootstrap.configuration.ConfigurationProvider;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;

/**
 * Standard Provider reads optional Server Address from Configuration Provider or selects from available ports
 */
public class StandardManagementServerAddressProvider implements ManagementServerAddressProvider {
    private static final int STANDARD_PORT = 52020;

    private static final int MAXIMUM_PORT = 52050;

    private static final String NOT_AVAILABLE_MESSAGE = "Management Server Port not available in range [%d-%d]".formatted(STANDARD_PORT, MAXIMUM_PORT);

    private static final String LOCALHOST_ADDRESS = "127.0.0.1:%d";

    private static final String HOST_ADDRESS = "%s:%d";

    private final ConfigurationProvider configurationProvider;

    public StandardManagementServerAddressProvider(final ConfigurationProvider configurationProvider) {
        this.configurationProvider = Objects.requireNonNull(configurationProvider);
    }

    /**
     * Get Management Server Address with port number
     *
     * @return Management Server Address
     */
    @Override
    public Optional<String> getAddress() {
        final Optional<String> address;

        final Optional<URI> managementServerAddress = configurationProvider.getManagementServerAddress();
        if (managementServerAddress.isPresent()) {
            final URI serverAddress = managementServerAddress.get();
            final String hostAddress = HOST_ADDRESS.formatted(serverAddress.getHost(), serverAddress.getPort());
            address = Optional.of(hostAddress);
        } else {
            final int serverPort = getServerPort();
            address = Optional.of(LOCALHOST_ADDRESS.formatted(serverPort));
        }

        return address;
    }

    private int getServerPort() {
        final OptionalInt portFound = IntStream.range(STANDARD_PORT, MAXIMUM_PORT)
                .filter(StandardManagementServerAddressProvider::isPortFree)
                .findFirst();

        return portFound.orElseThrow(() -> new IllegalStateException(NOT_AVAILABLE_MESSAGE));
    }

    private static boolean isPortFree(final int port) {
        try (ServerSocket ignored = new ServerSocket(port)) {
            return true;
        } catch (final IOException e) {
            return false;
        }
    }
}
