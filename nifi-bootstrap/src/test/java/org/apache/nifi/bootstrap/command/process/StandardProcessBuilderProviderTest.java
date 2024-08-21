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

import org.apache.nifi.bootstrap.configuration.ApplicationClassName;
import org.apache.nifi.bootstrap.configuration.ConfigurationProvider;
import org.apache.nifi.bootstrap.configuration.SystemProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardProcessBuilderProviderTest {
    private static final String SERVER_ADDRESS = "127.0.0.1:52020";

    private static final String SERVER_ADDRESS_PROPERTY = "-D%s=%s".formatted(SystemProperty.MANAGEMENT_SERVER_ADDRESS.getProperty(), SERVER_ADDRESS);

    @Mock
    private ConfigurationProvider configurationProvider;

    @Mock
    private ManagementServerAddressProvider managementServerAddressProvider;

    private StandardProcessBuilderProvider provider;

    @BeforeEach
    void setProvider() {
        provider = new StandardProcessBuilderProvider(configurationProvider, managementServerAddressProvider);
    }

    @Test
    void testGetApplicationProcessBuilder(@TempDir final Path workingDirectory) {
        when(configurationProvider.getLibraryDirectory()).thenReturn(workingDirectory);
        when(configurationProvider.getConfigurationDirectory()).thenReturn(workingDirectory);
        when(managementServerAddressProvider.getAddress()).thenReturn(Optional.of(SERVER_ADDRESS));

        final ProcessBuilder processBuilder = provider.getApplicationProcessBuilder();

        assertNotNull(processBuilder);

        final List<String> command = processBuilder.command();

        final String currentCommand = ProcessHandle.current().info().command().orElse(null);
        final String firstCommand = command.getFirst();
        assertEquals(currentCommand, firstCommand);

        final String lastCommand = command.getLast();
        assertEquals(ApplicationClassName.APPLICATION.getName(), lastCommand);

        assertTrue(command.contains(SERVER_ADDRESS_PROPERTY));
    }
}
