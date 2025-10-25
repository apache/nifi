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

import org.apache.nifi.bootstrap.command.io.FileResponseStreamHandler;
import org.apache.nifi.bootstrap.configuration.ConfigurationProvider;
import org.apache.nifi.bootstrap.configuration.ManagementServerPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class StandardBootstrapCommandProviderTest {

    private StandardBootstrapCommandProvider provider;

    @BeforeEach
    void setProvider() {
        ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
        provider = new StandardBootstrapCommandProvider(configurationProvider);
    }

    @Test
    void testGetBootstrapCommandNull() {
        final BootstrapCommand bootstrapCommand = provider.getBootstrapCommand(null);

        assertNotNull(bootstrapCommand);
        assertInstanceOf(UnknownBootstrapCommand.class, bootstrapCommand);
    }

    @Test
    void testGetBootstrapCommandWillBeDiagnosticIfDiagnosticArgumentIsProvided() {
        final BootstrapCommand bootstrapCommand = provider.getBootstrapCommand(new String[]{"diagnostics"});

        assertNotNull(bootstrapCommand);
        assertInstanceOf(ManagementServerBootstrapCommand.class, bootstrapCommand);

        ManagementServerBootstrapCommand command = (ManagementServerBootstrapCommand) bootstrapCommand;
        assertEquals(ManagementServerPath.HEALTH_DIAGNOSTICS, command.getManagementServerPath());
    }

    @Test
    void testThatDiagnosticCommandWillSavetoFileIfFileParameterIsProvided() {
        final BootstrapCommand bootstrapCommand = provider.getBootstrapCommand(new String[]{"diagnostics", "/tmp/file-to-save"});

        assertNotNull(bootstrapCommand);
        assertInstanceOf(ManagementServerBootstrapCommand.class, bootstrapCommand);

        ManagementServerBootstrapCommand command = (ManagementServerBootstrapCommand) bootstrapCommand;
        assertEquals(ManagementServerPath.HEALTH_DIAGNOSTICS, command.getManagementServerPath());
        assertInstanceOf(FileResponseStreamHandler.class, command.getResponseStreamHandler());
    }

    static Stream<Arguments> diagnosticsArguments() {
        return Stream.of(
                Arguments.of((Object) (new String[]{"diagnostics", "--verbose", "/tmp/file-to-save"})),
                Arguments.of((Object) (new String[]{"diagnostics", "/tmp/file-to-save", "--verbose" }))
        );
    }

    @ParameterizedTest
    @MethodSource("diagnosticsArguments")
    void testThatDiagnosticCommandWillSavetoFileIfFileParameterIsProvidedAndVerboseIsUsed(String[] arguments) {
        final BootstrapCommand bootstrapCommand = provider.getBootstrapCommand(arguments);

        assertNotNull(bootstrapCommand);
        assertInstanceOf(ManagementServerBootstrapCommand.class, bootstrapCommand);

        ManagementServerBootstrapCommand command = (ManagementServerBootstrapCommand) bootstrapCommand;
        assertEquals(ManagementServerPath.HEALTH_DIAGNOSTICS, command.getManagementServerPath());
        assertInstanceOf(FileResponseStreamHandler.class, command.getResponseStreamHandler());
    }
}
