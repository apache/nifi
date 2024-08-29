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

import org.apache.nifi.bootstrap.command.process.ProcessHandleProvider;
import org.apache.nifi.bootstrap.configuration.ApplicationClassName;
import org.apache.nifi.bootstrap.configuration.ConfigurationProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GetRunCommandBootstrapCommandTest {
    private static final String CONFIGURATION_DIRECTORY = "conf";

    private static final String SPACE_SEPARATOR = " ";

    @Mock
    private ConfigurationProvider configurationProvider;

    @Mock
    private ProcessHandleProvider processHandleProvider;

    @Test
    void testRun(@TempDir final Path workingDirectory) throws IOException {
        final Path configurationDirectory = workingDirectory.resolve(CONFIGURATION_DIRECTORY);
        assertTrue(configurationDirectory.toFile().mkdir());

        final Path applicationProperties = configurationDirectory.resolve(Properties.class.getSimpleName());
        Files.writeString(applicationProperties, SPACE_SEPARATOR);

        when(configurationProvider.getApplicationProperties()).thenReturn(applicationProperties);
        when(configurationProvider.getLibraryDirectory()).thenReturn(workingDirectory);
        when(configurationProvider.getConfigurationDirectory()).thenReturn(configurationDirectory);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final PrintStream printStream = new PrintStream(outputStream);

        final GetRunCommandBootstrapCommand command = new GetRunCommandBootstrapCommand(configurationProvider, processHandleProvider, printStream);
        command.run();

        final CommandStatus commandStatus = command.getCommandStatus();

        assertNotNull(commandStatus);
        assertEquals(CommandStatus.SUCCESS, commandStatus);

        final String runCommand = outputStream.toString().trim();
        final List<String> runCommands = List.of(runCommand.split(SPACE_SEPARATOR));

        final String lastCommand = runCommands.getLast();
        assertEquals(ApplicationClassName.APPLICATION.getName(), lastCommand);
    }
}
