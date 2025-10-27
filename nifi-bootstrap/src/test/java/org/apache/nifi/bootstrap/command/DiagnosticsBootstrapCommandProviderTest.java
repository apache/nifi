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

import com.sun.net.httpserver.HttpServer;
import org.apache.nifi.bootstrap.command.process.ProcessHandleProvider;
import org.apache.nifi.bootstrap.configuration.ManagementServerPath;
import org.apache.nifi.bootstrap.configuration.SystemProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DiagnosticsBootstrapCommandProviderTest {
    private static final String JAVA_COMMAND = "java";

    private static final String VERBOSE_ARGUMENT = "--verbose";

    private static final String[] EMPTY_ARGUMENTS = new String[0];

    private static final String[] VERBOSE_ARGUMENTS = new String[]{JAVA_COMMAND, VERBOSE_ARGUMENT};

    private static final URI DIAGNOSTICS_URI = URI.create(ManagementServerPath.HEALTH_DIAGNOSTICS.getPath());

    private static final URI DIAGNOSTICS_VERBOSE_URI = URI.create("%s?verbose=true".formatted(ManagementServerPath.HEALTH_DIAGNOSTICS.getPath()));

    private static final String MANAGEMENT_SERVER_ARGUMENT_FORMAT = "-D%s=127.0.0.1:%d";

    private static final String LOCALHOST_ADDRESS = "127.0.0.1";

    private static final int RANDOM_PORT = 0;

    private static final int BACKLOG = 0;

    private static final int EMPTY_CONTENT_LENGTH = -1;

    private final List<URI> requestUris = new ArrayList<>();

    @Mock
    private ProcessHandleProvider processHandleProvider;

    @Mock
    private ProcessHandle processHandle;

    @Mock
    private ProcessHandle.Info processHandleInfo;

    private HttpServer httpServer;

    private DiagnosticsBootstrapCommandProvider provider;

    @BeforeEach
    void setProvider() throws IOException {
        final InetSocketAddress bindAddress = new InetSocketAddress(LOCALHOST_ADDRESS, RANDOM_PORT);
        httpServer = HttpServer.create(bindAddress, BACKLOG);
        httpServer.createContext(ManagementServerPath.HEALTH_DIAGNOSTICS.getPath(), exchange -> {
            final URI requestUri = exchange.getRequestURI();
            requestUris.add(requestUri);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, EMPTY_CONTENT_LENGTH);
        });
        httpServer.start();

        provider = new DiagnosticsBootstrapCommandProvider(processHandleProvider);
    }

    @AfterEach
    void stopServer() {
        httpServer.stop(0);
    }

    @Test
    void testGetBootstrapCommandEmptyArgumentsRunStopped() {
        final BootstrapCommand bootstrapCommand = provider.getBootstrapCommand(EMPTY_ARGUMENTS);

        assertNotNull(bootstrapCommand);
        assertInstanceOf(ManagementServerBootstrapCommand.class, bootstrapCommand);

        bootstrapCommand.run();
        final CommandStatus commandStatus = bootstrapCommand.getCommandStatus();
        assertEquals(CommandStatus.STOPPED, commandStatus);
    }

    @Test
    void testGetBootstrapCommandEmptyArgumentsRunSuccess() {
        setProcessHandle();

        final BootstrapCommand bootstrapCommand = provider.getBootstrapCommand(EMPTY_ARGUMENTS);

        assertCommandStatusSuccess(bootstrapCommand);

        final URI firstUri = requestUris.getFirst();
        assertEquals(DIAGNOSTICS_URI, firstUri);
    }

    @Test
    void testGetBootstrapCommandVerboseArgument() {
        setProcessHandle();

        final BootstrapCommand bootstrapCommand = provider.getBootstrapCommand(VERBOSE_ARGUMENTS);

        assertCommandStatusSuccess(bootstrapCommand);

        final URI firstUri = requestUris.getFirst();
        assertEquals(DIAGNOSTICS_VERBOSE_URI, firstUri);
    }

    @Test
    void testGetBootstrapCommandPathArgument(@TempDir final Path tempDir) {
        setProcessHandle();

        final Path diagnosticsOutputPath = tempDir.resolve(UUID.randomUUID().toString());
        final String diagnosticsOutputPathArgument = diagnosticsOutputPath.toString();
        final String[] arguments = new String[]{JAVA_COMMAND, diagnosticsOutputPathArgument};

        final BootstrapCommand bootstrapCommand = provider.getBootstrapCommand(arguments);

        assertCommandStatusSuccess(bootstrapCommand);

        final URI firstUri = requestUris.getFirst();
        assertEquals(DIAGNOSTICS_URI, firstUri);

        final boolean diagnosticsOutputPathExists = Files.exists(diagnosticsOutputPath);
        assertTrue(diagnosticsOutputPathExists);
    }

    @Test
    void testGetBootstrapCommandPathVerboseArguments(@TempDir final Path tempDir) {
        setProcessHandle();

        final Path diagnosticsOutputPath = tempDir.resolve(UUID.randomUUID().toString());
        final String diagnosticsOutputPathArgument = diagnosticsOutputPath.toString();
        final String[] arguments = new String[]{JAVA_COMMAND, diagnosticsOutputPathArgument, VERBOSE_ARGUMENT};

        final BootstrapCommand bootstrapCommand = provider.getBootstrapCommand(arguments);

        assertCommandStatusSuccess(bootstrapCommand);
        assertVerboseAndPathArgumentsProcessed(diagnosticsOutputPath);
    }

    @Test
    void testGetBootstrapCommandVerbosePathArguments(@TempDir final Path tempDir) {
        setProcessHandle();

        final Path diagnosticsOutputPath = tempDir.resolve(UUID.randomUUID().toString());
        final String diagnosticsOutputPathArgument = diagnosticsOutputPath.toString();
        final String[] arguments = new String[]{JAVA_COMMAND, VERBOSE_ARGUMENT, diagnosticsOutputPathArgument};

        final BootstrapCommand bootstrapCommand = provider.getBootstrapCommand(arguments);

        assertCommandStatusSuccess(bootstrapCommand);
        assertVerboseAndPathArgumentsProcessed(diagnosticsOutputPath);
    }

    private void assertVerboseAndPathArgumentsProcessed(final Path expectedOutputPath) {
        final URI firstUri = requestUris.getFirst();
        assertEquals(DIAGNOSTICS_VERBOSE_URI, firstUri);

        final boolean expectedOutputPathFound = Files.exists(expectedOutputPath);
        assertTrue(expectedOutputPathFound);
    }

    private void assertCommandStatusSuccess(final BootstrapCommand bootstrapCommand) {
        assertNotNull(bootstrapCommand);
        assertInstanceOf(ManagementServerBootstrapCommand.class, bootstrapCommand);

        bootstrapCommand.run();
        final CommandStatus commandStatus = bootstrapCommand.getCommandStatus();
        assertEquals(CommandStatus.SUCCESS, commandStatus);

        assertFalse(requestUris.isEmpty());
    }

    private void setProcessHandle() {
        when(processHandleProvider.findApplicationProcessHandle()).thenReturn(Optional.of(processHandle));
        when(processHandle.info()).thenReturn(processHandleInfo);

        final String managementServerAddressArgument = getServerAddressArgument();
        final String[] arguments = new String[]{managementServerAddressArgument};
        when(processHandleInfo.arguments()).thenReturn(Optional.of(arguments));
    }

    private String getServerAddressArgument() {
        final InetSocketAddress bindAddress = httpServer.getAddress();
        final int serverPort = bindAddress.getPort();
        return MANAGEMENT_SERVER_ARGUMENT_FORMAT.formatted(SystemProperty.MANAGEMENT_SERVER_ADDRESS.getProperty(), serverPort);
    }
}
