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

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.nifi.bootstrap.command.io.LoggerResponseStreamHandler;
import org.apache.nifi.bootstrap.command.io.ResponseStreamHandler;
import org.apache.nifi.bootstrap.command.process.ProcessHandleProvider;
import org.apache.nifi.bootstrap.configuration.ManagementServerPath;
import org.apache.nifi.bootstrap.configuration.SystemProperty;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ManagementServerBootstrapCommandTest {
    private static final Logger logger = LoggerFactory.getLogger(ManagementServerBootstrapCommandTest.class);

    private static final String LOCALHOST = "127.0.0.1";

    private static final int RANDOM_PORT = 0;

    private static final int BACKLOG = 10;

    private static final int STOP_DELAY = 0;

    private static final String ADDRESS_ARGUMENT = "-D%s=%s:%d";

    private static final String RESPONSE_STATUS = "Status";

    @Mock
    private ProcessHandleProvider processHandleProvider;

    @Mock
    private ProcessHandle processHandle;

    @Mock
    private ProcessHandle.Info processHandleInfo;

    @Test
    void testStopped() {
        final ResponseStreamHandler responseStreamHandler = new LoggerResponseStreamHandler(logger);

        final ManagementServerBootstrapCommand command = new ManagementServerBootstrapCommand(processHandleProvider, ManagementServerPath.HEALTH_CLUSTER, responseStreamHandler);

        command.run();

        final CommandStatus commandStatus = command.getCommandStatus();
        assertEquals(CommandStatus.STOPPED, commandStatus);
    }

    @Test
    void testClusterHealthStatusConnected() throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final ResponseStreamHandler responseStreamHandler = responseStream -> {
            try {
                responseStream.transferTo(outputStream);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        final HttpServer httpServer = startServer(HttpURLConnection.HTTP_OK);

        final ManagementServerBootstrapCommand command = new ManagementServerBootstrapCommand(processHandleProvider, ManagementServerPath.HEALTH_CLUSTER, responseStreamHandler);
        command.run();

        final CommandStatus commandStatus = command.getCommandStatus();
        assertEquals(CommandStatus.SUCCESS, commandStatus);
        assertEquals(RESPONSE_STATUS, outputStream.toString());

        httpServer.stop(STOP_DELAY);
    }

    @Test
    void testClusterHealthStatusDisconnected() throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final ResponseStreamHandler responseStreamHandler = responseStream -> {
            try {
                responseStream.transferTo(outputStream);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        final HttpServer httpServer = startServer(HttpURLConnection.HTTP_UNAVAILABLE);

        final ManagementServerBootstrapCommand command = new ManagementServerBootstrapCommand(processHandleProvider, ManagementServerPath.HEALTH_CLUSTER, responseStreamHandler);
        command.run();

        final CommandStatus commandStatus = command.getCommandStatus();
        assertEquals(CommandStatus.COMMUNICATION_FAILED, commandStatus);
        assertEquals(RESPONSE_STATUS, outputStream.toString());

        httpServer.stop(STOP_DELAY);
    }

    private HttpServer startServer(final int responseCode) throws IOException {
        final InetSocketAddress bindAddress = new InetSocketAddress(LOCALHOST, RANDOM_PORT);
        final HttpServer httpServer = HttpServer.create(bindAddress, BACKLOG);
        final HttpHandler httpHandler = exchange -> {
            exchange.sendResponseHeaders(responseCode, RESPONSE_STATUS.length());
            try (OutputStream responseBody = exchange.getResponseBody()) {
                responseBody.write(RESPONSE_STATUS.getBytes(StandardCharsets.UTF_8));
            }
        };
        httpServer.createContext(ManagementServerPath.HEALTH_CLUSTER.getPath(), httpHandler);
        httpServer.start();

        final InetSocketAddress serverAddress = httpServer.getAddress();
        final String addressArgument = ADDRESS_ARGUMENT.formatted(SystemProperty.MANAGEMENT_SERVER_ADDRESS.getProperty(), LOCALHOST, serverAddress.getPort());
        final String[] arguments = new String[]{addressArgument};

        when(processHandleProvider.findApplicationProcessHandle()).thenReturn(Optional.of(processHandle));
        when(processHandle.info()).thenReturn(processHandleInfo);
        when(processHandleInfo.arguments()).thenReturn(Optional.of(arguments));

        return httpServer;
    }
}
