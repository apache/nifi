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

import org.apache.nifi.bootstrap.command.io.HttpRequestMethod;
import org.apache.nifi.bootstrap.command.io.ResponseStreamHandler;
import org.apache.nifi.bootstrap.command.process.ManagementServerAddressProvider;
import org.apache.nifi.bootstrap.command.process.ProcessHandleManagementServerAddressProvider;
import org.apache.nifi.bootstrap.command.process.ProcessHandleProvider;
import org.apache.nifi.bootstrap.command.process.VirtualMachineManagementServerAddressProvider;
import org.apache.nifi.bootstrap.configuration.ApplicationClassName;
import org.apache.nifi.bootstrap.configuration.ManagementServerPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Sequence of Bootstrap Commands
 */
class ManagementServerBootstrapCommand implements BootstrapCommand {

    private static final Logger commandLogger = LoggerFactory.getLogger(ApplicationClassName.BOOTSTRAP_COMMAND.getName());

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(15);

    private static final String SERVER_URI = "http://%s%s";

    private static final char QUERY_SEPARATOR = '?';

    private final ProcessHandleProvider processHandleProvider;

    private final HttpRequestMethod httpRequestMethod;

    private final ManagementServerPath managementServerPath;

    private final String managementServerQuery;

    private final int successStatusCode;

    private final ResponseStreamHandler responseStreamHandler;

    private CommandStatus commandStatus = CommandStatus.ERROR;

    ManagementServerBootstrapCommand(
            final ProcessHandleProvider processHandleProvider,
            final ManagementServerPath managementServerPath,
            final ResponseStreamHandler responseStreamHandler
    ) {
        this(processHandleProvider, HttpRequestMethod.GET, managementServerPath, null, HttpURLConnection.HTTP_OK, responseStreamHandler);
    }

    ManagementServerBootstrapCommand(
            final ProcessHandleProvider processHandleProvider,
            final HttpRequestMethod httpRequestMethod,
            final ManagementServerPath managementServerPath,
            final String managementServerQuery,
            final int successStatusCode,
            final ResponseStreamHandler responseStreamHandler
    ) {
        this.processHandleProvider = Objects.requireNonNull(processHandleProvider);
        this.httpRequestMethod = Objects.requireNonNull(httpRequestMethod);
        this.managementServerPath = Objects.requireNonNull(managementServerPath);
        this.managementServerQuery = managementServerQuery;
        this.successStatusCode = successStatusCode;
        this.responseStreamHandler = Objects.requireNonNull(responseStreamHandler);
    }

    @Override
    public CommandStatus getCommandStatus() {
        return commandStatus;
    }

    @Override
    public void run() {
        final Optional<ProcessHandle> applicationProcessHandle = processHandleProvider.findApplicationProcessHandle();

        if (applicationProcessHandle.isEmpty()) {
            commandStatus = CommandStatus.STOPPED;
            getCommandLogger().info("Application Process STOPPED");
        } else {
            run(applicationProcessHandle.get());
        }
    }

    protected void run(final ProcessHandle applicationProcessHandle) {
        final ManagementServerAddressProvider managementServerAddressProvider = getManagementServerAddressProvider(applicationProcessHandle);
        final Optional<String> managementServerAddress = managementServerAddressProvider.getAddress();

        final long pid = applicationProcessHandle.pid();
        if (managementServerAddress.isEmpty()) {
            getCommandLogger().info("Application Process [{}] Management Server address not found", pid);
            commandStatus = CommandStatus.ERROR;
        } else {
            final URI managementServerUri = getManagementServerUri(managementServerAddress.get());
            try (HttpClient httpClient = getHttpClient()) {
                final HttpRequest httpRequest = getHttpRequest(managementServerUri);

                final HttpResponse<InputStream> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
                final int statusCode = response.statusCode();
                try (InputStream responseStream = response.body()) {
                    onResponseStatus(applicationProcessHandle, statusCode, responseStream);
                }
            } catch (final Exception e) {
                commandStatus = CommandStatus.COMMUNICATION_FAILED;
                getCommandLogger().info("Application Process [{}] Management Server [{}] communication failed", pid, managementServerUri);
            }
        }
    }

    protected void onResponseStatus(final ProcessHandle applicationProcessHandle, final int statusCode, final InputStream responseStream) {
        final long pid = applicationProcessHandle.pid();

        if (successStatusCode == statusCode) {
            commandStatus = CommandStatus.SUCCESS;
            getCommandLogger().info("Application Process [{}] Command Status [{}] HTTP {}", pid, commandStatus, statusCode);
            responseStreamHandler.onResponseStream(responseStream);
        } else {
            commandStatus = CommandStatus.COMMUNICATION_FAILED;
            getCommandLogger().warn("Application Process [{}] Command Status [{}] HTTP {}", pid, commandStatus, statusCode);
            responseStreamHandler.onResponseStream(responseStream);
        }
    }

    protected Logger getCommandLogger() {
        return commandLogger;
    }

    protected HttpRequest getHttpRequest(final URI managementServerUri) {
        return HttpRequest.newBuilder()
                .method(httpRequestMethod.name(), HttpRequest.BodyPublishers.noBody())
                .uri(managementServerUri)
                .timeout(READ_TIMEOUT)
                .build();
    }

    protected URI getManagementServerUri(final String managementServerAddress) {
        final StringBuilder builder = new StringBuilder();

        final String serverUri = SERVER_URI.formatted(managementServerAddress, managementServerPath.getPath());
        builder.append(serverUri);

        if (managementServerQuery != null) {
            builder.append(QUERY_SEPARATOR);
            builder.append(managementServerQuery);
        }

        return URI.create(builder.toString());
    }

    protected HttpClient getHttpClient() {
        final HttpClient.Builder builder = HttpClient.newBuilder();
        builder.connectTimeout(CONNECT_TIMEOUT);
        return builder.build();
    }

    private ManagementServerAddressProvider getManagementServerAddressProvider(final ProcessHandle applicationProcessHandle) {
        final ManagementServerAddressProvider managementServerAddressProvider;

        final ProcessHandle.Info applicationProcessHandleInfo = applicationProcessHandle.info();
        final Optional<String[]> arguments = applicationProcessHandleInfo.arguments();
        if (arguments.isPresent()) {
            managementServerAddressProvider = new ProcessHandleManagementServerAddressProvider(applicationProcessHandle);
        } else {
            // Use Virtual Machine Attach API when ProcessHandle does not support arguments as described in JDK-8176725
            managementServerAddressProvider = new VirtualMachineManagementServerAddressProvider(applicationProcessHandle);
        }

        return managementServerAddressProvider;
    }
}
