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
package org.apache.nifi.runtime;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.nifi.NiFiServer;
import org.apache.nifi.cluster.ClusterDetailsFactory;
import org.apache.nifi.cluster.ConnectionState;
import org.apache.nifi.controller.DecommissionTask;
import org.apache.nifi.runtime.util.HttpExchangeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_BAD_METHOD;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;

/**
 * HTTP Handler for Cluster Health status operations
 */
class HealthClusterHttpHandler implements HttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(HealthClusterHttpHandler.class);

    private static final String CONTENT_TYPE_HEADER = "Content-Type";

    private static final String TEXT_PLAIN = "text/plain";

    private static final int NO_RESPONSE_BODY = -1;

    private static final String GET_METHOD = "GET";

    private static final String DELETE_METHOD = "DELETE";

    private static final String STATUS = "Cluster Status: %s\n";

    private static final String DECOMMISSION_STATUS = "Decommission Status: %s\n";

    private static final DecommissionStatus IN_PROGRESS = new DecommissionStatus(DecommissionState.IN_PROGRESS, null);

    private static final DecommissionStatus COMPLETED = new DecommissionStatus(DecommissionState.COMPLETED, null);

    private final NiFiServer server;

    private volatile DecommissionStatus decommissionStatus;

    HealthClusterHttpHandler(final NiFiServer server) {
        this.server = Objects.requireNonNull(server);
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        HttpExchangeUtils.drainRequestBody(exchange);

        final String requestMethod = exchange.getRequestMethod();

        try (final OutputStream responseBody = exchange.getResponseBody()) {
            if (GET_METHOD.contentEquals(requestMethod)) {
                exchange.getResponseHeaders().set(CONTENT_TYPE_HEADER, TEXT_PLAIN);
                final ConnectionState connectionState = getConnectionState();
                final String status = getStatus(connectionState);
                final byte[] response = status.getBytes(StandardCharsets.UTF_8);
                final int responseCode = getResponseCode(connectionState);
                exchange.sendResponseHeaders(responseCode, response.length);
                responseBody.write(response);
            } else if (DELETE_METHOD.contentEquals(requestMethod)) {
                startDecommission();

                exchange.getResponseHeaders().set(CONTENT_TYPE_HEADER, TEXT_PLAIN);
                final String status = getStatus(ConnectionState.OFFLOADING);
                final byte[] response = status.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(HTTP_ACCEPTED, response.length);
                responseBody.write(response);
            } else {
                exchange.sendResponseHeaders(HTTP_BAD_METHOD, NO_RESPONSE_BODY);
            }
        }
    }

    private synchronized void startDecommission() {
        final DecommissionStatus currentDecommissionStatus = decommissionStatus;
        if (currentDecommissionStatus != null && currentDecommissionStatus.state() == DecommissionState.IN_PROGRESS) {
            logger.info("Decommission requested but a Decommission is already in progress");
            return;
        }

        final DecommissionTask decommissionTask = server.getDecommissionTask();
        decommissionStatus = IN_PROGRESS;
        Thread.ofVirtual().name(DecommissionTask.class.getSimpleName()).start(() -> {
            try {
                decommissionTask.decommission();
                decommissionStatus = COMPLETED;
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                decommissionStatus = DecommissionStatus.failed(e);
            } catch (final Throwable e) {
                logger.error("Decommission failed", e);
                decommissionStatus = DecommissionStatus.failed(e);
            }
        });
    }

    private String getStatus(final ConnectionState connectionState) {
        final String clusterStatus = STATUS.formatted(connectionState);

        final DecommissionStatus currentDecommissionStatus = decommissionStatus;
        if (currentDecommissionStatus == null) {
            return clusterStatus;
        }

        return clusterStatus + DECOMMISSION_STATUS.formatted(currentDecommissionStatus);
    }

    private int getResponseCode(final ConnectionState connectionState) {
        final int responseCode;

        if (ConnectionState.CONNECTED == connectionState || ConnectionState.CONNECTING == connectionState) {
            responseCode = HTTP_OK;
        } else {
            responseCode = HTTP_UNAVAILABLE;
        }

        return responseCode;
    }

    private ConnectionState getConnectionState() {
        final ConnectionState connectionState;

        final ClusterDetailsFactory clusterDetailsFactory = server.getClusterDetailsFactory();
        if (clusterDetailsFactory == null) {
            connectionState = ConnectionState.UNKNOWN;
        } else {
            connectionState = clusterDetailsFactory.getConnectionState();
        }

        return connectionState;
    }

    private enum DecommissionState {
        IN_PROGRESS,
        COMPLETED,
        FAILED
    }

    private record DecommissionStatus(DecommissionState state, String failureMessage) {

        private static DecommissionStatus failed(final Throwable throwable) {
            final String message = throwable.getMessage();
            final String failureMessage = message == null ? throwable.getClass().getSimpleName() : message.replaceAll("\\R", " ");
            return new DecommissionStatus(DecommissionState.FAILED, failureMessage);
        }

        @Override
        public String toString() {
            if (failureMessage == null) {
                return state.name();
            }

            return state.name() + ": " + failureMessage;
        }
    }
}
