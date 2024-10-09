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
import org.apache.nifi.controller.status.history.StatusHistoryDump;
import org.apache.nifi.controller.status.history.StatusHistoryDumpFactory;
import org.apache.nifi.runtime.util.HttpExchangeUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.net.HttpURLConnection.HTTP_BAD_METHOD;
import static java.net.HttpURLConnection.HTTP_OK;

/**
 * HTTP Handler for Health Status History operations
 */
class HealthStatusHistoryHttpHandler implements HttpHandler {
    private static final String CONTENT_TYPE_HEADER = "Content-Type";

    private static final String APPLICATION_JSON = "application/json";

    private static final int STREAM_RESPONSE_BODY = 0;

    private static final int NO_RESPONSE_BODY = -1;

    private static final String GET_METHOD = "GET";

    private static final Pattern DAYS_QUERY_PATTERN = Pattern.compile("^days=(\\d+)$");

    private static final int DAYS_GROUP = 1;

    private static final int DAYS_DEFAULT = 1;

    private final NiFiServer server;

    HealthStatusHistoryHttpHandler(final NiFiServer server) {
        this.server = Objects.requireNonNull(server);
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        HttpExchangeUtils.drainRequestBody(exchange);

        final String requestMethod = exchange.getRequestMethod();

        try (final OutputStream responseBody = exchange.getResponseBody()) {
            if (GET_METHOD.contentEquals(requestMethod)) {
                exchange.getResponseHeaders().set(CONTENT_TYPE_HEADER, APPLICATION_JSON);
                exchange.sendResponseHeaders(HTTP_OK, STREAM_RESPONSE_BODY);

                final URI requestUri = exchange.getRequestURI();
                final int daysRequested = getDaysRequested(requestUri);

                final StatusHistoryDumpFactory statusHistoryDumpFactory = server.getStatusHistoryDumpFactory();
                final StatusHistoryDump statusHistoryDump = statusHistoryDumpFactory.create(daysRequested);
                statusHistoryDump.writeTo(responseBody);
            } else {
                exchange.sendResponseHeaders(HTTP_BAD_METHOD, NO_RESPONSE_BODY);
            }
        }
    }

    private int getDaysRequested(final URI requestUri) {
        final int daysRequested;

        final String query = requestUri.getQuery();
        if (query == null) {
            daysRequested = DAYS_DEFAULT;
        } else {
            final Matcher matcher = DAYS_QUERY_PATTERN.matcher(query);
            if (matcher.matches()) {
                final String daysGroup = matcher.group(DAYS_GROUP);
                daysRequested = Integer.parseInt(daysGroup);
            } else {
                daysRequested = DAYS_DEFAULT;
            }
        }

        return daysRequested;
    }
}
