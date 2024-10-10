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
import org.apache.nifi.runtime.util.HttpExchangeUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static java.net.HttpURLConnection.HTTP_BAD_METHOD;
import static java.net.HttpURLConnection.HTTP_OK;

/**
 * HTTP Handler for Health status operations
 */
class HealthHttpHandler implements HttpHandler {
    private static final String CONTENT_TYPE_HEADER = "Content-Type";

    private static final String TEXT_PLAIN = "text/plain";

    private static final int NO_RESPONSE_BODY = -1;

    private static final String GET_METHOD = "GET";

    private static final String STATUS_UP = "Status: UP\n";

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        HttpExchangeUtils.drainRequestBody(exchange);

        final String requestMethod = exchange.getRequestMethod();

        try (final OutputStream responseBody = exchange.getResponseBody()) {
            if (GET_METHOD.contentEquals(requestMethod)) {
                exchange.getResponseHeaders().set(CONTENT_TYPE_HEADER, TEXT_PLAIN);
                final byte[] response = STATUS_UP.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(HTTP_OK, response.length);
                responseBody.write(response);
            } else {
                exchange.sendResponseHeaders(HTTP_BAD_METHOD, NO_RESPONSE_BODY);
            }
        }
    }
}
