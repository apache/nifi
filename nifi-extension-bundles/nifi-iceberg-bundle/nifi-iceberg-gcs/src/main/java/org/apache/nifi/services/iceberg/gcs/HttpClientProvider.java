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
package org.apache.nifi.services.iceberg.gcs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Set;

import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_CLIENT_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_GATEWAY_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.AUTHORIZATION;

/**
 * HTTP Client Provider encapsulates request and response methods with a configured HTTP Client and standard settings
 */
class HttpClientProvider {

    private static final Logger logger = LoggerFactory.getLogger(HttpClientProvider.class);

    private static final String BEARER_FORMAT = "Bearer %s";

    /** Connect Timeout based on default setting from Google Cloud Storage library */
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(20);

    /** Request Timeout provides failsafe behavior */
    static final Duration REQUEST_TIMEOUT = Duration.ofMinutes(5);

    private static final Duration DEFAULT_INITIAL_BACKOFF = Duration.ofSeconds(1);

    private static final int SLEEP_MULTIPLIER = 5;

    private static final int MAX_ATTEMPTS = 3;

    private static final int HTTP_TOO_MANY_REQUESTS = 429;

    /** HTTP status codes considered retriable aligned with Google Cloud Storage library */
    static final Set<Integer> RETRIABLE_STATUS_CODES = Set.of(
            HTTP_CLIENT_TIMEOUT,
            HTTP_TOO_MANY_REQUESTS,
            HTTP_INTERNAL_ERROR,
            HTTP_BAD_GATEWAY,
            HTTP_UNAVAILABLE,
            HTTP_GATEWAY_TIMEOUT
    );

    private final String bearerToken;
    private final HttpClient httpClient;
    private final Duration initialBackoff;

    HttpClientProvider(final String bearerToken) {
        this(bearerToken, DEFAULT_INITIAL_BACKOFF);
    }

    HttpClientProvider(final String bearerToken, final Duration initialBackoff) {
        this.bearerToken = bearerToken;
        this.initialBackoff = initialBackoff;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .build();
    }

    /**
     * Send HTTP request with retry on status codes considered retriable according to Google Cloud Storage API
     *
     * @param request HTTP Request to be executed
     * @param bodyHandler HTTP Response Body Handler
     * @return HTTP Response
     * @param <T> HTTP Response Body Type
     * @throws IOException Thrown on failure after retries exhausted
     */
    <T> HttpResponse<T> send(final HttpRequest request, final HttpResponse.BodyHandler<T> bodyHandler) throws IOException {
        HttpResponse<T> response = null;
        IOException lastException = null;

        final String requestMethod = request.method();
        final URI requestUri = request.uri();
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                response = httpClient.send(request, bodyHandler);

                final int statusCode = response.statusCode();
                if (RETRIABLE_STATUS_CODES.contains(statusCode) && attempt < MAX_ATTEMPTS) {
                    logger.info("{} [{}] HTTP {} for attempt {} of {}", requestMethod, requestUri, statusCode, attempt, MAX_ATTEMPTS);
                    sleep(attempt);
                    continue;
                }

                return response;
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("%s [%s] request interrupted".formatted(requestMethod, requestUri), e);
            } catch (final IOException e) {
                lastException = e;
                if (attempt < MAX_ATTEMPTS) {
                    logger.info("{} [{}] request failed for attempt {} of {}", requestMethod, requestUri, attempt, MAX_ATTEMPTS, e);
                    sleep(attempt);
                }
            }
        }

        if (response == null) {
            throw lastException;
        }

        return response;
    }

    HttpRequest.Builder newRequestBuilder(final String uri) {
        final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(uri))
                .timeout(REQUEST_TIMEOUT);
        if (bearerToken != null) {
            builder.header(AUTHORIZATION.getHeader(), BEARER_FORMAT.formatted(bearerToken));
        }
        return builder;
    }

    void close() {
        httpClient.close();
    }

    private void sleep(final int attempt) throws IOException {
        final long duration = initialBackoff.toMillis() * attempt * SLEEP_MULTIPLIER;
        try {
            Thread.sleep(duration);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Retry interrupted", e);
        }
    }
}
