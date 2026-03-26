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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.apache.nifi.services.iceberg.gcs.GoogleCloudStorageHeader.AUTHORIZATION;

/**
 * HTTP Client Provider encapsulates request and response methods with a configured HTTP Client and standard settings
 */
class HttpClientProvider {

    private static final String BEARER_FORMAT = "Bearer %s";

    /** Connect Timeout based on default setting from Google Cloud Storage library */
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(20);

    /** Request Timeout provides failsafe behavior */
    static final Duration REQUEST_TIMEOUT = Duration.ofMinutes(5);

    private final String bearerToken;
    private final HttpClient httpClient;

    HttpClientProvider(final String bearerToken) {
        this.bearerToken = bearerToken;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .build();
    }

    <T> HttpResponse<T> send(final HttpRequest request, final HttpResponse.BodyHandler<T> bodyHandler) throws IOException {
        try {
            return httpClient.send(request, bodyHandler);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("HTTP %s [%s] request interrupted".formatted(request.method(), request.uri()), e);
        }
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
}
