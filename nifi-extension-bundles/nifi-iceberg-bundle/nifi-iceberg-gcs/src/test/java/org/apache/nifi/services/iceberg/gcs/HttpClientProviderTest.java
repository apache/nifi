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

import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_CLIENT_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_GATEWAY_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HttpClientProviderTest {

    private static final int HTTP_TOO_MANY_REQUESTS = 429;

    private static final Duration TEST_BACKOFF = Duration.ofMillis(10);

    private static final String LOCALHOST_URI_FORMAT = "http://localhost:%d/test";

    private static final String GET_METHOD = "GET";

    private static final int EXPECTED_REQUESTS_WITH_RETRY = 2;

    private static final int EXPECTED_REQUESTS_RETRIES_EXHAUSTED = 3;

    private MockWebServer mockWebServer;

    private HttpClientProvider httpClientProvider;

    @BeforeEach
    void startServer() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        httpClientProvider = new HttpClientProvider(null, TEST_BACKOFF);
    }

    @AfterEach
    void closeServer() {
        httpClientProvider.close();
        mockWebServer.close();
    }

    @Test
    void testSendSuccess() throws IOException {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_OK)
                .build());

        final HttpResponse<String> response = sendGetRequest();

        assertEquals(HTTP_OK, response.statusCode());
        assertEquals(1, mockWebServer.getRequestCount());
    }

    @ParameterizedTest
    @ValueSource(ints = {HTTP_CLIENT_TIMEOUT, HTTP_TOO_MANY_REQUESTS, HTTP_INTERNAL_ERROR, HTTP_BAD_GATEWAY, HTTP_UNAVAILABLE, HTTP_GATEWAY_TIMEOUT})
    void testSendRetriableStatusCodeRetried(final int statusCode) throws IOException {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(statusCode)
                .build());
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_OK)
                .build());

        final HttpResponse<String> response = sendGetRequest();

        assertEquals(HTTP_OK, response.statusCode());
        assertEquals(EXPECTED_REQUESTS_WITH_RETRY, mockWebServer.getRequestCount());
    }

    @Test
    void testSendForbiddenStatusCodeNotRetried() throws IOException {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HTTP_FORBIDDEN)
                .build());

        final HttpResponse<String> response = sendGetRequest();

        assertEquals(HTTP_FORBIDDEN, response.statusCode());
        assertEquals(1, mockWebServer.getRequestCount());
    }

    @Test
    void testSendRetriesExhaustedReturnsLastResponse() throws IOException {
        for (int i = 0; i < EXPECTED_REQUESTS_RETRIES_EXHAUSTED; i++) {
            mockWebServer.enqueue(new MockResponse.Builder()
                    .code(HTTP_UNAVAILABLE)
                    .build());
        }

        final HttpResponse<String> response = sendGetRequest();

        assertEquals(HTTP_UNAVAILABLE, response.statusCode());
        assertEquals(EXPECTED_REQUESTS_RETRIES_EXHAUSTED, mockWebServer.getRequestCount());
    }

    @Test
    void testSendException() {
        mockWebServer.close();

        assertThrows(IOException.class, this::sendGetRequest);
    }

    private HttpResponse<String> sendGetRequest() throws IOException {
        final String uri = LOCALHOST_URI_FORMAT.formatted(mockWebServer.getPort());
        final HttpRequest request = httpClientProvider.newRequestBuilder(uri)
                .method(GET_METHOD, HttpRequest.BodyPublishers.noBody())
                .build();
        return httpClientProvider.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
