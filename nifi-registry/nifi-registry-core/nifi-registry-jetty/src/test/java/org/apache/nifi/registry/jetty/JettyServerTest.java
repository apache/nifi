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
package org.apache.nifi.registry.jetty;

import org.apache.nifi.registry.jetty.handler.HandlerProvider;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class JettyServerTest {
    private static final String RANDOM_PORT = "0";

    private static final String LOCALHOST = "127.0.0.1";

    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    private static final String APPLICATION_PATH_DIRECTORY = "/nifi-registry/";

    @Mock
    private HandlerProvider handlerProvider;

    @Timeout(15)
    @Test
    void testStartStop() throws Exception {
        final Map<String, String> requiredProperties = new LinkedHashMap<>();
        requiredProperties.put(NiFiRegistryProperties.WEB_HTTP_PORT, RANDOM_PORT);
        requiredProperties.put(NiFiRegistryProperties.WEB_HTTP_HOST, LOCALHOST);
        final NiFiRegistryProperties properties = new NiFiRegistryProperties(requiredProperties);

        final JettyServer server = new JettyServer(properties, handlerProvider);

        try {
            server.start();

            final Iterator<URI> applicationUrls = server.getApplicationUrls().iterator();
            assertTrue(applicationUrls.hasNext());

            final URI applicationUrl = applicationUrls.next();
            assertFalse(applicationUrl.toString().isEmpty());

            assertHttpRequestsCompleted(applicationUrl);
        } finally {
            server.stop();
        }
    }

    void assertHttpRequestsCompleted(final URI serverUri) throws IOException, InterruptedException {
        try (HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .build()
        ) {
            assertRedirectRequestsCompleted(httpClient, serverUri);
        }
    }

    void assertRedirectRequestsCompleted(final HttpClient httpClient, final URI serverUri) throws IOException, InterruptedException {
        final HttpRequest applicationPathRequest = HttpRequest.newBuilder(serverUri)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        final HttpResponse<Void> applicationPathResponse = assertResponseStatusCode(httpClient, applicationPathRequest, HttpStatus.MOVED_TEMPORARILY_302);
        final Optional<String> applicationPathLocationFound = applicationPathResponse.headers().firstValue(HttpHeader.LOCATION.lowerCaseName());
        assertTrue(applicationPathLocationFound.isPresent());

        final String applicationPathLocation = applicationPathLocationFound.get();
        assertEquals(APPLICATION_PATH_DIRECTORY, applicationPathLocation);

        final String scheme = serverUri.getScheme();
        final String host = serverUri.getHost();
        final int port = serverUri.getPort();
        final URI rootUri = URI.create(String.format("%s://%s:%d/", scheme, host, port));

        final HttpRequest rootRequest = HttpRequest.newBuilder(rootUri)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        final HttpResponse<Void> rootResponse = assertResponseStatusCode(httpClient, rootRequest, HttpStatus.MOVED_TEMPORARILY_302);
        final Optional<String> rootLocationFound = rootResponse.headers().firstValue(HttpHeader.LOCATION.lowerCaseName());
        assertTrue(rootLocationFound.isPresent());

        final String rootLocation = rootLocationFound.get();
        assertEquals(APPLICATION_PATH_DIRECTORY, rootLocation);
    }

    HttpResponse<Void> assertResponseStatusCode(final HttpClient httpClient, final HttpRequest request, final int statusCodeExpected) throws IOException, InterruptedException {
        final HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        assertEquals(statusCodeExpected, response.statusCode());
        return response;
    }
}
