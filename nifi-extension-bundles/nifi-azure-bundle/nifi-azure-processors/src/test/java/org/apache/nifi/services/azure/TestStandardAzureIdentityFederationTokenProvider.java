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
package org.apache.nifi.services.azure;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.client.api.HttpEntityHeaders;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpRequestHeadersSpec;
import org.apache.nifi.web.client.api.HttpRequestMethod;
import org.apache.nifi.web.client.api.HttpRequestUriSpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestStandardAzureIdentityFederationTokenProvider {
    private static final String TOKEN_RESPONSE = """
            {
              "access_token": "access-token",
              "token_type": "Bearer",
              "expires_in": "7200",
              "scope": "https://storage.azure.com/.default"
            }
            """;
    private static final String TOKEN_ENDPOINT = "https://example.com/token";

    private TestRunner runner;
    private StandardAzureIdentityFederationTokenProvider tokenProvider;

    @BeforeEach
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);

        tokenProvider = new StandardAzureIdentityFederationTokenProvider();
        runner.addControllerService("identity-provider", tokenProvider);

        final MockOAuth2AccessTokenProvider assertionProvider = new MockOAuth2AccessTokenProvider();
        runner.addControllerService("assertion-provider", assertionProvider);
        runner.enableControllerService(assertionProvider);

        final MockWebClientServiceProvider webClientServiceProvider = new MockWebClientServiceProvider(TOKEN_RESPONSE, TOKEN_ENDPOINT);
        runner.addControllerService("web-client", webClientServiceProvider);
        runner.enableControllerService(webClientServiceProvider);

        runner.setProperty(tokenProvider, StandardAzureIdentityFederationTokenProvider.TENANT_ID, "tenant-id");
        runner.setProperty(tokenProvider, StandardAzureIdentityFederationTokenProvider.CLIENT_ID, "client-id");
        runner.setProperty(tokenProvider, StandardAzureIdentityFederationTokenProvider.SCOPE, "https://storage.azure.com/.default");
        runner.setProperty(tokenProvider, StandardAzureIdentityFederationTokenProvider.TOKEN_ENDPOINT, TOKEN_ENDPOINT);
        runner.setProperty(tokenProvider, StandardAzureIdentityFederationTokenProvider.CLIENT_ASSERTION_PROVIDER, "assertion-provider");
        runner.setProperty(tokenProvider, StandardAzureIdentityFederationTokenProvider.WEB_CLIENT_SERVICE, "web-client");

        runner.enableControllerService(tokenProvider);
    }

    @Test
    public void testParsesStringExpiresIn() {
        runner.assertValid(tokenProvider);

        final AccessToken accessToken = tokenProvider.getAccessDetails();

        assertEquals("access-token", accessToken.getAccessToken());
        assertEquals(7200, accessToken.getExpiresIn());
    }

    private static class MockOAuth2AccessTokenProvider extends AbstractControllerService implements OAuth2AccessTokenProvider {
        @Override
        public AccessToken getAccessDetails() {
            final AccessToken accessToken = new AccessToken();
            accessToken.setAccessToken("client-assertion");
            accessToken.setExpiresIn(600L);
            return accessToken;
        }
    }

    private static class MockWebClientServiceProvider extends AbstractControllerService implements WebClientServiceProvider {
        private final MockWebClientService webClientService;
        private final String tokenEndpoint;

        private MockWebClientServiceProvider(final String responseBody, final String tokenEndpoint) {
            this.tokenEndpoint = tokenEndpoint;
            this.webClientService = new MockWebClientService(responseBody, tokenEndpoint);
        }

        @Override
        public HttpUriBuilder getHttpUriBuilder() {
            return new MockHttpUriBuilder(tokenEndpoint);
        }

        @Override
        public WebClientService getWebClientService() {
            return webClientService;
        }
    }

    private static class MockHttpUriBuilder implements HttpUriBuilder {
        private final URI uri;

        private MockHttpUriBuilder(final String tokenEndpoint) {
            this.uri = URI.create(tokenEndpoint);
        }

        @Override
        public URI build() {
            return uri;
        }

        @Override
        public HttpUriBuilder scheme(final String scheme) {
            return this;
        }

        @Override
        public HttpUriBuilder host(final String host) {
            return this;
        }

        @Override
        public HttpUriBuilder port(final int port) {
            return this;
        }

        @Override
        public HttpUriBuilder encodedPath(final String encodedPath) {
            return this;
        }

        @Override
        public HttpUriBuilder addPathSegment(final String pathSegment) {
            return this;
        }

        @Override
        public HttpUriBuilder addQueryParameter(final String name, final String value) {
            return this;
        }
    }

    private static class MockWebClientService implements WebClientService {
        private final String responseBody;
        private final String tokenEndpoint;

        private MockWebClientService(final String responseBody, final String tokenEndpoint) {
            this.responseBody = responseBody;
            this.tokenEndpoint = tokenEndpoint;
        }

        @Override
        public HttpRequestUriSpec method(final HttpRequestMethod requestMethod) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HttpRequestUriSpec delete() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HttpRequestUriSpec get() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HttpRequestUriSpec head() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HttpRequestUriSpec patch() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HttpRequestUriSpec post() {
            return new MockHttpRequestUriSpec(responseBody, tokenEndpoint);
        }

        @Override
        public HttpRequestUriSpec put() {
            throw new UnsupportedOperationException();
        }
    }

    private static class MockHttpRequestUriSpec implements HttpRequestUriSpec {
        private final String responseBody;
        private final String tokenEndpoint;

        private MockHttpRequestUriSpec(final String responseBody, final String tokenEndpoint) {
            this.responseBody = responseBody;
            this.tokenEndpoint = tokenEndpoint;
        }

        @Override
        public HttpRequestBodySpec uri(final URI uri) {
            return new MockHttpRequestBodySpec(responseBody, uri != null ? uri : URI.create(tokenEndpoint));
        }
    }

    private static class MockHttpRequestBodySpec implements HttpRequestBodySpec {
        private final HttpResponseEntity responseEntity;

        private MockHttpRequestBodySpec(final String responseBody, final URI uri) {
            this.responseEntity = new MockHttpResponseEntity(responseBody, uri);
        }

        @Override
        public HttpRequestHeadersSpec body(final InputStream inputStream, final OptionalLong contentLength) {
            return this;
        }

        @Override
        public HttpRequestHeadersSpec body(final String body) {
            return this;
        }

        @Override
        public HttpRequestBodySpec header(final String headerName, final String headerValue) {
            return this;
        }

        @Override
        public HttpResponseEntity retrieve() {
            return responseEntity;
        }
    }

    private static class MockHttpResponseEntity implements HttpResponseEntity {
        private final String responseBody;
        private final URI uri;

        private MockHttpResponseEntity(final String responseBody, final URI uri) {
            this.responseBody = responseBody;
            this.uri = uri;
        }

        @Override
        public int statusCode() {
            return 200;
        }

        @Override
        public HttpEntityHeaders headers() {
            return new MockHttpEntityHeaders();
        }

        @Override
        public InputStream body() {
            return new ByteArrayInputStream(responseBody.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public URI uri() {
            return uri;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static class MockHttpEntityHeaders implements HttpEntityHeaders {
        @Override
        public Optional<String> getFirstHeader(final String headerName) {
            return Optional.empty();
        }

        @Override
        public List<String> getHeader(final String headerName) {
            return Collections.emptyList();
        }

        @Override
        public Collection<String> getHeaderNames() {
            return Collections.emptyList();
        }
    }
}
