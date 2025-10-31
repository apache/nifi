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
package org.apache.nifi.processors.gcp.credentials.service;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
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
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.apache.nifi.processors.gcp.credentials.service.StandardGCPIdentityFederationTokenProvider.AUDIENCE;
import static org.apache.nifi.processors.gcp.credentials.service.StandardGCPIdentityFederationTokenProvider.SUBJECT_TOKEN_PROVIDER;
import static org.apache.nifi.processors.gcp.credentials.service.StandardGCPIdentityFederationTokenProvider.WEB_CLIENT_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandardGCPIdentityFederationTokenProviderTest {
    private static final String SERVICE_ID = "federation-provider";
    private static final String SUBJECT_PROVIDER_ID = "subject-token-provider";
    private static final String WEB_CLIENT_SERVICE_ID = "web-client";
    private static final String SAMPLE_AUDIENCE = "//iam.googleapis.com/projects/123456789/locations/global/workloadIdentityPools/sample/providers/provider-id";
    private static final String SAMPLE_ACCESS_TOKEN = "ya29.sample-access-token";
    private static final String SUBJECT_TOKEN = "subject-token";

    private TestRunner runner;
    private StandardGCPIdentityFederationTokenProvider tokenProvider;
    private CapturingWebClientServiceProvider webClientServiceProvider;
    private MockSubjectTokenProvider subjectTokenProvider;

    @BeforeEach
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        tokenProvider = new StandardGCPIdentityFederationTokenProvider();
        runner.addControllerService(SERVICE_ID, tokenProvider);

        webClientServiceProvider = new CapturingWebClientServiceProvider();
        runner.addControllerService(WEB_CLIENT_SERVICE_ID, webClientServiceProvider);
        runner.enableControllerService(webClientServiceProvider);

        subjectTokenProvider = new MockSubjectTokenProvider();
        runner.addControllerService(SUBJECT_PROVIDER_ID, subjectTokenProvider);
        runner.enableControllerService(subjectTokenProvider);
    }

    @Test
    public void testTokenExchange() throws Exception {
        runner.setProperty(tokenProvider, WEB_CLIENT_SERVICE, WEB_CLIENT_SERVICE_ID);
        runner.setProperty(tokenProvider, SUBJECT_TOKEN_PROVIDER, SUBJECT_PROVIDER_ID);
        runner.setProperty(tokenProvider, AUDIENCE, SAMPLE_AUDIENCE);

        runner.enableControllerService(tokenProvider);
        runner.assertValid(tokenProvider);

        final AccessToken accessToken = tokenProvider.getAccessDetails();
        assertNotNull(accessToken);
        assertEquals(SAMPLE_ACCESS_TOKEN, accessToken.getAccessToken());

        final String requestBody = webClientServiceProvider.getCapturedRequestBody();
        assertNotNull(requestBody);
        assertEquals(StandardGCPIdentityFederationTokenProviderTest.SUBJECT_TOKEN, subjectTokenProvider.getProvidedToken());
        assertEquals(URI.create("https://sts.googleapis.com/v1/token"), webClientServiceProvider.getCapturedRequestUri());
        // Validate core parameters are present
        final String expectedAudienceParam = "audience=" + encode(SAMPLE_AUDIENCE);
        final String expectedSubjectParam = "subject_token=" + encode(SUBJECT_TOKEN);
        final String expectedGrantType = "grant_type=" + encode("urn:ietf:params:oauth:grant-type:token-exchange");

        assertTrue(requestBody.contains(expectedAudienceParam));
        assertTrue(requestBody.contains(expectedSubjectParam));
        assertTrue(requestBody.contains(expectedGrantType));
    }

    private static String encode(final String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static final class MockSubjectTokenProvider extends AbstractControllerService implements OAuth2AccessTokenProvider {
        private String providedToken;

        @Override
        public AccessToken getAccessDetails() {
            final AccessToken accessToken = new AccessToken();
            accessToken.setAdditionalParameter("id_token", StandardGCPIdentityFederationTokenProviderTest.SUBJECT_TOKEN);
            providedToken = StandardGCPIdentityFederationTokenProviderTest.SUBJECT_TOKEN;
            return accessToken;
        }

        public String getProvidedToken() {
            return providedToken;
        }

        @Override
        public void refreshAccessDetails() {
            // no-op
        }
    }

    private static final class CapturingWebClientServiceProvider extends AbstractControllerService implements WebClientServiceProvider {
        private final CapturingWebClientService webClientService = new CapturingWebClientService();

        @Override
        public WebClientService getWebClientService() {
            return webClientService;
        }

        String getCapturedRequestBody() {
            return webClientService.getCapturedRequestBody();
        }

        URI getCapturedRequestUri() {
            return webClientService.getCapturedRequestUri();
        }

        @Override
        public HttpUriBuilder getHttpUriBuilder() {
            return new NoOpHttpUriBuilder();
        }
    }

    private static final class CapturingWebClientService implements WebClientService {
        private static final int STATUS_SUCCESS = 200;
        private static final String RESPONSE_BODY = "{\"access_token\":\"" + StandardGCPIdentityFederationTokenProviderTest.SAMPLE_ACCESS_TOKEN + "\",\"expires_in\":3600,\"token_type\":\"Bearer\"}";

        private String capturedRequestBody;
        private URI capturedUri;

        @Override
        public HttpRequestUriSpec post() {
            return new CapturingRequestSpec();
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
        public HttpRequestUriSpec put() {
            throw new UnsupportedOperationException();
        }

        String getCapturedRequestBody() {
            return capturedRequestBody;
        }

        URI getCapturedRequestUri() {
            return capturedUri;
        }

        private final class CapturingRequestSpec implements HttpRequestUriSpec, HttpRequestBodySpec {

            @Override
            public HttpRequestBodySpec uri(final URI uri) {
                capturedUri = uri;
                return this;
            }

            @Override
            public HttpRequestBodySpec header(final String headerName, final String headerValue) {
                return this;
            }

            @Override
            public HttpRequestHeadersSpec body(final InputStream inputStream, final OptionalLong contentLength) {
                throw new UnsupportedOperationException();
            }

            @Override
            public HttpRequestHeadersSpec body(final String body) {
                capturedRequestBody = body;
                return this;
            }

            @Override
            public HttpResponseEntity retrieve() {
                return new StaticHttpResponseEntity(STATUS_SUCCESS, capturedUri, RESPONSE_BODY);
            }
        }
    }

    private static final class StaticHttpResponseEntity implements HttpResponseEntity {
        private final int statusCode;
        private final URI uri;
        private final byte[] responseBody;

        private StaticHttpResponseEntity(final int statusCode, final URI uri, final String responseBody) {
            this.statusCode = statusCode;
            this.uri = uri;
            this.responseBody = responseBody.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public int statusCode() {
            return statusCode;
        }

        @Override
        public HttpEntityHeaders headers() {
            return new EmptyHttpEntityHeaders();
        }

        @Override
        public InputStream body() {
            return new ByteArrayInputStream(responseBody);
        }

        @Override
        public URI uri() {
            return uri;
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }

    private static final class EmptyHttpEntityHeaders implements HttpEntityHeaders {

        @Override
        public Optional<String> getFirstHeader(final String headerName) {
            return Optional.empty();
        }

        @Override
        public List<String> getHeader(final String headerName) {
            return List.of();
        }

        @Override
        public Collection<String> getHeaderNames() {
            return List.of();
        }
    }

    private static final class NoOpHttpUriBuilder implements HttpUriBuilder {
        @Override
        public URI build() {
            return null;
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
}
