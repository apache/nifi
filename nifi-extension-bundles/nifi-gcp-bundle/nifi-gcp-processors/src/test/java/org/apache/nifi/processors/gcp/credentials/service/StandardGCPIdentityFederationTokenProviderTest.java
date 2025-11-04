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

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.util.StreamingContent;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.IdentityPoolCredentials;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import static org.apache.nifi.processors.gcp.credentials.service.StandardGCPIdentityFederationTokenProvider.AUDIENCE;
import static org.apache.nifi.processors.gcp.credentials.service.StandardGCPIdentityFederationTokenProvider.SUBJECT_TOKEN_PROVIDER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandardGCPIdentityFederationTokenProviderTest {
    private static final String SERVICE_ID = "federation-provider";
    private static final String SUBJECT_PROVIDER_ID = "subject-token-provider";
    private static final String SAMPLE_AUDIENCE = "//iam.googleapis.com/projects/123456789/locations/global/workloadIdentityPools/sample/providers/provider-id";
    private static final String SAMPLE_ACCESS_TOKEN = "ya29.sample-access-token";
    private static final String SUBJECT_TOKEN = "subject-token";

    private TestRunner runner;
    private StandardGCPIdentityFederationTokenProvider tokenProvider;
    private MockSubjectTokenProvider subjectTokenProvider;

    @BeforeEach
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        tokenProvider = new StandardGCPIdentityFederationTokenProvider();
        runner.addControllerService(SERVICE_ID, tokenProvider);

        subjectTokenProvider = new MockSubjectTokenProvider();
        runner.addControllerService(SUBJECT_PROVIDER_ID, subjectTokenProvider);
        runner.enableControllerService(subjectTokenProvider);
    }

    @Test
    public void testTokenExchange() throws Exception {
        runner.setProperty(tokenProvider, SUBJECT_TOKEN_PROVIDER, SUBJECT_PROVIDER_ID);
        runner.setProperty(tokenProvider, AUDIENCE, SAMPLE_AUDIENCE);

        runner.enableControllerService(tokenProvider);
        runner.assertValid(tokenProvider);

        final CapturingHttpTransportFactory transportFactory = new CapturingHttpTransportFactory();
        final IdentityPoolCredentials credentials = (IdentityPoolCredentials) tokenProvider.getGoogleCredentials(transportFactory);

        final com.google.auth.oauth2.AccessToken accessToken = credentials.refreshAccessToken();
        assertNotNull(accessToken);
        assertEquals(SAMPLE_ACCESS_TOKEN, accessToken.getTokenValue());
        assertEquals(1, subjectTokenProvider.getAccessCount());

        final String requestBody = transportFactory.getRequestBody();
        assertNotNull(requestBody);
        assertTrue(requestBody.contains("grant_type=" + encode("urn:ietf:params:oauth:grant-type:token-exchange")));
        assertTrue(requestBody.contains("audience=" + encode(SAMPLE_AUDIENCE)));
        assertTrue(requestBody.contains("subject_token=" + encode(SUBJECT_TOKEN)));

        final URI requestUri = transportFactory.getRequestUri();
        assertEquals(URI.create("https://sts.googleapis.com/v1/token"), requestUri);
    }

    private static String encode(final String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static final class MockSubjectTokenProvider extends AbstractControllerService implements OAuth2AccessTokenProvider {
        private int accessCount;

        @Override
        public AccessToken getAccessDetails() {
            accessCount++;
            final AccessToken accessToken = new AccessToken();
            accessToken.setAdditionalParameter("id_token", SUBJECT_TOKEN);
            accessToken.setExpiresIn(300);
            return accessToken;
        }

        @Override
        public void refreshAccessDetails() {
            // no-op for test
        }

        int getAccessCount() {
            return accessCount;
        }
    }

    private static final class CapturingHttpTransportFactory implements HttpTransportFactory {
        private static final String RESPONSE_JSON = ("""
                {
                  "access_token": "%s",
                  "expires_in": 3600,
                  "token_type": "Bearer",
                  "issued_token_type": "urn:ietf:params:oauth:token-type:access_token"
                }
                """).formatted(SAMPLE_ACCESS_TOKEN);

        private volatile String requestBody;
        private volatile URI requestUri;

        @Override
        public HttpTransport create() {
            return new CapturingHttpTransport();
        }

        String getRequestBody() {
            return requestBody;
        }

        URI getRequestUri() {
            return requestUri;
        }

        private final class CapturingHttpTransport extends HttpTransport {

            @Override
            protected LowLevelHttpRequest buildRequest(final String method, final String url) {
                requestUri = URI.create(url);
                return new CapturingLowLevelHttpRequest();
            }
        }

        private final class CapturingLowLevelHttpRequest extends LowLevelHttpRequest {
            @Override
            public void addHeader(final String name, final String value) {
                // headers not required for verification
            }

            @Override
            public LowLevelHttpResponse execute() throws IOException {
                final StreamingContent streamingContent = getStreamingContent();
                if (streamingContent != null) {
                    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    streamingContent.writeTo(outputStream);
                    requestBody = outputStream.toString(StandardCharsets.UTF_8);
                }
                return new CapturingLowLevelHttpResponse();
            }
        }

        private static final class CapturingLowLevelHttpResponse extends LowLevelHttpResponse {
            private static final byte[] RESPONSE_BYTES = RESPONSE_JSON.getBytes(StandardCharsets.UTF_8);

            @Override
            public InputStream getContent() {
                return new ByteArrayInputStream(RESPONSE_BYTES);
            }

            @Override
            public String getContentEncoding() {
                return null;
            }

            @Override
            public long getContentLength() {
                return RESPONSE_BYTES.length;
            }

            @Override
            public String getContentType() {
                return "application/json";
            }

            @Override
            public String getStatusLine() {
                return "HTTP/1.1 200 OK";
            }

            @Override
            public int getStatusCode() {
                return 200;
            }

            @Override
            public String getReasonPhrase() {
                return "OK";
            }

            @Override
            public int getHeaderCount() {
                return 0;
            }

            @Override
            public String getHeaderName(final int index) {
                return null;
            }

            @Override
            public String getHeaderValue(final int index) {
                return null;
            }
        }
    }
}
