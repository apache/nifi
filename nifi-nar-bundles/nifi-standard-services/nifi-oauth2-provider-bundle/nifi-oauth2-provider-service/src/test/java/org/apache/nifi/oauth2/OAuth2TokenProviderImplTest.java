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

package org.apache.nifi.oauth2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OAuth2TokenProviderImplTest {
    private TestRunner runner;

    private MockWebServer mockWebServer;

    private OAuth2TokenProvider oAuth2TokenProvider;

    @BeforeEach
    public void setup() throws Exception {
        mockWebServer = new MockWebServer();
        final String url = mockWebServer.url("/").toString();

        runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

            }
        });
        oAuth2TokenProvider = new OAuth2TokenProviderImpl();
        runner.addControllerService("provider", oAuth2TokenProvider);
        runner.setProperty(oAuth2TokenProvider, OAuth2TokenProvider.ACCESS_TOKEN_URL, url);
        runner.enableControllerService(oAuth2TokenProvider);
        runner.assertValid();
    }

    @Test
    public void testClientCredentialGrant() throws AccessTokenAcquisitionException, JsonProcessingException {
        enqueueTokenResponse();
        final AccessToken token = oAuth2TokenProvider.getAccessTokenByClientCredentials(
                "test-client",
                UUID.randomUUID().toString()
        );
        assertAccessTokenFound(token);
    }

    @Test
    public void testErrorHandler() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(500));
        assertThrows(AccessTokenAcquisitionException.class, () -> oAuth2TokenProvider.getAccessTokenByClientCredentials(
                "test-client",
                UUID.randomUUID().toString()
        ));
    }

    @Test
    public void testPasswordGrant() throws AccessTokenAcquisitionException, JsonProcessingException {
        enqueueTokenResponse();
        final AccessToken token = oAuth2TokenProvider.getAccessTokenByPassword(
                "test-client",
                UUID.randomUUID().toString(),
                "user",
                "password"
        );
        assertAccessTokenFound(token);
    }

    @Test
    public void testRefreshToken() throws AccessTokenAcquisitionException, JsonProcessingException {
        enqueueTokenResponse();
        final AccessToken token = oAuth2TokenProvider.refreshToken(
                new AccessToken("token", "refresh", "BEARER", 300, "test")
        );
        assertAccessTokenFound(token);
    }

    private void assertAccessTokenFound(final AccessToken accessToken) {
        assertNotNull(accessToken);
        assertEquals("access token", accessToken.getAccessToken());
        assertEquals(5300, accessToken.getExpiresIn());
        assertEquals("BEARER", accessToken.getTokenType());
        assertFalse(accessToken.isExpired());
    }

    private void enqueueTokenResponse() throws JsonProcessingException {
        final Map<String, Object> token = new HashMap<>();
        token.put("access_token", "access token");
        token.put("refresh_token", "refresh token");
        token.put("token_type", "BEARER");
        token.put("expires_in", 5300);
        token.put("scope", "test scope");
        final String accessToken = new ObjectMapper().writeValueAsString(token);
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).addHeader("Content-Type", "application/json").setBody(accessToken));
    }
}