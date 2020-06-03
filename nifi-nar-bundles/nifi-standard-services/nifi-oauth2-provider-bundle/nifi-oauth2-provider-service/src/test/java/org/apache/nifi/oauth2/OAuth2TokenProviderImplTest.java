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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.TestServer;
import org.eclipse.jetty.server.Request;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OAuth2TokenProviderImplTest {
    private TestRunner runner;
    private static TestServer server;
    private static String url;
    private OAuth2TokenProvider oAuth2TokenProvider;

    private static FakeOAuth2Server handler;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
        // useful for verbose logging output
        // don't commit this with this property enabled, or any 'mvn test' will be really verbose
        // System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard", "debug");

        // create a Jetty server on a random port
        server = new TestServer();
        server.startServer();

        // this is the base url with the random port
        url = server.getUrl();

        handler = new FakeOAuth2Server();

        server.addHandler(handler);
    }

    @Before
    public void setup() throws Exception {
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
    public void testClientCredentialGrant() {
        Exception ex = null;
        AccessToken token = null;
        try {
            token = oAuth2TokenProvider.getAccessTokenByClientCredentials(
                    "test-client",
                    UUID.randomUUID().toString()
            );
        } catch (AccessTokenAcquisitionException e) {
            ex = e;
        } finally {
            commonTest(ex, token);
        }
    }

    @Test
    public void testErrorHandler() {
        Exception ex = null;

        try {
            handler.setThrowException(true);
            oAuth2TokenProvider.getAccessTokenByClientCredentials(
                    "test-client",
                    UUID.randomUUID().toString()
            );
        } catch (AccessTokenAcquisitionException e) {
            ex = e;
        } finally {
            handler.setThrowException(false);
            assertTrue(ex instanceof AccessTokenAcquisitionException);
        }
    }

    @Test
    public void testPasswordGrant() {
        Exception ex = null;
        AccessToken token = null;
        try {
            token = oAuth2TokenProvider.getAccessTokenByPassword(
                    "test-client",
                    UUID.randomUUID().toString(),
                    "user",
                    "password"
            );
        } catch (AccessTokenAcquisitionException e) {
            ex = e;
        } finally {
            commonTest(ex, token);
        }
    }

    @Test
    public void testRefreshToken() {
        Exception ex = null;
        AccessToken token = null;
        try {
            token = oAuth2TokenProvider.refreshToken(
                    new AccessToken("token", "refresh", "BEARER", 300, "test")
            );
        } catch (AccessTokenAcquisitionException e) {
            ex = e;
        } finally {
            commonTest(ex, token);
        }
    }

    private void commonTest(Exception ex, AccessToken token) {
        assertNull(ex);
        assertNotNull(token);
        assertEquals("access token", token.getAccessToken());
        assertEquals(300, token.getExpires().intValue());
        assertEquals("BEARER", token.getTokenType());
        assertFalse(token.isExpired());
    }

    public static final class FakeOAuth2Server extends AbstractHandler {
        boolean throwException = false;

        public void setThrowException(boolean throwException) {
            this.throwException = throwException;
        }

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            baseRequest.setHandled(true);

            if (throwException) {
                response.setStatus(500);
            } else {
                Map<String, Object> token = new HashMap<>();
                token.put("access_token", "access token");
                token.put("refresh_token", "refresh token");
                token.put("token_type", "BEARER");
                token.put("expires_in", 300);
                token.put("scope", "test scope");

                response.setContentType("application/json");
                response.getWriter().write(new ObjectMapper().writeValueAsString(token));
            }
        }
    }
}