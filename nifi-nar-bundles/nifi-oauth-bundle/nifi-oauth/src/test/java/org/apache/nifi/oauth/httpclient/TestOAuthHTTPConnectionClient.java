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
package org.apache.nifi.oauth.httpclient;

import org.apache.nifi.oauth.OAuth2TestBase;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.message.types.GrantType;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.servlet.ServletHandler;
import org.json.JSONObject;
import org.junit.*;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Base64;

public class TestOAuthHTTPConnectionClient extends OAuth2TestBase {
    private static final JSONObject AUTH_RESPONSE;
    private static final String FIELD_ACCESS_TOKEN = "access_token";
    private static final String FIELD_TOKEN_TYPE = "token_type";
    private static final String FIELD_EXPIRE_TIME = "expire_time";
    private static final String FIELD_EXPIRE_IN = "expires_in";
    private static final String FIELD_SCOPE = "scope";

    static {
        AUTH_RESPONSE = new JSONObject();
        AUTH_RESPONSE.put(FIELD_ACCESS_TOKEN, "4324ewewr");
        AUTH_RESPONSE.put(FIELD_TOKEN_TYPE, "some type");
        AUTH_RESPONSE.put(FIELD_EXPIRE_TIME, 4323243423l);
        AUTH_RESPONSE.put(FIELD_EXPIRE_IN, 324234323342l);
        AUTH_RESPONSE.put(FIELD_SCOPE, "some scope");
    }

    @BeforeClass
    public static void initServer() throws Exception {
        // initialize the handlers
        ServletHandler secureServerHandler = new ServletHandler();
        secureServerHandler.addServletWithMapping(TestHttpsRequestServlet.class, "/testHttpsRequest");
        secureServerHandler.addServletWithMapping(TestOAuthExceptionIfServerErrorServlet.class, "/testOAuthExceptionIfServerError");

        OAuth2TestBase.initServer(secureServerHandler, null);
    }

    @Test
    public void testNonHttpsRequest() throws Exception {
        OAuthClientRequest.TokenRequestBuilder authTokenRequest =
                new OAuthClientRequest.TokenRequestBuilder(unsecureServer.getUrl())
                        .setClientId("")
                        .setClientSecret("")
                        .setGrantType(GrantType.CLIENT_CREDENTIALS);

        OAuthClientRequest headerRequest = authTokenRequest.buildHeaderMessage();
        headerRequest.setHeader("Cache-Control", "no-cache");
        headerRequest.setHeader("Content-Type", "application/x-www-form-urlencoded");
        headerRequest.setHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString("".getBytes()));

        OAuthHTTPConnectionClient conn = null;
        try {
            conn = new OAuthHTTPConnectionClient(null, null, null, null, null);
            try {
                conn.execute(headerRequest, null, "POST",
                        OAuthHTTPConnectionClient.CustomOAuthAccessTokenResponse.class);
                Assert.fail("OAuthSystemException meant to be thrown if non-HTTPS request made");
            } catch (OAuthSystemException e) {
            }
        } finally {
            if (conn != null) conn.shutdown();
        }
    }

    @Test
    public void testHttpsRequest() throws OAuthSystemException, OAuthProblemException {
        setDefaultSSLSocketFactory();

        String fakeClientId = "fdsafdsfds";
        String fakeClientSecret = "423432ewr";
        OAuthClientRequest.TokenRequestBuilder authTokenRequest =
                new OAuthClientRequest.TokenRequestBuilder(secureServer.getSecureUrl() + "/testHttpsRequest")
                        .setClientId(fakeClientId)
                        .setClientSecret(fakeClientSecret)
                        .setGrantType(GrantType.CLIENT_CREDENTIALS);
        OAuthClientRequest headerRequest = authTokenRequest.buildHeaderMessage();
        headerRequest.setHeader("Cache-Control", "no-cache");
        headerRequest.setHeader("Content-Type", "application/x-www-form-urlencoded");
        headerRequest.setHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((fakeClientId + ":" + fakeClientSecret).getBytes()));
        headerRequest.setBody("grant_type=client_credentials");

        OAuthHTTPConnectionClient conn = null;
        try {
            conn = new OAuthHTTPConnectionClient(
                    FIELD_ACCESS_TOKEN,
                    FIELD_TOKEN_TYPE,
                    FIELD_SCOPE,
                    FIELD_EXPIRE_IN,
                    FIELD_EXPIRE_TIME);
            OAuthHTTPConnectionClient.CustomOAuthAccessTokenResponse authResp = conn.execute(
                    headerRequest,
                    null,
                    "POST",
                    OAuthHTTPConnectionClient.CustomOAuthAccessTokenResponse.class);
            if (authResp != null) {
                Assert.assertEquals(authResp.getAccessToken(), AUTH_RESPONSE.getString(FIELD_ACCESS_TOKEN));
                Assert.assertEquals(authResp.getScope(), AUTH_RESPONSE.getString(FIELD_SCOPE));
                Assert.assertEquals(authResp.getExpiresIn(), new Long(AUTH_RESPONSE.getLong(FIELD_EXPIRE_IN)));
                Assert.assertEquals(authResp.getTokenType(), AUTH_RESPONSE.getString(FIELD_TOKEN_TYPE));
            } else {
                Assert.fail("Received a null response from OAuth2 authentication service");
            }
        } finally {
            if (conn != null) conn.shutdown();
        }
    }

    @Test
    public void testOAuthExceptionIfServerError() throws OAuthProblemException, OAuthSystemException {
        setDefaultSSLSocketFactory();

        String fakeClientId = "fdsafdsfds";
        String fakeClientSecret = "423432ewr";
        OAuthClientRequest.TokenRequestBuilder authTokenRequest =
                new OAuthClientRequest.TokenRequestBuilder(secureServer.getSecureUrl() + "/testOAuthExceptionIfServerError")
                        .setClientId(fakeClientId)
                        .setClientSecret(fakeClientSecret)
                        .setGrantType(GrantType.CLIENT_CREDENTIALS);
        OAuthClientRequest headerRequest = authTokenRequest.buildHeaderMessage();
        headerRequest.setHeader("Cache-Control", "no-cache");
        headerRequest.setHeader("Content-Type", "application/x-www-form-urlencoded");
        headerRequest.setHeader("Authorization",
                "Basic " + Base64.getEncoder().encodeToString((fakeClientId + ":" + fakeClientSecret).getBytes()));
        headerRequest.setBody("grant_type=client_credentials");

        OAuthHTTPConnectionClient conn = null;
        try {
            conn = new OAuthHTTPConnectionClient(
                    FIELD_ACCESS_TOKEN,
                    FIELD_TOKEN_TYPE,
                    FIELD_SCOPE,
                    FIELD_EXPIRE_IN,
                    FIELD_EXPIRE_TIME);

            try {
                conn.execute(
                        headerRequest,
                        null,
                        "POST",
                        OAuthHTTPConnectionClient.CustomOAuthAccessTokenResponse.class);
                Assert.fail("OAuthSystemException should be thrown if OAuth authentication service is broken");
            } catch (OAuthSystemException e) {
            }
        } finally {
            if (conn != null) conn.shutdown();
        }
    }

    public static class TestHttpsRequestServlet extends HttpServlet {

        private static final long serialVersionUID = 132432433242L;

        @Override
        protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
            response.setStatus(HttpStatus.OK_200);
            response.setContentType("application/json");
            response.getWriter().println(AUTH_RESPONSE);
            response.getWriter().flush();
        }
    }

    public static class TestOAuthExceptionIfServerErrorServlet extends HttpServlet {
        private static final long serialVersionUID = 132432454356242L;
        @Override
        protected void doPost(final HttpServletRequest request, final HttpServletResponse response) {
            response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
        }
    }
}
