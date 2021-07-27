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

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.nifi.controller.ConfigurationContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class PasswordBasedOauth2TokenProviderTest {
    private static final String AUTHORIZATION_SERVER_URL = "http://authorizationServerUrl";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String CLIENT_ID = "clientId";
    private static final String CLIENT_SECRET = "clientSecret";

    private PasswordBasedOauth2TokenProvider testSubject;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private OkHttpClient mockHttpClient;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConfigurationContext context;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        testSubject = new PasswordBasedOauth2TokenProvider() {
            @Override
            protected OkHttpClient createHttpClient(ConfigurationContext context) {
                return mockHttpClient;
            }
        };

        when(context.getProperty(PasswordBasedOauth2TokenProvider.AUTHORIZATION_SERVER_URL).evaluateAttributeExpressions().getValue()).thenReturn(AUTHORIZATION_SERVER_URL);
        when(context.getProperty(PasswordBasedOauth2TokenProvider.USERNAME).evaluateAttributeExpressions().getValue()).thenReturn(USERNAME);
        when(context.getProperty(PasswordBasedOauth2TokenProvider.PASSWORD).getValue()).thenReturn(PASSWORD);
        when(context.getProperty(PasswordBasedOauth2TokenProvider.CLIENT_ID).evaluateAttributeExpressions().getValue()).thenReturn(CLIENT_ID);
        when(context.getProperty(PasswordBasedOauth2TokenProvider.CLIENT_SECRET).getValue()).thenReturn(CLIENT_SECRET);

        testSubject.onEnabled(context);
    }

    @Test
    public void testAcquireNewToken() throws Exception {
        // GIVEN
        Request mockRequest = new Request.Builder()
            .url("http://unimportant_but_required")
            .build();

        String accessTokenValue = "access_token_value";

        Response response1 = new Response.Builder()
            .request(mockRequest)
            .protocol(Protocol.HTTP_2)
            .message("unimportant_but_required")
            .code(200)
            .body(ResponseBody.create(
                ("{ \"access_token\":\"" + accessTokenValue + "\" }").getBytes(),
                MediaType.parse("application/json")))
            .build();

        when(mockHttpClient.newCall(any(Request.class)).execute()).thenReturn(response1);

        // WHEN
        String actual = testSubject.getAccessToken();

        // THEN
        assertEquals(accessTokenValue, actual);
    }

    @Test
    public void testRefreshToken() throws Exception {
        // GIVEN
        Request mockRequest = new Request.Builder()
            .url("http://unimportant_but_required")
            .build();

        String accessTokenValue = "access_token_value1";
        String accessTokenValue2 = "access_token_value2";

        Response response1 = new Response.Builder()
            .request(mockRequest)
            .protocol(Protocol.HTTP_2)
            .message("unimportant_but_required")
            .code(200)
            .body(ResponseBody.create(
                ("{ \"access_token\":\"" + accessTokenValue + "\", \"expires_in\":\"0\", \"refresh_token\":\"not_checking_in_this_test\" }").getBytes(),
                MediaType.parse("application/json")))
            .build();

        Response response2 = new Response.Builder()
            .request(mockRequest)
            .protocol(Protocol.HTTP_2)
            .message("unimportant_but_required")
            .code(200)
            .body(ResponseBody.create(
                ("{ \"access_token\":\"" + accessTokenValue2 + "\" }").getBytes(),
                MediaType.parse("application/json")))
            .build();

        when(mockHttpClient.newCall(any(Request.class)).execute()).thenReturn(response1, response2);

        // WHEN
        testSubject.getAccessToken();
        String actual = testSubject.getAccessToken();

        // THEN
        assertEquals(accessTokenValue2, actual);
    }
}
