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

package org.apache.nifi.oauth;

import org.apache.nifi.reporting.InitializationException;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.servlet.ServletHandler;
import org.json.JSONObject;
import org.junit.*;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class TestOAuth2ClientCredentialsGrantControllerService extends OAuth2TestBase {
    private static final JSONObject VALID_AUTH_RESPONSE;
    private static final String FIELD_ACCESS_TOKEN = "access_token";
    private static final String FIELD_TOKEN_TYPE = "token_type";
    private static final String FIELD_EXPIRE_TIME = "expire_time";
    private static final String FIELD_EXPIRE_IN = "expires_in";
    private static final String FIELD_SCOPE = "scope";
    private static final String VALUE_CLIENT_ID = "3432432ewr";
    private static final String VALUE_CLIENT_SECRET = "fkdsajflkdsjfkdsjf";

    static {
        VALID_AUTH_RESPONSE = new JSONObject();
        VALID_AUTH_RESPONSE.put(FIELD_ACCESS_TOKEN, "4324ewewr");
        VALID_AUTH_RESPONSE.put(FIELD_TOKEN_TYPE, "24324378l");
        VALID_AUTH_RESPONSE.put(FIELD_EXPIRE_TIME, 4323243423l);
        VALID_AUTH_RESPONSE.put(FIELD_EXPIRE_IN, 324234323342l);
        VALID_AUTH_RESPONSE.put(FIELD_SCOPE, "some scope");
    }

    @BeforeClass
    public static void initServer() throws Exception {
        // initialize the handlers
        ServletHandler secureServerHandler = new ServletHandler();
        secureServerHandler.addServletWithMapping(TestAuthenticateServlet.class, "/testAuthenticate");

        OAuth2TestBase.initServer(secureServerHandler, null);
    }

    @Test
    @Ignore
    public void testAuthenticate() {
        setDefaultSSLSocketFactory();

        OAuth2ClientCredentialsGrantControllerService testAuthenticateService =
                new OAuth2ClientCredentialsGrantControllerService();
        runner.addControllerService("testAuthenticate", testAuthenticateService);
        runner.setProperty(testAuthenticateService,
                OAuth2ClientCredentialsGrantControllerService.AUTH_SERVER_URL,
                secureServer.getSecureUrl() + "/testAuthenticate");
        runner.setProperty(testAuthenticateService,
                OAuth2ClientCredentialsGrantControllerService.CLIENT_ID, VALUE_CLIENT_ID);
        runner.setProperty(testAuthenticateService,
                OAuth2ClientCredentialsGrantControllerService.CLIENT_SECRET, VALUE_CLIENT_SECRET);
        runner.setProperty(testAuthenticateService,
                OAuth2ClientCredentialsGrantControllerService.RESPONSE_ACCESS_TOKEN_FIELD_NAME, FIELD_ACCESS_TOKEN);
        runner.setProperty(testAuthenticateService,
                OAuth2ClientCredentialsGrantControllerService.RESPONSE_EXPIRE_IN_FIELD_NAME, FIELD_EXPIRE_IN);
        runner.setProperty(testAuthenticateService,
                OAuth2ClientCredentialsGrantControllerService.RESPONSE_EXPIRE_TIME_FIELD_NAME, FIELD_EXPIRE_TIME);
        runner.setProperty(testAuthenticateService,
                OAuth2ClientCredentialsGrantControllerService.RESPONSE_SCOPE_FIELD_NAME, FIELD_SCOPE);
        runner.setProperty(testAuthenticateService,
                OAuth2ClientCredentialsGrantControllerService.RESPONSE_TOKEN_TYPE_FIELD_NAME, FIELD_TOKEN_TYPE);
        runner.enableControllerService(testAuthenticateService);

	OAuth2ClientCredentialsGrantControllerService testAuthenticateService = (OAuth2ClientCredentialsGrantControllerService) runner.getControllerService("testAuthenticate");
        Assert.assertTrue(testAuthenticateService.authenticate());
    }

    public static class TestAuthenticateServlet extends HttpServlet {

        private static final long serialVersionUID = 1324324343223772L;

        @Override
        protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
            response.setStatus(HttpStatus.OK_200);
            response.setContentType("application/json");
            response.getWriter().println(VALID_AUTH_RESPONSE);
            response.getWriter().flush();
        }
    }
}
